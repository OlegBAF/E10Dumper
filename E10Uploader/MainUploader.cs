using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Specialized;
using System.Configuration;
using NLog;
using System.Linq;
using System.IO.Compression;

namespace E10Uploader
{
    class MainUploader
    {
        static void Main(string[] args)
        {
            Dictionary<string, string> queries = new Dictionary<string, string>();
            NameValueCollection sAll = ConfigurationManager.AppSettings;
            Tracer.Info("--------------->Loader Started<--------------------");
            try
            {
                foreach (string s in sAll.AllKeys)
                {
                    if (s.StartsWith("Query"))
                    {
                        string[] rs = sAll.Get(s).Split(':');
                        if (rs.Length == 2)
                            queries.Add(rs[0].ToLower(), rs[1]);
                    }
                }

                E10Loader.MultyInst = Convert.ToInt32(ConfigurationManager.AppSettings.Get("InstanceNum")) > 1;

                if (E10Loader.CheckSignalFile(ConfigurationManager.AppSettings.Get("FilesPath") ) ) 
                {
                    int inst = StartNewInstance(Convert.ToInt32(ConfigurationManager.AppSettings.Get("InstanceNum")), args);
                    E10Loader e10loader = new E10Loader(
                                                        ConfigurationManager.AppSettings.Get("AuxServer"),
                                                        ConfigurationManager.AppSettings.Get("FilesPath"),
                                                        Convert.ToInt32(ConfigurationManager.AppSettings.Get("BatchSize")),
                                                        inst);
                    
                    e10loader.Init();
                    e10loader.GetFiles();
                    int cnt = e10loader.ProcessFiles(queries, Convert.ToInt32(ConfigurationManager.AppSettings.Get("Days2Dump")));
                    Tracer.Info("--------------->Completed <--------------------");
                    Tracer.Info($"Uploaded{cnt} files");
                }
                else
                    Tracer.Info("No Signal file found. Exiting");

                //Console.ReadLine();
            }
            catch (Exception e)
            {
                Tracer.Error(e.Message);
                Environment.ExitCode = -1;
            }


        }

        static int StartNewInstance(int numOfInstances, string[] args)
        {
            int res = 1;
            if (args.Length > 0)
                res = Int32.Parse(args[0]);
            else if( numOfInstances > 1 )
            {
                for(int i = 2; i <= numOfInstances; i++ )
                    using (System.Diagnostics.Process myProcess = new System.Diagnostics.Process())
                    {
                        myProcess.StartInfo.UseShellExecute = true;
                        myProcess.StartInfo.FileName = System.Reflection.Assembly.GetEntryAssembly().Location;
                        myProcess.StartInfo.Arguments = i.ToString();
                        myProcess.Start();
                    }

            }
            return res;

        }
    }
}



class BCPFile
{
    public string Name = "";
    public DateTime Created;
    public DateTime Uploaded;
    public bool TruncReload = true;
    public bool NeedUpdate => Created > Uploaded;
    public string Error = "";
    public long Size = 0;
  
}


class E10Loader
{
    static public bool MultyInst = false;



    public SqlConnection StageCnn = new SqlConnection();
    public SqlConnection AuxCnn = new SqlConnection();
    
    public bool FreshMode = true;

    public string FilesPath = "";
    


    public int MaxAtts = 3;

    protected int SleepCount = 0;
    protected int MaxSleep = 3;
    protected int WaitTime = 10000;

    protected int BatchSize = 10000;
    protected int InstanceNumber = 1;



    protected List<BCPFile> BCPFiles = new List<BCPFile>();

    public E10Loader( string AuxCntStr, string filePath, int batchSize, int instNum )
    {
        AuxCnn.ConnectionString = AuxCntStr;
        FilesPath = filePath.EndsWith(@"\") ? filePath : filePath + @"\";
        BatchSize = batchSize;
        InstanceNumber = instNum;
        Tracer.Info($"--------->Starting instance #{InstanceNumber}");
        Tracer.instance = instNum.ToString();
    }

    public void Init()
    {
        Tracer.Debug($"Connecting to {AuxCnn.ConnectionString}");
        ConnectToSQL(AuxCnn, AuxCnn.ConnectionString);
    }

    public void GetFiles()
    {
                     
        DateTime newYear = DateTime.Parse("1/1/2000 0:0:0" , System.Globalization.CultureInfo.InvariantCulture);

        string sql = $"Select Name, LastUploaded, TruncReload  from __E10Tables";
        if (E10Loader.MultyInst)
            sql += $" where Instance = {InstanceNumber}";

        using (SqlDataReader rd = ReadSql(sql, AuxCnn))
        {
            while (rd.Read())
            {
                BCPFile bf = new BCPFile() 
                { 
                    Name = rd.GetString(0).ToLower(), 
                    Uploaded = rd.IsDBNull(1)? newYear:rd.GetDateTime(1), 
                    TruncReload = rd.IsDBNull(2) ? true : rd.GetBoolean(2),
                };

                FileInfo fi = new FileInfo( FilesPath+bf.Name+".bcp.zip" );
                if (fi.Exists)
                {

                    bf.Created = fi.LastWriteTimeUtc;
                    bf.Size = fi.Length;
                    if (bf.NeedUpdate)
                        UnZipFile(fi.FullName);
                    else
                        Tracer.Info($" File {bf.Name} will be skipped: file is already uploaded");
                }
                else
                    Tracer.Error(bf.Error = $"File {FilesPath + bf.Name + ".bcp"} not found but expected");
                BCPFiles.Add(bf);               
            }
        }
        foreach( BCPFile bf  in BCPFiles)
            ExecSql($"exec __Set_LoadState '{bf.Name}', {bf.Size.ToString()}, '{bf.Created.ToString()}', '{bf.Error}' ", AuxCnn);
    }



    public void UnZipFile(string filename)
    {
        Tracer.Info($"Unzipping file {filename}");
        string trgFile = Path.Combine(FilesPath, Path.GetFileNameWithoutExtension(filename));
        if (File.Exists(trgFile))
                File.Delete(trgFile);
        ZipArchive zip = ZipFile.Open(filename, ZipArchiveMode.Read);
        zip.ExtractToDirectory(FilesPath);      
        zip.Dispose();
    }


    public int ProcessFiles(Dictionary<string, string> queries, int days2Dump)
    {
        int res = 0;
        int atts = 0;
        SqlCommand cmd = AuxCnn.CreateCommand();
        cmd.CommandTimeout = 3600;

        foreach (BCPFile bcp in BCPFiles)
        {
            while (bcp.NeedUpdate && bcp.Error == "" )
            {
                string filename = $"{FilesPath}{bcp.Name}.bcp";
                try
                {
                    if (queries.ContainsKey(bcp.Name.ToLower()))
                    {
                        string qrt = queries[bcp.Name.ToLower()].Replace("{daysNum}", days2Dump.ToString());
                        Tracer.Info($"executing query for table {bcp.Name}:{qrt}");
                        ExecSql(qrt, AuxCnn);
                    }
                    if (bcp.TruncReload)
                    {
                        Tracer.Info($"truncate table {bcp.Name}");
                        ExecSql($"truncate table {bcp.Name}", AuxCnn);
                    }

                    cmd.CommandText = $"BULK INSERT {bcp.Name} FROM '{filename}' WITH ( DATAFILETYPE = 'native', BATCHSIZE = {BatchSize}, TABLOCK )";
                    Tracer.Info($"Executing {cmd.CommandText}");
                    cmd.ExecuteNonQuery();

                    ExecSql($"exec __Update_LoadState '{bcp.Name}' ", AuxCnn);
                    res++;
                    break;
                }
                catch (SqlException e)
                {
                    Tracer.Error($"File name:{filename}. "+ e.Message);
                    if( NoFile(e.Message) || new int[] { 4863, 4866 }.Contains(e.Number) )
                    {
                        ExecSql($"exec __Update_LoadState '{bcp.Name}', '{e.Message}' ", AuxCnn);
                        break;
                    }
                    else
                        ProcessException(e, ref atts);
                }
                catch (Exception e)
                {
                    Tracer.Error(e.Message);
                    throw e;
                }
            }

        }
        return res;
    }


    protected void ExecSql(string sql, SqlConnection cnt)
    {
        ConnectToSQL(cnt);
        using (SqlCommand cmd = new SqlCommand(sql, cnt))
        {
            cmd.CommandTimeout = 3600;
            cmd.ExecuteNonQuery();
        }

    }


    protected SqlDataReader ReadSql(string sql, SqlConnection cnt)
    {
        ConnectToSQL(cnt);
        using (SqlCommand cmd = new SqlCommand(sql, cnt))
            return cmd.ExecuteReader();

    }

    void ConnectToSQL(SqlConnection cnt, string cntString = "")
    {
        int atts = 0;

        cntString = cntString == "" ? cnt.ConnectionString : cntString;

        if (cntString.ToLower() != cnt.ConnectionString.ToLower() && cnt.State == System.Data.ConnectionState.Open)
            cnt.Close();

        while (cnt.State != System.Data.ConnectionState.Open)
        {
            try
            {
                cnt.ConnectionString = cntString;
                cnt.Open();
                Tracer.Debug($"Connected to {cntString}");
            }
            catch (Exception e)
            {
                ProcessException(e, ref atts);
            }
        }

    }

    public bool NoFile(string s ) => s.ToLower().StartsWith("cannot bulk load. the file") && s.ToLower().EndsWith("does not exist.");

    public void ProcessException(Exception e, ref int atts)
    {
        Tracer.Error(e.Message);

        if ((e is SqlException) && IsFatalException((SqlException)e))
            throw new Exception($"Fatal exception occured.Terminating.");
        else if (atts <= MaxAtts)
        {
            Tracer.Info($"Attempt of recover {atts++} out od {MaxAtts}");
            System.Threading.Thread.Sleep(5000);
            return;
        }
        else if (SleepCount < MaxSleep)
        {
            
            Tracer.Info($"Attempt to  wait {SleepCount++} out od {MaxSleep}.Wiating for {WaitTime} ms");            
            System.Threading.Thread.Sleep(WaitTime);
            atts = 0;
            return;
        }
        else
        {
            throw new Exception($"Amount of attemts eceeded allowed limit of {MaxSleep}.Terminating.");
        }


    }

    public Boolean IsFatalException(SqlException e)
    {
        int[] fatal_errors = { 53, 207, 208, 4060, 4064, 18456 };

        bool res = false;
        for (int i = 0; i < e.Errors.Count && !res; i++)
            res = Array.Exists(fatal_errors, el => el == e.Errors[i].Number);

        return res;
    }

    static public bool CheckSignalFile( string filesPath )
    {
        bool res = false;
        string signal = Path.Combine(filesPath, "__signal");
        Tracer.Debug($"Checking for signal file {signal}");
        res = File.Exists(signal);
        Tracer.Debug( res? "Signal file found" : "Signal file not found");
        return res;
    }

}


class Tracer
{

    static Logger log = LogManager.GetCurrentClassLogger();
    public static string instance = "1";
    static public void Info(string Message) =>  log.Info($"inst={instance}::" + Message);
    static public void Debug(string Message) => log.Debug($"inst={instance}::"+Message);

    static public void Error(string Message)
    {
        log.Error($"inst={instance}::"+Message);
        //Console.WriteLine("Error ---->");
        //Console.WriteLine(Message);
        //Console.WriteLine("<----");
    }
}
