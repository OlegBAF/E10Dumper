using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using System.Threading.Tasks;
using System.IO.Compression;

namespace E10Dumper
{

    class E10Table
    {
        public readonly string Name;
        public bool UseQuery = false;
        public string LastValue = "";
        public string query = "";
               
        public E10Table(string a, string b, string c)
        {
            Name = a;
            query = b;
            LastValue = c;
        }

        public E10Table(SqlDataReader rd, Dictionary<string, string> queries )
        {
            Name = Convert.ToString(rd["Name"]);      
            LastValue = Convert.ToString(rd["LastStagedValue"]);
            if (queries.ContainsKey(Name.ToLower()))
            {
                UseQuery = true;
                query = queries[Name.ToLower()];
            }                                   
        }
    }

    class SQLDumper
    {

        public SqlConnection e10Cnn = new SqlConnection();
        public SqlConnection AuxCnn = new SqlConnection();
        public SqlConnectionStringBuilder E10CntBuilder = null;

        protected Dictionary<string, string> Queries;


        public bool FreshMode = true;
        public bool NonCriticalError = false;
        public string Days2Dump = "2";

        public string DumpPath = "";
        public string DestPath = "";
        public string SignalFile { get => Path.Combine(DestPath, "__signal"); }



        public int MaxAtts = 3;

        protected int SleepCount = 0;
        protected int MaxSleep = 3;
        protected int WaitTime = 10000;



        public SQLDumper(string E10cntStr, string AuxcntStr, string outPath, string destPath, Dictionary<string, string> queries)
        {
            e10Cnn.ConnectionString = E10cntStr;
            E10CntBuilder = new SqlConnectionStringBuilder(E10cntStr);
            AuxCnn.ConnectionString = AuxcntStr;
            DumpPath = outPath.EndsWith(@"\") ? outPath : outPath + @"\";
            DestPath = destPath.EndsWith(@"\") ? destPath : destPath + @"\";
            Queries = queries;
        }

        public bool Init()
        {
            bool result = false;
            Tracer.Debug($"Connecting to {e10Cnn.ConnectionString}");
            ConnectToSQL(e10Cnn, e10Cnn.ConnectionString);
            if ( result = IsThisActiveNode() )
            {
                Tracer.Debug($"Connecting to {AuxCnn.ConnectionString}");
                ConnectToSQL(AuxCnn, AuxCnn.ConnectionString);
                FreshMode = !IsInterrupted();
                if (FreshMode)
                {
                    Tracer.Info("Starting in fresh mode");
                    ExecSql("EXEC Start_Processing", AuxCnn);
                    File.Delete(SignalFile);
                }
                else
                    Tracer.Info("Starting in interrupted mode");
            }

            return result;
        }

        public bool IsThisActiveNode()
        {
            bool res = true;
            try
            {
                using (SqlCommand cmd = new SqlCommand("SELECT NodeName FROM fn_virtualservernodes() where is_current_owner = 1", e10Cnn))
                {
                    string nodeName = (string)cmd.ExecuteScalar();
                    if ( !String.IsNullOrEmpty(nodeName) )
                        res = (nodeName == Environment.MachineName);
                }
            }
            finally
            {
                Tracer.Info($"Current server is " +  (res?"":"Not-") +"Active Node.");
            }
            return res;
        }

        public List<E10Table> GetE10TablesToDump()
        {
            List<E10Table> E10Tables = new List<E10Table>();
            using (SqlDataReader reader = ReadSql("SELECT NAME, LastStagedValue FROM __E10TABLES WHERE ISNULL(Dumped, 0) =  0", AuxCnn))
            {
                while (reader.Read())
                    E10Tables.Add(new E10Table(reader, Queries));

                reader.Close();
            }
            return E10Tables;
        }


        protected SqlDataReader ReadSql(string sql, SqlConnection cnt)
        {
            ConnectToSQL(cnt);
            using (SqlCommand cmd = new SqlCommand(sql, cnt))
                return cmd.ExecuteReader();

        }


        protected void ExecSql(string sql, SqlConnection cnt)
        {
            ConnectToSQL(cnt);
            using (SqlCommand cmd = new SqlCommand(sql, cnt))
                cmd.ExecuteNonQuery();

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

        public void ProcessException(Exception e, ref int atts)
        {
            Tracer.Error(e.Message);

            if (e is SqlException && IsFatalException((SqlException)e))
                throw new Exception($"Fatalexception occured.Terminating.");


            if (atts <= MaxAtts)
            {
                Tracer.Info($"Attempt of recover {atts++} out od {MaxAtts}");
                System.Threading.Thread.Sleep(5000);
                return;
            }
            else if (SleepCount < MaxSleep)
            {
                Tracer.Info($"Attempt to  wait {SleepCount++} out od {MaxSleep}.Waiting for {WaitTime} ms");
                System.Threading.Thread.Sleep(WaitTime);
                atts = 0;
                return;
            }
            else
            {
                throw new Exception($"Amount of attemts exceeded allowed limit of {MaxSleep}.Terminating.");
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


        public int dumpTables(string days2Dump)
        {
            int res = 0;
            int atts = 0;
            while (1 == 1)
            {
                try
                {
                    List<E10Table> E10Tables = GetE10TablesToDump();
                    foreach (E10Table tbl in E10Tables)
                    {
                        dumpTable(tbl, DumpPath, days2Dump);
                        res++;
                    }

                    break;
                }
                catch (Exception e)
                {
                    ProcessException(e, ref atts);
                }
            }
            return res;

        }

        public void dumpTable(E10Table table, string outPath, string days2Dump)
        {
            string _sql = "";
            string lastParam = "";

            int dumpedRecs = 0;
            string error = "";


            if (!table.UseQuery)
            {
                _sql =
                    $"DECLARE @R int " +
                    $"  EXEC @R=master..xp_cmdshell 'bcp {E10CntBuilder.InitialCatalog}.{table.Name} OUT {outPath}{table.Name}.bcp -T -n -S {E10CntBuilder.DataSource}'" +
                    $" SELECT @R";
            }
            else
            {
                string qrt = table.query.Replace("{daysNum}", days2Dump);
                qrt = qrt.Replace("{DBName}", E10CntBuilder.InitialCatalog);
                _sql = $"EXEC master..xp_cmdshell 'bcp \" {qrt}\" QUERYOUT {outPath}{table.Name}.bcp -T -n -S {E10CntBuilder.DataSource}'";
            }
            SqlCommand cmd = new SqlCommand(_sql, AuxCnn);
            cmd.CommandTimeout = 300;
            Tracer.Info($"Dumping table {table.Name}:{_sql}");

            //IAsyncResult result = cmd.BeginExecuteReader();                      
            //while (!result.IsCompleted) System.Threading.Thread.Sleep(100);
            //SqlDataReader reader = cmd.EndExecuteReader(result);

            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                if (CheckResult(reader, ref dumpedRecs, out error))
                    updateDumpState(table.Name, dumpedRecs, lastParam);
                else
                    updateDumpState(table.Name, error);
            }
            Tracer.Info($"BCP for table {table.Name} completed. Affected {dumpedRecs} rows.");

        }


        public bool CheckResult(SqlDataReader rd, ref int dumpedRecs, out string error)
        {
            string row = "";
            bool res = false;
            error = "";
            while (rd.Read())
            {
                if (!rd.IsDBNull(0))
                {
                    row = rd.GetString(0);
                    if (row.EndsWith(" rows copied."))
                    {
                        row = row.Replace(" rows copied.", "");
                        dumpedRecs = Convert.ToInt32(row.Trim());
                        res = true;
                        break;
                    }
                    else
                        error = error + " " + row;
                }
            }
            if (rd.NextResult() && rd.Read() && !rd.IsDBNull(0))
                res = rd.GetInt32(0) == 0;
            rd.Close();
            return res;
        }

        protected void updateDumpState(string tableName, int recNum, string lastParam)
        {
            ExecSql($"Exec Update_DumpState @TableName='{tableName}', @Dumped=1, @recNum={recNum}, @LastParam='{lastParam}'", AuxCnn);
        }

        protected void updateDumpState(string tableName, string error)
        {
            Tracer.Error($"Error during BCP-ing table {tableName}:{error}");
            error = error.Replace("'", "");
            ExecSql($"Exec Update_DumpState @TableName='{tableName}', @Dumped=0, @Error='{error}'", AuxCnn);
        }




        public List<string> GetE10TablesToCopy()
        {
            List<string> res = new List<string>();
            using (SqlDataReader reader = ReadSql("SELECT NAME FROM __E10TABLES WHERE Dumped = 1 AND ISNULL(Copied, 0) =  0", AuxCnn))
            {
                while (reader.Read())
                    res.Add(Convert.ToString(reader["Name"]));
                reader.Close();
            }
            //Tracer.Debug(string.Join(",", res));
            return res;
        }

        public string DestFileName(string table) => Path.Combine(DestPath, table + ".bcp");
        public string ScrFileName(string table) => Path.Combine(DumpPath, table + ".bcp");


        public int ZiPFiles()
        {
            Tracer.Info($"Zipping files up");
            int res = 0;
            List<string> fl= GetE10TablesToCopy();
            String[] files = fl.ToArray();
            Parallel.For(0, files.Length, index => 
            {
                string srcFile = ScrFileName(files[index]);
                string zipfile = srcFile + ".zip";
                if (File.Exists(srcFile ))
                {
                    
                    if (File.Exists(zipfile))
                        File.Delete(zipfile);
                    ZipArchive zip = ZipFile.Open(zipfile, ZipArchiveMode.Create);
                    zip.CreateEntryFromFile(srcFile, Path.GetFileName(srcFile), CompressionLevel.Optimal);
                    zip.Dispose();
                }
                else
                {
                    Tracer.Error($"File not found :{srcFile}");
                }

            });
            return res;

        }


        public int CopyFiles()
        {
            int atts = 0;
            int res = 0;
            while (1 == 1)
            {

                try
                {
                    List<string> E10Tables = GetE10TablesToCopy();
                    foreach (string tbl in E10Tables)
                    {
                        string destFile = DestFileName(tbl) + ".zip", srcFile = ScrFileName(tbl) + ".zip";
                        Directory.CreateDirectory(DestPath);
                        Tracer.Debug($"Coping file {srcFile} to {destFile}");
                        if (File.Exists(srcFile))
                        {
                            File.Copy(srcFile, destFile, true);
                            res++;
                            updateCopyState(tbl);
                        }
                        else
                        {
                            Tracer.Debug($"File {srcFile} is not found, but expected");
                            NonCriticalError = true;
                        }


                    }
                    break;
                }
                catch (Exception e)
                {
                    ProcessException(e, ref atts);
                }
            }
            return res;

        }

        public void updateCopyState(string tableName) => ExecSql($"Exec Update_CopyState '{tableName}'", AuxCnn);

        public bool IsInterrupted()
        {
            bool res = true ;
            Tracer.Debug($"Checking for signal file:{SignalFile}");
            if (res = File.Exists(SignalFile))
                Tracer.Debug($"Signal file is found");
            return !res;
        }

        public void CreateSignalFile()
        {
            Tracer.Debug($"Writing signal file to :{SignalFile}");
            File.WriteAllText(SignalFile, "qq");
        }




    }
}
