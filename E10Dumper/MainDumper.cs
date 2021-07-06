using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Collections.Specialized;
using NLog;

namespace E10Dumper
{
    class MainDumper
    {
              
        static void Main(string[] args)
        {
            Tracer.Info("--------------->Dumper Started<--------------------");
            Dictionary<string, string> queries = new Dictionary<string, string>();

            try
            {
                NameValueCollection sAll=ConfigurationManager.AppSettings;

                foreach (string s in sAll.AllKeys)
                {
                    if (s.StartsWith("Query"))
                    {
                        string[] rs = sAll.Get(s).Split(':');
                        if (rs.Length == 2)
                            queries.Add(rs[0].ToLower(), rs[1]);
                    }
                }


                SQLDumper dumper = new SQLDumper(
                                ConfigurationManager.AppSettings.Get("E10Server"),
                                ConfigurationManager.AppSettings.Get("AuxServer"),
                                ConfigurationManager.AppSettings.Get("DumpPath"),
                                ConfigurationManager.AppSettings.Get("DestPath"),
                                queries );
                if (dumper.Init())
                {
                    int dumped = dumper.dumpTables(ConfigurationManager.AppSettings.Get("Days2Dump"));
                    dumper.ZiPFiles();
                    int copied = dumper.CopyFiles();
                    if (!dumper.NonCriticalError)
                    {
                        dumper.CreateSignalFile();
                        Tracer.Info("-->Completed without errors<--------------------");

                    }
                    else
                        Tracer.Info("-->Completed with errors<--------------------");

                    Tracer.Info($"-->Tables dumped: {dumped} ; Files copied: {copied}");
                }
                else
                    Tracer.Warning($"-->Current node is not active: stopping.No data is transferred");



                //Console.ReadLine();

            }
            catch (Exception e)
            {
                Tracer.Error(e.Message);
                Environment.ExitCode = -1;
            }
        }



        
    }


    class Tracer
    {

        static Logger log = LogManager.GetCurrentClassLogger();
        static public void Info(string Message)
        {
            log.Info(Message);
        }

        static public void Debug(string Message) => log.Debug(Message);

        static public void Error(string Message)
        {
            log.Error(Message);
            //Console.WriteLine("Error ---->");
            //Console.WriteLine(Message);
            //Console.WriteLine("<----");
        }

        static public void Warning(string Message) =>log.Warn(Message);
        

    }
}

