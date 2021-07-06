using System;
using System.IO;
using System.Diagnostics;
using System.IO.Compression;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MyNamespace
{

    class E10Table
    {
        public readonly string Name;
        public bool UseQuery = false;
        public string LastValue = "";
        public string query = "";
    }

        class MyClassCS
    {
        static void Main(string[] args)
        {

            //qq();
            EventLog myLog = new EventLog("E10SignalWatcher", "ANA-SQL-PROD" );
            
            var lastEntry = myLog.Entries[myLog.Entries.Count - 1];
            var last_error_Message = lastEntry.Message;

            if(lastEntry.EntryType == EventLogEntryType.Error)


            for (int index = myLog.Entries.Count - 1; index > 0; index--)
            {
                var errLastEntry = myLog.Entries[index];
                if (errLastEntry.EntryType == EventLogEntryType.Error)
                {
                    //this is the last entry with Error
                    var appName = errLastEntry.Source;
                    break;
                }
            }

            //string file = @"c:\temp\E10dump\erp.InvcDtl.bcp";
            //ZipArchive zip = ZipFile.Open(@"c:\temp\qq.zip", ZipArchiveMode.Create);
            //zip.CreateEntryFromFile(file, Path.GetFileName(file), CompressionLevel.Optimal);
            //zip.Dispose();


            /*String[] files = Directory.GetFiles(@"e:\e10dumps", "*.bcp");


            Parallel.For(0, files.Length,
                   index => {
                       if (File.Exists(files[index] ))
                       {
                           string zipfile = files[index] + ".zip";
                           if (File.Exists(zipfile)) 
                               File.Delete(zipfile);
                           ZipArchive zip = ZipFile.Open(zipfile, ZipArchiveMode.Create);
                           zip.CreateEntryFromFile(files[index], Path.GetFileName(files[index]), CompressionLevel.Optimal);
                           zip.Dispose();
                           Console.WriteLine($"Done :{zipfile}");
                       }
                       else
                       {
                           Console.WriteLine($"File not found :{files[index]}");
                       }

                   });
*/
            Console.WriteLine("It's Over... ");
            Console.ReadLine();
        }


        static void qq()
        {
            EventLog eventLog1 = new EventLog("E10SignalWatcher", "ANA-SQL-PROD");
            eventLog1.Source = "BAF Source";
            eventLog1.WriteEntry("Test info message. Ignore", EventLogEntryType.Information);
        }


    }
}


