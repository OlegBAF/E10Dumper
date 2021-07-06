using System;
using System.IO;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO.Compression;
using System.Data;
using System.Data.SqlClient;


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

            //string file = @"c:\temp\E10dump\erp.InvcDtl.bcp";
            //ZipArchive zip = ZipFile.Open(@"c:\temp\qq.zip", ZipArchiveMode.Create);
            //zip.CreateEntryFromFile(file, Path.GetFileName(file), CompressionLevel.Optimal);
            //zip.Dispose();

            //List<string> files = new List<string>();



            //String[] files = Directory.GetFiles(@"f:\e10dumps", "*.bcp");

            SqlConnection cnt = new SqlConnection("Data Source=ana-sql-prod;Integrated Security=True");
            cnt.Open();
            using (SqlCommand cmd = new SqlCommand("SELECT NodeName FROM fn_virtualservernodes() where is_current_owner = 1", cnt))
            {
                string res = (string)cmd.ExecuteScalar();
                bool qq = res == Environment.MachineName;
                Console.WriteLine(qq);
            }



            Console.WriteLine(Environment.MachineName);

            /*Parallel.For(0, files.Length,
                   index => {
                       if (File.Exists(files[index]))
                       {
                           string zipfile = files[index] + ".zip";
                           if (File.Exists(zipfile))
                               File.Delete(zipfile);
                           ZipArchive zip = ZipFile.Open(zipfile, ZipArchiveMode.Create);
                           //zip.ExtractToDirectory();
                           //CreateEntryFromFile(files[index], Path.GetFileName(files[index]), CompressionLevel.Optimal);
                           zip.Dispose();
                           Console.WriteLine($"Done :{zipfile}");
                       }
                       else
                       {
                           Console.WriteLine($"File not found :{files[index]}");
                       }

                   });

            Console.WriteLine("It's Over... ");
            */
            Console.ReadLine();
        }


    }
}


