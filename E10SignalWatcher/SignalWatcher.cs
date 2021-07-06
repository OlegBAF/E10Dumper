using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Timers;
using System.Data.SqlClient;

namespace E10SignalWatcher
{
    public partial class SignalWatcher : ServiceBase
    {
        FileSystemWatcher watcher;
        Timer timer = new Timer();
        string filesPath;
        string LoaderExe;
        string cntString = "";
        int reruns = 0;
        int MaxReRuns = 3;
        string SSISCmd = "";
        bool wasTimerEnbaled = false;

        internal void TestStartupAndStop(string[] args)
        {
            this.OnStart(args);
            Console.ReadLine();
            this.OnStop();
        }

        public SignalWatcher()
        {
            InitializeComponent();

            eventLog1 = new EventLog();
            if (!EventLog.SourceExists("BAF Source"))
                EventLog.CreateEventSource("BAF Source", "E10SignalWatcher");
            eventLog1.Source = "BAF Source";
            eventLog1.Log = "E10SignalWatcher";

            watcher = new FileSystemWatcher();
            watcher.Filter = "__signal";
            watcher.NotifyFilter = NotifyFilters.Attributes | NotifyFilters.CreationTime | NotifyFilters.FileName | NotifyFilters.LastAccess | NotifyFilters.LastWrite;

            timer.Enabled = false;
            timer.Elapsed += new ElapsedEventHandler(this.TimerElapsed);
            timer.Interval = 60000;
           
        }

        protected override void OnStart(string[] args)
        {
            NameValueCollection sAll = ConfigurationManager.AppSettings;
            filesPath = sAll["FilesPath"];
            filesPath = filesPath.EndsWith(@"\") ? filesPath : filesPath + @"\";
            LoaderExe = sAll["LoaderExe"];
            cntString = sAll["AuxServer"];
            MaxReRuns = Convert.ToInt32(sAll["MaxReruns"]);
            SSISCmd = sAll["SSISrunCommand"];

            watcher.Path = filesPath;
            watcher.EnableRaisingEvents = true;
            watcher.Created += OnFileCreated;

            System.Reflection.Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
            System.Diagnostics.FileVersionInfo fvi = System.Diagnostics.FileVersionInfo.GetVersionInfo(assembly.Location);
            string version = fvi.FileVersion;

            eventLog1.WriteEntry($"E10Signal Watcher is started.Version {version}");
        }


        protected override void OnPause()
        {
            watcher.Created -= OnFileCreated;
            wasTimerEnbaled = timer.Enabled;
            timer.Enabled = false;
            eventLog1.WriteEntry("Service is paused, but Uploader is still can be running.");
            base.OnPause();
        }


        protected override void OnContinue()
        {
            watcher.Created += OnFileCreated;
            timer.Enabled = wasTimerEnbaled;
            eventLog1.WriteEntry("Service is continued.");
            base.OnContinue();
        }

        protected override void OnStop()
        {
            watcher.Created -= OnFileCreated;
            timer.Enabled = false;
            eventLog1.WriteEntry("Service is stopped, but Uploader is still can be running. ");
        }





        private void OnFileCreated(object sender, FileSystemEventArgs e)
        {
            eventLog1.WriteEntry($"Signal File detected. Starting <{LoaderExe}>");
            try
            {
                reruns = 0;
                if (StartUploader())
                {
                    eventLog1.WriteEntry($"<{LoaderExe}> started.");
                    timer.Start();
                }
                else
                    eventLog1.WriteEntry($"Could not start {LoaderExe}", EventLogEntryType.Error);
            }
            catch (Exception ex)
            {
                eventLog1.WriteEntry($"Error while starting Uploader:{ex.Message}", EventLogEntryType.Error);
            }
        }

        private bool StartUploader()
        {
            Process proc = new System.Diagnostics.Process();
            proc.StartInfo.UseShellExecute = true;
            proc.StartInfo.FileName = LoaderExe;
            return proc.Start();
        }

        public void TimerElapsed(object sender, ElapsedEventArgs args)
        {
            timer.Stop();

            string exename = Path.GetFileNameWithoutExtension(LoaderExe);
            Process[] pname = Process.GetProcessesByName(exename);
            if (pname.Length == 0)
            {
                if (UploadedNoErros())
                {
                    eventLog1.WriteEntry($"E10 data uploaded successfuly.");
                    RunSSIS();
                }
                else if (++reruns <= MaxReRuns)
                {
                    eventLog1.WriteEntry($"E10 data uploaded with errors. Restarting <{LoaderExe}>. Attempt#{reruns}", EventLogEntryType.Warning);
                    if (StartUploader())
                        timer.Start();
                }
                else
                {
                    eventLog1.WriteEntry($"E10 data uploaded with errors. Number of attempts to rerun Uploader has exceeded maximum.", EventLogEntryType.Error);
                }
            }
            else
            {
                eventLog1.WriteEntry($"<{LoaderExe}> is still running");
                timer.Start();
            }

        }

        protected bool UploadedNoErros()
        {
            eventLog1.WriteEntry($"Checking status of E10 data uploading.");
            bool res = false;
            try
            {
                using (SqlConnection cnt = new SqlConnection(cntString))
                {

                    cnt.Open();
                    using (SqlCommand cmd = new SqlCommand("__GetLoadState", cnt))
                    {

                        cmd.CommandTimeout = 3600;
                        res = (Int32)cmd.ExecuteScalar() == 0;
                    }
                }
            }
            catch(Exception e)
            {
                eventLog1.WriteEntry($"Error executing <__GetLoadState> : {e.Message}", EventLogEntryType.Error);
            }
            return res;
        }

        protected void RunSSIS()
        {
            eventLog1.WriteEntry($"Starting SSIS");
            try
            {
                using (SqlConnection cnt = new SqlConnection(cntString))
                {
                    cnt.Open();
                    using (SqlCommand cmd = new SqlCommand(SSISCmd, cnt))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    cnt.Close();
                }
            }
            catch (Exception e)
            {
                eventLog1.WriteEntry($"Error executing <SSISCmd> : {e.Message}", EventLogEntryType.Error);
            }


        }


    }
}
