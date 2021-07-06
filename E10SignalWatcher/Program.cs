using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace E10SignalWatcher
{
    static class Program
    {
        static void Main(string[] args)
        {
            if (Environment.UserInteractive)
            {
                SignalWatcher svc  = new SignalWatcher();
                svc.TestStartupAndStop(args);
            }
            else
            {
                ServiceBase[] ServicesToRun;
                ServicesToRun = new ServiceBase[]
                {
                new SignalWatcher()
                };
                ServiceBase.Run(ServicesToRun);

            }




        }
    }
}
