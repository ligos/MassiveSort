using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;

using MurrayGrant.MassiveSort.Actions;

namespace MurrayGrant.MassiveSort
{
    class Program
    {
        public static int Main(string[] args)
        {
            var conf = new Conf();
            var verbSelected = "";
            if (!CommandLine.Parser.Default.ParseArguments(args, conf, (v, o) =>
            {
                verbSelected = v;
            }))
            {
                Console.WriteLine("Error when parsing arguments.");
                PrintUsage(verbSelected);
                return 1;
            }
            if (String.IsNullOrEmpty(verbSelected))
            {
                Console.WriteLine("You must select a verb.");
                PrintUsage("");
                return 1;
            }

            // Based on command line verb, determine what we will do.
            ICmdVerb action;
            switch(verbSelected.ToLower())
            {
                case "merge":
                    action = new MergeMany(conf.MergeOptions.ExtraParsing());
                    break;
                case "cleantemp":
                    action = new CleanTemp(conf.CleanTempOptions);
                    break;
                default:
                    Console.WriteLine("Unknown verb '{0}'.", verbSelected);
                    PrintUsage("");
                    return 1;
            }
            
            // Check all is OK.
            if (!action.IsValid())
            {
                Console.WriteLine(action.GetValidationError());
                PrintUsage(verbSelected);
                return 1;
            }

            // Make it so, number one!
            var sw = Stopwatch.StartNew();
            try
            {
                action.Do();
                sw.Stop();
            }
            finally
            {
                var asDisposable = action as IDisposable;
                if (asDisposable != null)
                    asDisposable.Dispose();
            }

            Console.WriteLine("Total run time {0:N1}.", sw.Elapsed.ToSizedString());

            if (Environment.UserInteractive && Debugger.IsAttached)
            {
                Console.Write("Press a key to end.");
                Console.ReadKey();
            }
            return 0;
        }



        private static void PrintUsage(string forVerb)
        {
            Console.WriteLine("Usage....");
        }
    }
}
