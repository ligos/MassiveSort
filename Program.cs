// Copyright 2015 Murray Grant
//
//    Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
            bool helpRequested = false;
            var parseSucceeded = CommandLine.Parser.Default.ParseArguments(args, conf, (v, o) =>
            {
                verbSelected = v ?? "";
                if (String.Equals(verbSelected, "help", StringComparison.CurrentCultureIgnoreCase))
                    helpRequested = true;
                else if (conf != null)
                    helpRequested = conf.HelpWasRequested;
            });
            

            // Based on command line verb, determine what we will do.
            ICmdVerb action = null;
            string usageText = null;
            string errorText = null;
            if (!parseSucceeded)
                errorText = "Error: Unable to parse arguments.";

            var verb = verbSelected.ToLower();
            if (parseSucceeded && verb == "merge") 
                action = new MergeMany(conf.MergeOptions.ExtraParsing());
            else if (!parseSucceeded && verb == "merge")
                usageText = MergeConf.GetUsageText();
            else if (parseSucceeded && verb == "cleantemp")
                action = new CleanTemp(conf.CleanTempOptions);
            else if (!parseSucceeded && verb == "cleantemp")
                usageText = CleanTempConf.GetUsageText();
            else if (verb == "help" && args.Length == 1) {
                errorText = "Here's some help:";
                usageText = Conf.GetUsageText();
            } else if (verb == "help" && args.Length == 2) {
                errorText = "";
                usageText = GetHelpMessageForVerb(args[1]);
            } else if (!parseSucceeded && String.IsNullOrEmpty(verbSelected)) {
                errorText = "Error: You must select a verb.";
                usageText = Conf.GetUsageText();
            } else if (!parseSucceeded) {
                errorText = "Error: Unknown verb - " + verbSelected;
                usageText = Conf.GetUsageText();
            
            } else if (parseSucceeded)
                throw new Exception("Unknown verb: " + verbSelected);
            else
                throw new Exception("Unexpected state.");
           

            // Check all is OK.
            if (parseSucceeded && action != null && !action.IsValid())
            {
                errorText = "Error: " + action.GetValidationError();
                usageText = action.GetUsageMessage();
            }

            if (!parseSucceeded || !String.IsNullOrEmpty(errorText) || helpRequested)
            {
                // Failure case.
                Console.WriteLine();
                if (!helpRequested)
                    Console.WriteLine(errorText);
                Console.WriteLine(usageText);

                if (Environment.UserInteractive && Debugger.IsAttached)
                {
                    Console.Write("Press a key to end.");
                    Console.ReadKey();
                }
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

        private static string GetHelpMessageForVerb(string v)
        {
            var verb = (v ?? "").ToLower();
            switch (verb)
            {
                case "merge":
                    return MergeConf.GetUsageText();
                case "cleantemp":
                    return CleanTempConf.GetUsageText();
                default:
                    return "Unknown verb: " + v + "\n" + Conf.GetUsageText();
            }
        }

    }
}
