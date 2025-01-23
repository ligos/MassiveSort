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
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;

using MurrayGrant.MassiveSort.Actions;
using CommandLine;

namespace MurrayGrant.MassiveSort
{
    class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                Console.OutputEncoding = Encoding.UTF8;

                Console.WriteLine("{0} v{1} - {2}", About.ProductName, About.Version, About.Copyright);
                Console.WriteLine();

                //var conf = new Conf();
                bool helpRequested = false;
                Type[] verbTypes = [typeof(HelpConf), typeof(AboutConf), typeof(AnalyseConf), typeof(CleanTempConf), typeof(CrashConf), typeof(MergeConf)];
                var parser = new CommandLine.Parser(settings =>
                {
                    settings.HelpWriter = null;
                    settings.AutoHelp = false;
                    settings.AutoVersion = false;
                    settings.CaseInsensitiveEnumValues = true;
                });
                var parseResult = parser.ParseArguments(args, verbTypes);
                var parseSucceeded = parseResult.Tag == ParserResultType.Parsed;


                // Based on command line verb, determine what we will do.
                ICmdVerb action = null;
                string usageText = null;
                string errorText = null;
                if (!parseSucceeded)
                    errorText = "Error: Unable to parse arguments.";

                if (parseResult.Value is MergeConf mc)
                    action = new MergeMany(mc.ExtraParsing());
                else if (parseResult.Value is AnalyseConf ac)
                    action = new Analyse(ac.ExtraParsing());
                else if (parseResult.Value is CrashConf cc)
                    action = new Crash(cc);
                else if (parseResult.Value is CleanTempConf ctc)
                    action = new CleanTemp(ctc);
                else if (parseResult.Value is AboutConf)
                    action = new About();
                else if (parseResult.Value is HelpConf hc)
                    action = new Help(hc);
                //} else if (!parseSucceeded && String.IsNullOrEmpty(verbSelected)) {
                //    errorText = "Error: You must select a verb.";
                //    usageText = Conf.GetUsageText();
                //} else if (!parseSucceeded) {
                //    errorText = "Error: Unknown verb - " + verbSelected;
                //    usageText = Conf.GetUsageText();

                //} else if (parseSucceeded)
                //    throw new Exception("Unknown verb: " + verbSelected);
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
                Console.CancelKeyPress += Console_CancelKeyPress;
                var sw = Stopwatch.StartNew();
                try
                {
                    action.Do(_CancelSource.Token);
                    sw.Stop();
                }
                finally
                {
                    var asDisposable = action as IDisposable;
                    if (asDisposable != null)
                        asDisposable.Dispose();
                }
                Console.CancelKeyPress -= Console_CancelKeyPress;

                Console.WriteLine("Total run time {0:N1}.", sw.Elapsed.ToSizedString());

                if (Environment.UserInteractive && Debugger.IsAttached)
                {
                    Console.Write("Press a key to end.");
                    Console.ReadKey();
                }
                return 0;
            }
            catch (Exception ex)
            {
                // Catch-all exception handling.
                Console.Error.WriteLine("Unexpected Exception:" + Environment.NewLine + ex.ToFullString());
                if (Environment.UserInteractive) Console.Beep();
                Console.Error.WriteLine();

                ExceptionAndComputerDetail crashDetail = null;
                try {
                    crashDetail = CrashDumper.CreateErrorDetails(ex);
                } catch (Exception ex2) {
                    Console.Error.WriteLine("Unable to create additional crash details.");
                    Console.Error.WriteLine(ex2.ToFullString());
                    return -2;
                }

                try {
                    var folderSavedIn = CrashDumper.Save(crashDetail);
                    Console.Error.WriteLine("Additional details saved in: " + folderSavedIn);
                } catch (Exception ex2) {
                    Console.Error.WriteLine("Unable to save additional crash details.");
                    Console.Error.WriteLine(ex2.ToFullString());
                    Console.Error.WriteLine();
                    Console.Error.WriteLine(crashDetail.ToString());
                    return -3;
                }
                return -1;
            }
        }

        private readonly static CancellationTokenSource _CancelSource = new CancellationTokenSource();
        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            _CancelSource.Cancel();
            e.Cancel = true;
            Console.WriteLine("CTRL+C received: cancelling run...");
        }
    }
}
