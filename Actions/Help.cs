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
using CommandLine;

namespace MurrayGrant.MassiveSort.Actions
{
    [Verb("help")]
    public sealed class HelpConf : CommonConf
    {
        [Value(0)]
        public string Verb { get; set; }
    }

    public class Help(HelpConf conf) : ICmdVerb
    {
        readonly HelpConf Conf = conf;

        public string GetUsageMessage() => "";

        public bool IsValid() => true;

        public string GetValidationError() => "";

        public void Do(CancellationToken token)
        {
            Console.WriteLine("Usage: MassiveSort <verb> <arguments> [options]");
            Console.WriteLine();

            if (string.IsNullOrEmpty(Conf.Verb) || string.Equals(Conf.Verb, "help", StringComparison.InvariantCultureIgnoreCase))
            {
                // Display overall help
                Console.WriteLine(
                    """
                    merge        Merges and sorts one or more files to a new copy
                    cleanTemp    Cleans unused files from temporary folders
                    crash        Tests handling an unexpected error
                    about        Shows copyright and version information
                    help         Gets help on a verb, or shows this list

                    All verbs have a --help option, or use 'help <verb>' to get further help.

                    """
                );
                Console.WriteLine($"Additional information can be found at {About.ProjectUrl}");
            }
            else if (string.Equals(Conf.Verb, "merge", StringComparison.InvariantCultureIgnoreCase))
            {
                Console.WriteLine(MergeConf.GetUsageText());
            }
            else if (string.Equals(Conf.Verb, "cleantemp", StringComparison.InvariantCultureIgnoreCase))
            {
                Console.WriteLine(CleanTempConf.GetUsageText());
            }
            else if (string.Equals(Conf.Verb, "crash", StringComparison.InvariantCultureIgnoreCase))
            {
                Console.WriteLine(CrashConf.GetUsageText());
            }
            else if (string.Equals(Conf.Verb, "about", StringComparison.InvariantCultureIgnoreCase))
            {
                Console.WriteLine(AboutConf.GetUsageText());
            }
            else
                Console.WriteLine("Unknown verb: " + Conf.Verb);

            Console.WriteLine();
        }
    }
}
