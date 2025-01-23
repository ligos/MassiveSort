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
    [Verb("crash")]
    public sealed class CrashConf : CommonConf
    {
        [Option('f', "from")]
        public ExceptionFrom From { get; set; }
        public enum ExceptionFrom
        {
            Throw,
            Task,
        }

        public static string GetUsageText()
        {
            return
"""
Help for 'crash' verb:

-f --from    Where to throw the exception from.
              - Throw: Simple throw statement on main thead (default).
              - Task: Thrown from within Task object.
""";
        }
    }

    public class Crash : ICmdVerb
    {
        private readonly CrashConf _Conf;
        
        public Crash(CrashConf conf)
        {
            _Conf = conf;
        }

        public bool IsValid()
        {
            return true;
        }
        public string GetValidationError()
        {
            return "";
        }

        public void Do(CancellationToken token)
        {
            if (_Conf.Help) {
                new Help(new HelpConf() { Verb = "crash" }).Do(token);
                return;
            }

            // All this does is throw an exception for testing purposes.

            if (_Conf.From == CrashConf.ExceptionFrom.Throw)
                throw new Exception("Test exception. Normal throw.");
            else if (_Conf.From == CrashConf.ExceptionFrom.Task)
            {
                var t = Task.Run(() => {
                    throw new Exception("Test exception. Thrown from inside Task.");
                });
                t.Wait();
            }
        }
    }
}
