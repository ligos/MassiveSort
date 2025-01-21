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
    [Verb("cleantemp")]
    public sealed class CleanTempConf : CommonConf
    {
        [Option('t', "temp-folder")]
        public string TempFolder { get; set; }

        public static string GetUsageText()
        {
            return Conf.FirstUsageLineText + @"
-t --temp-folder    Folder to search for temporary files. 
                    Default: %TEMP%\MassiveSort  
";
        }
    }

    public class CleanTemp : ICmdVerb
    {
        private readonly CleanTempConf _Conf;
        private const double oneMbAsDouble = 1024.0 * 1024.0;

        public CleanTemp(CleanTempConf conf)
        {
            _Conf = conf;
        }

        public string GetUsageMessage()
        {
            return CleanTempConf.GetUsageText();
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
            // Check the default temp folder.
            var defaultTempSize = 0L;
            var defaultTemp = Helpers.GetBaseTempFolder();
            Console.Write("Cleaning default temp folder '{0}'...", defaultTemp);

            if (token.IsCancellationRequested)
                return;

            if (Directory.Exists(defaultTemp))
            {
                var dir = new DirectoryInfo(defaultTemp);
                defaultTempSize = dir.EnumerateFiles("*", SearchOption.AllDirectories).Sum(x => x.Length);
                foreach (var x in dir.EnumerateFiles())
                    x.Delete();
                if (token.IsCancellationRequested)
                    return;
                foreach (var x in dir.EnumerateDirectories())
                    x.Delete(true);
                if (token.IsCancellationRequested)
                    return;
                Console.WriteLine(" Deleted {0:N1}MB.", defaultTempSize / oneMbAsDouble);
            }
            else
            {
                Console.WriteLine(" Does not exist.");
            }


            if (token.IsCancellationRequested)
                return;

            // If one was provided via the command line, check it as well.
            if (String.IsNullOrEmpty(_Conf.TempFolder))
            {
                var customTempSize = 0L;
                var customTemp = Helpers.GetBaseTempFolder();
                Console.Write("Cleaning custom temp folder '{0}'...", customTemp);

                if (Directory.Exists(customTemp))
                {
                    var dir = new DirectoryInfo(customTemp);
                    customTempSize = dir.EnumerateFiles("*", SearchOption.AllDirectories).Sum(x => x.Length);
                    foreach (var x in dir.EnumerateFileSystemInfos())
                        x.Delete();
                    if (token.IsCancellationRequested)
                        return;
                    Console.WriteLine(" Deleted {0:N1}MB.", customTempSize / oneMbAsDouble);
                }
                else
                {
                    Console.WriteLine(" Does not exist.");
                }
            }
        }
    }
}
