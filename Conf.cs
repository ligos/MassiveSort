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
using CommandLine;

namespace MurrayGrant.MassiveSort
{
    public interface ICmdVerb
    {
        bool IsValid();
        string GetValidationError();
        string GetUsageMessage();

        void Do(CancellationToken token);
    }

    public class Conf
    {
        public Conf()
        {
        }

        //public bool HelpWasRequested 
        //{ 
        //    get {
        //        return (this.MergeOptions != null && this.MergeOptions.Help)
        //            || (this.CleanTempOptions != null && this.CleanTempOptions.Help);
        //    } 
        //}

        //[HelpVerbOption()]
        public string GetUsage(string verb)
        {
            // Without this, the command line parser throws a null ref exception.
            return "";
        }
        public static readonly string FirstUsageLineText = "  Usage: MassiveSort <verb> <arguments> [options]\n";
        public static string GetUsageText()
        {
            return FirstUsageLineText + String.Format(@"
merge        Merges and sorts one or more files to a new copy
cleanTemp    Cleans unused files from temporary folders                    
help         Gets help on a verb, or shows this list                    

Additional information can be found at {0}
", Actions.About.ProjectUrl);
        }
        
    }

    public abstract class CommonConf
    {
        [Option('?', "help")]
        public bool Help { get; set; }


        [Option("debug")]
        public bool Debug { get; set; }
    }

}
