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
    public sealed class AboutConf : CommonConf
    {
        public static string GetUsageText()
        {
            return Conf.FirstUsageLineText;
        }
    }

    public class About : ICmdVerb
    {
        public static readonly string Copyright = typeof(Program).Assembly.CustomAttributes.First(x => x.AttributeType == typeof(System.Reflection.AssemblyCopyrightAttribute)).ConstructorArguments.First().Value.ToString();
        public static readonly string Version = typeof(Program).Assembly.CustomAttributes.First(x => x.AttributeType == typeof(System.Reflection.AssemblyFileVersionAttribute)).ConstructorArguments.First().Value.ToString();
        public static readonly string ProjectUrl = typeof(Program).Assembly.CustomAttributes.First(x => x.AttributeType == typeof(System.Reflection.AssemblyMetadataAttribute) && x.ConstructorArguments.First().Value.ToString() == "project-url").ConstructorArguments.Skip(1).First().Value.ToString();

        public About()
        {
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
            // Display license, authorship and 3rd party library details.
            
            Console.WriteLine("About MassiveSort");
            Console.WriteLine(Copyright);
            Console.WriteLine(ProjectUrl);
            Console.WriteLine("Available under terms of Apache License (see LICENSE.txt)");
            Console.WriteLine();
            Console.WriteLine("Third Part Library Credits:");
            Console.WriteLine("Command Line Parser Library © 2005-2013 Giacomo Stelluti Scala & Contributors");
            Console.WriteLine("  https://github.com/gsscoder/commandline");
            Console.WriteLine();
        }
    }
}
