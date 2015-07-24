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
    #region Config
    public sealed class AnalyseConf : CommonConf
    {
        public AnalyseConf()
            : base()
        {
        }

        public static string GetUsageText()
        {
            return "";
        }


        [OptionArray('i', "input")]
        public string[] Inputs { get; set; }


        [Option('t', "temp-folder")]
        public string TempFolder { get; set; }


        public AnalyseConf ExtraParsing()
        {
            return this;
        }

        public string GetValidationMessage()
        {
            return "";
        }
        public bool IsValid { get { return String.IsNullOrEmpty(GetValidationMessage()); } }
    }
    #endregion


    public class Analyse : ICmdVerb, IDisposable
    {
        private readonly AnalyseConf _Conf;
        private readonly IProgress<BasicProgress> _Progress = new ConsoleProgressReporter();
        private CancellationToken _CancelToken;
        

        public Analyse(AnalyseConf conf)
        {
            _Conf = conf;
        }

        public string GetUsageMessage()
        {
            return MergeConf.GetUsageText();
        }

        public void Dispose()
        {
        }
        public bool IsValid()
        {
            return _Conf.IsValid;
        }
        public string GetValidationError()
        {
            return _Conf.GetValidationMessage();
        }

        public void Do(CancellationToken token)
        {
            this._CancelToken = token;

            PrintConf();        // Print the config settings, in debug mode.

        }

        private void PrintConf()
        {
            Console.WriteLine("Configuration:");
            Console.WriteLine("  TODO.....");
            Console.WriteLine();
        }

        public class RawByteAccumulator
        {
            // Accumulate stats per-file.
            public readonly string FullPath;

            public bool IsSorted;           // True if the file is sorted.
            public UInt64 TotalLines;
            public UInt64[] LineCountByLength;      // Initialise to 64 and grow as long lines are encountered. Up to 256. Beyond that, use a sparse array / dictionary.
            public List<UInt64> CountsByAllCategoryMask;        // CharCategory, added together, is the index.
            public List<UInt64> CountsByAnyCategoryMask;        // The nth bit of the CharCategory is the index.

            public readonly RawByteAccumulatorEx More;

            // These should probably be a bit mask into an array which defines each category.
            [Flags]
            public enum CharCategory : byte
            {
                UppercaseLetter = 0x01,
                LowercaseLetter = 0x02,
                Number = 0x04,
                Punctuation = 0x08,
                Whitespace = 0x10,
                LowControl = 0x20,
                HighNonAscii = 0x40,
            }

            public RawByteAccumulator Add(RawByteAccumulator other)
            {
                return this;
            }
        }

        public class RawByteAccumulatorEx
        {
            // These sit in a separate object, to improve the density of the more commonly used things.
            public UInt64[] CountsBySubstring;        // How often substrings occur in lines. IDictionary<SubStringIndex, Count>

            public List<UInt64[]> CountByStartingCategoryLength;       // Count of consecutive CharCategory at the start of a line. IDictionary<CharCategory, IDictionary<Length, Count>>
            public List<UInt64[]> CountByEndingCategoryLength;       // Count of consecutive CharCategory at the end of a line. IDictionary<CharCategory, IDictionary<Length, Count>>
            public List<UInt64[]> CountByConsecutiveCategoryLength;       // Count of consecutive CharCategory at any point in the line. IDictionary<CharCategory, IDictionary<Length, Count>>

            public UInt64[] CountByCharacterAndPosition;      // Big array of combination of each byte and each position. The byte is encoded in the low 8 bits of the index, the position in the next 8 bits. Max length of 256, no empty string.

        }
    }
}
