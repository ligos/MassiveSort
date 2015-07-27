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
using MurrayGrant.MassiveSort.Readers;


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

        [Option('w', "workers")]
        public int Workers { get; set; }

        /// <summary>
        /// Files larger than this (bytes) will be processed in smaller chunks.
        /// To allow large files to gain benefits of parallelism too.
        /// Default: 128MB
        /// </summary>
        public long LargeFileThresholdSize { get; set; }
        [Option("large-threshold")]
        public string LargeFileThresholdSize_Raw { get; set; }
        /// <summary>
        /// Large files are processed in chunks this big.
        /// To allow large files to gain benefits of parallelism too.
        /// Default: 32MB
        /// </summary>
        public long LargeFileChunkSize { get; set; }
        [Option("large-chunk")]
        public string LargeFileChunkSize_Raw { get; set; }



        [Option("line-buffer-size")]
        public string LineBufferSize_Raw { get; set; }
        public int LineBufferSize { get; set; }
        [Option("read-file-buffer-size")]
        public string ReadBufferSize_Raw { get; set; }
        public int ReadBufferSize { get; set; }


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

            var filesToProcess = this.GatherFiles();
            if (token.IsCancellationRequested) return;

            var aggregateSummaries = this.AnalyseFiles(filesToProcess);
            if (token.IsCancellationRequested) return;

            this.WriteResults(aggregateSummaries);
        }


        private IEnumerable<FileInfo> GatherFiles()
        {
            _Progress.Report(new BasicProgress(String.Format("Gathering files to analyse from '{0}'.", String.Join("; ", _Conf.Inputs)), true));

            var sw = Stopwatch.StartNew();
            var result = Helpers.GatherFiles(_Conf.Inputs);
            sw.Stop();

            return result;
        }

        private IEnumerable<RawByteAccumulator> AnalyseFiles(IEnumerable<FileInfo> files)
        {
            _Progress.Report(new BasicProgress(String.Format("Analysing {0:N0} file(s) totaling {1}.", files.Count(), files.Sum(x => x.Length).ToByteSizedString()), true));

            var chunks = new PlainRaw(_CancelToken).ConvertFilesToSplitChunks(files, _Conf.LargeFileThresholdSize, _Conf.LargeFileChunkSize);
            // TODO: parallel.

            // TODO: pull this into a loop so that once a file is fully processed it can be written.
            var fileSummaries = chunks
                .Select(ch => this.AnalyseFile(ch))
                .GroupBy(x => x.FullPath)
                .Select(g => g.Aggregate(new RawByteAccumulator(g.Key), (x, acc) => acc.Add(x)))
                .ToList();

            return fileSummaries;
        }

        private RawByteAccumulator AnalyseFile(FileChunk ch)
        {
            var acc = new RawByteAccumulator(ch.FullPath);
            var taskKey = new object();

            if (_CancelToken.IsCancellationRequested) return acc;
            _Progress.Report(new TaskProgress(String.Format("Analysing '{0}'...", ch.NameForProgress), false, taskKey));
            var sw = Stopwatch.StartNew();

            var reader = new PlainRaw(_CancelToken, _Conf.LineBufferSize, _Conf.ReadBufferSize);
            foreach (var line in reader.ReadAll(ch.FullPath, ch.StartOffset, ch.EndOffset))
            {
                acc.AddOneByLength(line.Count);
                if (_CancelToken.IsCancellationRequested) break;
            }
            acc.TotalLines = (ulong)reader.LinesRead;
            sw.Stop();

            _Progress.Report(new TaskProgress(" Done.", true, taskKey));
            return acc;
        }

        private void WriteResults(IEnumerable<RawByteAccumulator> accs)
        {
            if (_CancelToken.IsCancellationRequested) return;
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

            public RawByteAccumulator(string fullPath)
            {
                this.FullPath = fullPath;
                this.LineCountByLength = new UInt64[64];
            }

            public bool IsSorted;           // True if the file is sorted.
            public UInt64 TotalLines;
            public UInt64[] LineCountByLength;      // Initialise to 64 and grow as long lines are encountered.
            public void AddOneByLength(int length)
            {
                if (length > this.LineCountByLength.Length)
                    Array.Resize(ref this.LineCountByLength, length.ThisOrNextPowerOfTwo());
                this.LineCountByLength[length] = this.LineCountByLength[length] + 1;
            }

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
                if (this.FullPath != other.FullPath)
                    throw new Exception("Cannot add accumulators for different files.");

                this.TotalLines = this.TotalLines + other.TotalLines;

                var longestLineCountLength = Math.Max(this.LineCountByLength.Length, other.LineCountByLength.Length);
                if (this.LineCountByLength.Length < longestLineCountLength)
                    Array.Resize(ref this.LineCountByLength, longestLineCountLength);
                for (int i = 0; i < this.LineCountByLength.Length; i++)
                    this.LineCountByLength[i] = this.LineCountByLength[i] + other.LineCountByLength[i];
                
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
