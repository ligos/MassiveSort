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

    [Verb("analyse", aliases: [ "analyze" ])]
    public sealed class AnalyseConf : CommonConf
    {
        public AnalyseConf()
            : base()
        {
            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            this.TempFolder = Path.Combine(Helpers.GetBaseTempFolder(), pid.ToString());        // Temp folder is specific to process id, so we can run in parallel.
            this.FileEnding = "Analysis.txt";
            this.Workers = Helpers.PhysicalCoreCount();

            this.LargeFileThresholdSize = 128 * 1024L * 1024L;        // 128MB
            this.LargeFileChunkSize = 32 * 1024 * 1024;               // 32MB

            this.ReadBufferSize = 64 * 1024;                // Buffer size to use when reading files.
            this.LineBufferSize = 64 * 1024;                // Buffer size to use when reading lines (also max line length).

            this.OutputFormats = new OutputFormat[] { OutputFormat.Plain };
        }

        public static string GetUsageText()
        {
            return
"""
Help for 'analyse' verb:

TODO
""";
        }


        [Option('i', "input")]
        public IEnumerable<string> Inputs { get; set; }

        [Option('e', "file-ending")]
        public string FileEnding { get; set; }

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


        public OutputFormat[] OutputFormats { get; set; }
        public enum OutputFormat
        {
            Plain,
            Csv,
            Tsv,
            Json,
        }

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
        private DateTimeOffset _AnalysisStartedAt;

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
            if (_Conf.Help) {
                new Help(new HelpConf() { Verb = "analyse" }).Do(token);
                return;
            }

            this._CancelToken = token;
            PrintConf();        // Print the config settings, in debug mode.

            var filesToProcess = this.GatherFiles();
            _AnalysisStartedAt = DateTimeOffset.Now;
            if (token.IsCancellationRequested) return;

            var aggregateSummaries = this.AnalyseFiles(filesToProcess);
            if (token.IsCancellationRequested) return;

            this.WriteResults(aggregateSummaries);
        }


        private IEnumerable<FileInfo> GatherFiles()
        {
            _Progress.Report(new BasicProgress(String.Format("Gathering files to analyse from '{0}'.", String.Join("; ", _Conf.Inputs)), true));

            var sw = Stopwatch.StartNew();
            var result = Helpers.GatherFiles(_Conf.Inputs, new [] { this._Conf.FileEnding });
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
                acc.AddOneToCategoryMasks(line);
                if (_CancelToken.IsCancellationRequested) break;
            }
            acc.TotalLines = (ulong)reader.LinesRead;
            acc.FileSizeBytes = (ulong)(ch.EndOffset - ch.StartOffset);
            sw.Stop();

            _Progress.Report(new TaskProgress(" Done.", true, taskKey));
            return acc;
        }

        private void WriteResults(IEnumerable<RawByteAccumulator> accs)
        {
            if (_CancelToken.IsCancellationRequested) return;

            var outputFormats = this._Conf.OutputFormats ?? new AnalyseConf.OutputFormat[] { };
            foreach (var a in accs)
            {
                if (outputFormats.Contains(AnalyseConf.OutputFormat.Plain))
                {
                    var analysisPath = Path.ChangeExtension(a.FullPath, _Conf.FileEnding);
                    using (var writer = new StreamWriter(analysisPath, false, Constants.Utf8NoBom))
                    {
                        a.WriteAsPlainText(writer, _AnalysisStartedAt);
                        writer.Flush();
                    }
                }
                if (_CancelToken.IsCancellationRequested) return;
            }
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
                // Only 7 bits are used in the CharCategory masks.
                this.CountsByAllCategoryMask = new UInt64[128];
                this.CountsByAnyCategoryMask = new UInt64[7];
            }

            public bool IsSorted;           // True if the file is sorted.
            public UInt64 TotalLines;
            public UInt64 FileSizeBytes;
            public UInt64[] LineCountByLength;      // Initialise to 64 and grow as long lines are encountered.
            public void AddOneByLength(int length)
            {
                if (length > this.LineCountByLength.Length)
                    Array.Resize(ref this.LineCountByLength, length.ThisOrNextPowerOfTwo());
                this.LineCountByLength[length] = this.LineCountByLength[length] + 1;
            }

            public UInt64[] CountsByAllCategoryMask;        // CharCategory, added together, is the index.
            public UInt64[] CountsByAnyCategoryMask;        // The nth bit of the CharCategory is the index.
            public void AddOneToCategoryMasks(ByteArraySegment line)
            {
                // TODO: decode $HEX[] to a byte array.

                int allMasks = 0;
                for (int i = 0; i < line.Count; i++)
                {
                    var b = line.Array[line.Offset+i];
                    if (b >= 0x41 && b <= 0x5a)
                        // Upper letter
                        allMasks |= (int)CharCategory.UppercaseLetter;
                    else if (b >= 0x61 && b <= 0x7a)
                        // Lower letter.
                        allMasks |= (int)CharCategory.LowercaseLetter;
                    else if (b >= 0x30 && b <= 0x39)
                        // Number.
                        allMasks |= (int)CharCategory.Number;
                    else if (b == 0x20 || b == 0x09 || b == 0x0b)
                        // Whitespace (or tab).
                        allMasks |= (int)CharCategory.Whitespace;
                    else if (b <= 0x1f)
                        // Low control.
                        allMasks |= (int)CharCategory.LowControl;
                    else if (b >= 0x7f)
                        // High / non-ascii.
                        allMasks |= (int)CharCategory.HighNonAscii;
                    else
                        // Punctuation (which is scattered across the ascii table).
                        allMasks |= (int)CharCategory.Punctuation;
                }

                this.CountsByAllCategoryMask[allMasks] = this.CountsByAllCategoryMask[allMasks] + 1;
                if ((allMasks & 0x01) == 0x01)
                    this.CountsByAnyCategoryMask[0] = this.CountsByAnyCategoryMask[0] + 1;
                if ((allMasks & 0x02) == 0x02)
                    this.CountsByAnyCategoryMask[1] = this.CountsByAnyCategoryMask[1] + 1;
                if ((allMasks & 0x04) == 0x04)
                    this.CountsByAnyCategoryMask[2] = this.CountsByAnyCategoryMask[2] + 1;
                if ((allMasks & 0x08) == 0x08)
                    this.CountsByAnyCategoryMask[3] = this.CountsByAnyCategoryMask[3] + 1;
                if ((allMasks & 0x10) == 0x10)
                    this.CountsByAnyCategoryMask[4] = this.CountsByAnyCategoryMask[4] + 1;
                if ((allMasks & 0x20) == 0x20)
                    this.CountsByAnyCategoryMask[5] = this.CountsByAnyCategoryMask[5] + 1;
                if ((allMasks & 0x40) == 0x40)
                    this.CountsByAnyCategoryMask[6] = this.CountsByAnyCategoryMask[6] + 1;
                if ((allMasks & 0x80) == 0x80)
                    this.CountsByAnyCategoryMask[7] = this.CountsByAnyCategoryMask[7] + 1;
            }

            public readonly RawByteAccumulatorEx More;

            // These should probably be a bit mask into an array which defines each category.
            [Flags]
            public enum CharCategory : byte
            {
                Empty = 0x00,
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
                this.FileSizeBytes = this.FileSizeBytes + other.FileSizeBytes;

                var longestLineCountLength = Math.Max(this.LineCountByLength.Length, other.LineCountByLength.Length);
                if (this.LineCountByLength.Length < longestLineCountLength)
                    Array.Resize(ref this.LineCountByLength, longestLineCountLength);
                for (int i = 0; i < Math.Min(this.LineCountByLength.Length, other.LineCountByLength.Length); i++)
                    this.LineCountByLength[i] = this.LineCountByLength[i] + other.LineCountByLength[i];

                for (int i = 0; i < this.CountsByAllCategoryMask.Length; i++)
                    this.CountsByAllCategoryMask[i] = this.CountsByAllCategoryMask[i] + other.CountsByAllCategoryMask[i];
                for (int i = 0; i < this.CountsByAnyCategoryMask.Length; i++)
                    this.CountsByAnyCategoryMask[i] = this.CountsByAnyCategoryMask[i] + other.CountsByAnyCategoryMask[i];

                return this;
            }

            public void WriteAsPlainText(TextWriter writer, DateTimeOffset analysisDatestamp)
            {
                writer.WriteLine("MassiveSort - Analysis of file '{0}', as at {1}", Path.GetFileName(this.FullPath), analysisDatestamp);
                writer.WriteLine("File Size: {0:N0} bytes ({1:N1} MB)", this.FileSizeBytes, this.FileSizeBytes / Constants.OneMbAsDouble);
                writer.WriteLine("Total Lines: {0:N0}", this.TotalLines);
                writer.WriteLine("Average Line Length (bytes): {0:N2}", (double)this.FileSizeBytes / (double)this.TotalLines);

                writer.WriteLine();
                writer.WriteLine("Length (bytes) Histogram:");
                var lineCountByLength = this.LineCountByLength.RemoveTrailing(x => x == 0);
                for (int i = 0; i < lineCountByLength.Length; i++)
                    writer.WriteLine("{0}: {1:N0} ({2:P2})", i, lineCountByLength[i], (double)lineCountByLength[i] / (double)this.TotalLines);

                writer.WriteLine();
                writer.WriteLine("Charset Counts:");
                foreach (var ch in this.CountsByAllCategoryMask
                    .Select((count, i) => new { name = this.CharCategoryMaskToFriendly(i), count })
                    .GroupBy(x => x.name)
                    .Select(x => new
                    {
                        name = x.Key,
                        count = x.Sum(z => (decimal)z.count),
                    })
                    .Where(x => x.count > 0)
                    .OrderBy(x => x.name))
                    writer.WriteLine("{0}: {1:N0} ({2:P2})", ch.name, ch.count, (double)ch.count / (double)this.TotalLines);

                writer.WriteLine();
                writer.WriteLine("Lines With Char Category:");
                for (int i = 0; i < this.CountsByAnyCategoryMask.Length; i++)
                    writer.WriteLine("{0}: {1:N0} ({2:P2})", ((CharCategory)(1 << i)).ToString(), this.CountsByAnyCategoryMask[i], (double)this.CountsByAnyCategoryMask[i] / (double)this.TotalLines);

                writer.WriteLine();
                writer.WriteLine("Full Charset Counts:");
                for (int i = 0; i < this.CountsByAllCategoryMask.Length; i++)
                    if (this.CountsByAllCategoryMask[i] != 0)
                        writer.WriteLine("{0}: {1:N0} ({2:P2})", ((CharCategory)i).ToString(), this.CountsByAllCategoryMask[i], (double)this.CountsByAllCategoryMask[i] / (double)this.TotalLines);

            }
            public object ToObject(DateTimeOffset analysisDatestamp)
            {
                throw new NotImplementedException();
            }
            public IEnumerable<IEnumerable<string>> ToTableOfStrings(DateTimeOffset analysisDatestamp)
            {
                throw new NotImplementedException();
            }

            public string CharCategoryMaskToFriendly(int mask)
            {
                var result = "";

                if ((mask & (int)CharCategory.LowercaseLetter) == (int)CharCategory.LowercaseLetter
                    && (mask & (int)CharCategory.UppercaseLetter) == (int)CharCategory.UppercaseLetter)
                    result += "Mixed";
                else if ((mask & (int)CharCategory.LowercaseLetter) != (int)CharCategory.LowercaseLetter
                    && (mask & (int)CharCategory.UppercaseLetter) == (int)CharCategory.UppercaseLetter)
                    result += "Upper";
                else if ((mask & (int)CharCategory.LowercaseLetter) == (int)CharCategory.LowercaseLetter
                    && (mask & (int)CharCategory.UppercaseLetter) != (int)CharCategory.UppercaseLetter)
                    result += "Lower";

                if ((mask & (int)CharCategory.Number) == (int)CharCategory.Number)
                    result += "Number";

                if ((mask & (int)CharCategory.Whitespace) == (int)CharCategory.Whitespace
                    || (mask & (int)CharCategory.Punctuation) == (int)CharCategory.Punctuation)
                    result += "Symbol";

                if ((mask & (int)CharCategory.LowControl) == (int)CharCategory.LowControl)
                    result += "Special";

                if ((mask & (int)CharCategory.HighNonAscii) == (int)CharCategory.HighNonAscii)
                    result += "Extended";

                if (mask == 0)
                    result += "Empty";
                return result;
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
