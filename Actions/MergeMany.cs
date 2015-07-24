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
using System.Collections.Concurrent;
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
    public sealed class MergeConf : CommonConf
    {
        public MergeConf()
            : base()
        {
            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            this.TempFolder = Path.Combine(Helpers.GetBaseTempFolder(), pid.ToString());        // Temp folder is specific to process id, so we can run in parallel.

            this.MaxSortSize = 64 * 1024 * 1024;            // Largest size of files to sort in one chunk.
            this.ReadBufferSize = 64 * 1024;                // Buffer size to use when reading files.
            this.LineBufferSize = 64 * 1024;                // Buffer size to use when reading lines (also max line length).
            this.TempFileBufferSize = 128 * 1024;           // Buffer size to use when writing temp files.
            this.OutputBufferSize = 256 * 1024;             // Buffer size to use for the final merged output file.

            this.LeaveDuplicates = false;
            this.SaveDuplicates = false;
            this.AggressiveMemoryCollection = false;
            this.SaveStats = false;
            this.SplitWorkers = Helpers.PhysicalCoreCount();
            this.SortWorkers = Helpers.PhysicalCoreCount();
            this.IOWorkers = 8;                 // Default of 8 IO workers. Should provide a balance between SSD and HDD.
            this.MaxOutstandingSortedChunks = 2;

            this.LargeFileThresholdSize = 1 * 1024L * 1024L * 1024L;        // 1GB
            this.LargeFileChunkSize = 256 * 1024 * 1024;    // 256MB

            this.SortAlgorithm = SortAlgorithms.TimSort;    // Sort algorithm to use. 
            this.Comparer = Comparers.Clr;                  // IComparer implementation to use.

            this.Whitespace = WhitespaceOptions.NoChange;   // Make no changes to whitespace.
            this.WhitespaceChars = new byte[] { 0x20, 0x09, 0x0b };     // Whitespace characters.
            this.ConvertToDollarHex = false;                // Do not convert to $HEX[...] format by default.
        }

        public static string GetUsageText()
        {
            return Conf.FirstUsageLineText + @"
    Required Inputs / Outputs
-i --input    One or more files or folders to sort
-o --output   A file to write the output to
  
 
    Options
-t --temp-folder   Folder to use for writing temporary files
                   Default: %TEMP%\MassiveSort\<PID>
--leave-duplicates Leave duplicates in the output file
                   Default: remove duplicates
--save-duplicates  Save duplicates to a separate .duplicates file.
                   Default: do not save duplicates
--convert-to-dollar-hex
                   Converts non-ascii bytes to $HEX[] format
                   Default: make no changes to non-ascii bytes
--save-stats       Saves more detailed stats to .stats file.
--whitespace       Changes made to whitespace when processing
                   WARNING: this can make destructive changes to your inputs
                   - NoChange: no changes to whitespace (default)
                   - Trim: removes leading and trailing whitespace
                   - Strip: removes all whitespace
--whitespace-chars Byte(s) considered whitespace
                   Default: 0x09, 0x0b, 0x20


    Sorting
--max-sort-size    Largest chunk of files to sort as a group
                   Default: 64MB, major contributor to memory usage
--sort-algorithm   Sort agorithm to use, options:
                   - DefaultArray: Array.Sort()
                   - LinqOrderBy: Enumerable.OrderBy()
                   - TimSort: Timsort algorithm (default)

    Workers / Threads
--split-workers    Number of worker threads when splitting files
                   Default: number of physical cores in your PC
--sort-workers     Number of worker threads when sorting files
                   Default: number of physical cores in your PC
--io-workers       Number of worker threads for IO intensive operations
                   Default: 8 workers

    Large Files
--large-threshold  Files greater than this are considered large.
                   Default: 1GB
--large-chunk      Large files are processed in chunks this big.
                   Default: 256MB

    Buffers / Memory
--line-buffer-size Buffer size for reading lines
                   Default: 64KB
--read-file-buffer-size 
                   Buffer size for input file
                   Default: 64KB
--temp-file-buffer-size 
                   Buffer size for writing temp files
                   Default: 128KB
--output-file-buffer-size 
                   Buffer size for writing output file
                   Default: 256KB
--max-outstanding-sorted-chunks
                   Number of chunks to buffer in memory when writing
                   Default: 2, major contributor to memory usage
--aggressive-memory-collection 
                   Does a full garbage collection after each file processed
";
        }

        [OptionArray('i', "input")]
        public string[] Inputs { get; set; }


        [Option('o', "output")]
        public string OutputFile { get; set; }

        [Option('t', "temp-folder")]
        public string TempFolder { get; set; }

        [Option("max-sort-size")]
        public string MaxSortSize_Raw { get; set; }
        public int MaxSortSize { get; set; }
        [Option("line-buffer-size")]
        public string LineBufferSize_Raw { get; set; }
        public int LineBufferSize { get; set; }
        [Option("read-file-buffer-size")]
        public string ReadBufferSize_Raw { get; set; }
        public int ReadBufferSize { get; set; }
        [Option("temp-file-buffer-size")]
        public string TempFileBufferSize_Raw { get; set; }
        public int TempFileBufferSize { get; set; }
        [Option("output-file-buffer-size")]
        public string OutputBufferSize_Raw { get; set; }
        public int OutputBufferSize { get; set; }

        /// <summary>
        /// If true, duplicates are not removed. Defaults to false.
        /// </summary>
        [Option("leave-duplicates")]
        public bool LeaveDuplicates { get; set; }

        /// <summary>
        /// If true, does a full garbage collection after each split and sort. Defaults to false.
        /// </summary>
        [Option("aggressive-memory-collection")]
        public bool AggressiveMemoryCollection { get; set; }

        /// <summary>
        /// If true, writes stats to a parallel files to the OutputFile. Defaults to false.
        /// </summary>
        [Option("save-stats")]
        public bool SaveStats { get; set; }


        [Option("split-workers")]
        public int SplitWorkers { get; set; }
        [Option("sort-workers")]
        public int SortWorkers { get; set; }
        [Option("io-workers")]
        public int IOWorkers { get; set; }

        /// <summary>
        /// Files larger than this (bytes) will be processed in smaller chunks during the split phase.
        /// To allow large files to gain benefits of parallelism too.
        /// Default: 1GB
        /// </summary>
        public long LargeFileThresholdSize { get; set; }
        [Option("large-threshold")]
        public string LargeFileThresholdSize_Raw { get; set; }
        /// <summary>
        /// Large files are processed in chunks this big during the split phase.
        /// To allow large files to gain benefits of parallelism too.
        /// Default: 256MB
        /// </summary>
        public long LargeFileChunkSize { get; set; }
        [Option("large-chunk")]
        public string LargeFileChunkSize_Raw { get; set; }

        /// <summary>
        /// The maximum number of chunks which can be sorted, but not yet written to disk.
        /// Max is determined by available virtual memory. Default is 10 (which requires ~2GB page file with default settings).
        /// </summary>
        [Option("max-outstanding-sorted-chunks")]
        public int MaxOutstandingSortedChunks { get; set; }

        [Option("sort-algorithm")]
        public SortAlgorithms SortAlgorithm { get; set; }
        public enum SortAlgorithms
        {
            /// <summary>
            /// Automatically choose an algorithm based on data.
            /// </summary>
            Auto,

            /// <summary>
            /// Sorts using standard .NET Array.Sort() method.
            /// This is a quick sort in versions earlier than 4.5, and a hybrid sort (quick, heap, insertion) in 4.5 and newer.
            /// </summary>
            DefaultArray,

            /// <summary>
            /// Sorts using the Linq To Objects Enumerable.OrderBy() method.
            /// </summary>
            LinqOrderBy,

            /// <summary>
            /// Sorts using Tim Sort - https://en.wikipedia.org/wiki/Timsort
            /// </summary>
            TimSort,
        }

        [Option("comparer")]
        public Comparers Comparer { get; set; }
        public enum Comparers
        {
            /// <summary>
            /// A comparer in pure c#.
            /// </summary>
            Clr,

            /// <summary>
            /// A more optimised comparer which uses native P/Invoke to memcmp()
            /// </summary>
            Native,

        }

        [Option("whitespace")]
        public WhitespaceOptions Whitespace { get; set; }
        public enum WhitespaceOptions
        {
            /// <summary>
            /// No changes are made to whitespace characters. This is the default.
            /// </summary>
            NoChange,
            /// <summary>
            /// Leading and trailing whitespace characters are removed.
            /// </summary>
            Trim,
            /// <summary>
            /// All whitespace characters are removed.
            /// </summary>
            Strip,
        }

        /// <summary>
        /// Bytes which are considered whitespace.
        /// Default: 0x09, 0x0b, 0x20
        /// https://en.wikipedia.org/wiki/Whitespace_character
        /// </summary>
        [Option("whitespace-chars")]
        public byte[] WhitespaceChars { get; set; }

        /// <summary>
        /// If true, will convert all lines outside printable ASCII range to the $HEX[...] format. False by default.
        /// https://hashcat.net/trac/ticket/148
        /// </summary>
        [Option("convert-to-dollar-hex")]
        public bool ConvertToDollarHex { get; set; }

        // If true, will save duplicates to a parallel file (.duplicates). False by default.
        [Option("save-duplicates")]
        public bool SaveDuplicates { get; set; }


        public MergeConf ExtraParsing()
        {
            long size;
            if (!String.IsNullOrEmpty(this.MaxSortSize_Raw) && Helpers.TryParseByteSized(this.MaxSortSize_Raw, out size))
                MaxSortSize = (int)size;
            if (!String.IsNullOrEmpty(this.LineBufferSize_Raw) && Helpers.TryParseByteSized(this.LineBufferSize_Raw, out size))
                LineBufferSize = (int)size;
            if (!String.IsNullOrEmpty(this.ReadBufferSize_Raw) && Helpers.TryParseByteSized(this.ReadBufferSize_Raw, out size))
                ReadBufferSize = (int)size;
            if (!String.IsNullOrEmpty(this.TempFileBufferSize_Raw) && Helpers.TryParseByteSized(this.TempFileBufferSize_Raw, out size))
                TempFileBufferSize = (int)size;
            if (!String.IsNullOrEmpty(this.OutputBufferSize_Raw) && Helpers.TryParseByteSized(this.OutputBufferSize_Raw, out size))
                OutputBufferSize = (int)size;

            if (!String.IsNullOrEmpty(this.LargeFileThresholdSize_Raw) && Helpers.TryParseByteSized(this.LargeFileThresholdSize_Raw, out size))
                LargeFileThresholdSize = (int)size;
            if (!String.IsNullOrEmpty(this.LargeFileChunkSize_Raw) && Helpers.TryParseByteSized(this.LargeFileChunkSize_Raw, out size))
                LargeFileChunkSize = (int)size;

            return this;
        }
        public string GetValidationMessage()
        {
            var result = new StringBuilder();
            // Basic input / outputs.
            if (Inputs == null || !Inputs.Any())
                result.AppendLine("'input' argument is required.");
            if (String.IsNullOrEmpty(OutputFile))
                result.AppendLine("'output' argument is required.");

            long size;
            if (!String.IsNullOrEmpty(this.MaxSortSize_Raw) && !Helpers.TryParseByteSized(this.MaxSortSize_Raw, out size))
                result.Append("'max-sort-size' cannot be parsed.");
            if (!String.IsNullOrEmpty(this.LineBufferSize_Raw) && !Helpers.TryParseByteSized(this.LineBufferSize_Raw, out size))
                result.Append("'line-buffer-size' cannot be parsed.");
            if (!String.IsNullOrEmpty(this.ReadBufferSize_Raw) && !Helpers.TryParseByteSized(this.ReadBufferSize_Raw, out size))
                result.Append("'read-file-buffer-size' cannot be parsed.");
            if (!String.IsNullOrEmpty(this.TempFileBufferSize_Raw) && !Helpers.TryParseByteSized(this.TempFileBufferSize_Raw, out size))
                result.Append("'temp-file-buffer-size' cannot be parsed.");
            if (!String.IsNullOrEmpty(this.OutputBufferSize_Raw) && !Helpers.TryParseByteSized(this.OutputBufferSize_Raw, out size))
                result.Append("'output-file-buffer-size' cannot be parsed.");

            if (!String.IsNullOrEmpty(this.LargeFileThresholdSize_Raw) && !Helpers.TryParseByteSized(this.LargeFileThresholdSize_Raw, out size))
                result.Append("'large-threshold' cannot be parsed.");
            if (!String.IsNullOrEmpty(this.LargeFileChunkSize_Raw) && !Helpers.TryParseByteSized(this.LargeFileChunkSize_Raw, out size))
                result.Append("'large-chunk' cannot be parsed.");

            // Other sanity checks.
            if (MaxSortSize < 1024 * 256)
                result.AppendLine("'max-sort-size' must be at least 256KB.");
            if (MaxSortSize > 1024 * 1024 * 1024)
                result.AppendLine("'max-sort-size' must be less than 1GB.");

            if (LineBufferSize < 1024)
                result.AppendLine("'line-buffer-size' must be at least 1KB.");
            if (LineBufferSize > 1024 * 1024 * 16)
                result.AppendLine("'line-buffer-size' must be less than 16MB.");

            if (ReadBufferSize < 1024)
                result.AppendLine("'read-file-buffer-size' must be at least 1KB.");
            if (ReadBufferSize > 1024 * 1024 * 128)
                result.AppendLine("'read-file-buffer-size' must be less than 128MB.");

            if (TempFileBufferSize < 1024)
                result.AppendLine("'temp-file-buffer-size' must be at least 1KB.");
            if (TempFileBufferSize > 1024 * 1024 * 8)
                result.AppendLine("'temp-file-buffer-size' must less than 8MB.");

            if (OutputBufferSize < 1024)
                result.AppendLine("'output-file-buffer-size' must be at least 1KB.");
            if (OutputBufferSize > 1024 * 1024 * 8)
                result.AppendLine("'output-file-buffer-size' must be less than 8MB.");

            if (LargeFileThresholdSize < 1024 * 1024)
                result.AppendLine("'large-threshold' must be at least 1MB.");
            if (LargeFileChunkSize < 1024 * 1024)
                result.AppendLine("'large-chunk' must be at least 1MB.");
            if (LargeFileChunkSize > LargeFileThresholdSize)
                result.AppendLine("'large-chunk' cannot be larger than 'large-threshold'.");

            return result.ToString();
        }
        public bool IsValid { get { return String.IsNullOrEmpty(GetValidationMessage()); } }
    }
    #endregion

    public class MergeMany : ICmdVerb, IDisposable
    {
        private readonly double oneMbAsDouble = Helpers.OneMbAsDouble;
        private static string emptyShardFilename = "!";

        private readonly MergeConf _Conf;
        private ParallelOptions _ParallelOptsForConfiguredDegreeOfIOParallelism;
        private CancellationToken _CancelToken;

        private readonly IProgress<BasicProgress> _Progress = new ConsoleProgressReporter();

        private StreamWriter _StatsFile;

        public MergeMany(MergeConf conf)
        {
            _Conf = conf;
        }

        public string GetUsageMessage()
        {
            return MergeConf.GetUsageText();
        }

        public void Dispose()
        {
            // Try to clean up our temp folder at the end.
            CleanTempFolder();

            if (this._StatsFile != null)
            {
                this._StatsFile.Flush();
                this._StatsFile.Dispose();
                this._StatsFile = null;
            }
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

            // Configure TPL.
            var ioOpts = new ParallelOptions();
            ioOpts.MaxDegreeOfParallelism = _Conf.IOWorkers;
            this._ParallelOptsForConfiguredDegreeOfIOParallelism = ioOpts;

            InitTempFolder();   // A bit of house cleaning.
            if (token.IsCancellationRequested) return;

            // Initialise the stats file.
            if (_Conf.SaveStats)
            {
                this._StatsFile = new StreamWriter(_Conf.OutputFile + ".stats", false, Encoding.UTF8);
            }

            // Calculate approximate memory usage.
            PrintEstimatedMemoryUsage();
            if (token.IsCancellationRequested) return;

            // Snapshot the files we'll be working with.
            var filesToProcess = this.GatherFiles();
            if (token.IsCancellationRequested) return;


            // Stage 1: split / shard files into smaller chunks.
            var toSort = SplitFiles(filesToProcess);
            if (token.IsCancellationRequested) return;


            // Stage 2: sort and merge the files.
            SortFiles(toSort);

            // Be proactive about telling people we're wasting their disk space.
            WarnIfOldTempFilesExist(new DirectoryInfo(Helpers.GetBaseTempFolder()), new DirectoryInfo(_Conf.TempFolder));
        }


        #region Split
        private IEnumerable<FileInfo> GatherFiles()
        {
            if (_Conf.Debug)
                _Progress.Report(new BasicProgress(String.Format("Gathering files to merge from '{0}'.", String.Join("; ", _Conf.Inputs)), true));

            var sw = Stopwatch.StartNew();
            var result = _Conf.Inputs.SelectMany(i =>
                    Directory.Exists(i) ? new DirectoryInfo(i).EnumerateFiles("*", SearchOption.AllDirectories)
                                        : new FileInfo[] { new FileInfo(i) }
                )
                .ToList();
            sw.Stop();

            this.WriteStats("Found {0:N0} files to merge, totaling {1:N1}MB. Time to search: {2:N1}ms.", result.Count, result.Sum(x => x.Length) / oneMbAsDouble, sw.Elapsed.TotalMilliseconds);
            return result;
        }
        private IEnumerable<FileResult> SplitFiles(IEnumerable<FileInfo> files)
        {
            // Stage 1: read all files and split lines into buckets.

            _Progress.Report(new BasicProgress(String.Format("Splitting {0:N0} file(s) (round 1)...", files.Count()), true));
            this.WriteStats("Splitting {0:N0} file(s) (round 1)...", files.Count());
            var sw = Stopwatch.StartNew();
            var shardedFileDetails = this.DoTopLevelSplit(files);
            if (_CancelToken.IsCancellationRequested) return Enumerable.Empty<FileResult>();

            // Test to see if we need further levels of sharding to make files small enough to sort.
            int shardSize = 2;
            var filesLargerThanSortSize =
                new DirectoryInfo(_Conf.TempFolder)
                        .EnumerateFiles("*", SearchOption.AllDirectories)
                        .Where(f => f.Length > _Conf.MaxSortSize)
                        .ToList().AsEnumerable();
            while (filesLargerThanSortSize.Any())
            {
                _Progress.Report(new BasicProgress(String.Format("Splitting {0:N0} file(s) (round {1})...", filesLargerThanSortSize.Count(), shardSize), true));
                this.WriteStats("Splitting {0:N0} file(s) (round {1})...", filesLargerThanSortSize.Count(), shardSize);

                this.DoSubLevelSplit(filesLargerThanSortSize, shardSize, shardedFileDetails);
                if (_CancelToken.IsCancellationRequested) return Enumerable.Empty<FileResult>();

                shardSize++;
                filesLargerThanSortSize =
                    new DirectoryInfo(_Conf.TempFolder)
                        .EnumerateFiles("*", SearchOption.AllDirectories)
                        .Where(f => f.Length > _Conf.MaxSortSize)
                        .ToList().AsEnumerable();
            }
            sw.Stop();

            // Display summary information.
            var totalTimeSeconds = sw.Elapsed.TotalSeconds;
            var totalMB = files.Sum(x => x.Length) / oneMbAsDouble;
            var totalLines = shardedFileDetails.Values.Sum(x => x.Lines);
            _Progress.Report(new BasicProgress(String.Format("Finished splitting files in {0}.\n", sw.Elapsed.ToSizedString()), true));
            this.WriteStats("Finished splitting {0:N0} file(s) with {1:N0} lines ({2:N2} MB) in {3:N1} sec, {4:N0} lines / sec, {5:N1} MB / sec.", files.Count(), totalLines, totalMB, totalTimeSeconds, totalLines / totalTimeSeconds, totalMB / totalTimeSeconds);
            if (_CancelToken.IsCancellationRequested) return Enumerable.Empty<FileResult>();

            var toSort = shardedFileDetails.Where(x => File.Exists(x.Value.FullPath)).OrderBy(x => x.Key).Select(x => x.Value).ToList();
            return toSort;
        }
        private IDictionary<string, FileResult> DoTopLevelSplit(IEnumerable<FileInfo> files)
        {
            var chunks = this.ConvertFilesToSplitChunks(files);
            var shardFiles = CreateShardFiles("");
            var lineCounts = new long[shardFiles.Length];
            var result = new Dictionary<string, FileResult>(shardFiles.Length);
            try
            {
                // Each file is split in parallel, with the assumption that we synchronise on the resulting file streams in SplitFile().
                // Using a partitioner with a single element partition to keep the order of files.
                Partitioner.Create(0, chunks.Count, 1)
                    .AsParallel()
                    .WithDegreeOfParallelism(_Conf.SplitWorkers)
                    .WithMergeOptions(ParallelMergeOptions.NotBuffered)
                    .ForAll(chIdx =>
                    {
                        if (_CancelToken.IsCancellationRequested) return;
                        var ch = chunks[chIdx.Item1];       // Careful to only read the collection!
                        var taskKey = new object();
                        SplitFile(shardFiles, lineCounts, ch, 1, taskKey, true);
                    });
                for (int i = 0; i < shardFiles.Length; i++)
                    result.Add(shardFiles[i].Name, new FileResult(shardFiles[i].FullPath, shardFiles[i].Stream.Length, lineCounts[i]));
            }
            finally
            {
                var taskKey = new object();
                this.FlushFiles(shardFiles, null, 0L, result, taskKey);
            }

            if (_Conf.AggressiveMemoryCollection)
                GC.Collect();

            return result;
        }
        private void DoSubLevelSplit(IEnumerable<FileInfo> files, int shardSize, IDictionary<string, FileResult> result)
        {
            // The logic for sub level splits is slightly different.
            // We split each file individually and replace it at the end.
            // PERF: each file can be split in parallel, no synchronisation is required - except when we are splitting chunks from the same file.

            var filesSortedByLength = files.OrderByDescending(x => x.Length).ToList();              // Sort from largest to smallest to work on big stuff first and keep CPUs as busy as possible.
            Partitioner.Create(0, filesSortedByLength.Count, 1)
                .AsParallel()
                .WithDegreeOfParallelism(_Conf.SplitWorkers)
                .WithMergeOptions(ParallelMergeOptions.NotBuffered)
                .ForAll(fIdx =>
                {
                    if (_CancelToken.IsCancellationRequested) return;
                    var f = filesSortedByLength[fIdx.Item1];       // Careful to only read the collection!
                    var shardFiles = CreateShardFiles(Path.GetFileNameWithoutExtension(f.Name));
                    var lineCounts = new long[shardFiles.Length];
                    var taskKey = new object();

                    // Here's the actual split.
                    try
                    {
                        var ch = new SplitChunk(f, 0, 0, f.Length);
                        SplitFile(shardFiles, lineCounts, ch, shardSize, taskKey, false);
                        for (int i = 0; i < shardFiles.Length; i++)
                            result.Add(shardFiles[i].Name, new FileResult(shardFiles[i].FullPath, shardFiles[i].Stream.Length, lineCounts[i]));
                    }
                    finally
                    {
                        // Flush files.
                        this.FlushFiles(shardFiles, f.FullName, lineCounts.Last(), result, taskKey);
                    }

                    if (_Conf.AggressiveMemoryCollection)
                        GC.Collect();
                });
        }

        private IList<SplitChunk> ConvertFilesToSplitChunks(IEnumerable<FileInfo> files)
        {
            // Sort be size, descending, to process larger chunks first.
            // To try to keep more cores busy for longer and not end up with a single large chunk dominating split time.
            var largestToSmallestFiles = files.OrderByDescending(x => x.Length);

            var result = new List<SplitChunk>(files.Count());
            foreach (var f in largestToSmallestFiles)
            {
                if (f.Length < _Conf.LargeFileThresholdSize)
                    // Trivial case: the whole file is a chunk.
                    result.Add(new SplitChunk(f, 0, 0L, f.Length));
                else
                {
                    // Large file: need to split into chunks on line boundaries.
                    using (var fs = new FileStream(f.FullName, FileMode.Open, FileAccess.Read, FileShare.None))
                    {
                        int chunkNum = 1;
                        do
                        {
                            long startOffset = fs.Position;
                            if (fs.Position + _Conf.LargeFileChunkSize >= fs.Length)
                            {
                                // Last chunk.
                                result.Add(new SplitChunk(f, chunkNum, startOffset, fs.Length));
                                break;
                            }
                            fs.Seek(_Conf.LargeFileChunkSize, SeekOrigin.Current);

                            // Find a newline character to end the chunk on.
                            var b = (byte)fs.ReadByte();
                            while (!(b == Constants.NewLineAsByte || b == Constants.NewLineAsByteAlt) && fs.Position != 1)
                            {
                                fs.Seek(-2, SeekOrigin.Current);
                                b = (byte)fs.ReadByte();
                            }
                            long endOffset = fs.Position;

                            // Record the chunk.
                            result.Add(new SplitChunk(f, chunkNum, startOffset, endOffset));

                            // Find a non-newline to start the next chunk on.
                            while ((b == Constants.NewLineAsByte || b == Constants.NewLineAsByteAlt) && fs.Position != fs.Length)
                            {
                                b = (byte)fs.ReadByte();
                            }
                            fs.Seek(-1, SeekOrigin.Current);
                            chunkNum++;
                        } while (true);
                    }
                }
            }

            return result;
        }

        private void FlushFiles(ShardFile[] shardFiles, string moveLastShardToPath, long lastShardLineCount, IDictionary<string, FileResult> result, object taskKey)
        {
            // Close and flush the shard files created.
            _Progress.Report(new TaskProgress("Flushing data to temp files...", false, taskKey));
            var flushSw = Stopwatch.StartNew();

            var emptyShardPath = shardFiles.Last().Name;
            var toDelete = shardFiles.Where(x => x.Stream.Length == 0L).Select(x => x.FullPath).ToList();
            Parallel.ForEach(shardFiles, _ParallelOptsForConfiguredDegreeOfIOParallelism, fs =>
            {
                fs.Stream.Flush();
                fs.Dispose();
            });
            for (int i = 0; i < shardFiles.Length; i++)
                shardFiles[i] = null;

            // Delete any zero length files.
            Parallel.ForEach(toDelete, _ParallelOptsForConfiguredDegreeOfIOParallelism, f => { File.Delete(f); });

            // Replace the file we were just processing with the 'empty' shard.
            // This only happens on sub level splits.
            if (!String.IsNullOrEmpty(moveLastShardToPath))
            {
                File.Delete(moveLastShardToPath);
                result.Remove(moveLastShardToPath);
                // Everything may have been moved from the shard (common for $HEX[..]).
                if (File.Exists(emptyShardPath))
                {
                    File.Move(emptyShardPath, moveLastShardToPath);
                    result.Add(moveLastShardToPath, new FileResult(moveLastShardToPath, new FileInfo(moveLastShardToPath).Length, lastShardLineCount));
                }
            }

            flushSw.Stop();
            _Progress.Report(new TaskProgress(" Done", true, taskKey));
            this.WriteStats("Flushed data to temp files in {0:N0}ms.", flushSw.Elapsed.TotalMilliseconds);
        }

        private ShardFile[] CreateShardFiles(string initialShard)
        {
            var sw = Stopwatch.StartNew();
            var tempFolder = Path.GetFullPath(_Conf.TempFolder);
            var tempFileBufferSize = _Conf.TempFileBufferSize;
            ShardFile[] result = null;

            try
            {
                result = ParallelEnumerable.Range(0, 256 + 1)
                    .AsOrdered().WithDegreeOfParallelism(_Conf.IOWorkers)
                    .Select(i =>
                    {
                        var shardPath = "";
                        if (i < 256)
                            // The normal files.
                            shardPath = Path.Combine(tempFolder, initialShard + i.ToString("x2")) + ".txt";
                        else
                            // A file for empty string / no shard.
                            shardPath = Path.Combine(tempFolder, initialShard + emptyShardFilename) + ".txt";
                        return new ShardFile(shardPath, tempFileBufferSize);
                    })
                    .ToArray();
            }
            catch (Exception)
            {
                // Any failure and we close any files created so far and blow up.
                if (result != null)
                {
                    foreach (var f in result.Where(f => f != null))
                        f.Dispose();
                }
                throw;
            }
            sw.Stop();
            this.WriteStats("Created {0:N0} shard files for base '{1}' in {2:N1}ms.", result.Length, initialShard, sw.Elapsed.TotalMilliseconds);

            return result;
        }

        private class SplitChunk
        {
            public SplitChunk(FileInfo fi, int chunkNumber, long startOffset, long endOffset)
            {
                this.FullPath = fi.FullName;
                this.Name = fi.Name;
                this.ChunkNumberInFile = chunkNumber;
                this.StartOffset = startOffset;
                this.EndOffset = endOffset;
            }
            public readonly string FullPath;
            public readonly string Name;
            public readonly int ChunkNumberInFile;
            public string NameForProgress { get { return this.Name + (this.ChunkNumberInFile > 0 ? " #" + this.ChunkNumberInFile.ToString() : ""); } }
            public readonly long StartOffset;
            public readonly long EndOffset;
            public long Length { get { return this.EndOffset - this.StartOffset; } }
        }
        private long SplitFile(ShardFile[] shardFiles, long[] lineCounts, SplitChunk ch, int shardSize, object taskKey, bool lockStreams)
        {
            long linesTrimmed = 0;
            long linesStripped = 0;
            long linesConvertedToDollarHex = 0;
            // For additional processing which requires a copy of data.
            // The allocation size allow us to convert a full line buffer to $HEX[...] format.
            var extraBuffer = new byte[_Conf.LineBufferSize * 2 + Constants.DollarHexPrefix.Length + Constants.DollarHexSuffix.Length];
            bool trimWhitespace = (_Conf.Whitespace == MergeConf.WhitespaceOptions.Trim);
            bool stripWhitespace = (_Conf.Whitespace == MergeConf.WhitespaceOptions.Strip);
            bool convertToDollarHex = _Conf.ConvertToDollarHex;

            _Progress.Report(new TaskProgress(String.Format("Splitting '{0}'...", ch.NameForProgress), false, taskKey));
            var sw = Stopwatch.StartNew();

            var reader = new PlainRaw(_CancelToken, _Conf.LineBufferSize, _Conf.ReadBufferSize);
            foreach (var line in reader.ReadAll(ch.FullPath, ch.StartOffset, ch.EndOffset))
            {
                // Additional processing happens here.

                // The order of these means only one will ever be triggered.
                // The code, as it stands, will not cope with two copies of the line (the copies will overwrite each other on the 2nd call).
                var toWrite = line;

                // Convert to $HEX before trimming, as any removal of whitespace will break unicode (UTF-16) encoded strings.
                if (convertToDollarHex)
                {
                    var maybeChanged = this.ConvertToDollarHex(toWrite, extraBuffer);
                    if (toWrite != maybeChanged) linesConvertedToDollarHex++;
                    toWrite = maybeChanged;
                }
                // Trimming whitespace does not require a change to the buffer or any copying.
                if (trimWhitespace)
                {
                    var maybeChanged = this.TrimWhitespace(toWrite, _Conf.WhitespaceChars);
                    if (toWrite != maybeChanged) linesTrimmed++;
                    toWrite = maybeChanged;
                }
                // Stripping all whitespace may require a copy to the alternate buffer.
                if (stripWhitespace)
                {
                    var maybeChanged = this.StripWhitespace(toWrite, extraBuffer, _Conf.WhitespaceChars);
                    if (toWrite != maybeChanged) linesStripped++;
                    toWrite = maybeChanged;
                }


                // Write the word to the shard file.
                // PERF: about 45% of CPU time is spent in FileStream.Write(), contained in ShardWordToFile().
                if (lockStreams)
                    ShardWordToFileWithStreamLock(toWrite, shardSize, shardFiles, lineCounts);
                else
                    ShardWordToFileWithoutLock(toWrite, shardSize, shardFiles, lineCounts);
            }
            sw.Stop();
            _Progress.Report(new TaskProgress(" Done.", true, taskKey));

            this.WriteStats("File '{0}': {1:N0} lines processed in {2:N1}ms, {3:N1} lines / sec, {4:N1} MB / sec.", ch.NameForProgress, reader.LinesRead, sw.Elapsed.TotalMilliseconds, reader.LinesRead / sw.Elapsed.TotalSeconds, (ch.Length / oneMbAsDouble) / sw.Elapsed.TotalSeconds);
            this.WriteStats("File '{0}': {1:N0} line buffers read, {2:N0} line buffers skipped because lines were too long, {3:N0} additional seeks due to buffer alignment.", ch.NameForProgress, reader.BuffersRead, reader.BuffersSkipped, reader.ExtraSeeks);
            if (trimWhitespace)
                this.WriteStats("File '{0}': {1:N0} lines had whitespace trimmed.", ch.NameForProgress, linesTrimmed);
            if (stripWhitespace)
                this.WriteStats("File '{0}': {1:N0} lines had whitespace stripped.", ch.NameForProgress, linesStripped);
            if (convertToDollarHex)
                this.WriteStats("File '{0}': {1:N0} lines were converted to $HEX[].", ch.NameForProgress, linesConvertedToDollarHex);

            return reader.LinesRead;
        }


        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private void ShardWordToFileWithoutLock(ByteArraySegment seg, int shardSize, ShardFile[] shardFiles, long[] lineCounts)
        {
            // Determine the first character(s) to shard into separate files.
            int shard;
            if (shardSize > seg.Count)
                shard = shardFiles.Length - 1;     // Empty string / no shard.
            else
                shard = seg.Array[seg.Offset + (shardSize - 1)];

#if DEBUG
            if (System.Diagnostics.Debugger.IsAttached)
            {
                var wordAsBytes = seg.Array.Skip(seg.Offset).Take(seg.Count).ToArray();
                var wordAsNativeString = Encoding.Default.GetString(wordAsBytes);
                var wordAsUtf8String = Encoding.UTF8.GetString(wordAsBytes);
            }
#endif
            // Write the line to the file.
            var stream = shardFiles[shard].Stream;
            if (seg.Count > 0)
                stream.Write(seg.Array, seg.Offset, seg.Count);
            stream.WriteByte(Constants.NewLineAsByte);
            lineCounts[shard] = lineCounts[shard] + 1;
        }

        private void ShardWordToFileWithStreamLock(ByteArraySegment seg, int shardSize, ShardFile[] shardFiles, long[] lineCounts)
        {
            // The lock makes it highly unlikely to inline this function call, hence why the code is mostly duplicated.

            // Determine the first character(s) to shard into separate files.
            int shard;
            if (shardSize > seg.Count)
                shard = shardFiles.Length - 1;     // Empty string / no shard.
            else
                shard = seg.Array[seg.Offset + (shardSize - 1)];

#if DEBUG
            if (System.Diagnostics.Debugger.IsAttached)
            {
                var wordAsBytes = seg.Array.Skip(seg.Offset).Take(seg.Count).ToArray();
                var wordAsNativeString = Encoding.Default.GetString(wordAsBytes);
                var wordAsUtf8String = Encoding.UTF8.GetString(wordAsBytes);
            }
#endif
            // Write the line to the file.
            var stream = shardFiles[shard].Stream;
            lock (stream)
            {
                if (seg.Count > 0)
                    stream.Write(seg.Array, seg.Offset, seg.Count);
                stream.WriteByte(Constants.NewLineAsByte);
                lineCounts[shard] = lineCounts[shard] + 1;
            }
        }


        private ByteArraySegment TrimWhitespace(ByteArraySegment seg, byte[] whitespaceChars)
        {
            // Skip over any whitespace at the start.
            int newOffset = seg.Offset;
            for (int i = seg.Offset; i < seg.Offset + seg.Count; i++)
            {
                bool isWhitespace = false;
                for (int j = 0; j < whitespaceChars.Length; j++)
                    isWhitespace = isWhitespace | (seg.Array[i] == whitespaceChars[j]);
                if (isWhitespace)
                    newOffset = i;
                else
                    break;
            }

            // Skip over any whitespace at the end.
            int newLength = seg.Count - (newOffset - seg.Offset);
            for (int i = seg.Offset + seg.Count - 1; i >= newOffset; i--)
            {
                bool isWhitespace = false;
                for (int j = 0; j < whitespaceChars.Length; j++)
                    isWhitespace = isWhitespace | (seg.Array[i] == whitespaceChars[j]);
                if (isWhitespace)
                    newLength = (i - newOffset);
                else
                    break;
            }

            return new ByteArraySegment(seg.Array, newOffset, newLength);
        }
        private ByteArraySegment StripWhitespace(ByteArraySegment seg, byte[] otherBuf, byte[] whitespaceChars)
        {
            // Search for whitespace.
            bool hasWhitespace = false;
            for (int i = seg.Offset; i < seg.Offset + seg.Count; i++)
            {
                for (int j = 0; j < whitespaceChars.Length; j++)
                    hasWhitespace = hasWhitespace | (seg.Array[i] == whitespaceChars[j]);
                if (hasWhitespace)
                    break;
            }
            if (!hasWhitespace)
                // No whitespace found: return the original segment untouched.
                return seg;

            // Make a copy into the other buffer, but skip any whitespace.
            int oBufIdx = 0;
            for (int i = seg.Offset; i < seg.Offset + seg.Count; i++)
            {
                bool isWhitespace = false;
                for (int j = 0; j < whitespaceChars.Length; j++)
                    isWhitespace = isWhitespace | (seg.Array[i] == whitespaceChars[j]);
                if (!isWhitespace)
                {
                    otherBuf[oBufIdx] = seg.Array[i];
                    oBufIdx++;
                }
            }
            return new ByteArraySegment(otherBuf, 0, oBufIdx);
        }
        private ByteArraySegment ConvertToDollarHex(ByteArraySegment seg, byte[] otherBuf)
        {
            // The best definition of the $HEX[] convention is in Waffle's hashcat proposal: https://hashcat.net/trac/ticket/148

            // Check for the presence of "special" bytes.
            // That is, control bytes in the range 0x00 - 0x1f and 0x7f - 0xff.
            bool convert = false;
            for (int i = seg.Offset; i < seg.Offset + seg.Count; i++)
            {
                if (seg.Array[i] < 0x20 || seg.Array[i] > 0x7e)
                {
                    convert = true;
                    break;
                }
            }
            if (!convert)
                // No special bytes found: return the original segment untouched.
                return seg;

            // Make a copy into the other buffer, converting to hex.
            int oBufIdx = 0;
            // $HEX[ prefix.
            for (int i = 0; i < Constants.DollarHexPrefix.Length; oBufIdx++, i++)
                otherBuf[oBufIdx] = Constants.DollarHexPrefix[i];
            // Actual data.
            for (int i = seg.Offset; i < seg.Offset + seg.Count; oBufIdx += 2, i++)
                Helpers.WriteHexToArray(otherBuf, oBufIdx, seg.Array[i]);
            // ] suffix.
            for (int i = 0; i < Constants.DollarHexSuffix.Length; oBufIdx++, i++)
                otherBuf[oBufIdx] = Constants.DollarHexSuffix[i];

            return new ByteArraySegment(otherBuf, 0, oBufIdx);
        }
        #endregion


        #region Sort
        private void SortFiles(IEnumerable<FileResult> toSort)
        {
            if (File.Exists(_Conf.OutputFile))
                File.Delete(_Conf.OutputFile);

            _Progress.Report(new BasicProgress("Sorting files.", true));

            // Split into large chunks to sort.
            var sortChunks = this.SplitIntoChunksForBulkSorting(toSort);
            _Progress.Report(new BasicProgress(String.Format("There are {0:N0} chunk(s) to sort.", sortChunks.Count()), true));

            long totalLinesWritten = 0;
            long totalLinesRead = 0;
            var duplicatePath = _Conf.OutputFile + ".duplicates";

            var allSw = Stopwatch.StartNew();
            var flushSw = new Stopwatch();
            TimeSpan schedulerOverheadTime;
            using (var output = new FileStream(_Conf.OutputFile, FileMode.Create, FileAccess.Write, FileShare.None, _Conf.OutputBufferSize))
            using (var duplicateOutput = _Conf.SaveDuplicates ? new FileStream(duplicatePath, FileMode.Create, FileAccess.Write, FileShare.None, _Conf.OutputBufferSize) : null)
            {
                // Now sort each chunk.
                // PERF: can read and sort each chunk in parallel, but must write at the end in the correct sequence.
                schedulerOverheadTime = 
                    this.SortAndWriteChunks(sortChunks, 
                    (ch, chNum) => {
                        if (_CancelToken.IsCancellationRequested) return null;

                        var taskKey = new Object();
                        this.WriteStats("Chunk #{0}: starting parallel sort thread.", chNum);

                        _Progress.Report(new TaskProgress(String.Format("Sorting chunk {0:N0} ({1} - {2})...", chNum, ch.First().Name, ch.Last().Name), false, taskKey));
                        this.WriteStats("Sorting chunk {0:N0} with {3:N0} files ({1} - {2}: {4:N1}MB, {5:N0} lines)...", chNum, ch.First().Name, ch.Last().Name, ch.Count(), ch.Sum(x => x.Length) / oneMbAsDouble, ch.Sum(x => x.Lines));

                        // Read the files for the chunk into a single array for sorting.
                        // PERF: this represents ~5% of the time in this loop.
                        var readSw = Stopwatch.StartNew();
                        var chunkData = this.ReadFilesForSorting(ch);           // This allocates a large byte[].
                        var offsets = this.FindLineBoundariesForSorting(chunkData, ch);     // PERF: this is ~8%. This allocates a large Int64[].
                        var linesRead = offsets.Length;
                        readSw.Stop();
                        if (_CancelToken.IsCancellationRequested) return null;

                        // Actually sort them!
                        // PERF: this represents ~80% of the time in this loop.
                        // PERF: it's not entirely obvious from the trace, but a significant part of that time is in the comparer.
                        var sortSw = Stopwatch.StartNew();
                        var comparer = this.GetOffsetComparer(chunkData);
                        offsets = this.SortLines(chunkData, offsets, comparer);
                        sortSw.Stop();
                        if (_CancelToken.IsCancellationRequested) return null;

                        // Filter the sorted data to exclude duplicates.
                        // PERF: this represents ~10% of the time in this loop.
                        var deDupeSw = Stopwatch.StartNew();
                        var deDupTuple = this.DeDupe(chunkData, offsets, (IEqualityComparer<OffsetAndLength>)comparer);
                        var data = new IndexedFileData(chunkData, deDupTuple.Item1);
                        var duplicates = new IndexedFileData(chunkData, deDupTuple.Item2);
                        deDupeSw.Stop();

                        _Progress.Report(new TaskProgress(" Sorted. ", false, taskKey));
                        this.WriteStats("Chunk #{0}: ending parallel sort thread.", chNum);

                        return new
                        {
                            ch,
                            chNum,
                            linesRead,
                            data,
                            duplicates,
                            taskKey,
                            readTime = readSw.Elapsed,
                            sortTime = sortSw.Elapsed,
                            deDupTime = deDupeSw.Elapsed,
                        };
                    }, 
                    ch => {
                        if (_CancelToken.IsCancellationRequested) return;
                        this.WriteStats("Chunk #{0}: on sequential write thread.", ch.chNum);

                        // Remove duplicates and write to disk.
                        // PERF: this represents ~10% of the time in this loop. It cannot be parallelised.
                        var writeSw = Stopwatch.StartNew();
                        var linesWritten = this.WriteToFile(ch.data, output, ch.duplicates, duplicateOutput);
                        totalLinesWritten += linesWritten;
                        totalLinesRead += ch.linesRead;
                        writeSw.Stop();

                        _Progress.Report(new TaskProgress(" Written.", true, ch.taskKey));
                        var chTime = ch.readTime + ch.sortTime + ch.deDupTime + writeSw.Elapsed;
                        this.WriteStats("Chunk #{0}: processed in {1:N2} sec. Read {2:N0} lines in {3:N1}ms, sorted in {4:N1}ms, {7:N0} duplicates removed in {8:N1}ms, wrote {5:N0} lines in {6:N1}ms.", ch.chNum, chTime.TotalSeconds, ch.linesRead, ch.readTime.TotalMilliseconds, ch.sortTime.TotalMilliseconds, linesWritten, writeSw.Elapsed.TotalMilliseconds, ch.linesRead - linesWritten, ch.deDupTime.TotalMilliseconds);

                        // Release references to the large arrays allocated when reading files.
                        ch.data.Dispose();
                        if (ch.duplicates != null)
                            ch.duplicates.Dispose();
                        if (_Conf.AggressiveMemoryCollection)
                            GC.Collect();
                    }
                ).Result;

                // Everything is written, so flush output file.
                flushSw.Start();
                output.Flush();
                if (duplicateOutput != null)
                    duplicateOutput.Flush();
                flushSw.Stop();
            }
            allSw.Stop();
            this.WriteStats("Finished writing, final flush took {0:N1}ms.", flushSw.Elapsed.TotalMilliseconds);

            var duplicatesRemoved = totalLinesRead - totalLinesWritten;
            var message = String.Format("Finished sorting{0} in {1}\n{2:N0} lines remain.", _Conf.LeaveDuplicates ? "" : " and removing duplicates", allSw.Elapsed.ToSizedString(), totalLinesWritten);
            if (!_Conf.LeaveDuplicates)
                message += String.Format("\n{0:N0} duplicates removed.", duplicatesRemoved);
            _Progress.Report(new BasicProgress(message, true));
            var outputSize = new FileInfo(_Conf.OutputFile).Length;
            if (!_Conf.LeaveDuplicates)
                _Progress.Report(new BasicProgress(String.Format("Processed {0} down to {1}.", toSort.Sum(x => x.Length).ToByteSizedString(), outputSize.ToByteSizedString()), true));
            else
                _Progress.Report(new BasicProgress(String.Format("Processed {0}.", toSort.Sum(x => x.Length).ToByteSizedString()), true));
            _Progress.Report(new BasicProgress("\n", true));
    
            this.WriteStats("Finished sorting in {0}. {1:N0} lines remain, {2:N0} duplicates removed.", allSw.Elapsed.ToSizedString(), totalLinesWritten, duplicatesRemoved);
            this.WriteStats("Sort task scheduling overhead {0:N1}ms. {1:P1} of total sort time.", schedulerOverheadTime.TotalMilliseconds, schedulerOverheadTime.TotalSeconds / allSw.Elapsed.TotalSeconds);
        }

        private async Task<TimeSpan> SortAndWriteChunks<T>(IList<IEnumerable<FileResult>> chunks, Func<IEnumerable<FileResult>, int, T> sorter, Action<T> writer)
        {
            // PLINQ works in most circumstances, but sometimes buffers output (even when asked not to)
            // which means it can deadlock. So we use our own scheduler logic with raw tasks.

            var totalChunks = chunks.Count();
            var sortTasks = new Task<T>[Math.Max(_Conf.SortWorkers, 1)];
            var sortChunkNums = new int[Math.Max(_Conf.SortWorkers, 1)];
            var writeTasks = new Task[Math.Max(_Conf.MaxOutstandingSortedChunks, 1)];
            var writeChunkNums = new int[Math.Max(_Conf.MaxOutstandingSortedChunks, 1)];

            if (_CancelToken.IsCancellationRequested) return TimeSpan.Zero;


            // Scheduler logic:
            // - Ensure the sort tasks are always populated and working.
            // - Ensure exactly one write task is running, which must be issued in order.
            // - After each task completes (and a regular timeout), check the above remains correct.

            var schedulerOverheadSw = Stopwatch.StartNew();
            int sortChunkIdx = 0;
            int writtenChunkNum = 0;
            do
            {
                // Have any of the write tasks completed? 
                // Note that only one of these should be running at any time.
                for (int i = 0; i < writeTasks.Length; i++)
                {
                    if (writeTasks[i] != null && (writeTasks[i].IsCompleted || writeTasks[i].IsFaulted))
                    {
                        // Retire it.
                        writtenChunkNum = writeChunkNums[i];
                        writeTasks[i] = null;
                        writeChunkNums[i] = 0;
                    }
                }

                // Move any completed sort tasks to the write queue.
                var emptyWriteSlots = writeTasks.Count(x => x == null);
                for (int i = 0; i < sortTasks.Length; i++)
                {
                    if (sortTasks[i] != null        // Not empty task.
                        && (sortTasks[i].IsCompleted || sortTasks[i].IsFaulted)     // Completed task (or faulted).
                        && sortChunkNums[i] <= writtenChunkNum + emptyWriteSlots)   // Available to fit in .
                    {
                        int writeIdx = writeTasks.IndexWhere(t => t == null);
                        if (writeIdx != -1)
                        {
                            // Add write task.
                            var ch = sortTasks[i].Result;
                            writeTasks[writeIdx] = new Task(() => writer(ch));      // This is not started just yet.
                            writeChunkNums[writeIdx] = sortChunkNums[i];
                            // Retire the sort task.
                            sortTasks[i] = null;
                            sortChunkNums[i] = 0;
                        }
                    }
                }

                // If no write tasks are running, and the next one is available, start it.
                var nextChunkNumToWrite = writtenChunkNum + 1;
                var nextToWriteIdx = writeChunkNums.IndexWhere(num => num == nextChunkNumToWrite);
                if (writeTasks.All(ch => ch == null || ch.Status == TaskStatus.Created) && nextToWriteIdx != -1)
                {
                    writeTasks[nextToWriteIdx].Start();
                }

                // If there are any remaining chunks to sort, add and start them.
                for (int i = 0; i < sortTasks.Length; i++)
                {
                    if (sortTasks[i] == null && sortChunkIdx < chunks.Count)
                    {
                        var ch = chunks[sortChunkIdx];
                        var chNum = sortChunkIdx + 1;
                        sortChunkNums[i] = chNum;
                        sortTasks[i] = new Task<T>(() => sorter(ch, chNum));
                        sortTasks[i].Start();            // These are started as soon as they are available.
                        sortChunkIdx++;
                    }
                }

                // Processed all chunks: end of loop.
                if (writtenChunkNum >= totalChunks)
                    break;

                // Wait for one of the tasks to complete.
                schedulerOverheadSw.Stop();
                await Task.WhenAny(sortTasks.Concat(writeTasks).Where(t => t != null));
                if (_CancelToken.IsCancellationRequested) return schedulerOverheadSw.Elapsed;
                schedulerOverheadSw.Start();
            } while (true);

            schedulerOverheadSw.Stop();
            return schedulerOverheadSw.Elapsed;
        }

        private IList<IEnumerable<FileResult>> SplitIntoChunksForBulkSorting(IEnumerable<FileResult> toSort)
        {
            var sw = Stopwatch.StartNew();
            var cumulativeSize = 0L;
            var sortChunks = new List<IEnumerable<FileResult>>();
            var chunk = new List<FileResult>();
            foreach (var fi in toSort)
            {
                if (cumulativeSize + fi.Length > _Conf.MaxSortSize)
                {
                    sortChunks.Add(chunk);
                    chunk = new List<FileResult>();
                    cumulativeSize = 0L;
                }
                chunk.Add(fi);
                cumulativeSize += fi.Length;
            }
            sortChunks.Add(chunk);
            sw.Stop();

            this.WriteStats("Created {0:N0} x {1:N1}MB chunk(s) to sort in {2:N1}ms.", sortChunks.Count(), _Conf.MaxSortSize / oneMbAsDouble, sw.Elapsed.TotalMilliseconds);

            return sortChunks;
        }
        private IComparer<byte[]> GetByteArrayComparer()
        {
            switch (_Conf.Comparer)
            {
                case MergeConf.Comparers.Clr:
                    return Comparers.ClrByteArrayComparer.Value;
                case MergeConf.Comparers.Native:
                    return Comparers.PInvokeByteArrayComparer.Value;
                default:
                    throw new Exception("Unknown comparer: " + _Conf.Comparer);
            }
        }
        private IEqualityComparer<byte[]> GetByteArrayEqualityComparer()
        {
            var result = (IEqualityComparer<byte[]>)GetByteArrayComparer();
            return result;
        }

        private IComparer<OffsetAndLength> GetOffsetComparer(byte[] data)
        {
            switch (_Conf.Comparer)
            {
                case MergeConf.Comparers.Clr:
                    return new Comparers.ClrOffsetComparer(data);
                case MergeConf.Comparers.Native:
                    throw new NotImplementedException("Native Offset Comparer is not yet implemented.");
                default:
                    throw new Exception("Unknown comparer: " + _Conf.Comparer);
            }
        }

        private byte[] ReadFilesForSorting(IEnumerable<FileResult> fs)
        {
            // Read each file in one hit.
            // PERF: this could be done in parallel, but is unlikely to help as this is IO dominated and doing a sequential read anyway.
            var sw = Stopwatch.StartNew();
            var data = new byte[(int)fs.Sum(x => x.Length)];
            {
                int offset = 0;
                foreach (var f in fs)
                {
                    using (var stream = new FileStream(f.FullPath, FileMode.Open, FileAccess.Read, FileShare.None))
                    {
                        stream.Read(data, offset, (int)f.Length);
                    }
                    offset += (int)f.Length;
                }
            }
            sw.Stop();
            this.WriteStats("Read {0:N0} file(s) {1:N1}MB in {2:N1}ms.", fs.Count(), fs.Sum(x => x.Length) / oneMbAsDouble, sw.Elapsed.TotalMilliseconds);
            return data;
        }
        private OffsetAndLength[] FindLineBoundariesForSorting(byte[] data, IEnumerable<FileResult> fs)
        {
            // Create an index into the files based on new lines.
            // Because we've processed all incoming files, we know there will be a single new line character after each line.
            // PERF: this could be done in parallel once we align to the start of words.
            var sw = Stopwatch.StartNew();
            var offsets = new OffsetAndLength[fs.Sum(x => x.Lines)];
            int offset = 0;
            int start = 0;
            int end = 0;
            for (int i = 0; i < data.Length; i++)
            {
                if (data[i] == Constants.NewLine)
                {
                    offsets[offset] = new OffsetAndLength(start, end - start);
                    offset++;
                    start = end + 1;
                    end = i + 1;
                }
                else
                {
                    end++;
                }
            }
            sw.Stop();
            this.WriteStats("Found {0:N0} lines in {1:N1}ms.", offsets.Length, sw.Elapsed.TotalMilliseconds);
            return offsets;
        }

        private OffsetAndLength[] SortLines(byte[] chunkData, OffsetAndLength[] offsets, IComparer<OffsetAndLength> comparer)
        {
            var sw = Stopwatch.StartNew();
            if (_Conf.SortAlgorithm == MergeConf.SortAlgorithms.Auto || _Conf.SortAlgorithm == MergeConf.SortAlgorithms.DefaultArray)
                Array.Sort(offsets, comparer);
            else if (_Conf.SortAlgorithm == MergeConf.SortAlgorithms.LinqOrderBy)
                offsets = offsets.OrderBy(x => x, comparer).ToArray();
            else if (_Conf.SortAlgorithm == MergeConf.SortAlgorithms.TimSort)
                offsets.TimSort(comparer.Compare);
            else
                throw new Exception("Unknown sort algorithm: " + _Conf.SortAlgorithm);
            sw.Stop();
            this.WriteStats("Sorted {0:N0} lines ({1:N1}MB) in {2:N1}ms.", offsets.Length, chunkData.Length / oneMbAsDouble, sw.Elapsed.TotalMilliseconds);
            return offsets;
        }

        private Tuple<OffsetAndLength[], OffsetAndLength[]> DeDupe(byte[] chunkData, OffsetAndLength[] offsets, IEqualityComparer<OffsetAndLength> comparer)
        {
            var sw = Stopwatch.StartNew();

            var uniques = new List<OffsetAndLength>(offsets.Length);
            var dups = _Conf.SaveDuplicates ? new List<OffsetAndLength>(offsets.Length / 4) : null;

            bool saveDuplicates = _Conf.SaveDuplicates;
            bool leaveDuplicates = _Conf.LeaveDuplicates;

            // Nothing needs to be done!
            if (leaveDuplicates && !saveDuplicates)
                return Tuple.Create(offsets, (OffsetAndLength[])null);

            var previous = OffsetAndLength.Empty;
            var lastDuplicate = OffsetAndLength.Empty;
            for (int i = 0; i < offsets.Length; i++)
            {
                var current = offsets[i];
                var isDuplicate = i > 0 && comparer.Equals(current, previous);

                // Record unique lines.
                if (leaveDuplicates || !isDuplicate)
                    uniques.Add(current);

                // Record duplicate lines.
                if (isDuplicate && saveDuplicates)
                {
                    var isDuplicateDuplicate = comparer.Equals(current, lastDuplicate);
                    if (!isDuplicateDuplicate)
                    {
                        dups.Add(current);
                        lastDuplicate = current;
                    }
                }

                previous = current;
            }

            var result = Tuple.Create(uniques.ToArray(), dups == null ? null : dups.ToArray());

            sw.Stop();
            this.WriteStats("De-duplicated {0:N0} lines(s), {1:N1}MB in {2:N1}ms. {3:N0} line(s) remain, {4:N0} duplicates removed.", offsets.Length, chunkData.Length / oneMbAsDouble, sw.Elapsed.TotalMilliseconds, uniques.Count, offsets.Length - uniques.Count);
            return result;
        }

        private long WriteToFile(IndexedFileData data, FileStream output, IndexedFileData duplicates, FileStream duplicateOutput)
        {
            long linesWritten = 0L;

            // Write unique lines.
            var chunkData = data.Chunk;
            var offsets = data.LineOffsets;
            for (int i = 0; i < offsets.Length; i++)
            {
                var current = offsets[i];
                output.Write(chunkData, current.Offset, current.Length);
                output.WriteByte(Constants.NewLineAsByte);
                linesWritten++;
            }

            // Write duplicate lines, if any.
            if (_Conf.SaveDuplicates && duplicates != null && duplicateOutput != null)
            {
                chunkData = duplicates.Chunk;
                offsets = duplicates.LineOffsets;
                for (int i = 0; i < offsets.Length; i++)
                {
                    var current = offsets[i];
                    duplicateOutput.Write(chunkData, current.Offset, current.Length);
                    duplicateOutput.WriteByte(Constants.NewLineAsByte);
                }

            }
            return linesWritten;
        }
        #endregion


        #region WriteStats()
        private void WriteStats(string format, object arg1)
        {
            if (!_Conf.SaveStats) return;
            lock (_StatsFile)
            {
                _StatsFile.WriteLine(format, arg1);
            }
        }
        private void WriteStats(string format, object arg1, object arg2)
        {
            if (!_Conf.SaveStats) return;
            lock (_StatsFile)
            {
                _StatsFile.WriteLine(format, arg1, arg2);
            }
        }
        private void WriteStats(string format, object arg1, object arg2, object arg3)
        {
            if (!_Conf.SaveStats) return;
            lock (_StatsFile)
            {
                _StatsFile.WriteLine(format, arg1, arg2, arg3);
            }
        }
        private void WriteStats(string format, params object[] args)
        {
            if (!_Conf.SaveStats) return;
            lock (_StatsFile)
            {
                _StatsFile.WriteLine(format, args);
            }
        }
        #endregion

        private void PrintEstimatedMemoryUsage()
        {
            if (!_Conf.Debug)
                return;

            // During the phase 1 read (splitting / sharding), the max memory usage is:
            var estForSplitPerWorker = (_Conf.ReadBufferSize * 3) + (257 * _Conf.TempFileBufferSize);      // 257 is the number of files open - 256 for a byte of sharding, plus one empty shard.
            var estForSortPerWorker = _Conf.OutputBufferSize        // Output file buffer
                                    + (_Conf.SaveDuplicates ? _Conf.OutputBufferSize : 0)       // Output buffer when saving duplicates.
                                    + _Conf.MaxSortSize             // Sorting buffer
                                    + (_Conf.MaxSortSize / 9 * System.Runtime.InteropServices.Marshal.SizeOf(typeof(OffsetAndLength)))    // Index into sort buffer. 9 is a conservative guess at the average line length.
                                    + (_Conf.MaxSortSize / 9 * System.Runtime.InteropServices.Marshal.SizeOf(typeof(OffsetAndLength)))    // De-Duplication array. 9 is a conservative guess at the average line length.
                                    + (_Conf.MaxSortSize / 9 * System.Runtime.InteropServices.Marshal.SizeOf(typeof(OffsetAndLength)))    // Additional sorting buffers / stack. 9 is a conservative guess at the average line length.
                                    + (10 * 1024 * 1024);           // TPL / Parallel overhead (eg: additional thread, TPL buffering and marshalling).
            Console.WriteLine("Estimated Memory Usage:");
            Console.WriteLine("  General Overhead: ~20-30MB");
            Console.WriteLine("  Split Phase (per worker): {0:N1}MB", estForSplitPerWorker / oneMbAsDouble);
            Console.WriteLine("  Split Phase for {1} worker(s): {0:N1}MB", (estForSplitPerWorker * _Conf.SplitWorkers) / oneMbAsDouble, _Conf.SplitWorkers);
            Console.WriteLine("  Sort Phase (per worker): {0:N1}MB", estForSortPerWorker / oneMbAsDouble);
            Console.WriteLine("  Sort Phase for {1} worker(s): {0:N1}MB", ((long)estForSortPerWorker * _Conf.SortWorkers) / oneMbAsDouble, _Conf.SortWorkers);
            Console.WriteLine("  Sort Phase for {1} outstanding chunks: {0:N1}MB", ((long)estForSortPerWorker * (_Conf.SortWorkers + _Conf.MaxOutstandingSortedChunks)) / oneMbAsDouble, _Conf.SortWorkers + _Conf.MaxOutstandingSortedChunks);
            Console.WriteLine("Note on memory usage:");
            Console.WriteLine("  .NET uses garbage collection; you may see higher memory use for short times.");
            Console.WriteLine("  Consider using --aggressive-memory-collection if you are running out of RAM.");
            Console.WriteLine();
        }


        private void PrintConf()
        {
            if (_Conf.Debug)
            {
                Console.WriteLine("Configuration:");
                Console.WriteLine("  Max Sort Size: " + _Conf.MaxSortSize.ToByteSizedString());
                Console.WriteLine("  Read File Buffer Size: " + _Conf.ReadBufferSize.ToByteSizedString());
                Console.WriteLine("  Line Buffer Size: " + _Conf.LineBufferSize.ToByteSizedString());
                Console.WriteLine("  Temp File Buffer Size: " + _Conf.TempFileBufferSize.ToByteSizedString());
                Console.WriteLine("  Output File Buffer Size: " + _Conf.OutputBufferSize.ToByteSizedString());
                Console.WriteLine("  Split Workers: " + _Conf.SplitWorkers);
                Console.WriteLine("  Sort Workers: " + _Conf.SortWorkers);
                Console.WriteLine("  IO Workers: " + _Conf.IOWorkers);
                Console.WriteLine("  Temp Folder: " + _Conf.TempFolder);
                Console.WriteLine("  Max Outstanding Chunks: " + _Conf.MaxOutstandingSortedChunks);
                Console.WriteLine("  Sort Algorithm: " + _Conf.SortAlgorithm);
                Console.WriteLine("  Comparer: " + _Conf.Comparer);
                Console.WriteLine("  Leave Duplicates: " + _Conf.LeaveDuplicates);
                Console.WriteLine("  Save Stats: " + _Conf.SaveStats);
                Console.WriteLine("  Whitespace: " + _Conf.Whitespace);
                Console.WriteLine("  Convert to $HEX[]: " + _Conf.ConvertToDollarHex);
                Console.WriteLine();
            }
        }

        private void InitTempFolder()
        {
            CleanTempFolder();
            Directory.CreateDirectory(_Conf.TempFolder);
        }
        private void CleanTempFolder()
        {
            if (Directory.Exists(_Conf.TempFolder))
                Directory.Delete(_Conf.TempFolder, true);
        }
        private void WarnIfOldTempFilesExist(DirectoryInfo tempBase, DirectoryInfo excludeThisFolder)
        {
            var excludePath = excludeThisFolder.FullName;
            if (!tempBase.Exists)
                return;
            var tempFiles = tempBase.EnumerateFiles("*", SearchOption.AllDirectories)
                                .Where(x => !String.Equals(x.Directory.FullName, excludeThisFolder.FullName, StringComparison.OrdinalIgnoreCase))
                                .ToList();
            if (tempFiles.Any())
            {
                Console.WriteLine();
                Console.WriteLine("Warning: {0:N1}MB of old working files remain in '{1}'.", tempFiles.Sum(x => x.Length) / oneMbAsDouble, tempBase.FullName);
                Console.WriteLine("Use the 'cleantemp' verb to remove them.");
                Console.WriteLine();
            }
        }
    }
}