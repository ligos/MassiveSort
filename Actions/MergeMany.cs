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
using System.Buffers;

namespace MurrayGrant.MassiveSort.Actions
{
    #region Config
    
    [Verb("merge")]
    public sealed class MergeConf : CommonConf
    {
        public MergeConf()
            : base()
        {
            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            this.TempFolder = Path.Combine(Helpers.GetBaseTempFolder(), pid.ToString());        // Temp folder is specific to process id, so we can run in parallel.

            this.MaxSortSize = 64 * 1024 * 1024;            // Largest size of files to sort in one chunk.
            this.SlabSize = 4 * 1024 * 1024;                // Size of each slab in the SlabArray.
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
            this.SplitCount = 16;
            this.ForceLargeSort = false;

            this.LargeFileThresholdSize = 1 * 1024L * 1024L * 1024L;        // 1GB
            this.LargeFileChunkSize = 256 * 1024 * 1024;    // 256MB

            this.SortAlgorithm = SortAlgorithms.TimSort;    // Sort algorithm to use. 
            this.SortOrder = SortOrders.Dictionary;         // IComparer implementation to use - default to natural dictionary order.

            this.Whitespace = WhitespaceOptions.NoChange;   // Make no changes to whitespace.
            this.WhitespaceChars = Constants.DefaultWhitespace.ToArray();     // Whitespace characters.
            this.ConvertToDollarHex = false;                // Do not convert to $HEX[...] format by default.
        }

        public static string GetUsageText()
        {
            return
"""
Help for 'merge" verb:

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
-s --sort-by       Order to sort lines in
                   - Dictionary: natural dictionary order (default)
                   - Length: length first, then dictionary order
--max-sort-size    Largest chunk of files to sort as a group
                   Major contributor to memory usage
                   Default: 64MB, Min: 256KB
                   Max: 63TB (with max --slab-size of 1GB)
                   Max: 255GB (with default --slab-size of 4MB)
--slab-size        Memory allocations are made in chunks of this size
                   Increasing this allows larger --max-sort-size
                   Must be a power of 2
                   Default: 4MB, Min: 256KB, Max: 1GB
--sort-algorithm   Sort agorithm to use, options:
                   - DefaultArray: Array.Sort()
                   - LinqOrderBy: Enumerable.OrderBy()
                   - TimSort: Timsort algorithm (default)
--split-count      Number of split iterations.
                   Default: 16
--force-large-sort Force sorting shards larger than max-sort-size
                   You must still have sufficent physical RAM
                   Default: False

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
""";
        }

        [Option('i', "input")]
        public IEnumerable<string> Inputs { get; set; }


        [Option('o', "output")]
        public string OutputFile { get; set; }

        [Option('t', "temp-folder")]
        public string TempFolder { get; set; }

        [Option("max-sort-size")]
        public string MaxSortSize_Raw { get; set; }
        public long MaxSortSize { get; set; }
        [Option("slab-size")]
        public string SlabSize_Raw { get; set; }
        public int SlabSize { get; set; }
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

        [Option('s', "sort-by")]
        public SortOrders SortOrder { get; set; }
        public enum SortOrders
        {
            /// <summary>
            /// Natural dictionary order. Eg: a, aa, ab, abc, b, bb
            /// </summary>
            Dictionary,
            /// <summary>
            /// Length, then dictionary order. Eg: a, b, aa, ab, bb, abc
            /// </summary>
            Length,
        }

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

        [Option("split-count")]
        public int SplitCount { get; set; }

        [Option("force-large-sort")]
        public bool ForceLargeSort { get; set; }

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
        public IEnumerable<byte> WhitespaceChars { get; set; }
        internal ReadOnlyMemory<byte> WhitespaceCharsAsBytes { get; set; }

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
                MaxSortSize = size;
            if (!String.IsNullOrEmpty(this.SlabSize_Raw) && Helpers.TryParseByteSized(this.SlabSize_Raw, out size))
                SlabSize = (int)size;
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

            // For some reason, the command line parser is very keen on setting WhitespaceChars to an empty array.
            // Even if the parameter is not present on the command line.
            // So, we handle that case here.
            if (WhitespaceChars.Any())
                WhitespaceCharsAsBytes = WhitespaceChars.ToArray();
            else
                WhitespaceCharsAsBytes = Constants.DefaultWhitespace;

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
            if (!String.IsNullOrEmpty(this.SlabSize_Raw) && !Helpers.TryParseByteSized(this.SlabSize_Raw, out size))
                result.Append("'slab-size' cannot be parsed.");
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

            if (SplitCount < 1 || SplitCount > 128)
                result.AppendLine("'shard-count' must be between 1 and 128.");

            // Other sanity checks.
            if (SlabSize < 1024 * 256)
                result.AppendLine("'slab-size' must be at least 256KB.");
            if (SlabSize > 1024 * 1024 * 1024)
                result.AppendLine("'slab-size' must be less than 1GB.");
            if (!IsPowerOfTwo(SlabSize))
                result.AppendLine("'slab-size' must be a power of 2.");

            if (MaxSortSize < 1024 * 256)
                result.AppendLine("'max-sort-size' must be at least 256KB.");
            if (MaxSortSize > 64L * 1024 * 1024 * 1024 * 1024)
                result.AppendLine("'max-sort-size' must be less than 64TB.");
            if (MaxSortSize > (long)SlabSize * 65532L)  // 4 slabs are reserved as overhead, because we might not be able to fill them to capacity
                result.AppendLine("'max-sort-size' must be less than 'slab-size' * 65532.");

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

        bool IsPowerOfTwo(int x)
        {
            int result = x, remainder;
            do
            {
                (result, remainder) = Math.DivRem(result, 2);
                if (result == 0)
                    return true;
            } while (remainder == 0);

            return false;
        }
    }
    #endregion

    public sealed class MergeMany : ICmdVerb, IDisposable
    {
        private readonly double oneMbAsDouble = Constants.OneMbAsDouble;
        private static string emptyShardFilename = "!";

        private readonly MergeConf _Conf;
        private ParallelOptions _ParallelOptsForConfiguredDegreeOfIOParallelism;
        private CancellationToken _CancelToken;

        private readonly IProgress<BasicProgress> _Progress = new ConsoleProgressReporter();

        private StreamWriter _StatsFile;
        private readonly MemoryPool<byte> _MemoryPool = MemoryPool<byte>.Shared;

        public MergeMany(MergeConf conf)
        {
            _Conf = conf;
        }

        public void Dispose()
        {
            // Try to clean up our temp folder at the end.
            try
            {
                CleanTempFolder();
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine($"Unable to clean temp folder '{_Conf.TempFolder}' when stopping ({ex.GetType().Name}: {ex.Message}).");
                Console.WriteLine("Use the 'cleantemp' verb to remove them.");
                Console.WriteLine();
            }

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
            if (_Conf.Help) {
                new Help(new HelpConf() { Verb = "merge" }).Do(token);
                return;
            }

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
            // TODO: exclude / include filters.
            var result = Helpers.GatherFiles(_Conf.Inputs, Enumerable.Empty<string>());
            sw.Stop();

            this.WriteStats("Found {0:N0} files to merge, totaling {1:N1}MB. Time to search: {2:N1}ms.", result.Count, result.Sum(x => x.Length) / oneMbAsDouble, sw.Elapsed.TotalMilliseconds);
            return result;
        }
        private IEnumerable<FileResult> SplitFiles(IEnumerable<FileInfo> files)
        {
            // Stage 1: read all files and split lines into buckets.

            _Progress.Report(new BasicProgress(String.Format("Splitting {0:N0} file(s) (round 1)...", files.Count()), true));
            if (!files.Any()) return Enumerable.Empty<FileResult>();
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
                if (shardSize > _Conf.SplitCount && _Conf.ForceLargeSort)
                {
                    _Progress.Report(new BasicProgress(String.Format("Splitting stopped. {0:N0} file(s) remain larger than {1}. Will attempt to sort in RAM.", filesLargerThanSortSize.Count(), _Conf.MaxSortSize.ToByteSizedString()), true));
                    this.WriteStats("Splitting stopped. Following files remain larger than {0}:", _Conf.MaxSortSize.ToByteSizedString());
                    foreach (var f in filesLargerThanSortSize)
                        this.WriteStats("  {0}: {1}", f.Name, f.Length.ToByteSizedString(2));
                    break;
                }
                else if (shardSize > _Conf.SplitCount && !_Conf.ForceLargeSort)
                    throw new ApplicationException($"Splitting stopped after {shardSize-1} rounds and was unable to reduce all shards to less than {_Conf.MaxSortSize.ToByteSizedString()}. This indicates a large number of simiar or duplicate lines. Try increasing --split-count or --max-sort-size to process these files. Or enable --force-large-sort to try sorting anyway. You may need to decrease --sort-workers to avoid running out of memory.");

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
            var buffer = new byte[_Conf.LineBufferSize];
            var chunks = new PlainRaw(_CancelToken).ConvertFilesToSplitChunks(files, _Conf.LargeFileThresholdSize, _Conf.LargeFileChunkSize);
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
                        var ch = new FileChunk(f, 0, 0, f.Length);
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

        private long SplitFile(ShardFile[] shardFiles, long[] lineCounts, FileChunk ch, int shardSize, object taskKey, bool lockStreams)
        {
            long linesTrimmed = 0;
            long linesStripped = 0;
            long linesConvertedToDollarHex = 0;

            bool trimWhitespace = (_Conf.Whitespace == MergeConf.WhitespaceOptions.Trim);
            bool stripWhitespace = (_Conf.Whitespace == MergeConf.WhitespaceOptions.Strip);
            bool convertToDollarHex = _Conf.ConvertToDollarHex;

            // The allocation size allow us to convert a full line buffer to $HEX[...] format.
            var sizeForDollarHex = _Conf.LineBufferSize * 2 + Constants.DollarHexPrefix.Length + 1 /* ']' */ + 1 /* '\n' */;
            using (var buffer = _MemoryPool.Rent(_Conf.LineBufferSize))
            using (var whitespaceBuffer = _MemoryPool.Rent(_Conf.LineBufferSize))
            using (var dollarHexBuffer = _MemoryPool.Rent(sizeForDollarHex))
            {
                _Progress.Report(new TaskProgress(String.Format("Splitting '{0}'...", ch.NameForProgress), false, taskKey));
                var sw = Stopwatch.StartNew();

                var reader = new PlainRaw(_CancelToken, _Conf.ReadBufferSize);
                foreach (var line in reader.ReadAll(buffer.Memory, ch.FullPath, ch.StartOffset, ch.EndOffset))
                {
                    // Additional processing happens here.

                    // The order of these means only one will ever be triggered.
                    // The code, as it stands, will not cope with two copies of the line (the copies will overwrite each other on the 2nd call).
                    var toWrite = line.Span;

                    // Convert to $HEX before trimming, as any removal of whitespace will break unicode (UTF-16) encoded strings.
                    if (convertToDollarHex)
                        linesConvertedToDollarHex += this.ConvertToDollarHex(toWrite, dollarHexBuffer.Memory.Span, out toWrite);

                    // Trimming whitespace does not require a change to the buffer or any copying.
                    if (trimWhitespace)
                        linesTrimmed += this.TrimWhitespace(toWrite, _Conf.WhitespaceCharsAsBytes.Span, out toWrite);

                    // Stripping all whitespace may require a copy to the alternate buffer.
                    if (stripWhitespace)
                        linesStripped += this.StripWhitespace(toWrite, whitespaceBuffer.Memory.Span, _Conf.WhitespaceCharsAsBytes.Span, out toWrite);


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
        }


        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private void ShardWordToFileWithoutLock(ReadOnlySpan<byte> wordBytes, int shardSize, ShardFile[] shardFiles, long[] lineCounts)
        {
            // Determine the first character(s) to shard into separate files.
            int shard;
            if (shardSize > wordBytes.Length)
                shard = shardFiles.Length - 1;     // Empty string / no shard.
            else
                shard = wordBytes[shardSize - 1];

#if DEBUG
            if (System.Diagnostics.Debugger.IsAttached)
            {
                var wordAsBytes = wordBytes.ToArray();
                var wordAsNativeString = Encoding.Default.GetString(wordBytes);
                var wordAsUtf8String = Encoding.UTF8.GetString(wordBytes);
            }
#endif
            // Write the line to the file.
            var stream = shardFiles[shard].Stream;
            if (wordBytes.Length > 0)
                stream.Write(wordBytes);
            stream.WriteByte(Constants.NewLineAsByte);
            lineCounts[shard] = lineCounts[shard] + 1;
        }

        private void ShardWordToFileWithStreamLock(ReadOnlySpan<byte> wordBytes, int shardSize, ShardFile[] shardFiles, long[] lineCounts)
        {
            // The lock makes it highly unlikely to inline this function call, hence why the code is mostly duplicated.

            // Determine the first character(s) to shard into separate files.
            int shard;
            if (shardSize > wordBytes.Length)
                shard = shardFiles.Length - 1;     // Empty string / no shard.
            else
                shard = wordBytes[shardSize - 1];

#if DEBUG
            if (System.Diagnostics.Debugger.IsAttached && wordBytes.Length > 0)
            {
                var wordAsBytes = wordBytes.ToArray();
                var wordAsNativeString = Encoding.Default.GetString(wordBytes);
                var wordAsUtf8String = Encoding.UTF8.GetString(wordBytes);
            }
#endif
            // Write the line to the file.
            var stream = shardFiles[shard].Stream;
            lock (stream)
            {
                if (wordBytes.Length > 0)
                    stream.Write(wordBytes);
                stream.WriteByte(Constants.NewLineAsByte);
                lineCounts[shard] = lineCounts[shard] + 1;
            }
        }


        private long TrimWhitespace(ReadOnlySpan<byte> wordBytes, ReadOnlySpan<byte> whitespaceChars, out ReadOnlySpan<byte> result)
        {
            // Skip over any whitespace at the start.
            int newStart = 0;
            for (int i = 0; i < wordBytes.Length; i++)
            {
                bool isWhitespace = false;
                for (int j = 0; j < whitespaceChars.Length; j++)
                    isWhitespace |= (wordBytes[i] == whitespaceChars[j]);

                if (isWhitespace)
                    newStart = i+1;
                else
                    break;
            }

            // Skip over any whitespace at the end.
            int newEnd = wordBytes.Length;
            for (int i = wordBytes.Length - 1; i >= newStart; i--)
            {
                bool isWhitespace = false;
                for (int j = 0; j < whitespaceChars.Length; j++)
                    isWhitespace |= (wordBytes[i] == whitespaceChars[j]);

                if (isWhitespace)
                    newEnd = i - 1;
                else
                    break;
            }

            if (newStart >= newEnd)
                result = [];
            else
                result = wordBytes[newStart..newEnd];

            if (newStart != 0 || newEnd != wordBytes.Length)
                return 1L;
            else
                return 0L;
        }
        private long StripWhitespace(ReadOnlySpan<byte> wordBytes, Span<byte> secondBuffer, ReadOnlySpan<byte> whitespaceChars, out ReadOnlySpan<byte> result)
        {
            // Search for whitespace.
            bool hasWhitespace = false;
            for (int i = 0; i < wordBytes.Length; i++)
            {
                for (int j = 0; j < whitespaceChars.Length; j++)
                    hasWhitespace = hasWhitespace | (wordBytes[i] == whitespaceChars[j]);
                if (hasWhitespace)
                    break;
            }
            if (!hasWhitespace)
            {
                // No whitespace found: return the original span.
                result = wordBytes;
                return 0L;
            }

            // Make a copy into the other buffer, but skip any whitespace.
            int oBufIdx = 0;
            for (int i = 0; i < wordBytes.Length; i++)
            {
                bool isWhitespace = false;
                for (int j = 0; j < whitespaceChars.Length; j++)
                    isWhitespace = isWhitespace | (wordBytes[i] == whitespaceChars[j]);
                if (!isWhitespace)
                {
                    secondBuffer[oBufIdx] = wordBytes[i];
                    oBufIdx++;
                }
            }
            result = secondBuffer[..oBufIdx];
            return 1L;
        }
        private long ConvertToDollarHex(ReadOnlySpan<byte> wordBytes, Span<byte> secondBuffer, out ReadOnlySpan<byte> result)
        {
            // The best definition of the $HEX[] convention is in Waffle's hashcat proposal: https://hashcat.net/trac/ticket/148

            // Check for the presence of "special" bytes.
            // That is, control bytes in the range 0x00 - 0x1f and 0x7f - 0xff.
            bool convert = false;
            for (int i = 0; i < wordBytes.Length; i++)
            {
                if (wordBytes[i] < 0x20 || wordBytes[i] > 0x7e)
                {
                    convert = true;
                    break;
                }
            }
            if (!convert)
            {
                // No special bytes found: return the original segment untouched.
                result = wordBytes;
                return 0L;
            }

            // Make a copy into the other buffer, converting to hex.
            // $HEX[ prefix.
            Constants.DollarHexPrefix.Span.CopyTo(secondBuffer);
            var idx = Constants.DollarHexPrefix.Length;
            // Actual data.
            Helpers.WriteHexToSpan(wordBytes, secondBuffer[idx..]);
            idx += (wordBytes.Length * 2);  // Two hex bytes per binary byte.
            // ] suffix.
            secondBuffer[idx] = Constants.DollarHexSuffixAsByte;
            idx += 1;

            result = secondBuffer[..idx];
            return 1L;
        }
        #endregion


        #region Sort
        private void SortFiles(IEnumerable<FileResult> toSort)
        {
            if (File.Exists(_Conf.OutputFile))
                File.Delete(_Conf.OutputFile);
            if (!toSort.Any()) return;

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
            using (var slabDebugOutput = new FileStream(_Conf.OutputFile + ".slabdebug.txt", FileMode.Create, FileAccess.Write, FileShare.None, _Conf.OutputBufferSize))
            using (var originalDebugOutput = new FileStream(_Conf.OutputFile + ".debug.txt", FileMode.Create, FileAccess.Write, FileShare.None, _Conf.OutputBufferSize))
            using (var duplicateOutput = _Conf.SaveDuplicates ? new FileStream(duplicatePath, FileMode.Create, FileAccess.Write, FileShare.None, _Conf.OutputBufferSize) : null)
            {
                // Now sort each chunk.
                // PERF: can read and sort each chunk in parallel, but must write at the end in the correct sequence.
                schedulerOverheadTime = 
                    this.SortAndWriteChunks(sortChunks, 
                    (c) => {
                        if (_CancelToken.IsCancellationRequested) return null;

                        var taskKey = new Object();
                        this.WriteStats("Chunk #{0}: Starting parallel sort thread.", c.chunkNum);

                        _Progress.Report(new TaskProgress(String.Format("Sorting chunk {0:N0} ({1} - {2})...", c.chunkNum, c.files.First().Name, c.files.Last().Name), false, taskKey));
                        this.WriteStats("  Chunk #{0}: Sorting with {3:N0} files ({1} - {2}: {4:N1}MB, {5:N0} lines)...", c.chunkNum, c.files.First().Name, c.files.Last().Name, c.files.Count(), c.files.Sum(x => x.Length) / oneMbAsDouble, c.files.Sum(x => x.Lines));

                        // Read the files for the chunk into a single array for sorting.
                        // PERF: this represents ~5% of the time in this loop.
                        var readSw = Stopwatch.StartNew();
                        var (slabData, slabIndexes) = this.ReadFilesAndFindWordsForSorting(c.chunkNum, c.files);
                        var chunkData = this.ReadFilesForSorting(c.chunkNum, c.files);           // This allocates a large byte[].
                        var offsets = this.FindLineBoundariesForSorting(c.chunkNum, chunkData, c.files);     // PERF: this is ~8%. This allocates a large Int64[].
                        var linesRead = offsets.Length;
                        readSw.Stop();
                        if (_CancelToken.IsCancellationRequested) return null;

                        // Actually sort them!
                        // PERF: this represents ~80% of the time in this loop.
                        // PERF: it's not entirely obvious from the trace, but a significant part of that time is in the comparer.
                        var sortSw = Stopwatch.StartNew();
                        var comparer = this.GetOffsetComparer(chunkData);
                        var slabComparer = this.GetOffsetComparer(slabData);
                        offsets = this.SortLines(c.chunkNum, chunkData, offsets, comparer);
                        slabIndexes = this.SortLines(c.chunkNum, slabData, slabIndexes, slabComparer);
                        this.WriteToFile(new IndexedFileData2(slabData, slabIndexes), slabDebugOutput, null, null);
                        this.WriteToFile(new IndexedFileData(chunkData, offsets), originalDebugOutput, null, null);
                        sortSw.Stop();
                        if (_CancelToken.IsCancellationRequested) return null;

                        // Filter the sorted data to exclude duplicates.
                        // PERF: this represents ~10% of the time in this loop.
                        var deDupeSw = Stopwatch.StartNew();
                        var deDupTuple = this.DeDupe(c.chunkNum, chunkData, offsets, (IEqualityComparer<OffsetAndLength>)comparer);
                        var (chunkUniques, chunkDuplicates) = this.DeDupe(c.chunkNum, slabData, slabIndexes, (IEqualityComparer<SlabIndex>)slabComparer);
                        var data = new IndexedFileData(chunkData, deDupTuple.uniques);
                        var duplicates = new IndexedFileData(chunkData, deDupTuple.duplicates);
                        var sortedSlabData = new IndexedFileData2(slabData, chunkUniques);
                        var sortedSlabDuplicates = new IndexedFileData2(slabData, chunkDuplicates);
                        deDupeSw.Stop();

                        _Progress.Report(new TaskProgress(" Sorted. ", false, taskKey));
                        this.WriteStats("  Chunk #{0}: Ending parallel sort thread.", c.chunkNum);

                        return new
                        {
                            ch = c.chunkNum,
                            chNum = c.chunkNum,
                            linesRead,
                            data,
                            duplicates,
                            sortedSlabData,
                            sortedSlabDuplicates,
                            taskKey,
                            readTime = readSw.Elapsed,
                            sortTime = sortSw.Elapsed,
                            deDupTime = deDupeSw.Elapsed,
                        };
                    }, 
                    ch => {
                        if (_CancelToken.IsCancellationRequested) return;
                        this.WriteStats("Chunk #{0}: On sequential write thread.", ch.chNum);

                        // Remove duplicates and write to disk.
                        // PERF: this represents ~10% of the time in this loop. It cannot be parallelised.
                        var writeSw = Stopwatch.StartNew();
                        //var linesWritten = this.WriteToFile(ch.data, output, ch.duplicates, duplicateOutput);
                        var linesWritten = this.WriteToFile(ch.sortedSlabData, output, ch.sortedSlabDuplicates, duplicateOutput);
                        totalLinesWritten += linesWritten;
                        totalLinesRead += ch.linesRead;
                        writeSw.Stop();

                        // Release references to the large arrays allocated when reading files.
                        var memoryCleanSw = Stopwatch.StartNew();
                        ch.data.Dispose();
                        if (ch.duplicates != null)
                            ch.duplicates.Dispose();
                        if (_Conf.AggressiveMemoryCollection)
                            GC.Collect();
                        ch.sortedSlabData?.Data?.Dispose();
                        ch.sortedSlabDuplicates?.Data?.Dispose();
                        memoryCleanSw.Stop();

                        _Progress.Report(new TaskProgress(" Written.", true, ch.taskKey));
                        var chTime = ch.readTime + ch.sortTime + ch.deDupTime + writeSw.Elapsed + memoryCleanSw.Elapsed;
                        this.WriteStats($"Chunk #{ch.chNum} completed! Processed in {chTime.TotalSeconds:N2} sec. Read {ch.linesRead:N0} lines in {ch.readTime.TotalMilliseconds:N1}ms, sorted in {ch.sortTime.TotalMilliseconds:N1}ms, {ch.linesRead - linesWritten:N0} duplicates removed in {ch.deDupTime.TotalMilliseconds:N1}ms, wrote {linesWritten:N0} lines in {writeSw.Elapsed.TotalMilliseconds:N1}ms, memory clean up in {memoryCleanSw.Elapsed.TotalMilliseconds:N1}ms.");
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

        private async Task<TimeSpan> SortAndWriteChunks<T>(IList<IEnumerable<FileResult>> chunks, Func<(int chunkNum, IEnumerable<FileResult> files), T> sorter, Action<T> writer)
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
                        sortTasks[i] = new Task<T>(() => sorter((chNum, ch)));
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
            var cumulativeLines = 0;
            var sortChunks = new List<IEnumerable<FileResult>>();
            var chunk = new List<FileResult>();
            foreach (var fi in toSort)
            {
                if (cumulativeSize + fi.Length > _Conf.MaxSortSize
                    || cumulativeLines + fi.Lines > Int32.MaxValue)    // TODO: a LargeArray with Int64 indexer is required 
                {
                    sortChunks.Add(chunk);
                    chunk = new List<FileResult>();
                    cumulativeSize = 0L;
                    cumulativeLines = 0;
                }
                chunk.Add(fi);
                cumulativeSize += fi.Length;
                cumulativeLines += (int)fi.Lines;
            }
            sortChunks.Add(chunk);
            sortChunks.RemoveAll(x => !x.Any());  // Empty sort chunks might exist because some files are larger than MaxSortSize
            sw.Stop();

            this.WriteStats("Created {0:N0} x {1:N1}MB chunk(s) to sort in {2:N1}ms.", sortChunks.Count(), _Conf.MaxSortSize / oneMbAsDouble, sw.Elapsed.TotalMilliseconds);
            var largeChunks = sortChunks.Where(x => x.Sum(f => f.Length) > _Conf.MaxSortSize);
            foreach (var c in largeChunks)
                this.WriteStats($"  Chunk '{c.First().Name}' is {c.Sum(f => f.Length).ToByteSizedString(2)} - larger than MaxSortSize ({_Conf.MaxSortSize.ToByteSizedString()})");


            return sortChunks;
        }

        private IComparer<OffsetAndLength> GetOffsetComparer(byte[] data)
        {
            switch (_Conf.SortOrder)
            {
                case MergeConf.SortOrders.Dictionary:
                    return new Comparers.ClrOffsetDictionaryComparer(data);
                case MergeConf.SortOrders.Length:
                    return new Comparers.ClrOffsetLengthComparer(data);
                default:
                    throw new Exception("Unknown comparer: " + _Conf.SortOrder);
            }
        }
        private IComparer<SlabIndex> GetOffsetComparer(SlabArray data)
            => _Conf.SortOrder switch
            {
                MergeConf.SortOrders.Dictionary => new Comparers.ClrSlabDictionaryComparer(data),
                MergeConf.SortOrders.Length => new Comparers.ClrSlabLengthComparer(data),
                _ => throw new Exception("Unknown comparer: " + _Conf.SortOrder),
            };
        private (SlabArray data, SlabIndex[] lines) ReadFilesAndFindWordsForSorting(int chunkNum, IEnumerable<FileResult> fs)
        {
            // Read each file in slab sized chunks, and scan for line boundaries for sorting.
            // Because we've processed all incoming files, we know there will be a single new line character after each line.
            // PERF: this could be done in parallel, but is unlikely to help as this is IO dominated and doing a sequential read anyway.
            // PERF: this could be interleaved with IO and CPU work happening at the same time.
            var sw = Stopwatch.StartNew();
            var data = new SlabArray();
            var lines = new SlabIndex[fs.Sum(x => x.Lines)];  // TODO: a LargeArray with Int64 indexer is required.
            ushort slabNumber = 0;
            int lineIdx = 0;

            var slab = _MemoryPool.Rent(_Conf.SlabSize);
            var span = slab.Memory.Span;
            var slabIdx = 0;

            foreach (var f in fs)
            {
                using (var stream = new FileStream(f.FullPath, FileMode.Open, FileAccess.Read, FileShare.None))
                {
                    int bytesRead;
                    while ((bytesRead = stream.Read(span)) != 0)
                    {
                        // Find word boundaries and create SlabIndexes for each.
                        int readIdx = 0;
                        for (int i = 0; i < bytesRead; i++)
                        {
                            if (span[i] == Constants.NewLineAsByte)
                            {
                                var len = i - readIdx;
                                var line = new SlabIndex(slabNumber, slabIdx, len);
                                lines[lineIdx] = line;

                                readIdx += len + 1;  // There is always one newline, because previous steps removed duplicates.
                                slabIdx += len + 1;
                                ++lineIdx;
                            }
                        }

                        // The last word might not fit entirely in the slab.
                        // If so, we rewind a little and keep reading.
                        var lastLineIsPartial = span[bytesRead-1] != Constants.NewLineAsByte;
                        if (lastLineIsPartial)
                        {
                            for (int i = bytesRead - 1; i >= 0 && span[i] != Constants.NewLineAsByte; --i)
                            {
                                --stream.Position;
                                //--readIdx;
                                //--slabIdx;
                                span[i] = 0;  // Zero out "unused" bytes at the end, so its obvious where usable data ends.
                            }
                        }

                        var reachedEndOfSlab = readIdx >= span.Length || readIdx <= 0;
                        if (reachedEndOfSlab)
                        {
                            // If we are at the end of a slab, add it to the larger data collection, and allocate a new one.
                            data.AddSlab(slab);
                            
                            slab = _MemoryPool.Rent(_Conf.SlabSize);
                            span = slab.Memory.Span;
                            slabIdx = 0;
                            ++slabNumber;
                        }
                        else
                        {
                            // Otherwise, trim down the span to read a bit more (possibly from the next file).
                            span = span[readIdx..];
                        }
                    }
                }
            }
 
            // Don't forget the last slab!
            for (int i = span.Length - 1; i >= 0 && span[i] != Constants.NewLineAsByte; --i)
                span[i] = 0;  // Zero out "unused" bytes at the end, so its obvious where usable data ends.
            data.AddSlab(slab);

#if DEBUG
            // Sanity check the lines.
            var lastSlab = 0;
            var lastOffset = Int32.MinValue;
            var expectedNextOffset = 0;
            for (int i = 0; i < lines.Length; i++)
            {
                var l = lines[i];
                if (lastSlab != l.SlabNumber)
                {
                    lastOffset = 0;
                    expectedNextOffset = 0;
                }

                var lineSpan = data.GetSpan(l);
                var lineAsUtf8String = Encoding.UTF8.GetString(lineSpan);

                if (l.Offset != expectedNextOffset)
                    throw new Exception("Postcondition failure: unexpected next offset");
                if (lastOffset > l.Offset)
                    throw new Exception("Postcondition failure: offset went backwards");

                lastOffset = l.Offset;
                expectedNextOffset = l.Offset + l.Length + 1;
                lastSlab = l.SlabNumber;
            }
#endif
            sw.Stop();
            this.WriteStats($"  Chunk #{chunkNum}: Read {fs.Count():N0} file(s) {fs.Sum(x => x.Length) / oneMbAsDouble:N1}MB, into {data.SlabCount:N0} slab(s), and found {lines.Length:N0} lines in {sw.Elapsed.TotalMilliseconds:N1}ms.");
            return (data, lines);
        }
        private byte[] ReadFilesForSorting(int chunkNum, IEnumerable<FileResult> fs)
        {
            // Read each file in one hit.
            // PERF: this could be done in parallel, but is unlikely to help as this is IO dominated and doing a sequential read anyway.
            var sw = Stopwatch.StartNew();
            // TODO: replace this big allocation with something that can support Int64 bytes!
            //MemoryPool<byte>.Shared.Rent()
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
            this.WriteStats($"  Chunk #{chunkNum}: Read {fs.Count():N0} file(s) {fs.Sum(x => x.Length) / oneMbAsDouble:N1}MB in {sw.Elapsed.TotalMilliseconds:N1}ms.");
            return data;
        }
        private OffsetAndLength[] FindLineBoundariesForSorting(int chunkNum, byte[] data, IEnumerable<FileResult> fs)
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
            this.WriteStats($"  Chunk #{chunkNum}: Found {offsets.Length:N0} lines in {sw.Elapsed.TotalMilliseconds:N1}ms.");
            return offsets;
        }

        private OffsetAndLength[] SortLines(int chunkNum, byte[] chunkData, OffsetAndLength[] offsets, IComparer<OffsetAndLength> comparer)
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
            this.WriteStats($"  Chunk #{chunkNum}: Sorted {offsets.Length:N0} lines ({chunkData.Length / oneMbAsDouble:N1}MB) in {sw.Elapsed.TotalMilliseconds:N1}ms.");
            return offsets;
        }
        private SlabIndex[] SortLines(int chunkNum, SlabArray data, SlabIndex[] offsets, IComparer<SlabIndex> comparer)
        {
            // TODO: Slabs are about 2.5x as slow as previous sorting. Dig into the performance further.
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
            this.WriteStats($"  Chunk #{chunkNum}: Sorted {offsets.Length:N0} lines ({data.Length / oneMbAsDouble:N1}MB) in {sw.Elapsed.TotalMilliseconds:N1}ms.");
            return offsets;
        }

        private (OffsetAndLength[] uniques, OffsetAndLength[] duplicates) DeDupe(int chunkNum, byte[] chunkData, OffsetAndLength[] offsets, IEqualityComparer<OffsetAndLength> comparer)
        {
            var sw = Stopwatch.StartNew();

            var uniques = new List<OffsetAndLength>(offsets.Length);
            var dups = _Conf.SaveDuplicates ? new List<OffsetAndLength>(offsets.Length / 4) : null;

            bool saveDuplicates = _Conf.SaveDuplicates;
            bool leaveDuplicates = _Conf.LeaveDuplicates;

            // Nothing needs to be done!
            if (leaveDuplicates && !saveDuplicates)
                return (offsets, null);

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

            var result = (uniques.ToArray(), dups?.ToArray());

            sw.Stop();
            this.WriteStats($"  Chunk #{chunkNum}: De-duplicated {offsets.Length:N0} lines(s), {chunkData.Length / oneMbAsDouble:N1}MB in {sw.Elapsed.TotalMilliseconds:N1}ms. {uniques.Count:N0} line(s) remain, {offsets.Length - uniques.Count:N0} duplicates removed.");
            return result;
        }
        private (SlabIndex[] uniques, SlabIndex[] duplicates) DeDupe(int chunkNum, SlabArray data, SlabIndex[] offsets, IEqualityComparer<SlabIndex> comparer)
        {
            var sw = Stopwatch.StartNew();

            var uniques = new List<SlabIndex>(offsets.Length);
            var dups = _Conf.SaveDuplicates ? new List<SlabIndex>(offsets.Length / 4) : null;

            bool saveDuplicates = _Conf.SaveDuplicates;
            bool leaveDuplicates = _Conf.LeaveDuplicates;

            // Nothing needs to be done!
            if (leaveDuplicates && !saveDuplicates)
                return (offsets, null);

            var previous = SlabIndex.Empty;
            var lastDuplicate = SlabIndex.Empty;
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

            var result = (uniques.ToArray(), dups?.ToArray());

            sw.Stop();
            this.WriteStats($"  Chunk #{chunkNum}: De-duplicated {offsets.Length:N0} lines(s), {data.Length / oneMbAsDouble:N1}MB in {sw.Elapsed.TotalMilliseconds:N1}ms. {uniques.Count:N0} line(s) remain, {offsets.Length - uniques.Count:N0} duplicates removed.");
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
        private long WriteToFile(IndexedFileData2 data, FileStream output, IndexedFileData2 duplicates, FileStream duplicateOutput)
        {
            long linesWritten = 0L;

            // Write unique lines.
            var index = data.LineIndex;
            for (int i = 0; i < index.Length; i++)
            {
                var current = index[i];
                var span = data.Data.GetSpan(current);
                output.Write(span);
                output.WriteByte(Constants.NewLineAsByte);
                linesWritten++;
            }

            // Write duplicate lines, if any.
            if (_Conf.SaveDuplicates && duplicates != null && duplicateOutput != null)
            {
                index = data.LineIndex;
                for (int i = 0; i < index.Length; i++)
                {
                    var current = index[i];
                    var span = data.Data.GetSpan(current);
                    duplicateOutput.Write(span);
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
                Console.WriteLine("  Sort By: " + _Conf.SortOrder);
                Console.WriteLine("  Sort Algorithm: " + _Conf.SortAlgorithm);
                Console.WriteLine("  Split Count: " + _Conf.SplitCount);
                Console.WriteLine("  Force Large Sort: " + _Conf.ForceLargeSort);
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