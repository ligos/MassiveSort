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
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;
using CommandLine;
using Humanizer;
using Humanizer.Bytes;

namespace MurrayGrant.MassiveSort.Actions
{
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
            this.SaveStats = false;
            this.DegreeOfParallelism = Helpers.PhysicalCoreCount();
            this.DegreeOfIOParallelism = 8;                 // Default of 8 IO workers. Should provide a balance between SSD and HDD.

            this.SortAlgorithm = SortAlgorithms.TimSort;    // Sort algorithm to use. 
            this.Comparer = Comparers.Clr;                  // IComparer implementation to use.
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
        /// If true, writes stats to a parallel files to the OutputFile. Defaults to false.
        /// </summary>
        [Option("save-stats")]
        public bool SaveStats { get; set; }


        [Option('w', "workers")]
        public int DegreeOfParallelism { get; set; }
        [Option("io-workers")]
        public int DegreeOfIOParallelism { get; set; }

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

        public MergeConf ExtraParsing()
        {
            ByteSize s;
            if (!String.IsNullOrEmpty(this.MaxSortSize_Raw) && ByteSize.TryParse(this.MaxSortSize_Raw, out s))
                MaxSortSize = (int)s.Bytes;
            if (!String.IsNullOrEmpty(this.LineBufferSize_Raw) && ByteSize.TryParse(this.LineBufferSize_Raw, out s))
                LineBufferSize = (int)s.Bytes;
            if (!String.IsNullOrEmpty(this.ReadBufferSize_Raw) && ByteSize.TryParse(this.ReadBufferSize_Raw, out s))
                ReadBufferSize = (int)s.Bytes;
            if (!String.IsNullOrEmpty(this.TempFileBufferSize_Raw) && ByteSize.TryParse(this.TempFileBufferSize_Raw, out s))
                TempFileBufferSize = (int)s.Bytes;
            if (!String.IsNullOrEmpty(this.OutputBufferSize_Raw) && ByteSize.TryParse(this.OutputBufferSize_Raw, out s))
                OutputBufferSize = (int)s.Bytes;
            
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

            ByteSize s;
            if (!String.IsNullOrEmpty(this.MaxSortSize_Raw) && !ByteSize.TryParse(this.MaxSortSize_Raw, out s))
                result.Append("'max-sort-size' cannot be parsed.");
            if (!String.IsNullOrEmpty(this.LineBufferSize_Raw) && !ByteSize.TryParse(this.LineBufferSize_Raw, out s))
                result.Append("'line-buffer-size' cannot be parsed.");
            if (!String.IsNullOrEmpty(this.ReadBufferSize_Raw) && !ByteSize.TryParse(this.ReadBufferSize_Raw, out s))
                result.Append("'read-file-buffer-size' cannot be parsed.");
            if (!String.IsNullOrEmpty(this.TempFileBufferSize_Raw) && !ByteSize.TryParse(this.TempFileBufferSize_Raw, out s))
                result.Append("'temp-file-buffer-size' cannot be parsed.");
            if (!String.IsNullOrEmpty(this.OutputBufferSize_Raw) && !ByteSize.TryParse(this.OutputBufferSize_Raw, out s))
                result.Append("'output-file-buffer-size' cannot be parsed.");

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

            return result.ToString();
        }
        public bool IsValid { get { return String.IsNullOrEmpty(GetValidationMessage()); } }
    }


    public class MergeMany : ICmdVerb, IDisposable
    {
        const byte newlineByte = (byte)'\n';
        private const byte newline1 = (byte)'\n';
        private const byte newline2 = (byte)'\r';
        private const double oneMbAsDouble = 1024.0 * 1024.0;
        private static string emptyShardFilename = "!";

        private readonly MergeConf _Conf;
        private ParallelOptions _ParallelOptsForConfiguredDegreeOfParallelism;
        private ParallelOptions _ParallelOptsForConfiguredDegreeOfIOParallelism;

        private StreamWriter _StatsFile;

        public MergeMany(MergeConf conf)
        {
            _Conf = conf;
        }

        public void Dispose()
        {
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

        public void Do()
        {
            PrintConf();        // Print the config settings, in debug mode.

            // Configure TPL.
            var opts = new ParallelOptions();
            opts.MaxDegreeOfParallelism = _Conf.DegreeOfParallelism;
            this._ParallelOptsForConfiguredDegreeOfParallelism = opts;
            var ioOpts = new ParallelOptions();
            ioOpts.MaxDegreeOfParallelism = _Conf.DegreeOfIOParallelism;
            this._ParallelOptsForConfiguredDegreeOfIOParallelism = ioOpts;

            InitTempFolder();   // A bit of house cleaning.

            // Initialise the stats file.
            if (_Conf.SaveStats)
            {
                this._StatsFile = new StreamWriter(_Conf.OutputFile + ".stats", false, Encoding.UTF8);
            }

            // TODO: calculate approximate memory usage.

            try
            {
                // Snapshot the files we'll be working with.
                var filesToProcess = this.GatherFiles();


                // Stage 1: split / shard files into smaller chunks.
                var toSort = SplitFiles(filesToProcess);


                // Stage 2: sort and merge the files.
                SortFiles(toSort);
            }
            finally
            {
                // Try to clean up our temp folder at the end.
                CleanTempFolder();
            }
        }

        #region Split
        private IEnumerable<FileInfo> GatherFiles()
        {
            if (_Conf.Debug)
                Console.WriteLine("Gathering files to merge from '{0}'.", String.Join("; ", _Conf.Inputs));

            var sw = Stopwatch.StartNew();
            var result = _Conf.Inputs.SelectMany(i =>
                    Directory.Exists(i) ? new DirectoryInfo(i).EnumerateFiles("*", SearchOption.AllDirectories)
                                        : new FileInfo[] { new FileInfo(i) }
                )
                .OrderBy(x => x.FullName, StringComparer.CurrentCultureIgnoreCase)
                .ToList();
            sw.Stop();

            this.WriteStats("Found {0:N0} files to merge, totaling {1:N1}MB. Time to search: {2:N1}ms.", result.Count, result.Sum(x => x.Length) / oneMbAsDouble, sw.Elapsed.TotalMilliseconds);
            return result;
        }
        private IEnumerable<FileResult> SplitFiles(IEnumerable<FileInfo> files)
        {
            // Stage 1: read all files and split lines into buckets.

            Console.WriteLine("Splitting {0:N0} file(s) (round 1)...", files.Count());
            this.WriteStats("Splitting {0:N0} file(s) (round 1)...", files.Count());
            var sw = Stopwatch.StartNew();
            var shardedFileDetails = this.DoTopLevelSplit(files);

            // Test to see if we need further levels of sharding to make files small enough to sort.
            int shardSize = 2;
            var filesLargerThanSortSize = new DirectoryInfo(_Conf.TempFolder).EnumerateFiles("*", SearchOption.AllDirectories).Where(f => f.Length > _Conf.MaxSortSize);
            while (filesLargerThanSortSize.Any())
            {
                Console.WriteLine("Splitting {0:N0} file(s) (round {0})...", shardSize, filesLargerThanSortSize.Count());
                this.WriteStats("Splitting {0:N0} file(s) (round 1)...", shardSize, filesLargerThanSortSize.Count());

                this.DoSubLevelSplit(filesLargerThanSortSize, shardSize, shardedFileDetails);
                
                shardSize++;
                filesLargerThanSortSize = new DirectoryInfo(_Conf.TempFolder).EnumerateFiles("*", SearchOption.AllDirectories).Where(f => f.Length > _Conf.MaxSortSize);
            }
            sw.Stop();

            // Display summary information.
            var totalTimeSeconds = sw.Elapsed.TotalSeconds;
            var totalMB = files.Sum(x => x.Length) / oneMbAsDouble;
            var totalLines = shardedFileDetails.Values.Sum(x => x.Lines);
            Console.WriteLine("Finished splitting files in {0}.", totalTimeSeconds.Seconds().ToSizedString());
            Console.WriteLine();
            this.WriteStats("Finished splitting {0:N0} file(s) with {1:N0} lines ({2:N2} MB) in {3:N1} sec, {4:N0} lines / sec, {5:N1} MB / sec.", files.Count(), totalLines, totalMB, totalTimeSeconds, totalLines / totalTimeSeconds, totalMB / totalTimeSeconds);

            var toSort = shardedFileDetails.Where(x => File.Exists(x.Key)).OrderBy(x => x.Key).Select(x => x.Value).ToList();
            return toSort;
        }
        private IDictionary<string, FileResult> DoTopLevelSplit(IEnumerable<FileInfo> files)
        {
            var shardFiles = CreateShardFiles("");
            var lineCounts = new long[shardFiles.Length];
            var result = new Dictionary<string, FileResult>(shardFiles.Length);
            try
            {
                // Each file is split in parallel, with the assumption that we synchronise on the resulting file streams in .
                Parallel.ForEach(files, _ParallelOptsForConfiguredDegreeOfParallelism, f => {
                    SplitFile(shardFiles, lineCounts, f, 1);
                });
                for (int i = 0; i < shardFiles.Length; i++)
                    result.Add(shardFiles[i].Name, new FileResult(new FileInfo(shardFiles[i].Name), lineCounts[i]));
            }
            finally
            {
                this.FlushFiles(shardFiles, null, 0L, result);
            }

            return result;
        }
        private void DoSubLevelSplit(IEnumerable<FileInfo> files, int shardSize, IDictionary<string, FileResult> result)
        {
            // The logic for sub level splits is slightly different.
            // We split each file individually and replace it at the end.

            Parallel.ForEach(files, _ParallelOptsForConfiguredDegreeOfParallelism, f =>
            {
                var shardFiles = CreateShardFiles(Path.GetFileNameWithoutExtension(f.Name));
                var lineCounts = new long[shardFiles.Length];

                try
                {
                    // PERF: each file can be split in parallel, provided we synchronise on the resulting file streams.
                    SplitFile(shardFiles, lineCounts, f, shardSize);
                    for (int i = 0; i < shardFiles.Length; i++)
                        result.Add(shardFiles[i].Name, new FileResult(new FileInfo(shardFiles[i].Name), lineCounts[i]));
                }
                finally
                {
                    this.FlushFiles(shardFiles, f.FullName, lineCounts.Last(), result);
                }
            });
        }
        private void FlushFiles(FileStream[] shardFiles, string moveLastShardToPath, long lastShardLineCount, IDictionary<string, FileResult> result)
        {
            // Close and flush the shard files created.
            Console.Write("Flushing data to temp files...");
            var flushSw = Stopwatch.StartNew();

            var emptyShardPath = shardFiles.Last().Name;
            var toDelete = shardFiles.Where(x => x.Length == 0L).Select(x => x.Name).ToList();
            Parallel.ForEach(shardFiles, _ParallelOptsForConfiguredDegreeOfIOParallelism, fs =>
            {
                fs.Flush();
                fs.Close();
            });
            
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
                    result.Add(moveLastShardToPath, new FileResult(new FileInfo(moveLastShardToPath), lastShardLineCount));
                }
            }
            
            flushSw.Stop();
            Console.WriteLine(" Done.");
            this.WriteStats("Flushed data to temp files in {0:N0}ms.", flushSw.Elapsed.TotalMilliseconds);
        }

        private FileStream[] CreateShardFiles(string initialShard)
        {
            var sw = Stopwatch.StartNew();
            var result = new FileStream[256+1];
            var tempFolder = Path.GetFullPath(_Conf.TempFolder);
            var tempFileBufferSize = _Conf.TempFileBufferSize;

            try
            {
                // The normal files.
                Parallel.For(0, 256, _ParallelOptsForConfiguredDegreeOfIOParallelism, i =>
                {
                    var file = Path.Combine(tempFolder, initialShard + i.ToString("x2")) + ".txt";
                    var stream = new FileStream(file, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, tempFileBufferSize);
                    result[i] = stream;
                });

                // A file for empty string / no shard.
                {
                    var file = Path.Combine(tempFolder, initialShard + emptyShardFilename) + ".txt";
                    FileStream stream = new FileStream(file, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, tempFileBufferSize);
                    result[256] = stream;
                }
            }
            catch (Exception)
            {
                // Any failure and we close any files created so far and blow up.
                foreach (var f in result)
                    f.Close();
                throw;
            }
            sw.Stop();
            this.WriteStats("Created {0:N0} shard files for base '{1}' in {2:N1}ms.", result.Length, initialShard, sw.Elapsed.TotalMilliseconds);

            return result;
        }

        private long SplitFile(FileStream[] shardFiles, long[] lineCounts, FileInfo fi, int shardSize)
        {
            long linesRead = 0;
            long buffersSkipped = 0;
            long buffersRead = 0;
            long extraSeeks = 0;
            var lineBuffer = new byte[_Conf.LineBufferSize];
            // Hoping the JIT can remove the if knowing it's fixed at the function call time.
            bool shardWithLock = (shardSize == 1 && _ParallelOptsForConfiguredDegreeOfParallelism.MaxDegreeOfParallelism > 1);          

            Console.Write("Splitting '{0}'...", fi.Name);
            var sw = Stopwatch.StartNew();

            // Split the file into chunks.
            using (var stream = new FileStream(fi.FullName, FileMode.Open, FileAccess.Read, FileShare.Read, _Conf.ReadBufferSize))
            {
                // Read the file in buffer sized chunks.
                // This is perf critical code.
                while (ReadLineBuffer(stream, lineBuffer))
                {
                    buffersRead++;
                    int idx = 0;
                    OffsetAndLength ol;
                    do
                    {
                        // TODO: the way NextWord() is implemented will never return an empty string.
                        // Find the next word.
                        // PERF: about 30% of CPU time is spent in NextWord().
                        ol = NextWord(lineBuffer, idx);
                        
                        if (ol.Length >= 0 && ol.Offset >= 0)
                        {
                            // Write the word to the shard file.
                            // PERF: about 40% of CPU time is spent in FileStream.Write(), contained in ShardWordToFile().
                            linesRead++;
                            if (shardWithLock)      // Hoping the JIT can remove the if knowing it's fixed at the function call time.
                                ShardWordToFileWithStreamLock(lineBuffer, ol, shardSize, shardFiles, lineCounts);
                            else
                                ShardWordToFileWithoutLock(lineBuffer, ol, shardSize, shardFiles, lineCounts);
                            idx += ol.Length + 1;       // Assume at least one new line after the word.
                        }
                        else
                        {
                            // Can't process this word because we hit the end of the buffer.

                            if (idx == 0)
                            {
                                // Skip this line, because it did not fit entirely in the buffer.
                                buffersSkipped++;
                            }
                            else if (ol.Offset == -1)
                            {
                                // Got to the end of the line without finding the start of a word: no additional seek is required (no-op).
                            }
                            else if (ol.Length == -1)
                            {
                                // The buffer splits the word: seek backwards in the file slightly so the next buffer is at the start of the word.
                                stream.Position = stream.Position - (lineBuffer.Length - ol.Offset);
                                extraSeeks++;
                            }
                        }
                    } while (ol.Length >= 0 && ol.Offset >= 0);      // End of the buffer: read the next one.
                }
            }
            sw.Stop();

            Console.WriteLine(" Done.");
            this.WriteStats("File '{0}': {1:N0} lines processed in {2:N1}ms, {3:N1} lines / sec, {4:N1} MB / sec.", fi.Name, linesRead, sw.Elapsed.TotalMilliseconds, linesRead / sw.Elapsed.TotalSeconds, (fi.Length / oneMbAsDouble) / sw.Elapsed.TotalSeconds);
            this.WriteStats("File '{0}': {1:N0} line buffers read, {2:N0} line buffers skipped because lines were too long, {3:N0} additional seeks due to buffer alignment.", fi.Name, buffersRead, buffersSkipped, extraSeeks);

            return linesRead;
        }


        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private void ShardWordToFile(byte[] buf, OffsetAndLength word, int shardSize, FileStream[] shardFiles, long[] lineCounts)
        {
            // Determine the first character(s) to shard into separate files.
            int shard;
            if (shardSize > word.Length)
                shard = shardFiles.Length - 1;     // Empty string / no shard.
            else
                shard = buf[word.Offset + (shardSize - 1)];

#if DEBUG
            if (System.Diagnostics.Debugger.IsAttached)
            {
                var wordAsBytes = buf.Skip(word.Offset).Take(word.Length).ToArray();
                var wordAsNativeString = Encoding.Default.GetString(wordAsBytes);
                var wordAsUtf8String = Encoding.UTF8.GetString(wordAsBytes);
            }
#endif
            // Write the line to the file.
            var stream = shardFiles[shard];
            bool shardWithLock = (shardSize == 1 && _ParallelOptsForConfiguredDegreeOfParallelism.MaxDegreeOfParallelism > 1);
            if (shardWithLock)
            {
                lock(stream)
                {
                    stream.Write(buf, word.Offset, word.Length);
                    stream.WriteByte(newlineByte);
                    lineCounts[shard] = lineCounts[shard] + 1;
                }
            }
            else
            {
                stream.Write(buf, word.Offset, word.Length);
                stream.WriteByte(newlineByte);
                lineCounts[shard] = lineCounts[shard] + 1;
            }
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private void ShardWordToFileWithoutLock(byte[] buf, OffsetAndLength word, int shardSize, FileStream[] shardFiles, long[] lineCounts)
        {
            // Determine the first character(s) to shard into separate files.
            int shard;
            if (shardSize > word.Length)
                shard = shardFiles.Length - 1;     // Empty string / no shard.
            else
                shard = buf[word.Offset + (shardSize - 1)];

#if DEBUG
            if (System.Diagnostics.Debugger.IsAttached)
            {
                var wordAsBytes = buf.Skip(word.Offset).Take(word.Length).ToArray();
                var wordAsNativeString = Encoding.Default.GetString(wordAsBytes);
                var wordAsUtf8String = Encoding.UTF8.GetString(wordAsBytes);
            }
#endif
            // Write the line to the file.
            var stream = shardFiles[shard];
            stream.Write(buf, word.Offset, word.Length);
            stream.WriteByte(newlineByte);
            lineCounts[shard] = lineCounts[shard] + 1;
        }

        private void ShardWordToFileWithStreamLock(byte[] buf, OffsetAndLength word, int shardSize, FileStream[] shardFiles, long[] lineCounts)
        {
            // The lock makes it highly unlikely to inline this function call, hence why the code is mostly duplicated.

            // Determine the first character(s) to shard into separate files.
            int shard;
            if (shardSize > word.Length)
                shard = shardFiles.Length - 1;     // Empty string / no shard.
            else
                shard = buf[word.Offset + (shardSize - 1)];

#if DEBUG
            if (System.Diagnostics.Debugger.IsAttached)
            {
                var wordAsBytes = buf.Skip(word.Offset).Take(word.Length).ToArray();
                var wordAsNativeString = Encoding.Default.GetString(wordAsBytes);
                var wordAsUtf8String = Encoding.UTF8.GetString(wordAsBytes);
            }
#endif
            // Write the line to the file.
            var stream = shardFiles[shard];
            lock (stream)
            {
                stream.Write(buf, word.Offset, word.Length);
                stream.WriteByte(newlineByte);
                lineCounts[shard] = lineCounts[shard] + 1;
            }
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private bool ReadLineBuffer(FileStream stream, byte[] buf)
        {
            int bytesRead = stream.Read(buf, 0, buf.Length);
            if (bytesRead <= 0)
                // End of file.
                return false;

            if (bytesRead < buf.Length)
            {
                // Any left over space in the buffer is filled with new line characters.
                // These will be skipped in NextWord().
                for (int i = bytesRead; i < buf.Length; i++)
                    buf[i] = newline1;
            }
            return true;
        }


        private OffsetAndLength NextWord(byte[] buf, int startIdx)
        {
            if (startIdx >= buf.Length)
                // Past the end of the buffer.
                return new OffsetAndLength(-1, -1);

            // Ensure we aren't starting on a newline.
            if (buf[startIdx] == newline1 || buf[startIdx] == newline2)
                startIdx = NextNonNewlineInBuffer(buf, startIdx);

            if (startIdx == -1)
                // Got to end of buffer without finding the start of a word.
                return new OffsetAndLength(-1, -1);

            var endIdx = NextNewlineInBuffer(buf, startIdx);
            if (endIdx == -1)
                // Got to the end of the buffer without getting to the end of the word.
                return new OffsetAndLength(startIdx, -1);

            // Found the start and end of a word.
            return new OffsetAndLength(startIdx, endIdx - startIdx);
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private int NextNonNewlineInBuffer(byte[] buf, int startIdx)
        {
            for (int i = startIdx; i < buf.Length; i++)
            {
                if (buf[i] != newline1 && buf[i] != newline2)
                    return i;
            }
            return -1;
        }
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private int NextNewlineInBuffer(byte[] buf, int startIdx)
        {
            // PERF: might be worth trying pinvoke to memchr() 
            for (int i = startIdx; i < buf.Length; i++)
            {
                if (buf[i] == newline1 || buf[i] == newline2)
                    return i;
            }
            return -1;
        }
        #endregion


        #region Sort
        private void SortFiles(IEnumerable<FileResult> toSort)
        {
            if (File.Exists(_Conf.OutputFile))
                File.Delete(_Conf.OutputFile);

            long totalLinesWritten = 0;
            long totalLinesRead = 0;

            Console.WriteLine("Sorting files.");

            // Split into large chunks to sort.
            var sortChunks = this.SplitIntoChunksForBulkSorting(toSort);
            Console.WriteLine("There are {0:N0} chunk(s) to sort.", sortChunks.Count());

            var allSw = Stopwatch.StartNew();
            using (var output = new FileStream(_Conf.OutputFile, FileMode.Create, FileAccess.Write, FileShare.None, _Conf.OutputBufferSize))
            {
                // Now sort each chunk.
                // PERF: can read and sort each chunk in parallel, but must write at the end in the correct sequence.
                var sortedChunks = sortChunks
                    .AsParallel().AsOrdered()
                    .WithDegreeOfParallelism(_Conf.DegreeOfParallelism)
                    .WithMergeOptions(ParallelMergeOptions.NotBuffered)
                    .Select((ch, chNum) => {
                        Console.Write("Sorting chunk {0:N0} ({1} - {2})...", chNum+1, ch.First().File.Name, ch.Last().File.Name);
                        this.WriteStats("Sorting chunk {0:N0} with {3:N0} files ({1} - {2}: {4:N1}MB, {5:N0} lines)...", chNum+1, ch.First().File.Name, ch.Last().File.Name, ch.Count(), ch.Sum(x => x.File.Length) / oneMbAsDouble, ch.Sum(x => x.Lines));

                        // Read the files for the chunk into a single array for sorting.
                        // PERF: this represents ~10% of the time in this loop.
                        var readSw = Stopwatch.StartNew();
                        var chunkData = this.ReadFilesForSorting(ch);
                        var offsets = this.FindLineBoundariesForSorting(chunkData, ch);     // PERF: this is ~8%.
                        totalLinesRead += ch.Sum(x => x.Lines);
                        readSw.Stop();

                        // Actually sort them!
                        // PERF: this represents ~2/3 of the time in this loop.
                        // PERF: it's not entirely obvious from the trace, but a significant part of that time is in the comparer.
                        var sortSw = Stopwatch.StartNew();
                        var comparer = this.GetOffsetComparer(chunkData);
                        offsets = this.SortLines(chunkData, offsets, comparer);
                        sortSw.Stop();
                        Console.Write(" Sorted. ");

                        return new {
                            ch, 
                            chNum = chNum + 1,
                            chunkData,
                            offsets,
                            comparer,
                            readTime = readSw.Elapsed,
                            sortTime = sortSw.Elapsed,
                        };
                    });

                foreach (var ch in sortedChunks)
                {
                    // Remove duplicates and write to disk.
                    // PERF: this represents ~20% of the time in this loop. It cannot be parallelised.
                    var dedupAndWriteSw = Stopwatch.StartNew();
                    var linesWritten = this.WriteAndDeDupe(ch.chunkData, ch.offsets, output, (IEqualityComparer<OffsetAndLength>)ch.comparer);
                    totalLinesWritten += linesWritten;
                    dedupAndWriteSw.Stop();

                    Console.WriteLine(" Written.");
                    var chTime = ch.readTime + ch.sortTime + dedupAndWriteSw.Elapsed;
                    this.WriteStats("Chunk #{0}: processed in {1:N2} sec. Read {2:N0} lines in {3:N1}ms, sorted in {4:N1}ms, wrote {5:N0} lines in {6:N1}ms, {7:N0} duplicates removed.", ch.chNum, chTime.TotalSeconds, ch.offsets.Length, ch.readTime.TotalMilliseconds, ch.sortTime.TotalMilliseconds, linesWritten, dedupAndWriteSw.Elapsed.TotalMilliseconds, ch.offsets.Length - linesWritten);
                }

                output.Flush();
            }
            allSw.Stop();
            var duplicatesRemoved = totalLinesRead - totalLinesWritten;
            Console.WriteLine("Finished sorting{0} in {1}, {2:N0} lines remain.", _Conf.LeaveDuplicates ? "" : " and removing duplicates", allSw.Elapsed.ToSizedString(), totalLinesWritten);
            Console.WriteLine();
            this.WriteStats("Finished sorting in {0}. {1:N0} lines remain, {2:N0} duplicates removed.", allSw.Elapsed.ToSizedString(), totalLinesWritten, duplicatesRemoved);
        }

        private IEnumerable<IEnumerable<FileResult>> SplitIntoChunksForBulkSorting(IEnumerable<FileResult> toSort)
        {
            var sw = Stopwatch.StartNew();
            var cumulativeSize = 0L;
            var sortChunks = new List<List<FileResult>>();
            var chunk = new List<FileResult>();
            foreach (var fi in toSort)
            {
                if (cumulativeSize + fi.File.Length > _Conf.MaxSortSize)
                {
                    sortChunks.Add(chunk);
                    chunk = new List<FileResult>();
                    cumulativeSize = 0L;
                }
                chunk.Add(fi);
                cumulativeSize += fi.File.Length;
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
            // PERF: this could be done in parallel.
            var sw = Stopwatch.StartNew();
            var data = new byte[(int)fs.Sum(x => x.File.Length)];
            {
                int offset = 0;
                foreach (var f in fs)
                {
                    using (var stream = new FileStream(f.File.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        stream.Read(data, offset, (int)f.File.Length);
                    }
                    offset += (int)f.File.Length;
                }
            }
            sw.Stop();
            this.WriteStats("Read {0:N0} file(s) {1:N1}MB in {2:N1}ms.", fs.Count(), fs.Sum(x => x.File.Length) / oneMbAsDouble, sw.Elapsed.TotalMilliseconds);
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
                if (data[i] == newlineByte)
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

        private long WriteAndDeDupe(byte[] chunkData, OffsetAndLength[] offsets, FileStream output, IEqualityComparer<OffsetAndLength> comparer)
        {
            long linesWritten = 0;
            Func<int, OffsetAndLength, OffsetAndLength, bool> writeWordPredicate = (idx, c, p) => true;
            if (!_Conf.LeaveDuplicates)
                writeWordPredicate = (idx, c, p) => idx > 0 && !comparer.Equals(c, p);

            OffsetAndLength previous = OffsetAndLength.Empty;
            for (int i = 0; i < offsets.Length; i++)
            {
                var current = offsets[i];
                if (writeWordPredicate(offsets.Length, current, previous))
                {
                    output.Write(chunkData, current.Offset, current.Length);
                    output.WriteByte(newlineByte);
                    linesWritten++;
                }
                previous = current;
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
            lock(_StatsFile)
            {
                _StatsFile.WriteLine(format, args);
            }
        }
        #endregion

        private void PrintConf()
        {
            if (_Conf.Debug)
            {
                Console.WriteLine("Configuration:");
                Console.WriteLine("  Max Sort Size: " + _Conf.MaxSortSize.Bytes().ToString());
                Console.WriteLine("  Read File Buffer Size: " + _Conf.ReadBufferSize.Bytes().ToString());
                Console.WriteLine("  Line Buffer Size: " + _Conf.LineBufferSize.Bytes().ToString());
                Console.WriteLine("  Temp File Buffer Size: " + _Conf.TempFileBufferSize.Bytes().ToString());
                Console.WriteLine("  Output File Buffer Size: " + _Conf.OutputBufferSize.Bytes().ToString());
                Console.WriteLine("  Temp Folder: " + _Conf.TempFolder);
                Console.WriteLine("  Sort Algorithm: " + _Conf.SortAlgorithm);
                Console.WriteLine("  Comparer: " + _Conf.Comparer);
                Console.WriteLine("  Leave Duplicates: " + _Conf.LeaveDuplicates);
                Console.WriteLine("  Save Stats: " + _Conf.SaveStats);
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
    }
}
