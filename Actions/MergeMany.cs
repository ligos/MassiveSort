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
            this.ReadBufferSize = 256 * 1024;               // Buffer size to use when reading files.
            this.LineBufferSize = 64 * 1024;                // Buffer size to use when reading lines (also max line length).
            this.TempFileBufferSize = 128 * 1024;           // Buffer size to use when writing temp files.
            this.OutputBufferSize = 256 * 1024;             // Buffer size to use for the final merged output file.

            this.DegreeOfParallelism = Environment.ProcessorCount;      // TODO: default to the number of physical rather than logical cores.
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

        [Option('w', "workers")]
        public int DegreeOfParallelism { get; set; }


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
            if (MaxSortSize < 1024 * 1024)
                result.AppendLine("'max-sort-size' must be at least 1MB.");
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


    public class MergeMany : ICmdVerb
    {
        const byte newlineByte = (byte)'\n';
        private const byte newline1 = (byte)'\n';
        private const byte newline2 = (byte)'\r';
        private const double oneMbAsDouble = 1024.0 * 1024.0;
        private static string emptyShardFilename = "!";

        private readonly MergeConf _Conf;

        public MergeMany(MergeConf conf)
        {
            _Conf = conf;
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
            InitTempFolder();   // A bit of house cleaning.

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

            var result = _Conf.Inputs.SelectMany(i =>
                    Directory.Exists(i) ? new DirectoryInfo(i).EnumerateFiles("*", SearchOption.AllDirectories)
                                        : new FileInfo[] { new FileInfo(i) }
                )
                .OrderBy(x => x.FullName, StringComparer.CurrentCultureIgnoreCase)
                .ToList();

            if (_Conf.Debug)
                Console.WriteLine("Found {0:N0} files to merge.", result.Count());
            return result;
        }
        private IEnumerable<FileInfo> SplitFiles(IEnumerable<FileInfo> files)
        {
            var splitSw = new Stopwatch();
            var splitFlushSw = new Stopwatch();
            long totalLines = 0L, totalLinesProcessed = 0L;
            var totalLength = files.Sum(fi => fi.Length);
            if (totalLength <= _Conf.MaxSortSize)
            {
                Console.WriteLine("Skipping split phase as there are less than {0:N0}MB of files ({1:N2}MB).", _Conf.MaxSortSize / oneMbAsDouble, totalLength / oneMbAsDouble);
                return files;
            }


            // Stage 1: read all files and split lines into buckets.
            // This only happens if we have too many files.
            int shardSize = 1;
            long shardThreshold = Int64.MaxValue;
            splitSw.Start();
            var thisRoundOfSplitting = files.AsEnumerable();
            do
            {
                Console.WriteLine("Splitting files (round {0})...", shardSize);
                // Pre-create shard files.
                var shardFiles = CreateShardFiles("");     // TODO: the empty string needs to support shards greater than 1.
                try
                {
                    foreach (var f in files)
                    {
                        var lines = SplitFile(shardFiles, f, shardSize);
                        totalLinesProcessed += lines;
                        if (shardSize == 1)
                            totalLines += lines;
                    }
                }
                finally
                {
                    // Close and flush the shard files created.
                    splitSw.Stop();

                    Console.Write("Flushing data to temp files...");
                    splitFlushSw.Start();
                    var toDelete = shardFiles.Where(x => x.Length == 0L).Select(x => x.Name).ToList();
                    Parallel.ForEach(shardFiles, fs => {
                        fs.Flush();
                        fs.Close();
                    });
                    // Delete any zero length files.
                    foreach (var f in toDelete)
                        File.Delete(f);
                    splitFlushSw.Stop();
                    Console.WriteLine(" Done in {0:N1}ms.", splitFlushSw.Elapsed.TotalMilliseconds);

                    splitSw.Start();
                }
                shardSize++;
                shardThreshold = _Conf.MaxSortSize;
                thisRoundOfSplitting = new DirectoryInfo(_Conf.TempFolder).EnumerateFiles("*", SearchOption.AllDirectories).Where(f => f.Length > _Conf.MaxSortSize);
            } while (thisRoundOfSplitting.Any());
            splitSw.Stop();

            var totalTimeSeconds = splitSw.Elapsed.TotalSeconds + splitFlushSw.Elapsed.TotalSeconds;
            var totalMB = files.Sum(x => x.Length) / oneMbAsDouble;
            Console.WriteLine("Split {0:N0} file(s) with {1:N0} lines ({2:N2} MB) in {3:N1} sec, {4:N0} lines / sec, {5:N1} MB / sec.", files.Count(), totalLines, totalMB, totalTimeSeconds, totalLines / totalTimeSeconds, totalMB / totalTimeSeconds);
            return new DirectoryInfo(_Conf.TempFolder).EnumerateFiles("*", SearchOption.AllDirectories).OrderBy(f => f.Name).ToList();
        }


        private FileStream[] CreateShardFiles(string initialShard)
        {
            var result = new FileStream[256+1];
            var tempFolder = _Conf.TempFolder;
            var tempFileBufferSize = _Conf.TempFileBufferSize;

            try
            {
                // The normal files.
                Parallel.For(0, 256, i =>
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

            return result;
        }

        private long SplitFile(FileStream[] shardFiles, FileInfo fi, int shardSize)
        {
            long linesRead = 0;
            long buffersSkipped = 0;
            long buffersRead = 0;
            long extraSeeks = 0;
            var lineBuffer = new byte[_Conf.LineBufferSize];

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
                            ShardWordToFile(lineBuffer, ol, shardSize, shardFiles);
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
            Console.WriteLine("    {0:N0} lines processed in {1:N1}ms, {2:N1} lines / sec, {3:N1} MB / sec", linesRead, sw.Elapsed.TotalMilliseconds, linesRead / sw.Elapsed.TotalSeconds, (fi.Length / oneMbAsDouble) / sw.Elapsed.TotalSeconds);
            if (_Conf.Debug)
                Console.WriteLine("    {0:N0} line buffers read, {1:N0} line buffers skipped because lines were too long, {2:N0} additional seeks due to buffer alignment.", buffersRead, buffersSkipped, extraSeeks);
            return linesRead;
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private void ShardWordToFile(byte[] buf, OffsetAndLength word, int shardSize, FileStream[] shardFiles)
        {
            // Determine the first character(s) to shard into separate files.
            int shard;
            if (shardSize > word.Length)
                shard = shardFiles.Length-1;     // Empty string / no shard.
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
        private void SortFiles(IEnumerable<FileInfo> toSort)
        {
            if (File.Exists(_Conf.OutputFile))
                File.Delete(_Conf.OutputFile);

            long totalLinesWritten = 0;

            // Split into large chunks to sort.
            var cumulativeSize = 0L;
            var sortChunks = new List<List<FileInfo>>();
            var chunk = new List<FileInfo>();
            foreach (var fi in toSort)
            {
                if (cumulativeSize + fi.Length > _Conf.MaxSortSize)
                {
                    sortChunks.Add(chunk);
                    chunk = new List<FileInfo>();
                    cumulativeSize = 0L;
                }
                chunk.Add(fi);
                cumulativeSize += fi.Length;
            }
            sortChunks.Add(chunk);
            Console.WriteLine("There are {0:N0} x {1:N1}MB chunk(s) to sort.", sortChunks.Count, _Conf.MaxSortSize / oneMbAsDouble);

            var sortSw = Stopwatch.StartNew();
            using (var output = new FileStream(_Conf.OutputFile, FileMode.Create, FileAccess.Write, FileShare.None, _Conf.OutputBufferSize))
            {
                // Now sort each chunk.
                int chunkNum = 1;
                foreach (var ch in sortChunks)
                {
                    Console.Write("Sorting chunk {0:N0} with {3:N0} files ({1} - {2}: {4:N1}MB)...", chunkNum, ch.First().Name, ch.Last().Name, ch.Count, ch.Sum(x => x.Length) / oneMbAsDouble);
                    var chSw = Stopwatch.StartNew();
                    var lines = ch.SelectMany(f => f.YieldLinesAsByteArray((int)Math.Min(f.Length, _Conf.ReadBufferSize), _Conf.LineBufferSize)).ToList();
                    lines.Sort(ByteArrayComparer.Value);

                    foreach (var l in lines
                        // PERF: this Distinct() represents a stage 3 in time to process.
                                        .DistinctWhenSorted(ByteArrayComparer.Value))
                    {
                        output.Write(l, 0, l.Length);
                        output.WriteByte(newlineByte);
                        totalLinesWritten++;
                    }
                    chSw.Stop();
                    Console.WriteLine(" Done (in {0:N1}ms).", chSw.Elapsed.TotalMilliseconds);

                    chunkNum++;
                }
                output.Flush();
            }
            sortSw.Stop();
            Console.WriteLine("Sorted and removed duplicates, {0:N0} lines remain ({1:N1} sec)", totalLinesWritten, sortSw.Elapsed.TotalSeconds);
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
