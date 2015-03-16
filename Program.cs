using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;

namespace MurrayGrant.MassiveSort
{
    class Program
    {
        const byte newlineByte = (byte)'\n';
        const double oneMbAsDouble = 1024.0 * 1024.0;

        static void Main(string[] args)
        {
            var conf = new Conf();

            var sw = Stopwatch.StartNew();

            // Stage 0: figure out what we need to do.

            // A bit of house cleaning.
            if (Directory.Exists(conf.TempFolder)) 
                Directory.Delete(conf.TempFolder, true);
            Directory.CreateDirectory(conf.TempFolder);

            
            // Snapshot the file's we'll be working with.
            var filesToProcess = conf.RootDir.EnumerateFiles("*", SearchOption.AllDirectories).ToList();


            // Stage 1: split / shard files into smaller chunks.
            var toSort = SplitFiles(filesToProcess, conf);


            // Stage 2: sort and merge the files.
            SortFiles(toSort, conf);
            sw.Stop();

            Console.WriteLine("Total time: {0}", sw.Elapsed);
        }

        private static IEnumerable<FileInfo> SplitFiles(IEnumerable<FileInfo> files, Conf conf)
        {
            var splitSw = new Stopwatch();
            var totalLength = files.Sum(fi => fi.Length);
            var splitFiles = new Dictionary<string, FileStream>(StringComparer.Ordinal);
            long totalLines = 0L, totalLinesProcessed = 0L;
            if (totalLength > conf.MaxSortSize)
            {
                // Stage 1: read all files and split lines into buckets.
                // This only happens if we have too many files.
                int shardSize = 1;
                long shardThreshold = Int64.MaxValue;
                splitSw.Start();
                var thisRoundOfSplitting = files.AsEnumerable();
                var splitFlushSw = new Stopwatch();
                try
                {
                    do
                    {
                        Console.WriteLine("Splitting files (round {0})...", shardSize);
                        foreach (var f in files)
                        {
                            var lines = SplitFile(splitFiles, f, shardSize, conf);
                            totalLinesProcessed += lines;
                            if (shardSize == 1)
                                totalLines += lines;
                        }
                        shardSize++;
                        shardThreshold = conf.MaxSortSize;
                        thisRoundOfSplitting = new DirectoryInfo(conf.TempFolder).EnumerateFiles("*", SearchOption.AllDirectories).Where(f => f.Length > conf.MaxSortSize);
                    } while (thisRoundOfSplitting.Any());
                }
                finally
                {
                    splitFlushSw.Start();
                    foreach (var fs in splitFiles.Values)
                    {
                        fs.Flush();
                        fs.Close();
                    }
                    splitFlushSw.Stop();
                }
                splitSw.Stop();
                Console.WriteLine("Split {0:N0} file(s) with {1:N0} lines in {2:N1}ms ({3:N1}ms in flush)", files.Count(), totalLines, splitSw.Elapsed.TotalMilliseconds, splitFlushSw.Elapsed.TotalMilliseconds);
                return new DirectoryInfo(conf.TempFolder).EnumerateFiles("*", SearchOption.AllDirectories).OrderBy(f => f.Name).ToList();
            }
            else
            {
                Console.WriteLine("Skipping split phase as there are less than {0:N0}MB of files ({1:N2}MB).", conf.MaxSortSize / oneMbAsDouble, totalLength / oneMbAsDouble);
                return files;
            }
        }

        private static long SplitFile(Dictionary<string, FileStream> splitFiles, FileInfo fi, int shardSize, Conf conf)
        {
            long linesRead = 0;
            Console.Write("Splitting '{0}'...", fi.Name);

            // Split the file into chunks.
            foreach (var l in fi.YieldLinesAsByteArray((int)Math.Min((long)conf.ReadBufferSize, fi.Length)))
            {
                // Determine the first character(s) to shard into separate files.
                string shardAsHex = null;
                if (l.Length == 0)
                    shardAsHex = "!";		// Empty string is a pain because you can't shard a zero length thing.
                else
                    shardAsHex = l.ToHexString(shardSize);

                // Create a file, if it doesn't already exist.
                FileStream stream;
                if (!splitFiles.TryGetValue(shardAsHex, out stream))
                {
                    var file = Path.Combine(conf.TempFolder, shardAsHex) + ".txt";
                    stream = new FileStream(file, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, conf.SplitBufferSize);
                    splitFiles.Add(shardAsHex, stream);
                }

                // Write the line to the file.
                stream.Write(l, 0, l.Length);
                stream.WriteByte(newlineByte);

                // Increment counter.
                linesRead++;
            }

            Console.WriteLine(" Done.");
            return linesRead;
        }


        private static void SortFiles(IEnumerable<FileInfo> toSort, Conf conf)
        {
            if (conf.OutputFile.Exists) 
                conf.OutputFile.Delete();
            long totalLinesWritten = 0;
            var sortSw = Stopwatch.StartNew();
            using (var output = new FileStream(conf.OutputFile.FullName, FileMode.Create, FileAccess.Write, FileShare.None, conf.FinalOutputBufferSize))
            {
                // Split into large chunks to sort.
                var cumulativeSize = 0L;
                var sortChunks = new List<List<FileInfo>>();
                var chunk = new List<FileInfo>();
                foreach (var fi in toSort)
                {
                    if (cumulativeSize + fi.Length > conf.MaxSortSize)
                    {
                        sortChunks.Add(chunk);
                        chunk = new List<FileInfo>();
                        cumulativeSize = 0L;
                    }
                    chunk.Add(fi);
                    cumulativeSize += fi.Length;
                }
                sortChunks.Add(chunk);
                Console.WriteLine("There are {0:N0} chunk(s) to sort.", sortChunks.Count);

                // Now sort each chunk.
                int chunkNum = 1;
                foreach (var ch in sortChunks)
                {
                    Console.Write("Sorting chunk {0:N0} ({1} - {2})...", chunkNum, ch.First().Name, ch.Last().Name);
                    var lines = ch.SelectMany(f => f.YieldLinesAsByteArray((int)Math.Min(f.Length, conf.ReadBufferSize))).ToList();
                    var partitionedLines = System.Collections.Concurrent.Partitioner.Create(lines, true);

                    foreach (var l in partitionedLines
                                        .AsParallel().WithDegreeOfParallelism(conf.DegreeOfParallelism)
                                        .OrderBy(l => l, ByteArrayComparer.Value)
                                        // PERF: this Distinct() represents a stage 3 in time to process.
                                        //       it is processed on a single thread as well.
                                        .DistinctWhenSorted(ByteArrayComparer.Value))
                    {
                        output.Write(l, 0, l.Length);
                        output.WriteByte(newlineByte);
                        totalLinesWritten++;
                    }
                    Console.WriteLine(" Done.");

                    chunkNum++;
                }
                output.Flush();
            }
            sortSw.Stop();
            Console.WriteLine("Sorted {0:N0} lines in time {1:N1}ms", totalLinesWritten, sortSw.Elapsed.TotalMilliseconds);
        }
    }
}
