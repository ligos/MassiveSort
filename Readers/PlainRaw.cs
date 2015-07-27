using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace MurrayGrant.MassiveSort.Readers
{
    /// <summary>
    /// Class containing methods used to read lines from a text file.
    /// Lines are delimited by white space bytes and processed entirely as raw bytes; no strings allowed!
    /// </summary>
    public class PlainRaw
    {
        public readonly int _ReadBufferSize;
        public readonly int _LineBufferSize;
        public readonly CancellationToken _CancelToken;

        public long BuffersRead { get; private set; }
        public long LinesRead { get; private set; }
        public long ExtraSeeks { get; private set; }
        public long BuffersSkipped { get; private set; }

        public PlainRaw(CancellationToken cancelToken)
        {
            _CancelToken = cancelToken;
            _ReadBufferSize = 64 * 1024;
            _LineBufferSize = 64 * 1024;
        }
        public PlainRaw(CancellationToken cancelToken, int lineBufferSize, int readBufferSize)
        {
            _CancelToken = cancelToken;
            _ReadBufferSize = lineBufferSize;
            _LineBufferSize = readBufferSize;
        }


        public IEnumerable<ByteArraySegment> ReadAll(string fullPath)
        {
            var fileLength = new FileInfo(fullPath).Length;
            return this.ReadAll(fullPath, 0L, fileLength);
        }
        public IEnumerable<ByteArraySegment> ReadAll(string fullPath, long startOffset, long endOffset)
        {
            // Counters - these are set on public properties at the end of the loop.
            long buffersRead = 0;
            long linesRead = 0;
            long extraSeeks = 0;
            long buffersSkipped = 0;

            int bytesInBuffer = 0;
            bool emptyStringFound = false;

            // Line buffer.
            var lineBuffer = new byte[_LineBufferSize];


            // Split the file into chunks.
            using (var stream = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.Read, _ReadBufferSize))
            {
                stream.Position = startOffset;

                // Read the file in buffer sized chunks.
                // This is perf critical code.
                // PERF: about 30% of CPU time is spent in this loop and inlined functions (not in the marked functions).
                while ((bytesInBuffer = ReadLineBuffer(stream, lineBuffer, endOffset)) > 0)
                {
                    if (_CancelToken.IsCancellationRequested) break;
                    buffersRead++;
                    int idx = 0;
                    OffsetAndLength ol;

                    // Ensure an empty string is written if present in the buffer.
                    if (!emptyStringFound && BufferContainsEmptyString(lineBuffer, bytesInBuffer))
                    {
                        linesRead++;
                        yield return new ByteArraySegment();
                    }

                    do
                    {
                        // Find the next word.
                        // PERF: about 20% of CPU time is spent in NextWord().
                        ol = NextWord(lineBuffer, idx);

                        if (ol.Length >= 0 && ol.Offset >= 0)
                        {
                            // The offset and length are valid, yield to consumer.
                            var result = new ByteArraySegment(lineBuffer, ol.Offset, ol.Length);
                            yield return result;

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

            this.BuffersRead = buffersRead;
            this.LinesRead = linesRead;
        }


        public IList<FileChunk> ConvertFilesToSplitChunks(IEnumerable<FileInfo> files, long thresholdSize, long chunkSize)
        {
            // Sort be size, descending, to process larger chunks first.
            // To try to keep more cores busy for longer and not end up with a single large chunk dominating split time.
            var largestToSmallestFiles = files.OrderByDescending(x => x.Length);

            var result = new List<FileChunk>(files.Count());
            foreach (var f in largestToSmallestFiles)
            {
                if (f.Length < thresholdSize)
                    // Trivial case: the whole file is a chunk.
                    result.Add(new FileChunk(f, 0, 0L, f.Length));
                else
                {
                    // Large file: need to split into chunks on line boundaries.
                    using (var fs = new FileStream(f.FullName, FileMode.Open, FileAccess.Read, FileShare.None))
                    {
                        int chunkNum = 1;
                        do
                        {
                            long startOffset = fs.Position;
                            if (fs.Position + chunkSize >= fs.Length)
                            {
                                // Last chunk.
                                result.Add(new FileChunk(f, chunkNum, startOffset, fs.Length));
                                break;
                            }
                            fs.Seek(chunkSize, SeekOrigin.Current);

                            // Find a newline character to end the chunk on.
                            var b = (byte)fs.ReadByte();
                            while (!(b == Constants.NewLineAsByte || b == Constants.NewLineAsByteAlt) && fs.Position != 1)
                            {
                                fs.Seek(-2, SeekOrigin.Current);
                                b = (byte)fs.ReadByte();
                            }
                            long endOffset = fs.Position;

                            // Record the chunk.
                            result.Add(new FileChunk(f, chunkNum, startOffset, endOffset));

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


        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private bool BufferContainsEmptyString(byte[] buf, int len)
        {
            for (int i = 1; i < len; i++)
            {
                if ((buf[i - 1] == Constants.NewLineAsByte || buf[i - 1] == Constants.NewLineAsByteAlt)
                    && (buf[i] == Constants.NewLineAsByte || buf[i] == Constants.NewLineAsByteAlt))
                    return true;
            }
            return false;
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private int ReadLineBuffer(FileStream stream, byte[] buf, long endOffset)
        {
            var maxToRead = buf.Length;
            var fileRemaining = endOffset - stream.Position;
            if (fileRemaining < maxToRead)
                maxToRead = (int)fileRemaining;

            int bytesRead = stream.Read(buf, 0, maxToRead);
            if (bytesRead <= 0)
                // End of file / chunk.
                return 0;

            if (bytesRead < buf.Length)
            {
                // Any left over space in the buffer is filled with new line characters.
                // These will be skipped in NextWord().
                for (int i = bytesRead; i < buf.Length; i++)
                    buf[i] = Constants.NewLineAsByte;
            }
            return bytesRead;
        }


        private OffsetAndLength NextWord(byte[] buf, int startIdx)
        {
            if (startIdx >= buf.Length)
                // Past the end of the buffer.
                return new OffsetAndLength(-1, -1);

            // Ensure we aren't starting on a newline.
            if (buf[startIdx] == Constants.NewLineAsByte || buf[startIdx] == Constants.NewLineAsByteAlt)
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
                if (buf[i] != Constants.NewLineAsByte && buf[i] != Constants.NewLineAsByteAlt)
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
                if (buf[i] == Constants.NewLineAsByte || buf[i] == Constants.NewLineAsByteAlt)
                    return i;
            }
            return -1;
        }
    }
}
