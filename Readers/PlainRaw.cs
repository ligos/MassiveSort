using System;
using System.Buffers;
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
        public readonly CancellationToken _CancelToken;

        public long BuffersRead { get; private set; }
        public long LinesRead { get; private set; }
        public long ExtraSeeks { get; private set; }
        public long BuffersSkipped { get; private set; }
        public long LinesSkipped { get; private set; }

        public PlainRaw(CancellationToken cancelToken, int readBufferSize = 64 * 1024)
        {
            _CancelToken = cancelToken;
            _ReadBufferSize = 64 * 1024;
        }

        public IEnumerable<ReadOnlyMemory<byte>> ReadAll(Memory<byte> buffer, string fullPath)
        {
            var fileLength = new FileInfo(fullPath).Length;
            return this.ReadAll(buffer, fullPath, 0L, fileLength);
        }
        public IEnumerable<ReadOnlyMemory<byte>> ReadAll(Memory<byte> buffer, string fullPath, long startOffset, long endOffset)
        {
            if (startOffset > endOffset)
                throw new ArgumentOutOfRangeException("startOffset", startOffset, "Start must be before End.");

            // Counters - these are set on public properties at the end of the loop.
            long buffersRead = 0;
            long linesRead = 0;
            long extraSeeks = 0;
            long buffersSkipped = 0;
            long linesSkipped = 0;

            int bytesInBuffer = 0;
            bool emptyStringFound = false;
            bool isReadingLongLine = false;

            // Split the file into chunks.
            using (var stream = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.Read, _ReadBufferSize))
            {
                stream.Position = startOffset;

                // Read the file in buffer sized chunks.
                // This is perf critical code.
                // PERF: about 30% of CPU time is spent in this loop and inlined functions (not in the marked functions).
                while ((bytesInBuffer = ReadLineBuffer(stream, buffer.Span, endOffset)) > 0)
                {
                    if (_CancelToken.IsCancellationRequested) break;
                    ++buffersRead;
                    int idx = 0;
                    OffsetAndLength ol;

                    // Ensure an empty string is written if present in the buffer.
                    if (!emptyStringFound && BufferContainsEmptyString(buffer.Span))
                    {
                        emptyStringFound = true;
                        ++linesRead;
                        yield return Memory<byte>.Empty;
                    }

                    do
                    {
                        // Find the next word.
                        // PERF: about 20% of CPU time is spent in NextWord().
                        ol = NextWord(buffer.Span, idx);

                        if (ol.Length >= 0 && ol.Offset >= 0 && !isReadingLongLine)
                        {
                            // The offset and length are valid, yield to consumer.
                            ReadOnlyMemory<byte> result = buffer.Slice(ol.Offset, ol.Length);
                            ++linesRead;
                            yield return result;

                            idx = ol.Offset + ol.Length + 1;       // Assume at least one new line after the word.
                        }
                        else if (ol.Length >= 0 && ol.Offset >= 0 && isReadingLongLine)
                        {
                            // Reached the end of a long line.
                            // Don't return anything, but keep reading additional lines.
                            ++linesRead;
                            ++linesSkipped;
                            isReadingLongLine = false;
                            idx = ol.Offset + ol.Length + 1;       // Assume at least one new line after the word.
                        }
                        else
                        {
                            // Can't process this word because we hit the end of the buffer.

                            if (idx == 0)
                            {
                                // Skip this line, because it did not fit entirely in the buffer.
                                ++buffersSkipped;
                                isReadingLongLine = true;
                            }
                            else if (ol.Offset == -1)
                            {
                                // Got to the end of the line without finding the start of a word: no additional seek is required (no-op).
                            }
                            else if (ol.Length == -1)
                            {
                                // The buffer splits the word: seek backwards in the file slightly so the next buffer is at the start of the word.
                                stream.Position = stream.Position - (buffer.Length - ol.Offset);
                                ++extraSeeks;
                            }
                        }
                    } while (ol.Length >= 0 && ol.Offset >= 0);      // End of the buffer: read the next one.
                }
            }

            this.BuffersRead = buffersRead;
            this.LinesRead = linesRead;
            this.ExtraSeeks = extraSeeks;
            this.BuffersSkipped = buffersSkipped;
            this.LinesSkipped = linesSkipped;
        }


        public IList<FileChunk> ConvertFilesToSplitChunks(IEnumerable<FileInfo> files, long thresholdSize, long chunkSize, int lineBufferSize)
        {
            if (thresholdSize <= 0L)
                throw new ArgumentOutOfRangeException("thresholdSize", thresholdSize, "Threshold must be greater than zero.");
            if (chunkSize <= 0L)
                throw new ArgumentOutOfRangeException("chunkSize", chunkSize, "Chunk Size must be greater than zero.");
            if (thresholdSize <= chunkSize)
                throw new ArgumentOutOfRangeException("thresholdSize", thresholdSize, "Threshold must be greater than Chunk Size.");
            if (lineBufferSize <= 0L)
                throw new ArgumentOutOfRangeException("lineBufferSize", lineBufferSize, "Line Buffer must be greater than zero.");

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
                            int lineLength = 0;  // If it turns out cannot find a newline in a line buffer length, we will give up.
                            var b = (byte)fs.ReadByte();
                            while (!(b == Constants.NewLineAsByte || b == Constants.NewLineAsByteAlt) && fs.Position != 1 && lineLength < lineBufferSize)
                            {
                                fs.Seek(-2, SeekOrigin.Current);
                                lineLength += 2;
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
        private bool BufferContainsEmptyString(ReadOnlySpan<byte> buf)
        {
            for (int i = 1; i < buf.Length; i++)
            {
                if (
                    // \n\n
                    (buf[i - 1] == Constants.NewLineAsByte && buf[i] == Constants.NewLineAsByte)
                    // \r\r
                    || (buf[i - 1] == Constants.NewLineAsByteAlt && buf[i] == Constants.NewLineAsByteAlt)
                    // \r\n\r\n
                    || (i >= 4 && buf[i - 3] == Constants.NewLineAsByteAlt && buf[i - 2] == Constants.NewLineAsByte && buf[i - 1] == Constants.NewLineAsByteAlt && buf[i] == Constants.NewLineAsByte)
                    // \n\r\n\r
                    || (i >= 4 && buf[i - 3] == Constants.NewLineAsByte && buf[i - 2] == Constants.NewLineAsByteAlt && buf[i - 1] == Constants.NewLineAsByte && buf[i] == Constants.NewLineAsByteAlt)
                )
                    return true;
            }
            return false;
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private int ReadLineBuffer(FileStream stream, Span<byte> buf, long endOffset)
        {
            var maxToRead = buf.Length;
            var fileRemaining = endOffset - stream.Position;
            if (fileRemaining < maxToRead)
                maxToRead = (int)fileRemaining;
            var readBuf = buf.Slice(0, maxToRead);

            int bytesRead = stream.Read(readBuf);
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


        private OffsetAndLength NextWord(ReadOnlySpan<byte> buf, int startIdx)
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
        private int NextNonNewlineInBuffer(ReadOnlySpan<byte> buf, int startIdx)
        {
            for (int i = startIdx; i < buf.Length; i++)
            {
                if (buf[i] != Constants.NewLineAsByte && buf[i] != Constants.NewLineAsByteAlt)
                    return i;
            }
            return -1;
        }
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private int NextNewlineInBuffer(ReadOnlySpan<byte> buf, int startIdx)
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
