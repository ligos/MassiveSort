using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace MurrayGrant.MassiveSort
{
    public static class Helpers
    {
        public static IEnumerable<string> YieldLines(this FileInfo fi, Encoding enc, int bufferSize)
        {
            using (var stream = new FileStream(fi.FullName, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize))
            {
                foreach (var l in YieldLines(stream, enc))
                {
                    yield return l;
                }
            }
        }
        public static IEnumerable<string> YieldLines(this Stream stream, Encoding enc)
        {
            using (var reader = new StreamReader(stream, enc))
            {
                while (!reader.EndOfStream)
                {
                    yield return reader.ReadLine();
                }
            }
        }

        public static string GetBaseTempFolder()
        {
            return Path.Combine(Environment.GetEnvironmentVariable("TEMP"), "MassiveSort");
        }

        public static IEnumerable<byte[]> YieldLinesAsByteArray(this FileInfo fi, int streamBufferSize, int lineBufferSize)
        {
            using (var stream = new FileStream(fi.FullName, FileMode.Open, FileAccess.Read, FileShare.Read, streamBufferSize))
            {
                var buf = new byte[lineBufferSize];		// Max line length in bytes.
                int b = 0, i = 0;
                bool lastByteWasNewLine = false;
                while ((b = stream.ReadByte()) != -1)
                {
                    if (lastByteWasNewLine && (b == 0x0a || b == 0x0d))
                        continue;

                    // Look for an end of line or null byte.
                    if (i < buf.Length && !(b == 0x0a || b == 0x0d))
                    {
                        // Not found: keep looking.
                        buf[i] = (byte)b;
                        i++;
                        lastByteWasNewLine = false;
                    }
                    else
                    {
                        // Found: copy to new buffer and yield.
                        var result = new byte[i];
                        for (int j = 0; j < result.Length; j++)
                            result[j] = buf[j];
                        yield return result;

                        i = 0;
                        lastByteWasNewLine = true;
                    }
                }
            }
        }


        /// <summary>
        /// Hex string lookup table.
        /// </summary>
        private static readonly string[] HexStringTable = new string[]
        {
            // https://blogs.msdn.com/b/blambert/archive/2009/02/22/blambert-codesnip-fast-byte-array-to-hex-string-conversion.aspx?Redirected=true
            "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0A", "0B", "0C", "0D", "0E", "0F",
            "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1A", "1B", "1C", "1D", "1E", "1F",
            "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B", "2C", "2D", "2E", "2F",
            "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3A", "3B", "3C", "3D", "3E", "3F",
            "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4A", "4B", "4C", "4D", "4E", "4F",
            "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5A", "5B", "5C", "5D", "5E", "5F",
            "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6A", "6B", "6C", "6D", "6E", "6F",
            "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7A", "7B", "7C", "7D", "7E", "7F",
            "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8A", "8B", "8C", "8D", "8E", "8F",
            "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9A", "9B", "9C", "9D", "9E", "9F",
            "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "AA", "AB", "AC", "AD", "AE", "AF",
            "B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "BA", "BB", "BC", "BD", "BE", "BF",
            "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "CA", "CB", "CC", "CD", "CE", "CF",
            "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "DA", "DB", "DC", "DD", "DE", "DF",
            "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "EA", "EB", "EC", "ED", "EE", "EF",
            "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF"
        };
        public static string ToHexString(this byte[] bytes)
        {
            return ToHexString(bytes, bytes.Length);
        }
        public static string ToHexString(this byte[] bytes, int count)
        {
            var length = bytes.Length < count ? bytes.Length : count;
            switch (length)
            {
                // Optimised for short strings.
                case 0:
                    return "";
                case 1:
                    return HexStringTable[bytes[0]];
                case 2:
                    return HexStringTable[bytes[0]] + HexStringTable[bytes[1]];
                case 3:
                    return HexStringTable[bytes[0]] + HexStringTable[bytes[1]] + HexStringTable[bytes[2]];
                case 4:
                    return (HexStringTable[bytes[0]] + HexStringTable[bytes[1]]) + (HexStringTable[bytes[2]] + HexStringTable[bytes[3]]);
                default:
                    var sb = new StringBuilder();
                    for (int i = 0; i < length; i++)
                        sb.Append(HexStringTable[bytes[i]]);
                    return sb.ToString();
            }
        }


        public static IEnumerable<T> DistinctWhenSorted<T>(this OrderedParallelQuery<T> collection, IEqualityComparer<T> comparer)
        {
            T previousItem = default(T);
            bool first = true;
            foreach (var item in collection)
            {
                if (!first && !comparer.Equals(previousItem, item))
                {
                    yield return item;
                }
                previousItem = item;
                first = false;
            }
        }
        public static IEnumerable<T> DistinctWhenSorted<T>(this IOrderedEnumerable<T> collection, IEqualityComparer<T> comparer)
        {
            T previousItem = default(T);
            bool first = true;
            foreach (var item in collection)
            {
                if (!first && !comparer.Equals(previousItem, item))
                {
                    yield return item;
                }
                previousItem = item;
                first = false;
            }
        }
        public static IEnumerable<T> DistinctWhenSorted<T>(this IEnumerable<T> collection, IEqualityComparer<T> comparer)
        {
            T previousItem = default(T);
            bool first = true;
            foreach (var item in collection)
            {
                if (!first && !comparer.Equals(previousItem, item))
                {
                    yield return item;
                }
                previousItem = item;
                first = false;
            }
        }
    }
}
