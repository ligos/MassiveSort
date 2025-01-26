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
using System.Runtime.InteropServices;

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

        // https://superuser.com/questions/332610/where-is-the-temporary-directory-in-linux
        // https://technet.microsoft.com/en-us/library/cc749104(v=ws.10).aspx
        private static readonly string[] _TempEnvironmentVars = new[] { "TEMP", "TMP", "TMPDIR" };
        public static string GetBaseTempFolder()
        {
            foreach (var env in _TempEnvironmentVars)
            {
                var maybePath = Environment.GetEnvironmentVariable(env);
                if (!String.IsNullOrEmpty(maybePath))
                    return Path.Combine(maybePath, "MassiveSort");
            }
            if (Environment.OSVersion.Platform == PlatformID.Unix)
                return Path.Combine("/tmp", "MassiveSort");
            throw new Exception("Cannot find temporary folder. Tried environment variables: " + String.Join(",", _TempEnvironmentVars));
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

        private static readonly byte[] HexByteTable = new string[] { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f" }.Select(ch => Encoding.ASCII.GetBytes(ch).First()).ToArray();
        public static void WriteHexToArray(byte[] buf, int idx, byte toWrite)
        {
            buf[idx] = HexByteTable[(toWrite & 0x000000f0) >> 4];
            buf[idx+1] = HexByteTable[(toWrite & 0x0000000f)];
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
        public static IEnumerable<T> DistinctWhenSorted<T>(this IEnumerable<T> collection, Func<T, T, bool> comparer)
        {
            T previousItem = default(T);
            bool first = true;
            foreach (var item in collection)
            {
                if (!first && !comparer(previousItem, item))
                {
                    yield return item;
                }
                previousItem = item;
                first = false;
            }
        }

        public static int IndexWhere<T>(this T[] array, Func<T, bool> predicate)
        {
            for (int i = 0; i < array.Length; i++)
            {
                if (predicate(array[i]))
                    return i;
            }
            return -1;
        }

        public static string ToSizedString(this TimeSpan ts)
        {
            ts = new TimeSpan(Math.Abs(ts.Ticks));

            if (ts.TotalSeconds < 1)
                return ts.TotalMilliseconds.ToString("N2") + " ms";
            else if (ts.TotalSeconds < 10)
                return ts.TotalMilliseconds.ToString("N1") + " ms";
            else if (ts.TotalSeconds < 300)
                return ts.TotalSeconds.ToString("N2") + " sec";
            else if (ts.TotalMinutes < 90)
                return ts.TotalMinutes.ToString("N2") + " min";
            else if (ts.TotalHours < 36)
                return ts.TotalHours.ToString("N2") + " hrs";
            else
                return String.Format("{0:N0} day{2}, {1:N1} hrs.", ts.Days, (ts.TotalDays - ts.Days) / 24.0, ts.Days > 1 ? "s" : "");

        }

        public static int PhysicalCoreCount()
        {
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
                return PhysicalCoreCount_Win32();
            else if (Environment.OSVersion.Platform == PlatformID.Unix)
                return PhysicalCoreCount_Unix();
            else
                throw new NotImplementedException("PhysicalCoreCount() is not supported on " + Environment.OSVersion.Platform);
        }
        private static int PhysicalCoreCount_Win32()
        {
            Debug.Assert(RuntimeInformation.IsOSPlatform(OSPlatform.Windows));

            // http://stackoverflow.com/a/2670568
            int coreCount = 0;
            foreach (var item in new System.Management.ManagementObjectSearcher("Select NumberOfCores from Win32_Processor").Get())
            {
                coreCount += int.Parse(item["NumberOfCores"].ToString());
            }
            return coreCount;
        }
        private static int PhysicalCoreCount_Unix()
        {
            // https://stackoverflow.com/questions/6481005/how-to-obtain-the-number-of-cpus-cores-in-linux-from-the-command-line
            // https://unix.stackexchange.com/questions/33450/checking-if-hyperthreading-is-enabled-or-not
            var lines = File.ReadAllLines("/proc/cpuinfo", Encoding.UTF8).Where(l => !String.IsNullOrEmpty(l));
            var cpuCount = lines.Count(l => l.Contains("processor", StringComparison.OrdinalIgnoreCase));
            var hyperthreadingEnabled = lines.Any(l => l.Contains("flags") && l.Contains("ht"));
            if (hyperthreadingEnabled)
                return cpuCount / 2;
            else
                return cpuCount;
        }


        public static bool TryShellExecute(string command, out string result)
        {
            return TryShellExecute(command, "", out result);
        }
        public static bool TryShellExecute(string command, string args, out string result)
        {
            // https://stackoverflow.com/questions/4291912/process-start-how-to-get-the-output
            var si = new System.Diagnostics.ProcessStartInfo();
            si.FileName = command;
            si.Arguments = args;
            si.UseShellExecute = false;
            si.RedirectStandardInput = true;
            si.CreateNoWindow = true;
            using (var proc = new System.Diagnostics.Process())
            {
                proc.StartInfo = si;
                proc.Start();
                proc.WaitForExit(10000);        // 10 second timeout.
                if (proc.ExitCode == 0)
                    result = proc.StandardOutput.ReadToEnd();
                else
                    result = null;
                return (proc.ExitCode == 0);
            }
        }

        public static string ToByteSizedString(this int number, int decimals = 2)
        {
            return ToByteSizedString((long)number, decimals);
        }
        public static string ToByteSizedString(this long number, int decimals = 2)
        {
            if (number < 0)
                throw new ArgumentOutOfRangeException("number", number, "Number must be positive or zero.");

            if (number == 0)
                return "0 bytes";
            else if (number == 1)
                return "1 byte";
            else if (number < 1024L)
                return number.ToString() + " bytes";
            else if (number < 1024L * 1024L)
                return (number / (1024.0)).ToString("0." + new string('#', decimals)) + " KB";
            else if (number < 1024L * 1024L * 1024L)
                return (number / (1024.0 * 1024.0)).ToString("0." + new string('#', decimals)) + " MB";
            else if (number < 1024L * 1024L * 1024L * 1024L)
                return (number / (1024.0 * 1024.0 * 1024.0)).ToString("0." + new string('#', decimals)) + " GB";
            else if (number < 1024L * 1024L * 1024L * 1024L * 1024L)
                return (number / (1024.0 * 1024.0 * 1024.0 * 1024.0)).ToString("0." + new string('#', decimals)) + " TB";
            else if (number < 1024L * 1024L * 1024L * 1024L * 1024L * 1024L)
                return (number / (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0)).ToString("0." + new string('#', decimals)) + " PB";
            else
                throw new ArgumentOutOfRangeException("number", number, "PB are the largest supported unit.");
            
        }

        public static long ParseByteSized(string s)
        {
            long result;
            if (!TryParseByteSized(s, out result))
                throw new FormatException("Unable to parse '" + s + "'. Supported units: B, KB, MB, GB, TB, PB.");
            return result;
        }
        public static bool TryParseByteSized(string s, out long result)
        {
            // Can parse <num>[<unit>].
            // Null and empty string are not allowed.
            // Negatives are not parsed.
            // If no unit is specified, bytes are assumed.
            // Allowed units: B, KB, MB, GB, TB, PB.

            if (String.IsNullOrEmpty(s))
                throw new ArgumentNullException("s");

            // Find numeric part.
            var numIdxStart = 0;
            while (numIdxStart < s.Length && !Char.IsDigit(s, numIdxStart))
                numIdxStart++;
            if (numIdxStart >= s.Length)
            {
                // Unable to find any numbers.
                result = 0L;
                return false;
            }
            var numIdxEnd = numIdxStart;
            while (numIdxEnd < s.Length && (Char.IsDigit(s, numIdxEnd) || s[numIdxEnd] == '.'))     // Not localised.
                numIdxEnd++;
            var numericPart = s.Substring(numIdxStart, numIdxEnd - numIdxStart);
            // Parse numeric part.
            Double num;
            if (!Double.TryParse(numericPart, out num))
            {
                // Can't parse number.
                result = 0L;
                return false;
            }

            // Find units (optional).
            var unitIdxStart = numIdxEnd;
            while (unitIdxStart < s.Length && !Char.IsLetter(s, unitIdxStart))
                unitIdxStart++;
            var unitIdxEnd = unitIdxStart;
            while (unitIdxEnd < s.Length && Char.IsLetter(s, unitIdxEnd))
                unitIdxEnd++;
            var units = "B";     // Default if no units found.
            if (unitIdxStart <= s.Length && unitIdxEnd <= s.Length)
                units = s.Substring(unitIdxStart, unitIdxEnd - unitIdxStart);
            switch(units.ToUpper())
            {
                case "B":
                    result = (long)num;
                    break;
                case "KB":
                    result = (long)(num * 1024.0);
                    break;
                case "MB":
                    result = (long)(num * 1024.0 * 1024.0);
                    break;
                case "GB":
                    result = (long)(num * 1024.0 * 1024.0 * 1024.0);
                    break;
                case "TB":
                    result = (long)(num * 1024.0 * 1024.0 * 1024.0 * 1024.0);
                    break;
                case "PB":
                    result = (long)(num * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
                    break;
                default:
                    throw new FormatException("Unable to parse '" + s + "': unknown unit '" + units + "'.");
            }

            return true;
        }

        public static ICollection<FileInfo> GatherFiles(IEnumerable<string> paths, IEnumerable<string> endingsToExclude)
        {
            var result = paths.SelectMany(i =>
                                Directory.Exists(i) ? new DirectoryInfo(i).EnumerateFiles("*", SearchOption.AllDirectories)
                                   : File.Exists(i) ? new FileInfo[] { new FileInfo(i) }
                                   : new FileInfo[] { }
                        )
                        .Where(x => !endingsToExclude.Any(e => x.Name.EndsWith(e, StringComparison.CurrentCultureIgnoreCase)))
                        .ToList();
            return result;
        }

        public static int ThisOrNextPowerOfTwo(this int n)
        {
            // http://stackoverflow.com/a/12506181
            int power = 1;
            while (power < n)
                power *= 2;
            return power;
        }

        public static bool Contains(this string s, string toFind, StringComparison comparison)
        {
            return s.IndexOf(toFind, 0, comparison) != -1;
        }

        public static T[] RemoveTrailing<T>(this T[] array, Func<T, bool> remove)
        {
            int idx = 0;
            for (int i = array.Length - 1; i >= 0; i--)
            {
                if (!remove(array[i]))
                {
                    idx = i;
                    break;
                }
            }
            return array.Take(idx + 1).ToArray();
        }

        /// <summary>
        /// Gets a string which includes all inner exceptions.
        /// </summary>
        /// <returns></returns>
        public static String ToFullString(this Exception ex)
        {
            var e = ex;
            var result = e.ToString();

            while (e.InnerException != null)
            {
                e = e.InnerException;
                result = result + Environment.NewLine + e.ToString();
            }

            return result;
        }
    }
}
