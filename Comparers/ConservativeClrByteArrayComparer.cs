using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort.Comparers
{
    public class ConservativeClrByteArrayComparer : IComparer<byte[]>, IEqualityComparer<byte[]>
    {
        public static readonly ConservativeClrByteArrayComparer Value = new ConservativeClrByteArrayComparer();

        public bool Equals(byte[] first, byte[] second)
        {
            if (Object.ReferenceEquals(first, second))
                return true;
            if (first == null && second == null)
                return true;
            if (second == null || first == null)
                return false;
            if (first.Length != second.Length)
                return false;

            for (int i = 0; i < first.Length; i++)
            {
                if (first[i] != second[i])
                    return false;
            }
            return true;
        }

        public int GetHashCode(byte[] bytes)
        {
            int result = typeof(byte[]).GetHashCode();
            if (bytes == null)
                return result;

            int shift = 0;
            for (int i = 0; i < bytes.Length; i++)
            {
                result = result ^ (bytes[i] << shift);
                shift += 8;
                if (shift > 24)
                    shift = 0;
            }
            return result;
        }

        public int Compare(byte[] first, byte[] second)
        {
            if (first == null)
                return 1;
            if (second == null)
                return -1;

            if (first.Length == second.Length)
                // Same length: just return the comparison result.
                return this.CompareToLength(first, second, first.Length);
            else
            {
                // Different length is more of a pain.
                // Make sure we only compare common length parts.
                var shortestLen = Math.Min(first.Length, second.Length);
                var cmp = this.CompareToLength(first, second, shortestLen);
                if (cmp != 0)
                    // The common length differs: just return comparison result;
                    return cmp;
                else
                    // Common length is identical: longer comes after shorter.
                    // Note the subtraction can break if our difference is Int32.MaxValue or Int32.MinValue - I'm assuming that's not the case.
                    return first.Length - second.Length;
            }
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private int CompareToLength(byte[] first, byte[] second, int len)
        {
            // Assume that len <= first.Length and len <= second.Length
            for (int i = 0; i < len; i++)
            {
                var compareResult = first[i].CompareTo(second[i]);
                // Finish early if we find a difference.
                if (compareResult != 0)
                    return compareResult;
            }
            // Arrays are equal (at least to the length specified).
            return 0;
        }
    }
}
