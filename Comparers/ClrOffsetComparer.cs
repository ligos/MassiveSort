﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort.Comparers
{
    public class ClrOffsetComparer : IComparer<OffsetAndLength>, IEqualityComparer<OffsetAndLength>
    {
        private readonly byte[] _Data;

        public ClrOffsetComparer(byte[] data)
        {
            this._Data = data;
        }

        public bool Equals(OffsetAndLength first, OffsetAndLength second)
        {
            if (first.Equals(second))
                return true;
            if (first.Length != second.Length)
                return false;

            // PERF: tried to unroll this, but it didn't improve performance.
            for (int i = 0; i < first.Length; i++)
            {
                if (this._Data[first.Offset+i] != this._Data[second.Offset+i])
                    return false;
            }
            return true;
        }

        public int GetHashCode(OffsetAndLength x)
        {
            return x.GetHashCode();
        }

        public int Compare(OffsetAndLength first, OffsetAndLength second)
        {
            // PERF: this is the hot method when sorting.

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
        private int CompareToLength(OffsetAndLength first, OffsetAndLength second, int len)
        {
            // PERF: tried to unroll this, but it didn't improve performance.
            for (int i = 0; i < len; i++)
            {
                var compareResult = this._Data[first.Offset + i].CompareTo(this._Data[second.Offset + i]);
                // Finish early if we find a difference.
                if (compareResult != 0)
                    return compareResult;
            }
            // Arrays are equal (at least to the length specified).
            return 0;
        }
    }
}