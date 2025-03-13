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
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort.Comparers
{
    /// <summary>
    /// Compares dictionary then length.
    /// </summary>
    public class ClrOffsetDictionaryComparer : IComparer<OffsetAndLength>, IEqualityComparer<OffsetAndLength>
    {
        private readonly ReadOnlyMemory<byte> _Data;

        public ClrOffsetDictionaryComparer(ReadOnlyMemory<byte> data)
        {
            this._Data = data;
        }

        public bool Equals(OffsetAndLength first, OffsetAndLength second)
        {
            if (first.Equals(second))
                return true;
            if (first.Length != second.Length)
                return false;
            var data = this._Data.Span;

            // PERF: tried to unroll this, but it didn't improve performance.
            for (int i = 0; i < first.Length; i++)
            {
                if (data[first.Offset+i] != data[second.Offset+i])
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
            var data = this._Data.Span;

            // PERF: tried to unroll this, but it didn't improve performance.
            for (int i = 0; i < len; i++)
            {
                var compareResult = data[first.Offset + i].CompareTo(data[second.Offset + i]);
                // Finish early if we find a difference.
                if (compareResult != 0)
                    return compareResult;
            }
            // Arrays are equal (at least to the length specified).
            return 0;
        }
    }


    internal class ClrSlabDictionaryComparer(SlabArray data) : IComparer<SlabIndex>, IEqualityComparer<SlabIndex>
    {
        private readonly SlabArray _Data = data;

        public bool Equals(SlabIndex first, SlabIndex second)
        {
            if (first.Equals(second))
                return true;
            if (first.Length != second.Length)
                return false;

            var span1 = this._Data.GetSpan(first);
            var span2 = this._Data.GetSpan(second);
            if (span1.Length != span2.Length)
                return false;

            for (int i = 0; i < span1.Length; i++)
            {
                if (span1[i] != span2[i])
                    return false;
            }
            return true;
        }

        public int GetHashCode(SlabIndex x)
        {
            return x.GetHashCode();
        }

        public int Compare(SlabIndex first, SlabIndex second)
        {
            // PERF: this is the hot method when sorting.
            var span1 = this._Data.GetSpan(first);
            var span2 = this._Data.GetSpan(second);

            if (first.Length == second.Length)
                // Same length: just return the comparison result.
                return this.CompareToLength(span1, span2);
            else
            {
                // Different length is more of a pain.
                // Make sure we only compare common length parts.
                if (span1.Length > span2.Length)
                    span1 = span1[..span2.Length];
                else
                    span2 = span2[..span1.Length];
                var cmp = this.CompareToLength(span1, span2);
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
        private int CompareToLength(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
        {
            Debug.Assert(first.Length == second.Length);
            for (int i = 0; i < first.Length; i++)
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
