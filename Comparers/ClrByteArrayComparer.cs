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

namespace MurrayGrant.MassiveSort.Comparers
{
    public class ClrByteArrayComparer : IComparer<byte[]>, IEqualityComparer<byte[]>
    {
        public static readonly ClrByteArrayComparer Value = new ClrByteArrayComparer();

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

            // Unroll the loop based on length.
            // Usage of & rather than && should allow all operations to run in parallel, which should be faster on smaller lengths.
            switch(first.Length)
            {
                case 1:
                    return first[0] == second[0];
                case 2:
                    return first[0] == second[0] 
                         & first[1] == second[1];
                case 3:
                    return first[0] == second[0] 
                         & first[1] == second[1]
                         & first[2] == second[2];

                // TODO: perf analysis of BitConverter.
                case 4:
                    return first[0] == second[0]
                         & first[1] == second[1]
                         & first[2] == second[2]
                         & first[3] == second[3];
                case 5:
                    return first[0] == second[0]
                         & first[1] == second[1]
                         & first[2] == second[2]
                         & first[3] == second[3]
                         & first[4] == second[4];
                case 6:
                    return first[0] == second[0]
                         & first[1] == second[1]
                         & first[2] == second[2]
                         & first[3] == second[3]
                         & first[4] == second[4]
                         & first[5] == second[5];
                case 7:
                    return first[0] == second[0]
                         & first[1] == second[1]
                         & first[2] == second[2]
                         & first[3] == second[3]
                         & first[4] == second[4]
                         & first[5] == second[5]
                         & first[6] == second[6];
                case 8:
                    return first[0] == second[0]
                         & first[1] == second[1]
                         & first[2] == second[2]
                         & first[3] == second[3]
                         & first[4] == second[4]
                         & first[5] == second[5]
                         & first[6] == second[6]
                         & first[7] == second[7];

                default:
                    for (int i = 0; i < first.Length; i++)
                    {
                        if (first[i] != second[i])
                            return false;
                    }
                    return true;

            }
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
