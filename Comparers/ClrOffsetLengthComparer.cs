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
    /// <summary>
    /// Compares length then dictionary order.
    /// </summary>
    public class ClrOffsetLengthComparer : IComparer<OffsetAndLength>, IEqualityComparer<OffsetAndLength>
    {
        private readonly byte[] _Data;

        public ClrOffsetLengthComparer(byte[] data)
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
            
            var cmp = first.Length.CompareTo(second.Length);
            if (cmp != 0)
                // Different length: just return the difference between lengths.
                return cmp;
            // Same length compares actual bytes.
            for (int i = 0; i < first.Length; i++)
            {
                cmp = this._Data[first.Offset + i].CompareTo(this._Data[second.Offset + i]);
                // Finish early if we find a difference.
                if (cmp != 0)
                    return cmp;
            }
            // Arrays are equal.
            return 0;
        }
    }
}
