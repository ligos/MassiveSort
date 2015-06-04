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

namespace MurrayGrant.MassiveSort
{
    public struct OffsetAndLength
    {
        public static readonly OffsetAndLength Empty = new OffsetAndLength(0, 0);

        public readonly Int32 Offset;
        public readonly Int32 Length;

        public OffsetAndLength(Int32 offset, Int32 length)
        {
            this.Offset = offset;
            this.Length = length;
        }


        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;
            if (obj.GetType() != typeof(OffsetAndLength))
                return false;
            return this.Equals((OffsetAndLength)obj);
        }
        public bool Equals(OffsetAndLength other)
        {
            return this.Offset == other.Offset & this.Length == other.Length;
        }
        public static bool operator== (OffsetAndLength ol1, OffsetAndLength ol2)
        {
            return ol1.Equals(ol2);
        }
        public static bool operator!= (OffsetAndLength ol1, OffsetAndLength ol2)
        {
            return !ol1.Equals(ol2);
        }

        public override int GetHashCode()
        {
            return typeof(OffsetAndLength).GetHashCode()
                ^ (this.Offset.GetHashCode() << 3)
                ^ (this.Length.GetHashCode() << 6);
        }
    }
}
