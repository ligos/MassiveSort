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
    }
}
