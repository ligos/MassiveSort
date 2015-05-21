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
        public override int GetHashCode()
        {
            return typeof(OffsetAndLength).GetHashCode()
                ^ (this.Offset.GetHashCode() << 3)
                ^ (this.Length.GetHashCode() << 6);
        }
    }
}
