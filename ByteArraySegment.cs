using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort
{
    /// <summary>
    /// A similar struct to ArraySegment of T, but without some overheads.
    /// The range checks in the stock ArraySegment prevent the constructor from being inlined.
    /// </summary>
    public struct ByteArraySegment
    {
        public readonly byte[] Array;
        public readonly int Offset;
        public readonly int Count;

        public ByteArraySegment(byte[] array, int offset, int count)
        {
            this.Array = array;
            this.Offset = offset;
            this.Count = count;
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;
            if (obj.GetType() != typeof(ByteArraySegment))
                return false;
            return Equals((ByteArraySegment)obj);
        }
        public bool Equals(ByteArraySegment other)
        {
            return this.Array == other.Array
                && this.Count == other.Count
                && this.Offset == other.Count;
        }
        public override int GetHashCode()
        {
            var result = typeof(ByteArraySegment).GetHashCode() ^ this.Offset ^ this.Count;
            if (this.Array != null)
                result = result ^ this.Array.GetHashCode();
            return result;
        }
        public static bool operator ==(ByteArraySegment first, ByteArraySegment second)
        {
            return first.Equals(second);
        }
        public static bool operator !=(ByteArraySegment first, ByteArraySegment second)
        {
            return !(first == second);
        }
    }
}
