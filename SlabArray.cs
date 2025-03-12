using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort
{
    internal sealed class SlabArray : IDisposable
    {
        readonly List<IMemoryOwner<byte>> _Slabs = new List<IMemoryOwner<byte>>();

        public ushort SlabCount => (ushort)_Slabs.Count;
        public long Length => _Slabs.Sum(x => (long)x.Memory.Length);

        public void AddSlab(IMemoryOwner<byte> slab)
        {
            _Slabs.Add(slab);
        }

        public ReadOnlySpan<byte> GetSpan(SlabIndex idx)
            => _Slabs[idx.SlabNumber].Memory.Span.Slice(idx.Offset, idx.Length);

        public ReadOnlyMemory<byte> GetMemory(SlabIndex idx)
            => _Slabs[idx.SlabNumber].Memory.Slice(idx.Offset, idx.Length);

        public void Dispose()
        {
            foreach (var s in _Slabs)
                s.Dispose();
            _Slabs.Clear();
        }
    }

    internal readonly struct SlabIndex : IEquatable<SlabIndex>
    {
        public SlabIndex(ushort slabNumber, int offset, int length)
        {
            if (length > 0x0001_ffff)
                throw new ArgumentException(nameof(length), "Length must be less than or equal to 131071, but was " + length);

            _SlabNumber = slabNumber;
            _Length = (ushort)length;
            _Offset = (uint)offset + (((uint)length & 0x0001_0000u) << 15);
        }

        readonly UInt16 _SlabNumber;
        // Top bit of Offset is added to Length. So there are 17 bits of Length and 31 bits of Offset.
        readonly UInt16 _Length;
        readonly UInt32 _Offset;

        public ushort SlabNumber => _SlabNumber;

        public int Length 
            => (int)(_Length | ((_Offset & 0x8000_0000u) >> 15));

        public int Offset
            => (int)(_Offset & 0x7fff_ffffu);

        public override bool Equals([NotNullWhen(true)] object obj)
            => obj is SlabIndex x
            && Equals(x);

        public bool Equals(SlabIndex other)
            => other._SlabNumber == _SlabNumber
            && other._Offset == _Offset
            && other._Length == _Length;

        public override int GetHashCode()
            => HashCode.Combine(_SlabNumber, _Offset, _Length);

        public override string ToString()
            => $"Slab# {SlabNumber}, Offset: {Offset}, Length: {Length}";
    }
}
