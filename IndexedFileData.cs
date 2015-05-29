using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort
{
    public sealed class IndexedFileData : IDisposable
    {
        public byte[] Chunk;
        public OffsetAndLength[] LineOffsets;

        public IndexedFileData(byte[] chunk, OffsetAndLength[] offsets)
        {
            this.Chunk = chunk;
            this.LineOffsets = offsets;
        }

        public void Dispose()
        {
            this.Chunk = null;
            this.LineOffsets = null;
        }

        public override string ToString()
        {
            return String.Format("{0:N0} lines, {1:N1} MB", LineOffsets.Length, Chunk.Length / Helpers.OneMbAsDouble);
        }
    }
}
