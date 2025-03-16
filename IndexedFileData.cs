using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort
{
    public sealed record class IndexedFileData(SlabArray Data, SlabIndex[] LineIndex) : IDisposable
    {
        public override string ToString()
            => $"{LineIndex.Length:N0} lines, {Data.Length / Constants.OneMbAsDouble}MB";

        public void Dispose()
        {
            this.Data.Dispose();
            // Perhaps in future: this.LineIndex.Dispose();
        }
    }
}
