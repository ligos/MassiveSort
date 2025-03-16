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
        {
            return String.Format("{0:N0} lines, {1:N1} MB", LineIndex.Length, Data.Length / Constants.OneMbAsDouble);
        }

        public void Dispose()
        {
            this.Data.Dispose();
            // Perhaps in future: this.LineIndex.Dispose();
        }
    }
}
