using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace MurrayGrant.MassiveSort
{
    public sealed class ShardFile : IDisposable
    {
        // The Name property of the FileStream class isn't a simple field access, but does a security check as well.
        // This eventually takes out some locks in native code, which can (and does) deadlock with 4+ CPUs.

        public readonly string FullPath;
        public readonly string Name;
        public readonly FileStream Stream;

        public ShardFile(string fullPath, int bufferSize)
        {
            this.FullPath = fullPath;
            this.Name = Path.GetFileName(fullPath);
            this.Stream = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, bufferSize);
        }

        public void Dispose()
        {
            try
            {
                this.Stream.Dispose();
            }
            catch (ObjectDisposedException) { }
        }

    }
}
