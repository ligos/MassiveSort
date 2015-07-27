using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace MurrayGrant.MassiveSort
{
    public class FileChunk
    {
        public FileChunk(FileInfo fi, int chunkNumber, long startOffset, long endOffset)
        {
            this.FullPath = fi.FullName;
            this.Name = fi.Name;
            this.ChunkNumberInFile = chunkNumber;
            this.StartOffset = startOffset;
            this.EndOffset = endOffset;
        }
        public readonly string FullPath;
        public readonly string Name;
        public readonly int ChunkNumberInFile;
        public string NameForProgress { get { return this.Name + (this.ChunkNumberInFile > 0 ? " #" + this.ChunkNumberInFile.ToString() : ""); } }
        public readonly long StartOffset;
        public readonly long EndOffset;
        public long Length { get { return this.EndOffset - this.StartOffset; } }
    }
}
