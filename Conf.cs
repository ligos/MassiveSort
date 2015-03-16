using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace MurrayGrant.MassiveSort
{
    class Conf
    {
        // TODO: Move these to program arguments / config file.

        public readonly int MaxSortSize = 128 * 1024 * 1024;
        public readonly int ReadBufferSize = 1024 * 1024;
        public readonly int SplitBufferSize = 128 * 1024;
        public readonly int FinalOutputBufferSize = 1024 * 1024;


        public readonly DirectoryInfo RootDir = new DirectoryInfo(@"M:\wordlists\mergeTest.medium");
        public readonly FileInfo OutputFile = new FileInfo(@"M:\wordlists\mergeTest.medium.mg.txt");
        public readonly string TempFolder = new DirectoryInfo(@"C:\Users\Murray\Downloads\tmp").FullName;       // Surprisingly, FullName occupies ~50% of CPU in a hot loop!
        public readonly Encoding InputEncoding = Encoding.GetEncoding(1252);
        public readonly Encoding OutputEncoding = new UTF8Encoding(false);
        public readonly int DegreeOfParallelism = Environment.ProcessorCount;

    }
}
