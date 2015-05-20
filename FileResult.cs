using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort
{
    public class FileResult
    {
        public FileResult(FileInfo file, long lines)
        {
            this.File = file;
            this.Lines = lines; 
        }

        public readonly FileInfo File;
        public readonly long Lines;
    }
}
