using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using CommandLine;

namespace MurrayGrant.MassiveSort
{
    class Conf
    {
        public Conf()
        {
            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            this.TempFolder = Path.Combine(Environment.GetEnvironmentVariable("TEMP"), "MassiveSort", pid.ToString());
            this.DegreeOfParallelism = Environment.ProcessorCount;      // TODO: default to the number of physical rather than logical cores.

        }

        // TODO: Move these to program arguments / config file.

        public readonly int MaxSortSize = 128 * 1024 * 1024;
        public readonly int ReadBufferSize = 1024 * 1024;
        public readonly int SplitBufferSize = 128 * 1024;
        public readonly int FinalOutputBufferSize = 1024 * 1024;


        public bool IsValid { get { return !String.IsNullOrEmpty(this.InputRoot) && !String.IsNullOrEmpty(this.OutputFile); } }

        // TODO: change this to an array.
        [Option('i', "input")]
        public string InputRoot { get; set; }
        
        [Option('o', "output")]
        public string OutputFile { get; set; }
        
        [Option('t', "tempfolder")]
        public string TempFolder { get; set; }

        public readonly Encoding InputEncoding = Encoding.GetEncoding(1252);
        public readonly Encoding OutputEncoding = new UTF8Encoding(false);
        
        [Option('w', "workers")]
        public int DegreeOfParallelism { get; set; }

    }
}
