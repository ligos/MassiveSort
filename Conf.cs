using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using CommandLine;

namespace MurrayGrant.MassiveSort
{
    public interface ICmdVerb
    {
        bool IsValid();
        string GetValidationError();

        void Do();
    }

    public class Conf
    {
        public Conf()
        {
        }

        [VerbOption("merge")]
        public MurrayGrant.MassiveSort.Actions.MergeConf MergeOptions { get; set; }
        [VerbOption("cleantemp")]
        public MurrayGrant.MassiveSort.Actions.CleanTempConf CleanTempOptions { get; set; }


        [HelpVerbOption()]
        public string GetUsage(string verb)
        {
            return CommandLine.Text.HelpText.AutoBuild(this, verb);
        }
    }

    public abstract class CommonConf
    {
        [Option("debug")]
        public bool Debug { get; set; }
    }

}
