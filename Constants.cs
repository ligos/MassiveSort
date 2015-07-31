using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort
{
    public static class Constants
    {
        public static readonly double OneMbAsDouble = 1024.0 * 1024.0;

        public static readonly Encoding Utf8NoBom = new UTF8Encoding(false);

        public const char NewLine = '\n';
        public const byte NewLineAsByte = (byte)'\n';
        public const char NewLineAlt = '\r';
        public const byte NewLineAsByteAlt = (byte)'\r';

        public static readonly byte[] DollarHexPrefix = Encoding.ASCII.GetBytes("$HEX[");
        public static readonly byte[] DollarHexSuffix = Encoding.ASCII.GetBytes("]");

    }
}
