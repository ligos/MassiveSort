﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort
{
    public static class Constants
    {
        public static readonly double OneMbAsDouble = 1024.0 * 1024.0;
        public static readonly double OneGbAsDouble = 1024.0 * 1024.0 * 1024.0;

        public static readonly Encoding Utf8NoBom = new UTF8Encoding(false);

        public const char NewLine = '\n';
        public const byte NewLineAsByte = (byte)'\n';
        public const char NewLineAlt = '\r';
        public const byte NewLineAsByteAlt = (byte)'\r';

        public static readonly ReadOnlyMemory<byte> DollarHexPrefix = "$HEX["u8.ToArray();
        public const byte DollarHexSuffixAsByte = (byte)']';

        public static readonly ReadOnlyMemory<byte> DefaultWhitespace = new byte[] { 0x20, 0x09, 0x0b };
    }
}
