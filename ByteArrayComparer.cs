﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort
{
    public class ByteArrayComparer : IComparer<byte[]>, IEqualityComparer<byte[]>
    {
        public static readonly ByteArrayComparer Value = new ByteArrayComparer();

        public bool Equals(byte[] first, byte[] second)
        {
            //		if (Object.ReferenceEquals(first, second))
            //			return true;
            //		if (first == null && second == null)
            //			return true;
            //		if (second == null || first == null)
            //			return false;
            if (first.Length != second.Length)
                return false;

            // http://stackoverflow.com/a/1445405
            return memcmp(first, second, new UIntPtr((uint)first.Length)) == 0;
        }

        public int GetHashCode(byte[] bytes)
        {
            int result = typeof(byte[]).GetHashCode();
            if (bytes == null)
                return result;

            int shift = 0;
            for (int i = 0; i < bytes.Length; i++)
            {
                result = result ^ (bytes[i] << shift);
                shift += 8;
                if (shift > 24)
                    shift = 0;
            }
            return result;
        }

        public int Compare(byte[] first, byte[] second)
        {
            // See also http://stackoverflow.com/questions/3000803/how-to-call-memcmp-on-two-parts-of-byte-with-offset

            //		if (first == null)
            //			return 1;
            //		if (second == null)
            //			return -1;

            if (first.Length == second.Length)
                // Same length: just return memcmp() result.
                return memcmp(first, second, new UIntPtr((uint)first.Length));
            else 
            {
                // Different length is more of a pain.
                // Make sure we only compare common length parts.
                var shortestLen = Math.Min(first.Length, second.Length);
                var cmp = memcmp(first, second, new UIntPtr((uint)shortestLen));
                if (cmp != 0)
                    // The common length differs: just return memcmp() result;
                    return cmp;
                else 
                    // Common length is identical: longer comes after shorter.
                    // Note the subtraction can break if our difference is Int32.MaxValue or Int32.MinValue - I'm assuming that's not the case.
                    return first.Length - second.Length;
            }
        }

        [System.Runtime.InteropServices.DllImport("msvcrt.dll", CallingConvention = System.Runtime.InteropServices.CallingConvention.Cdecl)]
        [System.Security.SuppressUnmanagedCodeSecurity]
        static extern int memcmp(byte[] b1, byte[] b2, UIntPtr count);
    }
}
