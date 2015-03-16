using System;
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
            return memcmp(first, second, first.Length) == 0;
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
            //		if (first == null)
            //			return 1;
            //		if (second == null)
            //			return -1;

            return memcmp(first, second, first.Length);
        }

        [System.Runtime.InteropServices.DllImport("msvcrt.dll", CallingConvention = System.Runtime.InteropServices.CallingConvention.Cdecl)]
        [System.Security.SuppressUnmanagedCodeSecurity]
        static extern int memcmp(byte[] b1, byte[] b2, long count);
    }
}
