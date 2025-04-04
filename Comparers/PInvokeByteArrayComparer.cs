﻿// Copyright 2015 Murray Grant
//
//    Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort.Comparers
{
    public class PInvokeByteArrayComparer : IComparer<ReadOnlyMemory<byte>>, IEqualityComparer<ReadOnlyMemory<byte>>
    {
        public static readonly PInvokeByteArrayComparer Value = new PInvokeByteArrayComparer();

        public bool Equals(ReadOnlyMemory<byte> firstMem, ReadOnlyMemory<byte> secondMem)
        {
            var first = firstMem.Span;
            var second = secondMem.Span;

            if (first == second)
                return true;
            if (first.Length != second.Length)
                return false;

            // http://stackoverflow.com/a/1445405
            return memcmp(first, second, new UIntPtr((uint)first.Length)) == 0;
        }

        public int GetHashCode(ReadOnlyMemory<byte> memory)
        {
            var bytes = memory.Span;

            var result = 0;
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

        public int Compare(ReadOnlyMemory<byte> firstMem, ReadOnlyMemory<byte> secondMem)
        {
            // See also http://stackoverflow.com/questions/3000803/how-to-call-memcmp-on-two-parts-of-byte-with-offset

            var first = firstMem.Span;
            var second = secondMem.Span;

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
        static extern int memcmp(ReadOnlySpan<byte> b1, ReadOnlySpan<byte> b2, UIntPtr count);
    }
}
