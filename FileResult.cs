// Copyright 2015 Murray Grant
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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort
{
    public class FileResult
    {
        public FileResult(string path, long length, long lines)
        {
            this.FullPath = path;
            this.Name = Path.GetFileName(path);
            this.Length = length;
            this.Lines = lines; 
        }

        public readonly string FullPath;
        public readonly string Name;
        public readonly long Length;
        public readonly long Lines;

        public FileResult AddLines(long l)
        {
            return new FileResult(this.FullPath, this.Length, this.Lines + l);
        }

        public override string ToString()
            => $"{Name} ({Length / Constants.OneMbAsDouble:N1}MB) - Lines: {Lines:N0}";
    }
}
