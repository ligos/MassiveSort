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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MurrayGrant.MassiveSort
{
    public sealed class ConsoleProgressReporter : IProgress<BasicProgress>
    {
        private readonly Object _ProgressLock = new object();
        private readonly Dictionary<object, CursorPos> _TaskProgressToConsoleLine = new Dictionary<object, CursorPos>();
        private readonly Dictionary<object, string> _TaskProgressToBufferedLine = new Dictionary<object, string>();

        public ConsoleProgressReporter()
        {

        }
        public void Report(BasicProgress value)
        {
            lock (_ProgressLock)
            {
                this.ShowProgress(value);
            }
        }


        private void ShowProgress(BasicProgress p)
        {
            if (p.GetType() == typeof(TaskProgress))
            {
                var tp = (TaskProgress)p;
                var hasReachedEndOfBuffer = ((Console.WindowHeight + Console.WindowTop) >= (Console.BufferHeight - 1));
                if (Environment.UserInteractive && !Console.IsOutputRedirected && !hasReachedEndOfBuffer)
                {
                    // If we're running interactive, we can print as soon as events arrive.
                    // Just need to write them in the right place!!
                    // Note that this does not work if you exceed the console buffer size; things end up out of place.
                    var currentPos = new CursorPos(Console.CursorTop, Console.CursorLeft);
                    CursorPos pos;
                    var alreadyInProgress = _TaskProgressToConsoleLine.TryGetValue(tp.TaskKey, out pos);
                    var isFirst = !alreadyInProgress;
                    if (alreadyInProgress)
                    {
                        // Restore cursor position.
                        Console.CursorTop = pos.Top;
                        Console.CursorLeft = pos.Left;
                    }
                    
                    // Write message.
                    Console.Write(tp.Message);
                    if (tp.IsFinal)
                    {
                        // Remove the now completed task.
                        _TaskProgressToConsoleLine.Remove(tp.TaskKey);
                    }
                    else
                    {
                        // Memorise the cursor position for this task key.
                        _TaskProgressToConsoleLine[tp.TaskKey] = new CursorPos(Console.CursorTop, Console.CursorLeft);
                        // Drop down to next line for next event.
                        if (isFirst)
                            Console.WriteLine();
                    }

                    if (alreadyInProgress)
                    {
                        // Restore original position of cursor.
                        Console.CursorTop = currentPos.Top;
                        Console.CursorLeft = currentPos.Left;
                    }
                }
                else
                {
                    // Can't write to specific console lines, so buffer and write in one go.
                    string line;
                    if (!_TaskProgressToBufferedLine.TryGetValue(tp.TaskKey, out line))
                        line = "";
                    line += tp.Message;
                    if (tp.IsFinal)
                    {
                        Console.WriteLine(line);
                        // Remove the now completed task.
                        _TaskProgressToConsoleLine.Remove(tp.TaskKey);
                    }
                    else
                        _TaskProgressToBufferedLine[tp.TaskKey] = line;
                }
            }
            else
            {
                // For basic events, write as events arrive.
                if (!p.IsFinal)
                    Console.Write(p.Message);
                else
                    Console.WriteLine(p.Message);
            }
        }


        private struct CursorPos
        {
            public CursorPos(int top, int left)
            {
                this.Top = top;
                this.Left = left;
            }
            public readonly int Top;
            public readonly int Left;
            public override string ToString()
            {
                return String.Format("T: {0}, L: {1}", this.Top, this.Left);
            }
        }

    }

    public class BasicProgress
    {
        public BasicProgress(string message, bool isFinal)
        {
            this.Message = message;
            this.IsFinal = isFinal;
        }
        public readonly string Message;
        public readonly bool IsFinal;
    }
    public class TaskProgress : BasicProgress
    {
        public TaskProgress(string message, bool isFinal, object taskKey)
            : base(message, isFinal)
        {
            this.TaskKey = taskKey;
        }

        public readonly object TaskKey;
    }
}
