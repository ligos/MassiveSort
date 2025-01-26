// Copyright 2015 Murray Grant
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Management;
using System.IO;
using MurrayGrant.MassiveSort.Actions;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace MurrayGrant.MassiveSort
{
    public static class CrashDumper
    {
        public static string Save(ExceptionAndComputerDetail detail)
        {
            string savedIn;
            if (TrySaveInCurrentFolder(detail, out savedIn))
                return savedIn;
            if (TrySaveInHomeFolder(detail, out savedIn))
                return savedIn;
            throw new Exception("Could not save crash details in current folder or home folder.");
        }
        private static bool TrySaveInCurrentFolder(ExceptionAndComputerDetail detailsToPublish, out string savedIn)
        {
            try
            {
                var path = Path.Combine(Environment.CurrentDirectory, CrashDumpName(detailsToPublish.Timestamp));
                SaveDetailsIn(detailsToPublish, path);
                savedIn = path;
                return true;
            }
            catch (Exception)
            {
                savedIn = null;
                return false;
            }
        }
        private static bool TrySaveInHomeFolder(ExceptionAndComputerDetail detailsToPublish, out string savedIn)
        {
            try
            {
                var path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), CrashDumpName(detailsToPublish.Timestamp));
                SaveDetailsIn(detailsToPublish, path);
                savedIn = path;
                return true;
            }
            catch (Exception)
            {
                savedIn = null;
                return false;
            }
        }

        private static void SaveDetailsIn(ExceptionAndComputerDetail detailsToPublish, string fullPath)
        {
            using (var file = new FileStream(fullPath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var writer = new StreamWriter(file, Constants.Utf8NoBom))
            {
                writer.WriteLine(detailsToPublish.ToString());
            }
        }

        public static ExceptionAndComputerDetail CreateErrorDetails(Exception ex)
        {
            return new ExceptionAndComputerDetail(ex, About.ProductName);
        }

        private static string CrashDumpName(DateTimeOffset now)
        {
            return About.ProductName + "_CrashDetails_" + now.ToLocalTime().ToString("yyyyMMdd-hhmmss") + ".txt";
        }
        private readonly static PlatformID[] _UnixLike = new PlatformID[] { PlatformID.MacOSX, PlatformID.Unix };
        private readonly static PlatformID[] _WindowsLike = new PlatformID[] { PlatformID.Win32NT, PlatformID.Win32S, PlatformID.Win32Windows };
        private static DirectoryInfo GetHomeSaveLocation()
        {
            if (IsUnixLikeEnvironment())
                return new DirectoryInfo(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile));
            else if (IsWindowsLikeEnvironment())
                return new DirectoryInfo(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile));
            else
                throw new PlatformNotSupportedException("Unsupported environment: " + Environment.OSVersion.Platform);
        }

        public static bool IsUnixLikeEnvironment()
        {
            return _UnixLike.Contains(Environment.OSVersion.Platform);
        }
        public static bool IsWindowsLikeEnvironment()
        {
            return _WindowsLike.Contains(Environment.OSVersion.Platform);
        }
    }

    public class ExceptionAndComputerDetail
    {
        public DateTimeOffset Timestamp { get; set; }
        public String Product { get; set; }
        public Version Version { get; set; }
        public Exception Exception { get; set; }

        public ExceptionAndComputerDetail(Exception ex, String product)
        {
            this.Exception = ex;
            this.Product = product;
            this.Version = new Version(About.Version);
            this.Timestamp = DateTimeOffset.UtcNow;
        }

        public override string ToString()
        {
            var result = new StringBuilder(4096);
            result.AppendFormat("TimestampGMT: {0:o} {1}", this.Timestamp, Environment.NewLine);
            result.AppendFormat("TimestampLocal: {0:o} {1}", this.Timestamp.ToLocalTime(), Environment.NewLine);
            result.AppendFormat("Product: {0}{1}", this.Product, Environment.NewLine);
            result.AppendFormat("Version: {0}{1}", this.Version, Environment.NewLine);
            result.AppendLine();
            if (this.Exception != null)
            {
                result.AppendFormat("EXCEPTION{0}{1}{0}", Environment.NewLine, this.Exception.ToFullString());
                result.AppendLine();
            }
            AddPlatformDetails(result);
            result.AppendLine(CreateComputerDetails());
            result.AppendLine();
            return result.ToString();
        }

        string CreateComputerDetails()
        {
            if (CrashDumper.IsWindowsLikeEnvironment())
                return CreateWindowsComputerDetails();
            else if (CrashDumper.IsUnixLikeEnvironment())
                return CreateUnixComputerDetails();
            else
                throw new PlatformNotSupportedException();
        }


        string CreateWindowsComputerDetails()
        {
            Debug.Assert(RuntimeInformation.IsOSPlatform(OSPlatform.Windows));

            var entryAssembly = Assembly.GetEntryAssembly();
            var result = new StringBuilder(4096);
            var drives = DriveInfo.GetDrives();
            using (var procInfo = new ManagementObjectSearcher("SELECT * FROM Win32_Processor"))
            using (var baseboard = new ManagementObjectSearcher("SELECT * FROM Win32_BaseBoard"))
            using (var computerSystem = new ManagementObjectSearcher("SELECT * FROM Win32_ComputerSystem"))
            using (var videoController = new ManagementObjectSearcher("SELECT * FROM Win32_VideoController"))
            using (var osInfo = new ManagementObjectSearcher("SELECT * FROM Win32_OperatingSystem"))
            using (var proc = System.Diagnostics.Process.GetCurrentProcess())
            {
                result.AppendLine("WINDOWS");
                using (var item = computerSystem.Get().Cast<ManagementBaseObject>().First())
                    result.AppendFormat("Computer: {0} {1}{2}", item["Manufacturer"], item["Model"], Environment.NewLine);
                using (var item = osInfo.Get().Cast<ManagementBaseObject>().First())
                {
                    result.AppendFormat("WindowsDetail: {0}{1}", item["Name"], Environment.NewLine);
                    result.AppendFormat("TotalRam: {0:N0}kB{1}", item["TotalVisibleMemorySize"], Environment.NewLine);
                    result.AppendFormat("TotalVirtualRam: {0:N0}kB{1}", item["TotalVirtualMemorySize"], Environment.NewLine);
                    result.AppendFormat("FreeRam: {0:N0}kB{1}", item["FreePhysicalMemory"], Environment.NewLine);
                    result.AppendFormat("FreeVirtualRam: {0:N0}kB{1}", item["FreeVirtualMemory"], Environment.NewLine);
                    result.AppendFormat("Locale: {0}{1}", item["Locale"], Environment.NewLine);
                    result.AppendFormat("Language: {0}{1}", item["OSLanguage"], Environment.NewLine);
                    result.AppendFormat("CountryCode: {0}{1}", item["CountryCode"], Environment.NewLine);
                    result.AppendFormat("Architecture: {0}{1}", item["OSArchitecture"], Environment.NewLine);
                }
                result.AppendLine();
                result.AppendLine("PROCESS");
                result.AppendFormat("Exe: {0}{1}", entryAssembly.Location, Environment.NewLine);
                result.AppendFormat("Assembly: {0}{1}", entryAssembly.FullName, Environment.NewLine);
                result.AppendFormat("Architecture: {0}{1}", Environment.Is64BitProcess ? "64 bit" : "32 bit", Environment.NewLine);
                result.AppendFormat("WorkingSet: {0:N0}kB{1}", proc.WorkingSet64 / 1024, Environment.NewLine);
                result.AppendFormat("PrivateBytes: {0:N0}kB{1}", proc.PrivateMemorySize64 / 1024, Environment.NewLine);
                result.AppendFormat("VirtualSize: {0:N0}kB{1}", proc.VirtualMemorySize64 / 1024, Environment.NewLine);
                result.AppendFormat("PeakWorkingSet: {0:N0}kB{1}", proc.PeakWorkingSet64 / 1024, Environment.NewLine);
                result.AppendFormat("PeakVirtualSize: {0:N0}kB{1}", proc.PeakVirtualMemorySize64 / 1024, Environment.NewLine);
                result.AppendFormat("WallRuntime: {0:c}{1}", DateTime.Now.Subtract(proc.StartTime), Environment.NewLine);
                result.AppendFormat("CpuTime: {0:c}{1}", proc.TotalProcessorTime, Environment.NewLine);
                result.AppendLine();
                result.AppendLine("ASSEMBLIES");
                foreach (var a in AppDomain.CurrentDomain.GetAssemblies())
                    result.AppendLine(a.FullName);
                result.AppendLine();
                result.AppendLine("CPU");
                var cpus = procInfo.Get();
                result.AppendFormat("CpuCount: {0}{1}", cpus.Count, Environment.NewLine);
                foreach (var item in cpus)
                {
                    result.AppendFormat("{0}: {1} {2}{3}", item["DeviceID"], item["Name"], item["Description"], Environment.NewLine);
                    result.AppendFormat("{0}: PhysicalCores = {1} LogicalCores = {2}{3}", item["DeviceID"], item["NumberOfCores"], item["NumberOfLogicalProcessors"], Environment.NewLine);
                }
                result.AppendLine();
                result.AppendLine("DISK");
                foreach (var drive in DriveInfo.GetDrives().Where(x => x.IsReady))
                {
                    result.AppendFormat("{0} ({1}) => Type: {2}, Format: {3}, Capacity: {4:N2}, Available: {5:N2}{6}", drive.Name, drive.VolumeLabel, drive.DriveType, drive.DriveFormat, drive.TotalSize / Constants.OneGbAsDouble, drive.AvailableFreeSpace / Constants.OneGbAsDouble, Environment.NewLine);
                }
                return result.ToString();
            }
        }
        string CreateUnixComputerDetails()
        {
            Debug.Assert(
                RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                || RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                || RuntimeInformation.IsOSPlatform(OSPlatform.FreeBSD)
            );

            // For Linux:
            // - /etc/*release - details of system / distro
            // - /prov/version - kernal version details
            // - /proc/cpuinfo
            // - /proc/meminfo

            var entryAssembly = Assembly.GetEntryAssembly();
            var result = new StringBuilder(4096);
            var drives = DriveInfo.GetDrives();
            using (var proc = System.Diagnostics.Process.GetCurrentProcess())
            {
                result.AppendLine("RELEASE");
                foreach (var f in Directory.EnumerateFiles("/etc", "*release"))
                {
                    result.AppendLine(f + ":");
                    result.Append(File.ReadAllText(f));
                }
                result.AppendLine();
                if (File.Exists("/proc/version"))
                {
                    result.AppendLine("KERNAL");
                    result.Append(File.ReadAllText("/proc/version"));
                    result.AppendLine();
                }
                result.AppendLine("PROCESS");
                result.AppendFormat("Exe: {0}{1}", entryAssembly.Location, Environment.NewLine);
                result.AppendFormat("Assembly: {0}{1}", entryAssembly.FullName, Environment.NewLine);
                result.AppendFormat("Architecture: {0}{1}", Environment.Is64BitProcess ? "64 bit" : "32 bit", Environment.NewLine);
                result.AppendFormat("WorkingSet: {0:N0}kB{1}", proc.WorkingSet64 / 1024, Environment.NewLine);
                result.AppendFormat("PrivateBytes: {0:N0}kB{1}", proc.PrivateMemorySize64 / 1024, Environment.NewLine);
                result.AppendFormat("VirtualSize: {0:N0}kB{1}", proc.VirtualMemorySize64 / 1024, Environment.NewLine);
                result.AppendFormat("PeakWorkingSet: {0:N0}kB{1}", proc.PeakWorkingSet64 / 1024, Environment.NewLine);
                result.AppendFormat("PeakVirtualSize: {0:N0}kB{1}", proc.PeakVirtualMemorySize64 / 1024, Environment.NewLine);
                result.AppendFormat("WallRuntime: {0:c}{1}", DateTime.Now.Subtract(proc.StartTime), Environment.NewLine);
                result.AppendFormat("CpuTime: {0:c}{1}", proc.TotalProcessorTime, Environment.NewLine);
                result.AppendLine();
                result.AppendLine("ASSEMBLIES");
                foreach (var a in AppDomain.CurrentDomain.GetAssemblies())
                    result.AppendLine(a.FullName);
                result.AppendLine();
                if (File.Exists("/proc/cpuinfo"))
                {
                    result.AppendLine("CPU");
                    result.Append(File.ReadAllText("/proc/cpuinfo"));
                    result.AppendLine();
                }
                if (File.Exists("/proc/meminfo"))
                {
                    result.AppendLine("MEMORY");
                    result.Append(File.ReadAllText("/proc/meminfo"));
                    result.AppendLine();
                }
                result.AppendLine("DISK");
                foreach (var drive in DriveInfo.GetDrives().Where(d => d.IsReady))
                {
                    result.AppendFormat("{0}{1} => Type: {2}, Format: {3}, Capacity: {4:N2}, Available: {5:N2}{6}", drive.Name, String.IsNullOrEmpty(drive.VolumeLabel) ? "" : " (" + drive.VolumeLabel + ")", drive.DriveType, drive.DriveFormat, drive.TotalSize / Constants.OneGbAsDouble, drive.AvailableFreeSpace / Constants.OneGbAsDouble, Environment.NewLine);
                }
                return result.ToString();
            }
        }

        private void AddPlatformDetails(StringBuilder result)
        {
            result.AppendLine("PLATFORM");
            result.AppendLine("OSPlatform: " + Environment.OSVersion.Platform);
            result.AppendLine("OSVersion: " + Environment.OSVersion.VersionString);
            result.AppendLine("OSArchitecture: " + RuntimeInformation.OSArchitecture);
            result.AppendLine("OSDescription: " + RuntimeInformation.OSDescription);
            result.AppendLine("FrameworkVersion: " + Environment.Version);
            result.AppendLine("FrameworkDescription: " + RuntimeInformation.FrameworkDescription);
            result.AppendLine("ProcessArchitecture: " + RuntimeInformation.ProcessArchitecture);
            result.AppendLine("RuntimeIdentifier: " + RuntimeInformation.RuntimeIdentifier);
            result.AppendLine();
        }
    }
}
