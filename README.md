# MassiveSort #

Lets your sort and merge really, really big text files.

[Apache License](https://www.apache.org/licenses/LICENSE-2.0)

## Getting Started ##

* Make sure you have [.NET 8.0 installed](https://dotnet.microsoft.com/en-us/download/dotnet/8.0): _.NET Runtime_ is sufficent.
* [Download the latest release](https://github.com/ligos/MassiveSort/releases)
* Unzip to a folder of your choice
* `MassiveSort.exe merge -o sortedFile.txt -i unsortedFile.txt`

## Recent Changes ##

### 0.2.0 ###

* Update to use [.NET 8.0](https://dotnet.microsoft.com/en-us/download/dotnet/8.0).
  * Tested on Windows and Debian platforms. Other Linux distributions supported by dotnet should also work.
* Increase `--max-sort-size` to support sorting over 2GB of data in RAM.
  * Physical RAM is the limit to sort size. Tested on 96GB machine.
  * Increasing `--slab-size` allows up to 63TB to be sorted in RAM (in theory).
* Maximum sortable line size is now 128kB (131,072) bytes. Longer lines are skipped.
* Null bytes (ASCII `NUL` or `0x00`) are removed by default.
  * Use `--keep-nulls` to keep null bytes.
* Improved support for files with similar starting lines.
  * Splitting phase is limited to 16 iterations to avoid long path failures on Windows.
  * Use `--split-count` and --force-large-sort` to control this behaviour.
* Improved memory usage via dotnet `MemoryPool`; MassiveSort should not allocate excessive memory.
  * The `--aggressive-memory-collection` option is always active; command line option removed.  
* Improve logging via `--debug` and `--save-stats` options.
* Update to latest version of 3rd party libraries.
* Fixed several bugs discovered when implementing all the above!

### 0.1.6 ###

* Increase `--max-sort-size` maximum to 2GB
* Change old BitBucket references to GitHub

### 0.1.5 ###

* Fixed bugs in end of line handling and blank line detection

### 0.1.4 ###

* Better crash dump support

### 0.1.3 ###

* Linux support via Mono (minimal testing on Ubuntu 14.04 / Mono 3.2)
* `--sort-by` option to allow sorting by length / dictionary order.

## Other Options ##

Merge many files and even whole folders into a single, sorted file:

`MassiveSort.exe merge -o sortedAndMergedFile.txt -i unsortedFile.txt anotherFile.txt aDirectory\subFolder`

By default, MassiveSort removes duplicates. Use the `--leave-duplicates` options to keep them (if you're that attached to them).

MassiveSort can list your duplicates in a separate file with the `--save-duplicates` option.

MassiveSort can normalise special, non-ascii or non-printable characters into the $HEX[] format with the `--convert-to-dollar-hex` option.

MassiveSort can trim or remove whitespace with the `--whitespace` option (note that this can make destructive changes to the lines you are sorting, particularly if there are non-ascii encodings).

More details and examples can be found in the [Merge Verb](https://github.com/ligos/MassiveSort/wiki/Verb---Merge)

## Longer Term Goals ##

* Merge into a central file, with tags
* Configuration files for source files, so you don't to remember lots of options to import
* General purpose large scale merge sort on any `IEnumerable<T>`