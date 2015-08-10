# MassiveSort #

Lets your sort and merge really, really big text files.

[Apache License](https://www.apache.org/licenses/LICENSE-2.0)

## Getting Started ##

* Make sure you have .NET 4.5.2 / Mono 3.2+ installed
* [Download the latest release](https://bitbucket.org/ligos/massivesort/downloads)
* Unzip to a folder of your choice
* `MassiveSort.exe merge -o sortedFile.txt -i unsortedFile.txt`

## Recent Changes ##

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

More details and examples can be found in the [Merge Verb](https://bitbucket.org/ligos/massivesort/wiki/Verb%20-%20Merge)

## Longer Term Goals ##

* Merge into a central file, with tags
* Configuration files for source files, so you don't to remember lots of options to import
* General purpose large scale merge sort on any `IEnumerable<T>`