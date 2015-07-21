# MassiveSort #

Lets your sort and merge really, really big text files.

[Apache License](https://www.apache.org/licenses/LICENSE-2.0)

## Getting Started ##

* [Download the latest release](https://bitbucket.org/ligos/massivesort/downloads)
* Unzip to a folder of your choice
* `MassiveSort.exe merge -o sortedFile.txt -i unsortedFile.txt`

## Other Options ##

Merge many files and even whole folders into a single, sorted file:

`MassiveSort.exe merge -o sortedAndMergedFile.txt -i unsortedFile.txt anotherFile.txt aDirectory\subFolder`

By default, MassiveSort removes duplicates. Use the `--leave-duplicates` options to keep them (if you're that attached to them).

MassiveSort can list your duplicates in a separate file with the `--save-duplicates` option.

MassiveSort can normalise special, non-ascii or non-printable characters into the $HEX[] format with the `--convert-to-dollar-hex` option.

MassiveSort can trim or remove whitespace with the `--whitespace` option (note that this can make destructive changes to the lines you are sorting, particularly if there are non-ascii encodings).

More details can be found in the [Merge Verb](Verb - Merge)

## Longer Term Goals ##

* Linux support using Mono
* Merge into a central file, with tags
* Configuration files for source files, so you don't to remember lots of options to import
* General purpose large scale merge sort on any `IEnumerable<T>`