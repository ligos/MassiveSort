# MassiveSort #

Lets your sort and merge really, really big text files.

[Apache License](https://www.apache.org/licenses/LICENSE-2.0)

## Getting Started ##

* [Download the latest release](https://bitbucket.org/ligos/massivesort/downloads)
* Unzip to a folder of your choice
* `MassiveSort.exe merge -o sortedFile.txt -i unsortedFile.txt`

## More Things ##

Merge many files and even whole folders into a single, sorted file:

`MassiveSort.exe merge -o sortedAndMergedFile.txt -i unsortedFile.txt anotherFile.txt aDirectory\subFolder`

By default, MassiveSort removes duplicates. Use the `--leave-duplicates` options to keep them (if you're that attached to them).

See the [wiki for more details](https://bitbucket.org/ligos/massivesort/wiki).

## Longer Term Goals ##

* Merge into a central file, with tags
* Configuration files for source files, so you don't to remember lots of options to import
* General purpose large scale merge sort on any `IEnumerable<T>`