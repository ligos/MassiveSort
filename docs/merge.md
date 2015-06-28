# Verb - Merge #

Merges one or more text files into a single file. Lines are sorted and duplicates are removed.

## Algorithm ##

The basic algorithm for sorting is a merge sort by an initial chunk or shard, which is buffered to disk. Buffering to disk assumes more data than available memory, so this won't be as fast as a direct sort for small files (but, small files are sorted quickly anyway).

There are three phases:

### Phase 1 - Sharding ###

All input files are read (in parallel if multiple files) and each line is written to a file based on the first byte of the line.

Any files which are larger than `--max-sort-size` are split again on the 2nd byte. And so on until all files are less than `--max-sort-size`.

### Phase 2 - Sorting ###

Sorting is the major computational part of this process. Each sharded file is read in sorted order and the lines themselve are sorted. Files are sorted in parallel based on the number of workers.

### Phase 3 - Deduplication ###

As the final sorted results are written to the output file, duplicates are removed (unless duplicates are left).


## Memory Usage ##

Sorting, by nature, will use all the memory you can throw at it. Sorting gigabytes of files will use gigabytes of memory. The more memory you have, the better.

If you are curious how much memory your merge will need, add the `--debug` option and a memory usage estimate will be shown. And, you can use task manager, [Process Explorer](https://technet.microsoft.com/en-us/sysinternals/bb896653), [VMMap](https://technet.microsoft.com/en-us/sysinternals/dd535533) or [PerfView](https://www.microsoft.com/en-au/download/details.aspx?id=28567) to see the gruesome details.

The out of the box defaults should be OK if you have 2GB of available physical RAM (if you have a 4GB machine, this should be fine).

The two options which most influence memory usage are `--max-sort-size` and `--workers`. 

The `--max-sort-size` buffer size is allocated 3 times over when sorting for various purposes. Reducing (or increasing) it has a substantial impact on memory usage.

Every worker thread must allocate every buffer. So memory usage is multiplied by the number of workers.

## Command Line Arguments ##

All command line options are listed below.

### Required Arguments ###

* `-i` `--input` - One or more files or folders to sort
* `-o` `--output` - A file to write the output to
	
### General Options ###

* `-t` `--temp-folder` - Folder to use for writing temporary files
    * Default: `%TEMP%\MassiveSort\<PID>`
	* Putting the temp files on a different disk to your final output can improve IO performance
* `--leave-duplicates` - Leave duplicates in the output file
    * Default: remove duplicates
* `--save-duplicates` - Save duplicates to a separate .duplicates file.
    * Default: do not save duplicates
	* This has a small performance impact
* `--convert-to-dollar-hex` - Converts non-ascii bytes to $HEX[] format
	* Default: make no changes to non-ascii bytes
	* This has a small performance impact
* `--save-stats` - Saves more detailed stats to .stats file.
* `--whitespace` - Changes made to whitespace when processing. *WARNING*: this can make destructive changes to your inputs
    * `NoChange`: no changes to whitespace (default)
    * `Trim`: removes leading and trailing whitespace
	* `Strip`: removes all whitespace
* `--whitespace-chars` - Byte(s) considered whitespace
    * Default: 0x09, 0x0b, 0x20
	
### Sorting Options ###
	
* `--max-sort-size` - Largest chunk of files to sort as a group
    * Default: 64MB
	* This is the major contributor to memory usage, decrease this if you are running short of phyiscal ram
* `--sort-algorithm` - Sort agorithm to use, options:
    * `DefaultArray`: Array.Sort()
    * `LinqOrderBy`: Enumerable.OrderBy()
    * `TimSort`: [Timsort](https://en.wikipedia.org/wiki/Timsort) algorithm (default)
	
### Worker / Thread Options ###

* `-w` `--workers` - Number of worker threads
    * Default: number of physical cores in your PC
	* Major contributor to memory usage, decrease this is you are running short of physical ram
* `--io-workers` - Number of worker threads for IO intensive operations
    * Default: 8 workers
	* This has minimal memory impact, SSD users may wish to increase this to 32

### Buffers and Memory Options ###

* `--line-buffer-size` - Buffer size for reading lines, this is also the maximum line length
	* Default: 64KB
* `--read-file-buffer-size` - Buffer size for reading input files
    * Default: 64KB
* `--temp-file-buffer-size` - Buffer size for writing temp files
	* Default: 128KB
* `--output-file-buffer-size` - Buffer size for writing final output file
	* Default: 256KB
* `--max-outstanding-sorted-chunks` - Number of chunks to buffer in memory when writing
    * Default: 10
	* Major contributor to memory usage if your CPU sorts faster than your disk can write
* `--aggressive-memory-collection` - Does a full garbage collection after each file processed	
    * This does not reduce the amount of memory required (make buffer sizes or number of workers smaller for that). It helps keep excess memory usage minimal.
	

