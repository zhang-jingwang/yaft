# yaft
Yet Another Filesystem Tester.

# build
make

# run
You can mount Lustre file system at /mnt/lustre/ and then run the program

`# ./yaft`

Or you can specify the prefix of all the test files as following:

`# ./yaft /mnt/to/mountpoint/fileprefix`

# what does it test?

1. Write 1M block to a file sequentially
2. When the file reaches 10M, create and write a new file.
3. If ENOSPC occurs, check the content of some old files and remove them.
4. Do step 1 ~ 3 in multiple threads concurrently.

So this will test write, read, truncate and unlink. It will test the file system's behavior when space is almost full to see if the file system will leak resources. This is a good test for testing the stability of a file system.

If data corruption occurs, it will assert...
