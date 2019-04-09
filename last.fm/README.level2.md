# Level 2

## Description

Produce a list of 100 most popular Song titles, its Artist, and the number of times the songs have been played.


## Approach taken

This exercise uses RDDs as a first approach to transform data in memory. Only one of both files is used `userid-timestamp-artid-artname-traid-traname.tsv`. 

Please refer to the comments in the source code for more low-level detail. These are roughly the steps followed by the code.

1. Load `userid-timestamp-artid-artname-traid-traname.tsv` file as a text file.
2. Map each line in the file to a Row in a RDD. This process only uses artname and traname, ignoring the rest of information.
3. Use countByValue to count every time a artist-name of the song pair appears.
4. Sort that count in descending mode and take the top 100.
5. Write it into the `output-level2.tsv` file, line by line.

The format of the final file is:

`Artist Name`  `Song Name` - `Number of playbacks`

separated by tabs.