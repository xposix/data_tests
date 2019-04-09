# Level 1

## Description

Produce a list of Users and the number of distinct songs the user has played.

## Approach taken

This exercise uses RDDs as a first approach to transform data in memory. Only one of both files is used `userid-timestamp-artid-artname-traid-traname.tsv`. 

Please refer to the comments in the source code for more low-level detail. These are roughly the steps followed by the code.

1. Load `userid-timestamp-artid-artname-traid-traname.tsv` file as a text file.
2. Map each line in the file to a Row in a RDD. This process uses the user id and it concatenates artname and traname into one single field to be used as track id. This is due to artid and traid not being present in all the registers.
3. Used distinct on the result as per requested on the exercise.
4. Adding a new column with value 1 and then aggregate them based on user id.
5. Sort the result of the aggregation to find the totals per user. 
6. Write it into the `output-level1.tsv` file, line by line.

The format of the final file is:

`User ID` `Number of playbacks`

separated by tabs.