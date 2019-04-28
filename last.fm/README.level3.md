# Level 3

## Description

Produce a list of 10 longest sessions by elapsed time with the following details for each session:

- User
- Time of the First Song been played
- Time of the Last Song been played
- List of songs played (sorted in the order of play)

A session is defined by one or more songs played by the user, 
where each song is started within 20 minutes of the previous song's starting time.

Provide us with the source code, output and any supporting files, 
including a README file describing the approach you use to solve the problem.

## Approach taken

This exercise uses DataFrames to help with the amount of data to process. Only one of both files is used `userid-timestamp-artid-artname-traid-traname.tsv`.

Please refer to the comments in the source code for more low-level detail. These are roughly the steps followed by the code.

1. Load `userid-timestamp-artid-artname-traid-traname.tsv` file
2. Compare timestamp of every row with the preceding one.
3. If timestamps differ more than 20 minutes I mark those rows with a '1' on a new column called 'isNewSession'.
4. Aggregate that column to generate a session ID per user. Session IDs are repeated between users. So the pair User ID - Session ID is necessary to identify a session.
5. Aggregate the differences between timestamps to achieve an approximate session duration.
6. Add the correspondent first and last timestamp session to all the songs on every session.
7. Take the last songs of every session as a representative of that session.
8. Order them by duration and get the longest top 10
9. Go back to the original DataFrame from step 6 and filter by session id/user id to get the list of the songs.
10. Write changes to disk.

The process takes around 2.40 min to run and the final result sits on the `output-level3` folder and it was renamed to `result.tsv`
