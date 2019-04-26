# coding=UTF-8
#############################################
#  Level 3
# Produce a list of 10 longest sessions by elapsed time with 
# the following details for each session:
#  - User
#  - Time of the First Song been played
#  - Time of the Last Song been played
#  - List of songs played (sorted in the order of play)
#
# A session is defined by one or more songs played by the user, 
# where each song is started within 20 minutes of the previous song's starting time.
#
# Provide us with the source code, output and any supporting files, 
# including a README file describing the approach you use to solve the problem.
#  

# Expected output: list of the 10 sessions with the songs all concatenated into a single column.

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, TimestampType, StringType, StructType
from pyspark.sql import functions as func
from pyspark.sql.window import Window

import logging
import sys 

# Displaying UTF-8 by default
import sys
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

# Create a SparkSession
spark = SparkSession.builder.appName("Level3").getOrCreate()

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

# Creating DataFrame using a Reader
originalPlaybacks = spark.read.csv("userid-timestamp-artid-artname-traid-traname.tsv",
                                sep='\t',
                                schema=StructType([
                                        StructField("userid", StringType(), True), \
                                        StructField("timestamp", TimestampType(), True), \
                                        StructField("artid", StringType(), True), \
                                        StructField("artname", StringType(), True), \
                                        StructField("traid", StringType(), True), \
                                        StructField("traname", StringType(), True)
                                    ]))
### Starting the calculations ###
# Dropping unused columns from the original 
cleanOriginalPlaybacks = originalPlaybacks.drop('traid', 'artid')

# Bringing previous row's timestamp into a new column
result = cleanOriginalPlaybacks.withColumn('previous_timestamp',
                                            func.lag(originalPlaybacks['timestamp'])
                                            .over(Window.partitionBy('userid').orderBy("timestamp")))

# New column 'timestamp_difference' showing the timestamp differences
timeFmt = "yyyy-MM-dd'T'HH:mm:ssZ"
timeDiff = (func.unix_timestamp('timestamp', format=timeFmt)
            - func.unix_timestamp('previous_timestamp', format=timeFmt))
result = result.withColumn("timestamp_difference", timeDiff) 

# New column 'isNewSession' showing 1 on every first song of a session, otherwise 0.
isNewSession = (func.when((result['timestamp_difference'] > 1200) | (func.isnull(result['timestamp_difference'])), 1).otherwise(0))
result = result.withColumn("isNewSession", isNewSession)

# New column 'sessionId'. The session counter will start on every new userid.
result = result.withColumn('sessionId',
                                        func.sum(result['isNewSession'])
                                        .over(Window.partitionBy('userid').orderBy("timestamp").rangeBetween(Window.unboundedPreceding, 0)))
# Probar a quitar el userid
result = result.withColumn('sessionAcum',
                                        func.sum(func.when(result['isNewSession'] == 0, result['timestamp_difference']).otherwise(0) )
                                        .over(Window.partitionBy('userid','sessionId').orderBy("timestamp").rangeBetween(Window.unboundedPreceding, 0)))

result = result.withColumn('sessionLength',
                                        func.max(result['sessionAcum'])
                                        .over(Window.partitionBy('userid','sessionId').orderBy("timestamp").rangeBetween(0, Window.unboundedFollowing)))

result = result.withColumn('artistSongNames', func.concat_ws(' - ', result.artname,result.traname))

# Removing unnecessary columns 
result = result.drop('isNewSession', 'previous_timestamp', 'timestamp_difference', 'artname', 'traname')

# Saving each session's first song's timestamp on a new column
result = result.withColumn('firstSongTimestamp',
                                        func.min(result['timestamp'])
                                        .over(Window.partitionBy('userid','sessionId').orderBy("timestamp").rangeBetween(Window.unboundedPreceding, 0)))

# Saving each session's last song's timestamp on a new column
result = result.withColumn('lastSongTimestamp',
                                        func.max(result['timestamp'])
                                        .over(Window.partitionBy('userid','sessionId').orderBy("timestamp").rangeBetween(0, Window.unboundedFollowing)))

# Filtering the last song of the sessions as a representative of the session to use it for the top 10 longest sessions ranking
tenLongestSessions = result.filter(result['sessionLength'] == result['sessionAcum'])
tenLongestSessions = tenLongestSessions.select(result.userid, result.sessionId, result.sessionLength).orderBy(result.sessionLength.desc()).limit(10)

# Finding the songs related to those 10 sessions on the songs list. Adding some aliases to avoid warnings.
longestSessions = tenLongestSessions.alias('longestSessions')
songsList = result.alias('songsList')
finalResult = result.join(tenLongestSessions, ((songsList.userid == longestSessions.userid) & (songsList.sessionId == longestSessions.sessionId))
              ).select(result.userid, result.timestamp, result.artistSongNames, result.sessionLength,
              result.sessionId, result.firstSongTimestamp, result.lastSongTimestamp)
finalResult = finalResult.orderBy(result.sessionLength.desc(), result.userid, result.timestamp)

# User, Session ID, firstSongTimestamp, lastSongTimestamp
finalResult = finalResult.groupBy('userid','sessionId','firstSongTimestamp','lastSongTimestamp','sessionLength').agg(
                func.concat_ws(", ", func.collect_list(result.artistSongNames)).alias('Songs')
                ).orderBy(result.sessionLength.desc())

# Save results to disk
finalResult.coalesce(1).write.csv('output-level3', mode='overwrite', sep='\t', header=True)

spark.stop()






