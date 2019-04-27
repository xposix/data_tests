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
originalPlaybacks = originalPlaybacks.withColumn('artistSongNames', func.concat_ws(' - ', originalPlaybacks.artname,originalPlaybacks.traname))

# Dropping unused columns from the original 
cleanOriginalPlaybacks = originalPlaybacks.drop('traid', 'artid', 'artname', 'traname')


# Bringing previous row's timestamp into a new column in the current row
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

result = result.withColumn('sessionAcum',
                                        func.sum(func.when(result['isNewSession'] == 0, result['timestamp_difference']).otherwise(0) )
                                        .over(Window.partitionBy('userid','sessionId').orderBy("timestamp").rangeBetween(Window.unboundedPreceding, 0)))

# Removing unnecessary columns 
result = result.drop('isNewSession', 'previous_timestamp')

finalResult = result.orderBy('userid','sessionId','timestamp').groupBy('userid','sessionId').agg(
                func.min(result.timestamp).alias('firstSongTimestamp'),
                func.max(result.timestamp).alias('lastSongTimestamp'),
                func.max(result.sessionAcum).alias('sessionLength'),
                func.concat_ws(", ", func.collect_list(result.artistSongNames)).alias('SongsList')
                ).orderBy('sessionLength', ascending=False).limit(10)

# Save results to disk
finalResult.coalesce(1).write.csv('output-level3', mode='overwrite', sep='\t', header=True)

spark.stop()






