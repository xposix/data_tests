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

from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrameReader , SparkSession, Row
from pyspark.sql.types import StructField, TimestampType, StringType, StructType
from pyspark.sql import functions as func
from pyspark.sql.window import Window

import pprint
import logging
import sys 
import collections
import datetime
import time

# Displaying UTF-8 by default
import sys
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

# Create a SparkSession
spark = SparkSession.builder.appName("Level3").getOrCreate()

def getTime(seconds):
    sec = datetime.timedelta(seconds=seconds)
    d = datetime.datetime(1,1,1) + sec
    return ("%dd %02dh %02dm %02ds" % (d.day-1, d.hour, d.minute, d.second))

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

# Creating DataFrame using a Reader
schemaPlaybacks = spark.read.csv("userid-timestamp-artid-artname-traid-traname-100k.tsv",
                                sep='\t',
                                schema=StructType([
                                        StructField("userid", StringType(), True), \
                                        StructField("timestamp", TimestampType(), True), \
                                        StructField("artid", StringType(), True), \
                                        StructField("artname", StringType(), True), \
                                        StructField("traid", StringType(), True), \
                                        StructField("traname", StringType(), True)
                                    ]))
# schemaPlaybacks.createOrReplaceTempView("playbacks")

# Order playbacks so they are all consecutive by user and timestamp
# orderedPlaybacks = schemaPlaybacks.orderBy("userid","timestamp").dropDuplicates(["userid","timestamp"])
# orderedPlaybacks.createOrReplaceTempView("orderedPlaybacks")

playbacksWithPrevious = schemaPlaybacks.drop('traid').drop('artid').withColumn('previous_timestamp',
                                                func.lag(schemaPlaybacks['timestamp'])
                                                .over(Window.partitionBy('userid').orderBy("userid","timestamp")))

timeFmt = "yyyy-MM-dd'T'HH:mm:ssZ"
timeDiff = (func.unix_timestamp('timestamp', format=timeFmt)
            - func.unix_timestamp('previous_timestamp', format=timeFmt))
result = playbacksWithPrevious.withColumn("timestamp_difference", timeDiff) 

isNewSession = (func.when((result['timestamp_difference'] > 1200) | (func.isnull(result['timestamp_difference'])), 1).otherwise(0))
result2 = result.withColumn("isNewSession", isNewSession)

# The session counter will start on every new userid
result3 = result2.withColumn('sessionId',
                                        func.sum(result2['isNewSession'])
                                        .over(Window.partitionBy('userid').orderBy("userid","timestamp").rangeBetween(Window.unboundedPreceding, 0)))

result3 = result3.withColumn('sessionAcum',
                                         func.sum(func.when(result3['isNewSession'] == 0, result3['timestamp_difference']).otherwise(0) )
                                        .over(Window.partitionBy('userid','sessionId').orderBy("userid","timestamp").rangeBetween(Window.unboundedPreceding, 0)))

result3 = result3.withColumn('sessionLength',
                                        func.max(result3['sessionAcum'])
                                        .over(Window.partitionBy('userid','sessionId').orderBy("userid","timestamp").rangeBetween(0, Window.unboundedFollowing)))

result3 = result3.drop('isNewSession').drop('previous_timestamp').drop('timestamp_difference')

# Produce a list of 10 longest sessions by elapsed time with 
# the following details for each session:
#  - User
#  - Time of the First Song been played
#  - Time of the Last Song been played
#  - List of songs played (sorted in the order of play)

result3 = result3.withColumn('firstSongTimestamp',
                                        func.min(result3['timestamp'])
                                        .over(Window.partitionBy('userid','sessionId').orderBy("userid","timestamp").rangeBetween(0, Window.unboundedFollowing)))

result3 = result3.withColumn('lastSongTimestamp',
                                        func.max(result3['timestamp'])
                                        .over(Window.partitionBy('userid','sessionId').orderBy("userid","timestamp").rangeBetween(0, Window.unboundedFollowing)))

# result3 = result3.withColumn('lastSongTimestamp',
#                                         func.max(result3['timestamp'])
#                                         .over(Window.partitionBy('userid','sessionId').orderBy("userid","timestamp").rangeBetween(0, Window.unboundedFollowing)))

result3.show(100)
tenLongestSessions = result3.select(result3.userid, result3.sessionId, result3.sessionLength).orderBy(result3.sessionLength.desc()).limit(10)


# result3.show(50000)
# tenLongestSessions.show()
exit(-1)

# Removing the one-song sessions
calculatedSessionsFiltered = calculatedSessions.filter( ~ ((calculatedSessions.preceding_songs_count == 1) & (calculatedSessions.following_songs_count == 1)) )

# First set of calculations finished
sessionid = 0
songsBySession = {}
logging.info("Calculating sessions")
songs = calculatedSessionsFiltered.collect()

# Moving results to a dictionary to perform operations
logging.info("Calculating processing in dict")
for song in songs:
    if song.preceding_songs_count == 1:
        # Start of the session
        sessionid += 1
        first_timestamp = song.timestamp
        songsBySession[sessionid] = {}
        songsBySession[sessionid]['songs'] = []
    elif song.following_songs_count == 1:
        # End of the session
        songsBySession[sessionid]['first_timestamp'] = first_timestamp
        songsBySession[sessionid]['last_timestamp']  = song.timestamp
        songsBySession[sessionid]['userid'] = song.userid
    
    songsBySession[sessionid]['songs'].append({'timestamp': song.timestamp.isoformat(), 'artname': song.artname, 'traname': song.traname})

logging.info("Creating RDD")
final_rdd = spark.sparkContext.parallelize(songsBySession.items())

# Calculating durations
final_rdd_with_durations = final_rdd.map(lambda x: (x, (x[1]['last_timestamp'] - x[1]['first_timestamp']).total_seconds()))

# Sorting and extracting the top 10
final_rdd_with_durations_sorted = final_rdd_with_durations.sortBy(lambda x: x[1], ascending=False).take(10)

logging.info("Writting into disk...")
with open('output-level3.txt', 'w') as f:
    for session in final_rdd_with_durations_sorted:
        f.write("======= Session: " + str(session[0][0]) + " ========\n")
        f.write(" User: \t"  + str(session[0][1]['userid'] + "\n"))
        f.write(" Duration (s): \t"  + getTime(session[1]) + "\n")
        f.write(' Time of the first song: ' + str(session[0][1]['first_timestamp'])+ "\n")
        f.write(' Time of the last song:  ' + str(session[0][1]['last_timestamp'])+ "\n")
        f.write(" List of songs:\n")
        for song in session[0][1]['songs']:
            f.write("\t {0}\t{1} => {2}\n".format(song['timestamp'], song['artname'].encode('utf-8'), song['traname'].encode('utf-8')))

spark.stop()






