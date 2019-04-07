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
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructType
from pyspark.sql.functions import udf, lit
import pprint
import logging

import collections
import datetime
import time

# Create a SparkSession
spark = SparkSession.builder.appName("Level3").getOrCreate()

def getTime(seconds):
    sec = datetime.timedelta(seconds=seconds)
    d = datetime.datetime(1,1,1) + sec

    return ("%dd %02dh %02dm %02ds" % (d.day-1, d.hour, d.minute, d.second))

def mapper(line):
    fields = line.split('\t')
    #timestamp = unix_timestamp(fields[1],'%Y-%m-%dT%H:%M:%SZ')
    timestamp = datetime.datetime.strptime(fields[1],'%Y-%m-%dT%H:%M:%SZ')
    # return Row(userid=str(fields[0]), timestamp=timestamp, artname=str(fields[3]).encode("utf-8"), traname=str(fields[5]).encode("utf-8"))
    return Row(userid=str(fields[0]), timestamp=timestamp, artname=str(fields[3].encode("utf-8")), traname=str(fields[5].encode("utf-8")))
    # return Row(userid=str(fields[0]), timestamp=lines.unix_timestamp(fields[1],'%Y-%m-%dT%H:%M:%SZ'))

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
# Loading file...")
lines = spark.sparkContext.textFile("userid-timestamp-artid-artname-traid-traname.tsv")
playbacks = lines.map(mapper)
# ... file loaded")
# Infer the schema, and register the DataFrame as a table.
schemaPlaybacks = spark.createDataFrame(playbacks) 
# .cache() we don't need to cache this one
schemaPlaybacks.createOrReplaceTempView("playbacks")

# Order playbacks so they are all consecutive by user and timestamp
orderedPlaybacks = schemaPlaybacks.orderBy("userid","timestamp").dropDuplicates(["userid","timestamp"])
orderedPlaybacks.createOrReplaceTempView("orderedPlaybacks")

# Calculating beginning of each session
####
calculatedSessions = spark.sql(""" 
    SELECT *, count(*)
        OVER (PARTITION BY userid
            ORDER BY timestamp 
            RANGE BETWEEN interval 20 minutes preceding AND current row)   
        AS preceding_songs_count
    FROM orderedPlaybacks
    """)

calculatedSessions.createOrReplaceTempView("calculatedSessions")

# Calculating end of each session
calculatedSessions = spark.sql(""" 
    SELECT *, count(*)
        OVER (PARTITION BY userid
            ORDER BY timestamp 
            RANGE BETWEEN current row AND interval 20 minutes following)   
        AS following_songs_count
    FROM calculatedSessions
    """)

calculatedSessions.createOrReplaceTempView("calculatedSessions")
# Removing the one-song sessions
calculatedSessionsFiltered = calculatedSessions.filter( ~ ((calculatedSessions.preceding_songs_count == 1) & (calculatedSessions.following_songs_count == 1)) )

# Adding the session ID
# songList = calculatedSessionsFiltered.withColumn('sessionid', lit(0)).collect()

# Produce a list of 10 longest sessions by elapsed time with 
# the following details for each session:
#  - User
#  - Time of the First Song been played
#  - Time of the Last Song been played
#  - List of songs played (sorted in the order of play)
#
# First set of calculations finished
sessionid = 0
songsBySession = {}
logging.info("Calculating sessions")
songs = calculatedSessionsFiltered.collect()
logging.info("Calculating processing in dict")
for song in songs:
    if song.preceding_songs_count == 1:
        # Start of the session
        sessionid += 1
        first_timestamp = song.timestamp
        songsBySession[sessionid] = {}
        songsBySession[sessionid]['songs'] = []
    elif song.following_songs_count == 1:
        # total_duration = last_timestamp - first_timestamp
        ## FINISH THE SESSION
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

# calculatedSessionsFiltered.createOrReplaceTempView("calculatedSessionsFiltered")
# spark.sql('SELECT userid, timestamp, preceding_songs_count, following_songs_count FROM calculatedSessionsFiltered').show(50)

# sessionsStartFiltered = sessionstarttimes.filter( sessionstarttimes.related_songs_count == 1 )
# sessionsFinishFiltered = sessionfinishtimes.filter( sessionfinishtimes.related_songs_count == 1 )
# sessionsAndPlaybacks.show(40)
# sessionsAndPlaybacks.orderBy('related_songs_count', ascending=False).show()
# pp = pprint.PrettyPrinter(indent=2)
# pp.pprint(songsBySession)

# cond = [sessionsStartFiltered.userid == sessionsFinishFiltered.userid, sessionsStartFiltered.timestamp == sessionsFinishFiltered.timestamp]
# print("RESULT: "+ str(sessionsStartFiltered.join(sessionsFinishFiltered, cond, 'outer').count()))

# for i in range(len(sessionsStartFiltered)):
#     try:
#         print("ST: " + str(sessionsStartFiltered[i].userid) + " " + str(sessionsStartFiltered[i].timestamp) + " FN:" + str(sessionsFinishFiltered[i].userid) + " " + str(sessionsFinishFiltered[i].timestamp))
#     except Exception as exc:
#         print('Reaching the end of the list' + str(exc))

##############################
# # user_sessions = orderedPlaybacks.groupBy("userid")
# tenLongestSessions = spark.createDataFrame(user_sessions.head(10))
# tenLongestSessions.write.csv('output-level3.tsv', mode='overwrite', sep='\t')

# SQL can be run over DataFrames that have been registered as a table.
# teenagers = spark.sql("SELECT * FROM playbacks WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
# pp = pprint.PrettyPrinter(indent=2)
# pp.pprint(final_rdd_with_durations_sorted)
# for session in final_rdd_with_durations_sorted:
#     print("======= Session: " + str(session[0][0]) + " ========")
#     print(" User: \t"  + str(session[0][1]['userid']))
#     print(" Duration (s): \t"  + getTime(session[1]))
#     print(' Time of the first song: ' + str(session[0][1]['first_timestamp']))
#     print(' Time of the last song:  ' + str(session[0][1]['last_timestamp']))
#     print(' List of songs:')
#     for song in session[0][1]['songs']:
#         print("\t {0}\t{1} => {2}".format(song['timestamp'], song['artname'].encode('utf-8'), song['traname'].encode('utf-8')))

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

# We can also use functions instead of SQL queries:
# schemaPlaybacks.groupBy("userid").count().orderBy("userid").show()

spark.stop()






