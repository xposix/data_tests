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

from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections
import datetime

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.appName("Level3").getOrCreate()

def mapper(line):
    fields = line.split('\t')
    for i in fields:
        print(("{0}".format(i.encode('utf-8'))))
    timestamp = datetime.datetime.strptime(fields[1],'%Y-%m-%dT%H:%M:%SZ')
    # return Row(userid=str(fields[0]), timestamp=timestamp, artname=str(fields[3]).encode("utf-8"), traname=str(fields[5]).encode("utf-8"))
    return Row(userid=str(fields[0]), timestamp=timestamp, artname=str(fields[3].encode("utf-8")), traname=str(fields[5]).encode("utf-8"))

lines = spark.sparkContext.textFile("userid-timestamp-artid-artname-traid-traname-10k.tsv")
playbacks = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPlaybacks = spark.createDataFrame(playbacks).cache()
schemaPlaybacks.createOrReplaceTempView("playbacks")

schemaPlaybacks.show()

# SQL can be run over DataFrames that have been registered as a table.
# teenagers = spark.sql("SELECT * FROM playbacks WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
# for teen in teenagers.collect():
#   print(teen)

# We can also use functions instead of SQL queries:
# schemaPlaybacks.groupBy("userid").count().orderBy("userid").show()

spark.stop()






