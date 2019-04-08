#############################################
#  Level 1 
#  Produce a list of Users and the number of distinct songs the user has played.

from pyspark import SparkConf, SparkContext

def parseLine(line):
    ## Some of the entries don't have traid but all tracks have names.
    fields = line.split('\t')
    userid = fields[0]
    newartname = fields[3] + fields[5]

    return (userid, newartname)

conf = SparkConf().setMaster("local[*]").setAppName("level1")
sc = SparkContext(conf = conf)

# Opening and parsing the file into an RDD
inputFile = sc.textFile("userid-timestamp-artid-artname-traid-traname.tsv")
rdd = inputFile.map(parseLine)

# Removing duplicates, if any.
simplified_rdd = rdd.distinct()

# Calculating totals and sorting by user name
totals = simplified_rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda accumulated, current: accumulated + current)
ordered_totals = totals.sortBy(lambda x: x[0])

# Getting the results
results = ordered_totals.collect()

# Writing results into a file
with open('output-level1.tsv', 'w') as f:
    for result in results:
        print(result)
        f.write("{0}\t{1}\n".format(result[0],result[1]))