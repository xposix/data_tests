#############################################
#  Level 2 
#  Produce a list of 100 most popular Song titles, its Artist, and the number of times the songs have been played.

from pyspark import SparkConf, SparkContext
import collections
def parseLine(line):
    ## Some of the entries don't have traid but all tracks have names.
    fields = line.split('\t')
    artname = fields[3]
    traname = fields[5]
    # Returning only Artist and Track names
    return (artname, traname)

conf = SparkConf().setMaster("local[*]").setAppName("level1")
sc = SparkContext(conf = conf)

# Opening and parsing the file into an RDD
inputFile = sc.textFile("userid-timestamp-artid-artname-traid-traname.tsv")
rdd = inputFile.map(parseLine)

# Doing the calculations
result = rdd.countByValue()
# Sorting by the number of plays
sortedResults = collections.OrderedDict(sorted(result.items(), key=lambda x:x[1], reverse=True))

# Writing results into a file
counter=1
with open('output-level2.tsv', 'w') as f:
    for key, value in sortedResults.items()[:100]:
        if value > 2:
            print(("{0}: {1} ===> {2} ===> {3}".format(counter, key[0].encode('utf-8'),key[1].encode('utf-8'),value)))
            f.write("{0}\t{1}\t{2}\n".format(key[0].encode('utf-8'),key[1].encode('utf-8'),value))
        counter +=1

## TODO: pyspark.sql.functions.sumDistinct(col)
# Aggregate function: returns the sum of distinct values in the expression.