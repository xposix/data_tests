#############################################
#  Level 2 
#  Produce a list of 100 most popular Song titles, its Artist, and the number of times the songs have been played.

from pyspark import SparkConf, SparkContext
import collections
def parseLine(line):
    ## Some of the entries don't have traid but all tracks have names although some of the names are just characters.
    fields = line.split('\t')
    artname = fields[3]
    traname = fields[5]

    return (artname, traname)

conf = SparkConf().setMaster("local[*]").setAppName("level1")
sc = SparkContext(conf = conf)

inputFile = sc.textFile("userid-timestamp-artid-artname-traid-traname.tsv")
rdd = inputFile.map(parseLine)

# print("Before distinct: " + str(rdd.count()))
# simplified_rdd = rdd.distinct()

# print("After distinct: " + str(simplified_rdd.count()))

result = rdd.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items(), key=lambda x:x[1], reverse=True))
counter=1

with open('output-level2.tsv', 'w') as f:
    for key, value in sortedResults.items()[:100]:
        if value > 2:
            print(("{0}: {1} ===> {2} ===> {3}".format(counter, key[0].encode('utf-8'),key[1].encode('utf-8'),value)))
            f.write("{0}\t{1}\t{2}\n".format(key[0].encode('utf-8'),key[1].encode('utf-8'),value))
        counter +=1

# totals = rdd.map(lambda x: (x, 1)).reduceByKey(lambda accumulated, current: accumulated + current)

# ordered_totals = totals.sortBy(lambda x: x[1])
# print("AQUI:" + str(ordered_totals.top(1)))
# results = ordered_totals.top(100)

# with open('output-level2.tsv', 'w') as f:
#     for result in results:
#         print(result)
#         f.write("{0}{1}\n".format(result[0],result[1]))

## TODO: pyspark.sql.functions.sumDistinct(col)
# Aggregate function: returns the sum of distinct values in the expression.

# New in version 1.3.

## 



