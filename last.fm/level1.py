#############################################
#  Level 1 
#  Produce a list of Users and the number of distinct songs the user has played.

from pyspark import SparkConf, SparkContext

def parseLine(line):
    ## Some of the entries don't have traid but all tracks have names although some of the names are just characters.
    fields = line.split('\t')
    userid = fields[0]
    newartname = fields[3] + fields[5]

    return (userid, newartname)

conf = SparkConf().setMaster("local[*]").setAppName("level1")
sc = SparkContext(conf = conf)

inputFile = sc.textFile("userid-timestamp-artid-artname-traid-traname.tsv")
rdd = inputFile.map(parseLine)

print("Before distinct: " + str(rdd.count()))
simplified_rdd = rdd.distinct()

print("After distinct: " + str(simplified_rdd.count()))

totals = simplified_rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda accumulated, current: accumulated + current)
ordered_totals = totals.sortBy(lambda x: x[0])

# print(ordered_totals.take(200))
results = ordered_totals.collect()

with open('output-level1.tsv', 'w') as f:
    for result in results:
        print(result)
        f.write("{0}\t{1}\n".format(result[0],result[1]))