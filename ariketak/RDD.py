
from pyspark import SparkContext

sc = SparkContext(appName="RDD")

dataRDD = sc.parallelize([("Brooke",20), ("Denny",31), ("Jules",30), ("TD",35), ("Brooke",25)])


agesRDD = dataRDD.map(lambda x: (x[0], (x[1],1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0] / x[1][1]))

print(agesRDD.collect())
