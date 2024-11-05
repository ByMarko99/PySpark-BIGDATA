from pyspark import SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySpark 101 Exercises").getOrCreate()
sc = spark.sparkContext


df = spark.read.option("header", True).csv("/home/ir_inf/scripts/ariketak/pyspark-examples/simple-zipcodes.csv")

df.printSchema()

df.write.option("header", True).partitionBy("state","city").mode("overwrite").csv("/home/ir_inf/scripts/ariketak/pyspark-examples/zipcodes-state")