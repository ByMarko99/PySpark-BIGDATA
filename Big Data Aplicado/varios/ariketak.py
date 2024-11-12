# 1

from pyspark import SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySpark 101 Exercises").getOrCreate()
sc = spark.sparkContext

""" print(spark.version)

# 2
df = spark.createDataFrame([
("Alice", 1),
("Bob", 2),
("Charlie", 3),
], ["Name", "Value"]) """


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id

""" w = Window.orderBy(monotonically_increasing_id())

# Add index
df = df.withColumn("index", row_number().over(w) - 1)

df.show() """

# 3 
""" list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]

rdd = spark.sparkContext.parallelize(list(zip(list1, list2)))
# df = spark.createDataFrame(list(zip(list1,list2)), ["Column1", "Column2"])
df = rdd.toDF(["Column1", "Column2"])

df.show() """

# 4
""" list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]

rdd_A = sc.parallelize(list_A)
rdd_B = sc.parallelize(list_B)
print(rdd_A.subtract(rdd_B).collect()) """

# 5
""" list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]

rdd_A = sc.parallelize(list_A)
rdd_B = sc.parallelize(list_B)

result_rdd_A = rdd_A.subtract(rdd_B)
result_rdd_B = rdd_B.subtract(rdd_A)

result_rdd = result_rdd_A.union(result_rdd_B).sortBy(lambda x: x).collect()

print(result_rdd) """

# 6

""" data = [("A", 10), ("B", 20), ("C", 30), ("D", 40), ("E", 50), ("F", 15), ("G", 28), ("H", 54), ("I", 41), ("J", 86)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.show()
df.paralle

quantiles = df.approxQuantile("Age", [0.0, 0.25, 0.5, 0.75, 1.0], 0.01)


print("Min: ", quantiles[0])
print("25th percentile: ", quantiles[1])
print("Median: ", quantiles[2])
print("75th percentile: ", quantiles[3])
print("Max: ", quantiles[4]) """

# 7

""" from pyspark.sql import Row

data = [
Row(name='John', job='Engineer'),
Row(name='John', job='Engineer'),
Row(name='Mary', job='Scientist'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Scientist'),
Row(name='Sam', job='Doctor'),
]

df = spark.createDataFrame(data)
df.show()
df.groupBy("job").count().show()
 """
 
 
# 8

""" from pyspark.sql import Row

data = [
Row(name='John', job='Engineer'),
Row(name='John', job='Engineer'),
Row(name='Mary', job='Scientist'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Scientist'),
Row(name='Sam', job='Doctor'),
]

df = spark.createDataFrame(data)

df.show()

from pyspark.sql.functions import col, when

# Get the top 2 most frequent jobs
top_2_jobs = df.groupBy('job').count().orderBy('count', ascending=False).limit(2).select('job').rdd.flatMap(lambda x: x).collect()

# Replace all but the top 2 most frequent jobs with 'Other'
'''df.withColumn(columnName, columnExpression)
Arguments:
columnName: The name of the column to add or replace.
columnExpression: An expression that defines the values for the new column.

Syntax: when(condition, value)
Arguments:
condition: A boolean expression that specifies the condition to evaluate.
value: The value to return if the condition is true.

when(condition, value).otherwise(otherValue)
If the value in the job column is in the list top_2_jobs, return the value in the job column otherwise Other.
'''
df = df.withColumn('job', when(col('job').isin(top_2_jobs), col('job')).otherwise('Other'))

df.show() """

# 10

""" df = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["col1", "col2", "col3"])

old_names = ["col1", "col2", "col3"]

new_names = ["new_col1", "new_col2", "new_col3"]

df.show()

for old_name, new_name in zip(old_names, new_names):
    df = df.withColumnRenamed(old_name, new_name)

df.show() """

# 11

""" from pyspark.sql.functions import rand
from pyspark.ml.feature import Bucketizer

# Create a DataFrame with a single column "values" filled with random numbers
num_items = 100
df = spark.range(num_items).select(rand(seed=42).alias("values"))

df.show(5)

num_buckets = 10
quantiles = df.stat.approxQuantile("values", [i/num_buckets for i in range(num_buckets+1)], 0.01)

bucketizer = Bucketizer(splits=quantiles, inputCol="values", outputCol="buckets")

df_buck = bucketizer.transform(df)

df_buck.groupBy("buckets").count().show()

df_buck.show(5) """

# 21
""" 
data = [("2023-05-18","01 Jan 2010",), ("2023-12-31", "01 Jan 2010",)]
df = spark.createDataFrame(data, ["date_str_1", "date_str_2"])

df.show()

from pyspark.sql.functions import to_date, dayofmonth, weekofyear, dayofyear, dayofweek

df = df.withColumn("date_1", to_date(df.date_str_1, 'yyyy-MM-dd'))
df = df.withColumn("date_2", to_date(df.date_str_2, 'dd MMM yyyy'))

df = df.withColumn("day_of_month", dayofmonth(df.date_1))\
.withColumn("week_number", weekofyear(df.date_1))\
.withColumn("day_of_year", dayofyear(df.date_1))\
.withColumn("day_of_week", dayofweek(df.date_1))

df.show()
 """
# 39

from pyspark.sql import functions as F 
# Sample data
data = [(10, 25, 70),
(40, 5, 20),
(70, 80, 100),
(10, 2, 60),
(40, 50, 20)]

# Create DataFrame
df = spark.createDataFrame(data, ["col1", "col2", "col3"])

# Display original DataFrame
df.show()

df = df.withColumn("batuketa", df.col1 + df.col2 + df.col3)
df = df.filter(df.batuketa > 100)

# r = df.drop("batuketa")

df = df.withColumn('id', F.monotonically_increasing_id())

'''

+----+----+----+--------+---+
|col1|col2|col3|batuketa| id|
+----+----+----+--------+---+
|  10|  25|  70|     105|  0|
|  70|  80| 100|     250|  1|
|  40|  50|  20|     110|  2|
+----+----+----+--------+---+
'''

df_last_2 = df.sort(F.desc('id')).limit(2)

df_last_2.show()