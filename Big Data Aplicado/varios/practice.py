from pyspark import SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySpark 102 Exercises").getOrCreate()
sc = spark.sparkContext

""" 
 How to convert the index of a PySpark DataFrame into a column?

df = spark.createDataFrame([
("Alice", 1),
("Bob", 2),
("Charlie", 3),
], ["Name", "Value"])

df.show()

from pyspark.sql.functions import monotonically_increasing_id 

df_index = df.select("*").withColumn("id", monotonically_increasing_id())

df_index.show() """

""" 
 How to combine many lists to form a PySpark DataFrame?


list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]

rdd = spark.sparkContext.parallelize(list(zip(list1, list2)))
df = rdd.toDF(["Column1", "Column2"])

df.show()

data = [1, 2, 3] 
data1 = ["sravan", "bobby", "ojaswi"] 
  
# specify column names 
columns = ['ID', 'NAME'] 
  
# creating a dataframe by zipping the two lists 
dataframe = spark.createDataFrame(zip(data, data1), columns) 
  
# show data frame 
dataframe.show()  """

""" list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]


df = spark.createDataFrame(zip(list1, list2), ["Column1", "Column2"]) 
  
df.show()   """


""" 
4. How to get the items of list A not present in list B?

list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]

rdd_A = sc.parallelize(list_A)
rdd_B = sc.parallelize(list_B)

result_rdd = sorted(rdd_A.subtract(rdd_B).collect())

print(result_rdd) """

""" 
 How to get the items not common to both list A and list B?

list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]

rdd_A = sc.parallelize(list_A)
rdd_B = sc.parallelize(list_B)

# Perform subtract operation
result_rdd_A = rdd_A.subtract(rdd_B)
result_rdd_B = rdd_B.subtract(rdd_A)

# Union the two RDDs
result_rdd = result_rdd_A.union(result_rdd_B)

# Collect result
result_list = result_rdd.collect()

print(result_list) """




""" 
How to get the minimum, 25th percentile, median, 75th, and max of a numeric column?

data = [("A", 10), ("B", 20), ("C", 30), ("D", 40), ("E", 50), ("F", 15), ("G", 28), ("H", 54), ("I", 41), ("J", 86)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.show()

df.select("Age").summary("mean", "min", "25%", "75%", "max").show()


 """


""" 
How to get frequency counts of unique items of a column?

from pyspark.sql import Row

# Sample data
data = [
Row(name='John', job='Engineer'),
Row(name='John', job='Engineer'),
Row(name='Mary', job='Scientist'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Scientist'),
Row(name='Sam', job='Doctor'),
]

# create DataFrame
df = spark.createDataFrame(data)

# show DataFrame
df.show()

df.groupBy("job").count().show()
 """

""" 

How to keep only top 2 most frequent values as it is and replace everything else as ‘Other’?

from pyspark.sql import Row
from pyspark.sql.functions import col, when

# Sample data
data = [
Row(name='John', job='Engineer'),
Row(name='John', job='Engineer'),
Row(name='Mary', job='Scientist'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Scientist'),
Row(name='Sam', job='Doctor'),
]

# create DataFrame  
df = spark.createDataFrame(data)


df_other = df.withColumn("job", when(df["job"] == df.groupBy("job").count().orderBy(col("count").asc()).limit(1).select("job").collect()[0]["job"], "Other").otherwise(df["job"]))
df_other.show() """




