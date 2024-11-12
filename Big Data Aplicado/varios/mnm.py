from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

spark = SparkSession.builder.appName("KoloreKalkulua").getOrCreate()

df = spark.read.csv("mnm_dataset.csv", header=True, inferSchema=True)

# Fitxategiaren egitura aztertu
df.printSchema()

# Datuak bistaratu
df.show(5)

# Estatu eta kolore bakoitzeko kopuruaren batura kalkulatu
emaitza = df.groupBy("State", "Color").agg(sum("Count").alias("TotalQuantity"))
emaitza2 = df.groupBy("State", "Color").agg(
    sum("Count").alias("TotalQuantity"),
    count("Color").alias("ColorCount")
)

# Emaitzak ikusi
emaitza.show()
emaitza2.show()

spark.stop()
