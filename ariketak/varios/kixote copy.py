from pyspark.sql.functions import col, explode, collect_list, lit
from pyspark.sql import SparkSession, Row
from pyspark.ml.feature import Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover, Tokenizer, NGram, HashingTF, HashingTF, MinHashLSH, RegexTokenizer, SQLTransformer

#pyspark --remote sc://192.168.85.2
spark = SparkSession.builder.appName("Kixote").remote("sc://192.168.85.2").getOrCreate()

df = spark.read.text("hdfs://192.168.85.2:9000/fitxategiak/el_quijote.txt")
#df2 = df.select(explode("value").alias("text"))


pipeline = Pipeline(stages=[
    RegexTokenizer(
        pattern="", inputCol="value", outputCol="words", minTokenLength=1
    ),
    NGram(n=3, inputCol="words", outputCol="ngrams"),
    HashingTF(inputCol="ngrams", outputCol="vectors")
])

# tokenizer = Tokenizer(inputCol="word")
# tokenizer.setInputCol("value")
# tokenizer.transform(df).take(2)

# pipeline = Pipeline(stages=[tokenizer,
#                            NGram(n=3, inputCol="words", outputCol="ngrams"),
#                            HashingTF(inputCol="ngrams", outputCol="vectors"),
#                            MinHashLSH(inputCol="vectors", outputCol="lsh")])

pipeline = Pipeline(stages=[
    RegexTokenizer(
        pattern="", inputCol="value", outputCol="words", minTokenLength=1
    ),
    NGram(n=3, inputCol="words", outputCol="ngrams"),
    HashingTF(inputCol="ngrams", outputCol="vectors"),
    MinHashLSH(inputCol="vectors", outputCol="lsh")])

model = pipeline.fit(df)
textuaren_transformazioa = model.transform(df)
textuaren_transformazioa.take(1)
bilatzeko = spark.createDataFrame([Row(value='rocinante')])
bilatzeko_transformazioa = model.transform(bilatzeko)

textuaren_transformazioa.withColumn('values2', lit(bilatzeko_transformazioa.take(1)[0].value))
textuaren_transformazioa.withColumn('words2', lit(bilatzeko_transformazioa.take(1)[0].words))
textuaren_transformazioa.withColumn('ngrams2', lit(bilatzeko_transformazioa.take(1)[0].ngrams))
textuaren_transformazioa.withColumn('vectors2', lit(bilatzeko_transformazioa.take(1)[0].vectors))

textuaren_transformazioa.stages[1].approxSimilarityJoin(textuaren_transformazioa, bilatzeko_transformazioa,1.0, "confidence")

r = model.stages[-1].approxSimilarityJoin(textuaren_transformazioa, bilatzeko_transformazioa, 1.0,"confidence")

textuaren_transformazioa.withColum('bilatzeko',lit('rocinante'))
predict = model.transform(test)
print(predict)
#model.take(2)
#print(df2.take(3))
