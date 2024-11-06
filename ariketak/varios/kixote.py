from pyspark.sql import SparkSession
import re

spark = SparkSession.builder.appName("Kixote").getOrCreate()

df = spark.read.text("hdfs://192.168.85.2:9000/fitxategiak/el_quijote.txt")

df.show()

# The clean_split function splits a given string into a list of lowercase words, ignoring punctuation and treating words in a case-insensitive manner.
def clean_split(line):
    return re.findall(r'\w+', line.lower())

# Convertir DataFrame a RDD, RDD es una colección de objetos distribuidos
''' 
| value      |
|------------|
| El quijote |
| avanzó a...|
'''
rdd = df.rdd.map(lambda row: row[0])  

# The lambda function lambda row: row[0] takes each row and picks out the first column (the [0] part). So, if a row is like ['Hello', 'World'], it picks 'Hello'. This line of code takes the first column from each row in your DataFrame and makes a list (RDD) out of those values.

"""
| Column1 | Column2 |
|---------|---------|
|  Hello  |  World  |
|  Foo    |  Bar    |
"""

""" 
RDD (rdd): This RDD contains lines of text from the DataFrame. Each element is a line of text.

Example: ["Hello World", "Foo Bar"]
Function (clean_split): This function takes a line of text and splits it into words.

Example: clean_split("Hello World") returns ["hello", "world"]
Applying flatMap: The flatMap function applies clean_split to each line in the RDD and flattens the results into a single RDD of words.

Before flatMap: ["Hello World", "Foo Bar"]
After flatMap: ["hello", "world", "foo", "bar"]

"""

# The flatMap function in PySpark is used to transform each element of an RDD into multiple elements. It applies a function to each element of the RDD and then flattens the results
words_rdd = rdd.flatMap(clean_split)
# Convieerte a:

""" 
['Hello', 'Foo']
"""

# Contar las apariciones de "Dulcinea" y "Rocinante"
dulcinea_count = words_rdd.filter(lambda word: word.lower().startswith("dulcinea")).count()
rocinante_count = words_rdd.filter(lambda word: word.lower().startswith("rocinante")).count()

# Procesamiento paralelo de los resultados
summary_rdd = spark.sparkContext.parallelize([
    f"Dulcinea aparece {dulcinea_count} veces.",
    f"Rocinante aparece {rocinante_count} veces."
])

# Contar todas las palabras
word_counts = words_rdd.countByValue()

# Ordenar las palabras por frecuencia
sorted_word_counts = sorted(word_counts.items(), key=lambda item: item[1], reverse=True)

# Convertir el resultado ordenado a RDD
sorted_word_counts_rdd = spark.sparkContext.parallelize(sorted_word_counts)

# Definir la ruta de salida
# output_path = "./wcQuijote"
output_path = "hdfs://192.168.85.2:9000/user/marko_gros/wcQuijote"

# Guardar el resultado en un archivo de texto
summary_rdd.saveAsTextFile(output_path + "/dulcinea_rocinante")
sorted_word_counts_rdd.saveAsTextFile(output_path + "/word_count_total")

# ssh hadoop@192.168.85.2
# hdfs dfs -cat /user/marko_gros/wcQuijote/word_count_total/part-00000 | head -n 10
# hdfs dfs -rm -R /user/marko_gros/wcQuijote
