# Databricks notebook source
# DBTITLE 1,PySpark - Testes
# MAGIC %md
# MAGIC **Por Antonio Carlos**
# MAGIC 
# MAGIC June 2022

# COMMAND ----------

import sys
from  operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext

# COMMAND ----------

#if __name__ == "__main__":
     # if len(sys.argv) != 2:
       # print('Usage: wordcount <file>', file = sys.stderr)
        #exit(-1)
        
spark = SparkSession\
    .builder\
    .appName('PythonWordCount')\
    .getOrCreate()

sc = spark.sparkContext
text_file = sc.textFile("dbfs:/FileStore/tables/palavras.txt")

counts = text_file.flatMap(lambda line: line.split(' ')) \
                .map(lambda word: (word, 1)) \
                .reduceByKey(add)
output = counts.collect()
for (word, count) in output:
    print('%s: %i' % (word, count))
sc.stop()
spark.stop()

# COMMAND ----------

