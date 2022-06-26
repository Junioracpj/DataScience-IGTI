# Databricks notebook source
Por Antonio Carlos

# COMMAND ----------

#Spark SQL
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Aula interativa 02').getOrCreate()


# COMMAND ----------

titanic_df = spark.read.csv('destino/', header='True', infer=)
titanic_df.printSchema()

# COMMAND ----------

titanic_df.count()

# COMMAND ----------

# Quantas pessoas sobreviveram
titanic_df.groupBy('survived').count().show()
spark.sql('SELECT S')

# COMMAND ----------

df = spark.read.format()