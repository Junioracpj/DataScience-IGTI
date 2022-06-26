# Databricks notebook source
Por Antonio Carlos

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Rdd é poderoso, mas o spark trata isso como objeto, não fazendo otimização de certos recursos. O dataframa é uma opção a isso

# COMMAND ----------

# Versao em RDD
spark = (SparkSession.builder
        .appName('Ages')
        .getOrCreate())
dataRDD = spark.sparContext.parallelize([('Pedro', 38),
                                         ('Maria', 20),
                                         ('Pedro', 40),
                                         ('Rafael', 10)])
agesRDD = (dataRDD
              .map(lambda x: (x[0], (x[1], 1)))
              .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
              .map(lambda x: (x[0], (x[1][0] / x[1][1])))
           
agesRDD.collect()

# COMMAND ----------

# Versao em Dataframe
# vai permite que o programa lide com colunas nomeadas,
# permite que o programador execute transformacoes tipicas de analise de dados
spark = (SparkSession.builder
        .appName('Ages')
        .getOrCreate())
data_df = spark.createDataFrame([('Pedro', 38),
                                 ('Maria', 20),
                                 ('Pedro', 40),
                                 ('Rafael', 10)], ['Nome', 'Idade'])
avg_df = data_df.groupBy('Nome').agg(avg('Idade'))
avg_df.show()


# COMMAND ----------

# Existe a possibilidade de converter o dataframe do pandas para o do Spark e vice versa

pandas_df = young.toPandas() # Converte do spark para o pandas
spark_df = context.CreateDataFrame(pandas_df) # converte do pandas para o spark

# API de dataframe e mais rapida que a do RDD, porque tem mais conhecimento sobre os dados