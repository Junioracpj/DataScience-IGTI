# Databricks notebook source
Por Antonio Carlos

# COMMAND ----------

# Os dataframes em Spark, permite manipulacao dos dados e ate estatisticas
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import mean, min, max
from pyspark.sql.functions import struct

spark = SparkSession \
        .builder\
        .appName('PythonWordCount')\
        .getOrCreate()
sqlc = SQLContext(spark.sparkContext)
df = (sqlc.range(0, 1000 * 1000)
     .withColumn('uniform', rand(seed=10))
     .withColumn('normal', rand(seed=27)))
print('# rows: ', df.count())
df.show()

# COMMAND ----------

# Vai mostrar as principais metricas do DF
# principais manipulacoes

df.describe().show() # principais metricas
df.describe('id', 'normal').show()
df.select([mean('uniform'), min('uniform'), max('uniform')]).show() 

# COMMAND ----------

# covariancia

SqlContext = SQLContext(spark.sparkContext)
df = sqlContext.range(0, 1000 * 1000).withColumn('rand1', rand(seed=10))\
                                    .withColumn('rand2', rand(seed=27))

df.stat.cov('rand1', 'rand2') # covarianca entre as variaveis
df.agg({'rand2': 'variance'}).show() # covariancia dentro da propria variavel

# COMMAND ----------

# correlacao

SqlContext = SQLContext(spark.sparkContext)
df = sqlContext.range(0, 1000 * 1000).withColumn('rand1', rand(seed=10))\
                                    .withColumn('rand2', rand(seed=27))

print(df.stat.corr('rand1', 'rand2')) # correlacao
print(df.stat.corr('rand2', 'rand2')) 

# COMMAND ----------

names = ['pedro', 'maria', 'joao']
colors = ['verde', 'rosa', 'vermelho', 'preto', 'cinza']
df = sqlContext.createDataFrame([(names[i % 3], colors[i % 5])for i in range(100)],['name', 'color'])
# df.show()
df.stat.crosstab('name', 'color').show() # contagem de frequencia dos valores


# COMMAND ----------

df = sqlContext.createDataFrame([(1, 2, 3)if i % 2 == 0 else (i, 2 * i, i % 4)for i in range(100)], ['a', 'b', 'c'])
# df.show()

freq = df.stat.freqItems(['a', 'b', 'c'], 0.4) 
# demonstra as principais ocorrencias de cada coluna
# freq.collect()[0] 

freq = df.withColumn('ab', struct('a', 'b')).stat.freqItems(['ab'], 0.4)
# demonstra as principais ocorrencias em colunas juntas
freq.collect()[0]