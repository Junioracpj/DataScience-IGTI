# Databricks notebook source
Por Antonio Carlos

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import rand, randn, col
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import mean, min, max
from pyspark.sql.functions import struct
import pandas as pd

spark = SparkSession \
        .builder\
        .appName('TestePratico')\
        .getOrCreate()


# COMMAND ----------

#Lendo o arquivo
df = spark.read.csv("/FileStore/tables/all_stocks_5yr.csv", header='True', inferSchema='True')
df.show(3)

# COMMAND ----------

# Questao 01
df.count()
# contabiliza todos os dados


# COMMAND ----------

# Questao 02
df.filter(df.Name == 'AAPL').count()
# Contabilizando quantas vezes que a empresa aparece

# COMMAND ----------

# Questao 03
empresas = df.groupBy('open').count()
display(empresas)
# Contabilizando quantas empresas existem

# COMMAND ----------

# Questao 4
x = df.filter(df.close > df.open).count()
y = df.filter(df.close <= df.open).count()
print(x/(x+y)*100)
# Porcentagem de acoes que fechar maior que abriram


# COMMAND ----------

# Questao 5
df2 = df.filter(df.Name == 'AAPL')
df2.describe('high').show()
# descobrindo o maior valor de uma acao na historia

# COMMAND ----------

# Questao 6
df.createOrReplaceTempView("acoes")
spark.sql("select Name, stddev(close) as stddev from acoes group by Name order by  stddev(close) desc").show(5)
# descobrindo a acao de maior desvio padrao

# COMMAND ----------

# Questao 7
spark.sql("select date, round(sum(volume)) as total from acoes group by date order by  sum(volume) desc").show(5)
# Descobrindo a maior data volume de negociacoes

# COMMAND ----------

# Questao 8
spark.sql("select Name, round(sum(volume)) as total \
from acoes group by Name \
order by  sum(volume) desc").show(5)
# maiores volumes de acoes vendidas

# COMMAND ----------

# Questao 09
df2 = df.groupBy('Name').count()
df2.filter(df2.Name.startswith('A')).count()
# Contanto quantas acoes comecam com a letra A

# COMMAND ----------

#Questao 10
x = df.filter(df.high == df.close).count()
y = df.filter(df.high != df.close).count()
print(x/(x+y)*100)

# COMMAND ----------

# Questao 11
df2 = df.filter(df.Name == 'AAPL')
newdf = df2.withColumn("Diference", df.close - df.open)
newdf.describe('Diference').show()
newdf.filter(newdf.Diference == 8.25).show()
# Dia de maior diferenca de entrada e saida de uma acao

# COMMAND ----------

# Questao 12
df2 = df.filter(df.Name == 'AAPL')
df2.describe('volume').show()
# Media voolume acoes 


# COMMAND ----------

# Questao 13
df.show(2)
spark.sql("select count(distinct(Name))  tamanho from acoes \
where length(Name) = 1  \
union all \
select count(distinct(Name))  tamanho from acoes \
where length(Name) = 2 \
union all \
select count(distinct(Name))  tamanho from acoes \
where length(Name) = 3 \
union all \
select count(distinct(Name))  tamanho from acoes \
where length(Name) = 4 \
union all \
select count(distinct(Name))  tamanho from acoes \
where length(Name) = 5 \
"  ).show()
# quantidade de acoes com 1, 2 , 3, 4 e 5 caracteres


# COMMAND ----------

# Questao 14
spark.sql("select  Name, sum(volume) from acoes group by Name order by sum(volume) asc ").show(5)
# qual acao teve menos negociada na bolsa

# COMMAND ----------

# Questao 15
spark.sql(" select count(*)  total_transacoes from acoes   ").show(1)
df3 = spark.sql(" select count(*) total_close_maior_do_dia from acoes where close > open  and close > low and  close > high")
df3.createOrReplaceTempView("acoes4")
df3.show(1)

spark.sql(" select (round( (1 / 619040) * 100)) close_mais_alto_do_dia from acoes ").show(1)
# Com qual frequência o preço de fechamento é também o mais alto do dia