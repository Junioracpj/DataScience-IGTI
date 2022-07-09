# Databricks notebook source
# MAGIC %md
# MAGIC Utilizando MLlib

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession \
        .builder\
        .appName('DetecçãoAnomalias')\
        .getOrCreate()


# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

diretorioDataset="/FileStore/tables/temperature.csv" 
#diretório que contém o arquivo a ser utilizado

# COMMAND ----------

data = spark.read.format("csv") \
            .options(header="true", inferschema="true") \
            .load(diretorioDataset)  #realiza a leitura do dataset

# COMMAND ----------

data.show(5, False)

# COMMAND ----------

data.columns # mostra as colunas do dataset

# COMMAND ----------

data.printSchema()

# COMMAND ----------

data.count()

# COMMAND ----------

df = data.select('datetime', 'Vancouver')

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Tratando os dados

# COMMAND ----------

#filtrando apenas os dados que não possuem valores nulos 
from pyspark.sql.functions import col
dfNotNull=df.filter(col('Vancouver').isNotNull())
dfNotNull.show(5)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id 
#biblioteca para a construção dos índices
df_plots = dfNotNull.withColumn("indice", monotonically_increasing_id())  #cria os indices para realizar o plot

# COMMAND ----------

df_plots.show(5)

# COMMAND ----------

# mostrando os dados
display(df_plots.select('Vancouver', 'indice'))

# COMMAND ----------

#encontrando a média 
import numpy as np  #biblioteca utilizada para tratar vetores e matrizes
from pyspark.sql.functions import mean, stddev  #funções para encontrar a média e desvio padrão

# COMMAND ----------

list_stats = dfNotNull.select(mean(col('Vancouver')) \
                    .alias('media'),stddev(col('Vancouver')) \
                    .alias('desvioPadrao')) \
                    .collect() 
#cria uma lista com os valores

media = list_stats[0]['media']
desvio = list_stats[0]['desvioPadrao']
print("Média: ", media)
print("Desvio Padrão: ", desvio)

# COMMAND ----------

df_stats = dfNotNull.select(mean(col('Vancouver'))
                    .alias('media'),stddev(col('Vancouver'))
                    .alias('desvioPadrao')) 
#cria o dataset com a média e o desvio padrão

df_stats.show()

# COMMAND ----------

# estatistica basica do dataframe
dfNotNull.describe().show()

# COMMAND ----------

#definindo a funcao distancia
def distancia(x):
  media=283.8626
  desvio=6.6401
  return ((x - media)/desvio)

#definindo a funcao para verificar anomalias mais do que 2 desvios padrões (95% dos dados)
def anomalias(x):
  desvio=6.6401
  if (x>2):
    return 1
  else:
    return 0

#definindo as funções a serem utilizadas (registrando)
from pyspark.sql.types import DoubleType, IntegerType

distancia_udf_double = udf(lambda z: distancia(z), DoubleType())
anomalia_udf_int = udf(lambda z: anomalias(z), IntegerType())

# COMMAND ----------

data_new=dataNotNull.select('Vancouver',distancia_udf_double('Vancouver')
                            .alias('distancia'))
data_new.show()

# COMMAND ----------

from  pyspark.sql.functions import abs   
#biblioteca necessária para cálculo do valor absoluto

data_new=data_new.select('Vancouver','distancia', abs(col('distancia')) \
                         .alias("distanciaABS"))
data_new.show()

# COMMAND ----------

data_new=data_new.select('Vancouver',
                         'distancia', 
                         "distanciaABS",
                         anomalia_udf_int("distanciaABS") \
                         .alias("isAnomaly"))
data_new.show()

# COMMAND ----------

data_new.filter(col("isAnomaly")>0).show()

# COMMAND ----------

#visualizando o histograma
display(dfNotNull.select("Vancouver"))

# COMMAND ----------

spark.sparkContext.parallelize( [np.array([1.0, 10.0, 100.0]), np.array([2.0, 20.0, 200.0]), np.array([3.0, 30.0, 300.0])])  # an RDD of Vectors

# COMMAND ----------

# DBTITLE 1,Estatísticas com MLlib
from pyspark.mllib.stat import Statistics

coluna=dataNotNull.select("Vancouver")  #seleciona a coluna
coluna1= coluna.rdd.map(lambda x: [int (x[0])]) #aplica o map para transformar em vetor
estatistica=Statistics.colStats(coluna1) #aplica a estatística
print("Média: ",estatistica.mean())  # média
print("Variância: ", estatistica.variance())  # variância
print("Valores não nulos: ",estatistica.numNonzeros())  # numero de valores não zero

# COMMAND ----------

# DBTITLE 1,K-Means para anomalias
# MAGIC %fs ls /FileStore/tables	

# COMMAND ----------

diretorioDataset="/FileStore/tables/worldcities.csv"  
#diretório que contém o arquivo a ser utilizado

# COMMAND ----------

cities_df = spark.read \
                .format("csv") \
                .options(header="true", inferschema="true") \
                .load(diretorioDataset) 
#realiza a leitura do dataset

# COMMAND ----------

cities_df.printSchema()

# COMMAND ----------

#mostrando o dataset
cities_df.show()

# COMMAND ----------

#filtrando algumas cidades
cities_BR =cities_df.where(col("country")=="Brazil")
cities_BR.show(5)

# COMMAND ----------

cities_MX =cities_df.where(col("country")=="Mexico")
cities_MX.show(5)

# COMMAND ----------

cities_EUA =cities_df.where(col("country")=="United States")
cities_EUA.show(5)

# COMMAND ----------

#criando um novo dataset através da função join
df_concat = cities_BR.union(cities_MX)
df_concat.show(5)

# COMMAND ----------

#contando a quantidade de países diferentes
df_concat.groupby("country").count().show()

# COMMAND ----------

#adicionando cidades do Japão (nossas anomalias)
cities_JP =cities_df.where((col("city")=="Tokyo") | (col("city")=="Ōsaka"))
cities_JP.show()

# COMMAND ----------

#criando o dataset final
df_final = df_concat.union(cities_JP)

# COMMAND ----------

df_final.show()

# COMMAND ----------

from pyspark.ml.evaluation import ClusteringEvaluator 
#biblioteca utilizada para a avaliação em cada um dos clusters
from pyspark.ml.clustering import KMeans
#biblioteca utilizada para a criação do modelo de clusterização utilizando o K-means

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler 
#transformando os dados em vetores de características

vecAssembler = VectorAssembler(inputCols=["lat","lng"], outputCol="features")
#utilizada para transformar os dados em um vetor (define o objeto)
new_df = vecAssembler.transform(df_final) #Aplico a transformação
new_df.show()

# COMMAND ----------

#aplica o processo de clusterização
kmeans = KMeans(k=3, seed=1)  # declara o objeto - 3 clusters 
model = kmeans.fit(new_df.select('features')) #aplica o treinamento

# COMMAND ----------

#cria o dataset com a indicação sobre qual cluster cada conjunto de dados foi adicionado
df_final = model.transform(new_df)
df_final.show() 

# COMMAND ----------

df_final.groupby('prediction').count().show()

# COMMAND ----------

df_final.where(col("prediction")=='2').show()

# COMMAND ----------

df_final.where(col('prediction')=='2').show()