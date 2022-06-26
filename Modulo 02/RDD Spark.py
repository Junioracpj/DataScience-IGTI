# Databricks notebook source
# DBTITLE 1,RDD Spark
Por Antonio Carlos


# COMMAND ----------

Rdd são imutáveis, uma transformação aplicada a um RDD produz um ou mais RDDs.
Essas transformações são preguiçosas.

# COMMAND ----------

# map(func) - Cria um novo RDD passando cada elemento do RDD de origem pela função func. Cada elemento de entrada no rdd de origem é mapeamento em um único no RDD.

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = (SparkSession.builder
        .appName('Map')
        .getOrCreate())

data = spark.sparkContext.textFile('/destinoaquivo/')
mapfile = data.map(lamda line : (line, len(line))) 
# cria um RDD que pega uma linha e cria uma tupla com a linha e o tamanho da linha
mapFile.foreach(print)

# COMMAND ----------

# flatMap(func) -  similar ao map, mas cada item do RDD original pode ser mapeado em zero ou mais itens no novo RDD

data = spark.sparkContext.textFile('/destinoaquivo/')
flat = data.flatMap(lamda line : line.split()) 
# cada item do RDD pode ser mapeado, no final o RDD pode ser maior ou menor que o original
flatFile.foreach(print)

# COMMAND ----------

# filter(func) - cria um novo RDD que contem apenas os itens que foram retornados como verdadeiros 
data = spark.sparkContext.textFile('/destinoaquivo/')
flat =(data
       .flatMap(lamda line : line.split())
       .filter(lambda word: word.starswith('a')))
# vai criar um novo RDD que satisfação a função boleana -  começam com a    
flatFile.foreach(print)

# COMMAND ----------

# reduceByKey(func) - os pares(K,V) tem a mesma chave são agregados e combinados

list  = ['um', 'um', 'dois', 'dois', 'tres']
rdd = spark.sparkContext.parallelize(list) # transforma a lista em um RDD
rdd2 = (rdd.map(lambda w: (w, 1))
        .reduceByKey(lambda a,b: a+b))
rdd2.foreach(print)

# COMMAND ----------

# sortbykey(func) - ordena as tuplas (K, V) de acordo com a chave

list  = ['um', 'um', 'dois', 'dois', 'tres']
rdd = spark.sparkContext.parallelize(list) # transforma a lista em um RDD
rdd2 = (rdd.map(lambda w: (w, 1))
        .reduceByKey(lambda a,b: a+b)
        .sortByKey('asc'))
rdd2.foreach(print)

# COMMAND ----------

# union(rdd) - cria um rdd que é a junção de outros RDDs
# intersection(rdd) - cria um RDD com elementos comuns entre os dois RDDs

list  = ['um', 'um', 'dois', 'dois', 'tres']
list2 = ['um', 'quatro', 'cinco']
rdd = spark.sparkContext.parallelize(list)
rdd2 = spark.sparkContext.parallelize(list2)

rddInt = rdd.intersection(rdd2) # Intersecção entre os RDDs
rddUniao =  rdd.union(rdd2) # União entre os RDDs

# COMMAND ----------

# join(rdd) - Une informações baseando-se pela chave primaria da tupla

list  = [('Pedro', 23), ('Jose', 23)]
list2 = [('Pedro', 'BH'), ('Jose', 'SP')]
rdd = spark.sparkContext.parallelize(list)
rdd2 = spark.sparkContext.parallelize(list2)

rddJoin = rdd.join(rdd2)
rddJoin.foreach(print)

# COMMAND ----------

Ações - Ao constrario das funções acim,a não geram outros RDDs a partir de um primario
tornam o processo rápido por executarem somente leitura e observação do RDD

collect() -  retorna o conteudo do RDD para o Driver, em memória.
count() -  imprime o numero de itens do RDD
take(n) -  retorna N elementos aleatórios do RDD
countByValue() - para cada valor ele conta o numero de suas ocorrencias
reduce(func) - combina dois elementos de um RDD e produz um unico elemento
saveAsTextFile(path) - grava o conteudo do RDD em uma pasta