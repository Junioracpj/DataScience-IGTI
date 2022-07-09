# Databricks notebook source
Aprendizados sobre streaming para Big Data

# COMMAND ----------

# importante bibliotecas base e criando secao spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark  = SparkSession.builder.appName('StructuredNetworkWordCount').getOrCreate()

# COMMAND ----------

# cria o dataframe que sera responsavel por ler cada uma das linhas recebidas atraves do localhost e port 9999
# define a fonte (source) de dados
lines = spark.readStream \
            .format('socket') \ 
            .option('host', 'localhost') \
            .option('port', 9999) \
            .load()

# divide as linhas recebidas em cada palavra
words = lines.select(
    explode(
    split(lines.value, ' ')
    ).alias('word')
)
# cria o novo dataframe a ser responsavl por contar a quanidade de palavras digitadas
wordCounts = words.groupBy('word').count()

# COMMAND ----------

# confere o tipo de dataframe
lines.isStreaming

# COMMAND ----------

# define a consulta e como deve ser realizada a saida para o stream criado
query = wordCounts.writeStream.outputModel('update').format('console').start()
query.awaitTermination() # aguarda ate que a streaming query termine

# COMMAND ----------

Para rodar e necessario ter o NC instalado e utilizar o comando antes de iniciar a celular anterior
nc -lk 9999