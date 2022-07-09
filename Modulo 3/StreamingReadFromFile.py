# Databricks notebook source
# importante bibliotecas base e criando secao spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark  = SparkSession.builder.appName('StructuredNetworkWordCount').getOrCreate()

# COMMAND ----------

# cria o dataframe que sera responsavel por ler cada uma das linhas recebidas atraves dos arquivos direcionados no diretorio
# define a fonte (source) de dados
files_dir = spark.readStream \
            .format('text') \ 
            .option('inferSchema', 'true') \
            .option('maxFilesPerTrigger', 1) \
            .load('/localhost')

# COMMAND ----------

# confere o tipo de dataframe
lines.isStreaming

# COMMAND ----------

# divide as linhas recebidas em cada palavra
words = lines.select(
    explode(
    split(lines.value, ' ')
    ).alias('word')
)
# cria o novo dataframe a ser responsavl por contar a quanidade de palavras digitadas
wordCounts = words.groupBy('word').count()

# COMMAND ----------

# ordena as palavras que mais aparecem
from pyspark.sql.functions import desc
wordCounts = wordCounts.sort(desc('count')) 

# COMMAND ----------

# define a consulta (query) e como deve ser realizada a saida (sink) para o stream criado
query = wordCounts.writeStream.outputModel('complete').format('console').start()
query.awaitTermination() # aguarda ate que a streaming query termine