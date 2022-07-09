# Databricks notebook source
# importante bibliotecas base e criando secao spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark  = SparkSession.builder.appName('StructuredNetworkWordCount').getOrCreate()

# COMMAND ----------

# definindo um esquema para os dados
from pyspark.sql.types import StructType

userSchema = StructType().add('timestamp', 'timestamp').add('word', 'string')

# COMMAND ----------

# cria o dataframe que sera responsavel por ler cada uma das linhas recebidas atraves dos arquivos direcionados no diretorio
# define a fonte (source) de dados
files_dir = spark.readStream \
            .format('csv') \ 
            .schema(userSchema) \
            .option('includeTimestamp', 'true') \
            .option('header', 'true') \
            .option('delimiter', ':') \
            .option('maxFilesPerTrigger', 1) \
            .load('/localhost')

# COMMAND ----------

# print do esquema
files_dir.printSchema

# COMMAND ----------

# divide as linhas recebidas em cada palavra
words = files_dir.select(
    explode(
    split(files_dir.word, ' ')
    ).alias('word'), files_dir.timestamp
)

# COMMAND ----------

# agrupa os dados atraves da janela de tempo e computa sobre cada um dos grupos
windowedCounts = words.groupBy(
    windw(words.timestamp, '10 minutes', '5 minutes'),
    words.word).count()#.sort(asc('window'))

# COMMAND ----------

# define a consulta (query) e como deve ser realizada a saida (sink) para o stream criado
query = windowedCounts \
    .writeStream \
    .outputModel('complete') \
    .format('console') \
    .option('trucate', 'false') \
    .start()
query.awaitTermination() # aguarda ate que a streaming query termine