# Databricks notebook source
# DBTITLE 1,Aplicação de streaming na análise de logs
# MAGIC %fs ls /FileStore/tables 
# MAGIC # comando em linux

# COMMAND ----------

# define o caminho para a saida
output_path = '/tmp/pydata/Streaming/output/'
dbutils.fs.rm(output_path, True)
dbutils.fs.mkdirs(output_path)

# define o caminho para o chekpoint, necessario para utilizar algumas funcoes e garantir tolerancia a falhas
checkpoint_path = '/tmp/pydata/Streaming/checkpoint'
dbutils.fs.rm(checkpoint_path, True)
dbutils.fs.mkdirs(checkpoint_path)

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StringType, LongType, StructField, TimestampType
schema_entrada = StructType([
    StructField('bytes', LongType()),
    StructField('host', StringType()),
    StructField('http_reply', IntegerType()),
    StructField('request', StringType()),
    StructField('timestamp', StringType())
])

# COMMAND ----------

# DBTITLE 1,Criando o dataframe estatico
logsDirectoryStatic = '/local/.json' # local onde vai ser armazenado os dados
static = spark.read.json(logsDirectoryStatic, schema_entrada)


# COMMAND ----------

#print do schema
static.isStreaming

# COMMAND ----------

# DBTITLE 1,Conhecendo os dados
static.show(5) # demonstra as linhas primeiras linhas do dataframe

# COMMAND ----------

(static.count()) # quantidade de linhas

# COMMAND ----------

# retorna o valor medio de bytes gerados pelas consultas
from pyspark.sql.functions import avg
static.select(avg('bytes')).show()

# COMMAND ----------

# seleciona a quantidade de valores diferentes existentes na coluna host
from pyspark.sql.functions import asc, col, desc
grupo_host_distintos = static.select('host').distinct().sort(col('host').asc())
grupo_host_distintos.show()

# COMMAND ----------

# cria a tabela para utilizar a consulta via SQL
static.creatOrReplaceTempView('grupo_1')
%sql
SELECT DISTINCT host
    FROM grupo_1
ORDER BY host

# COMMAND ----------

# DBTITLE 1,Criando o modelo dinamico(Streaming)
# importando bibliotecas
from pyspark.sql.functions import input_file_name, current_timestamp

# COMMAND ----------

# define o caminho para a entrada dos arquivos log
logsDirectoryStreaming = '/directory/*.json' # local onde os arquivos vao ser armazenados

# COMMAND ----------

# definindo o modelo de Streaming
streamingDF = (spark.readStream
              .schema(schema_enttrada)
              .option('MaxFilesPerTrigger', 1)
              .json(logsDirecoryStreamin)
              .withColumn('INPUT_FILE_NAME', input_file_name())
              .withColumn('PROCESSED_TIME', current_timestamp())
              .withWatermark('PROCESSED_TIME', '1 minute')

# COMMAND ----------

# definindo a saida(sink)
query = (StramingDF.writeStream
              .format('parquet')
              .option('path', output_path)
              .option('checkpointLocation', checkpoint_path)
              .outputMode('appen')
              .queryName('logs')
              .trigger(processingTime='5 seconds')
                .start()
        )

# COMMAND ----------

# DBTITLE 1,Vizualizando as saidas em Tempo Real
# cria a tabela para realizar as consutas sobre os dados que estao sendo lidos
streamingDF.createOrReplaceTempView('logs_table_in')

# COMMAND ----------

# MAGIC %sql select COUNT(*) from logs_table_in where http_reply = 200

# COMMAND ----------

# define o esquema para leitura dos dados que estao na pasta de saida
# esquema modificado para que os dados possam ser adcionados ao formato de saida
schemaSaida = (StructType()
              .add('timestamp', TimestampType())
              .add('bytes', LongType())
              .add('host', StringType())
              .add('http_reply', IntegerType())
              .add('request', StringType())
              .add('INPUT_FILE_NAME', StringType())
              .add('PROCESSED_TIME', TimestampType())
              )

# COMMAND ----------

spark.connf.set('spark.sql.shuffle.partitions', '1') # define o valor para o shuffles

# define a configuracao para a leitura
saidasLogs = (spark.readStream.schema(schemaSaida)
             .format('parquet')
             .option('MaxFilesPerTrigger', 1)
             .load(output_path)
             .withWatermark('PROCESSED_TIME', '1 minute')
             )

# COMMAND ----------

# define a tabela temporaria para que seja possivel realizar as consultas sobre os dados 
saidasLogs.createOrReplaceTempView('logs_table_out')

# COMMAND ----------

# MAGIC %sql select COUNT(*) from logs_table_out

# COMMAND ----------

# MAGIC %sql Select host, AVG(bytes) as media FROM logs_table_out GROUP BY host

# COMMAND ----------

# MAGIC %sql select DISTINCT request from logs_table_out GROUP BY request