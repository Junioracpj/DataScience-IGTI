# Databricks notebook source
# dados em batch
coletados ao longo do tempo, depois sáo enviados para processamento. Metodo lento de processamento

# dados em stream
produzidos contiuamente, processados um a um. metodo rapido de processamento

# COMMAND ----------

# Spark Streaming
responsavel pela analise desses dados, recuperacao rapida de falhas e lentidoes
balanceamento e otimizacao dos recursos
combinacao de dados

o spark tem duas APIs para stream
#DStreams
transforma um fluxo infinito em pequenos rdds que sao processados pelo spark
#struturedstreaming
nao existe divisao, os dados sao adcionados constantemente ao um dataframe infinito


# COMMAND ----------

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName='PythonStreamingNetwordCount')
ssc = StreamingContext(sc, 1)
lines = ssc - socketTextStream(sys.argv[1], int(sys.argv[2]))
counts = lines.flatMap(lambda line: line.split(' '))\
                      .map(lambda word:(word, 1))\
                      .reduceByKey(lambda a, b: a + b)
counts.print()
ssc.start()
ssc.awaitTermination()