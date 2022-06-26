# Databricks notebook source
Por Antonio Carlos

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext


spark = SparkSession \
        .builder\
        .appName('SparkExamples')\
        .getOrCreate()

# COMMAND ----------

dept = [('Finance', 10), ('Marketing', 20), ('Sales', 30), ('IT', 40)]
rdd = spark.sparkContext.parallelize(dept) # cria o RDD

deptColumns = ['dept_name', 'dept_id'] # transforma em dataframe
df = rdd.toDF(deptColumns)
df.printSchema()
df.show(truncate=False)


# COMMAND ----------

# UDFs - são funcao criadas para estender a funcionalidade do sistema de bancos de dados
# muito parecido com as funcoes criadas no PYTHON

plusOne = udf(lambda x: x + 1)
spark.udf.register('plusOne', plusOne) 
# registrada e pode ser usada nas chamdas seguintes em SQL
spark.sql('SELECT plusOne(5)').show()



# COMMAND ----------

#Formato de dados que SQL suporta
# spark.apache.org/docs/latest/sql-data-sources.html
# Principais - CSV, JSON, texto, Parquet, tabelas do HIVE

path = 'examples/nome.csv'
df = spark.read.csv(path) #leitura em CSV
df.show()

path = 'examples/nome.json'
dfjson = spark.read.json(path) #leitura em JSON
dfjson.printSchema() # leitura mais bem definida

dftexto = spark.read.option('lineSep', ',').text(path)
dftexto.show()

path = 'examples/nome.json'
dfjson = spark.read.json(path) #leitura em JSON
dfjson.write.parquet('people.parquet') # transformacao em PARQUET
parquetdf = spark.read.parquet('people.parquet')
parquetdf = createOrReplaceTempView('people.parquet') # transformacao completa
dfjson.printSchema() # leitura mais bem definida