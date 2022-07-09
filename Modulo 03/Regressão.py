# Databricks notebook source
# MAGIC %md
# MAGIC Notebook utilizado para a aula sobre regressão univariada

# COMMAND ----------

from pyspark.sql import SparkSession 
# importa a biblioteca que cria a seção do spark

# COMMAND ----------

# inicia a seção para a utilização do spark
spark = SparkSession.builder.appName("RegressaoLinear").getOrCreate() 
# cria a seção caso não exista ou obtém a já criada

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

diretorioRegressao="/FileStore/tables/regressaoLinear.csv" 
# diretório que contém o arquivo a ser utilizado

# COMMAND ----------

#definindo o esquema dos dados a serem lidos
from pyspark.sql.types import *  #import dos tipos
schema=StructType().add("X",IntegerType(),True).add("Y",StringType(),True)  #define o esquema a ser utilizado


# COMMAND ----------

#lendo arquivos armazenados CSV com o esquema definido
pagamentoSeguro = spark.read \
                    .format('csv') \
                    .schema(schema) \
                    .options(header='true',delimiter=';') \
                    .load(diretorioRegressao)

pagamentoSeguro.printSchema()

# COMMAND ----------

pagamentoSeguro.show(5)

# COMMAND ----------

import pyspark.sql.functions as F
pagamentoSeguro=pagamentoSeguro.select(F.col('X') \
                                       .alias("Apolices"), F.col('Y') \
                                       .alias("Valor_Pago"))  
#adiciona nomes ao cabaçalho

pagamentoSeguro.show(5)

# COMMAND ----------

pagamentoSeguroPonto=pagamentoSeguro.withColumn("Valor_Pago_Novo", F.regexp_replace(F.col("Valor_Pago"), "[,]", "."))  
#troca o valor de "," para "." 

pagamentoSeguroPonto.show(5)

# COMMAND ----------

#modificando o tipo string para numérico
pagamentoSeguroFinal=pagamentoSeguroPonto.select(F.col('Apolices'),F.col('Valor_Pago_Novo'), pagamentoSeguroPonto.Valor_Pago_Novo.cast('float').alias('Valor_Pago_Float'))

pagamentoSeguroFinal.show(5)

# COMMAND ----------

pagamentoSeguroFinal.printSchema()

# COMMAND ----------

# DBTITLE 1,Iniciando o Processo de Regressão
pagamentoSeguroFinal.describe().show()


# COMMAND ----------

#transformando os dados (linhas) em vetores
from pyspark.ml.feature import VectorAssembler 
#importando a biblioteca responsável por criar o vetor a partir da coluna

assembler = VectorAssembler(inputCols=['Apolices'], outputCol='features')  #define o objeto para transformação
df_seguro = assembler.transform(pagamentoSeguroFinal) #aplica a transformação
df_seguro.printSchema()

# COMMAND ----------

df_seguro = df_seguro.select(['features','Valor_Pago_Float'])
df_seguro.show(5)

# COMMAND ----------

# DBTITLE 1,Criando o Modelo de Regressão
from pyspark.ml.regression import LinearRegression  #biblioteca que contém o modelo de regressão

lr = LinearRegression(maxIter=10, labelCol='Valor_Pago_Float') #define o objeto a ser utilizado para regressão
lrModel = lr.fit(df_seguro)

#Coeficientes angulares e lineares (a e b) da reta de regressão
print(f'Intercepto: {lrModel.intercept}\nCoeficiente Angular: {lrModel.coefficients.values}')

# COMMAND ----------

#print das estatísticas do modelo
modelsummary = lrModel.summary

print("Variância Explicada:", modelsummary.explainedVariance)
print('R_2: ', modelsummary.r2)
print('Erro médio quadrático: ',modelsummary.meanSquaredError)

modelsummary.residuals.show(5)


# COMMAND ----------

# DBTITLE 1,Realizando a Previsão Através do Modelo
modelsummary.predictions.show(5)