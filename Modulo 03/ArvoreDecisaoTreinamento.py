# Databricks notebook source
# MAGIC %md
# MAGIC Notebook utilizado para a aula sobre árvore de decisão e floresta randômica

# COMMAND ----------

from pyspark.sql import SparkSession 
#importa a biblioteca que cria a seção do spark

spark = SparkSession.builder.appName("ArvoreDecisao").getOrCreate() 
#cria a seção caso não exista ou obtém a já criada

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

diretorioArvore="/FileStore/tables/iris_bezdekIris.csv"  
#diretório que contém o arquivo a ser utilizado

#lendo arquivos armazenados CSV com o esquema definido
df_iris = spark.read \
            .format('csv') \
            .options(inferSchema=True,header='false',delimiter=',') \
            .load(diretorioArvore)

df_iris.printSchema()

# COMMAND ----------

df_iris.show(5)

# COMMAND ----------

#modificando o nome das colunas existentes no cabeçalho 
df_iris = df_iris.selectExpr("_c0 as sep_len", "_c1 as sep_wid", "_c2 as pet_len", "_c3 as pet_wid", "_c4 as label")
df_iris.show(5)

# COMMAND ----------

# DBTITLE 1,Conhecendo o Banco de Dados
#encontrando as "estatísticas"
df_iris.describe(['sep_len','sep_wid','pet_len','pet_wid']).show()

# COMMAND ----------

#definindo a visão do dataframe para ser utilizado como uma tabela pelo SQL
df_iris.createOrReplaceTempView("irisTable")
display(spark.sql('select * from irisTable '))

# COMMAND ----------

# DBTITLE 1,Iniciando o Processo de Construção e Aplicação da Árvore de Decisão
from pyspark.ml.linalg import Vectors  #biblioteca que contém funções de Algebra Linear
from pyspark.ml.feature import VectorAssembler #biblioteca que contém as funções para a construção de vetores

#criando o vetor de características
vector_assembler = VectorAssembler(inputCols=["sep_len", "sep_wid", "pet_len", "pet_wid"],outputCol="features")
df_temp = vector_assembler.transform(df_iris)
df_temp.show(5)

# COMMAND ----------

#removendo as colunas que não serão utilizadas
df_menor = df_temp.drop('sep_len', 'sep_wid', 'pet_len', 'pet_wid')
df_menor.show(5)

# COMMAND ----------

#aplicando as transformações para a coluna label
from pyspark.ml.feature import StringIndexer 
#cria o 'vetor' para cada uma das classes existentes na coluna label

l_indexer = StringIndexer(inputCol="label", outputCol="labelIndex")  #cria o objeto para a codificação
df_final = l_indexer.fit(df_menor).transform(df_menor)  #aplica a transformação
df_final.show(5)

# COMMAND ----------

#dividindo entre dados de treinamento e teste
(train, test) = df_final.randomSplit([0.7, 0.3])
test.show(5)

# COMMAND ----------

# DBTITLE 1,Definindo o Algoritmo
from pyspark.ml.classification import DecisionTreeClassifier  
#biblioteca para o algoritmo da árvore de decisão
from pyspark.ml.evaluation import MulticlassClassificationEvaluator 
#utilizada para encontrar as métricas de desempenho

modeloArvore = DecisionTreeClassifier(labelCol="labelIndex", featuresCol="features")  #definindo o modelo
model = modeloArvore.fit(train)  #aplicando o treinamento

# COMMAND ----------

#realizando a previsão
predictions = model.transform(test)
predictions.select("prediction", "labelIndex").show(5)

# COMMAND ----------

#encontrando as métricas de avaliação para o modelo
avaliacao = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction",metricName="accuracy")

acuracia = avaliacao.evaluate(predictions)
print("Acurácia do Modelo =  ",(acuracia))

# COMMAND ----------

# DBTITLE 1,Aplicação da Floresta Randômica
from pyspark.ml.classification import RandomForestClassifier  #classificador para o randomForest

modeloRF = RandomForestClassifier(labelCol="labelIndex",featuresCol="features", numTrees=10)  #define o modelo
modelRF = modeloRF.fit(train)

#realizando a previsão
predictions = modelRF.transform(test)
predictions.select("prediction", "labelIndex").show(5)

# COMMAND ----------

#encontrando as métricas de avaliação para o modelo
avaliacao = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction",metricName="accuracy")
acuracia = avaliacao.evaluate(predictions)
print("Acurácia do Modelo =  ",(acuracia))

# COMMAND ----------

print(modelRF)