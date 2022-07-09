# Databricks notebook source
# MAGIC %md
# MAGIC Notebook utilizado para a aula sobre SVM

# COMMAND ----------

from pyspark.sql import SparkSession 
#importa a biblioteca que cria a seção do spark

spark = SparkSession.builder.appName("SVM_MLP").getOrCreate() 
#cria a seção caso não exista ou obtém a já criada


# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

diretorioArvore="/FileStore/tables/iris_bezdekIris.csv"  #diretório que contém o arquivo a ser utilizado

# COMMAND ----------

#lendo arquivos armazenados CSV com o esquema definido
df_iris = spark.read \
            .format('csv') \
            .options(inferSchema=True,header='false',delimiter=',') \
            .load(diretorioArvore)

df_iris.printSchema()

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

# DBTITLE 1,Iniciando o Processo de Construção e Aplicação do SVM
from pyspark.ml.linalg import Vectors  #biblioteca que contém funções de Algebra Linear
from pyspark.ml.feature import VectorAssembler #biblioteca que contém as funções para a construção de vetores

#criando o vetor de características
vector_assembler = VectorAssembler(inputCols=["sep_len", "sep_wid", "pet_len", "pet_wid"],outputCol="features")
df_temp = vector_assembler.transform(df_iris)
df_temp.show(5)

# COMMAND ----------

df_temp.printSchema()

# COMMAND ----------

#removendo as colunas que não serão utilizadas
df_menor = df_temp.drop('sep_len', 'sep_wid', 'pet_len', 'pet_wid')
df_menor.show(5)

# COMMAND ----------

#aplicando as transformações para a coluna label
from pyspark.ml.feature import StringIndexer  #cria o 'vetor' para cada uma das classes existentes na coluna label

l_indexer = StringIndexer(inputCol="label", outputCol="labelEncoder")  #cria o objeto para a codificação
df_final = l_indexer.fit(df_menor).transform(df_menor)  #aplica a transformação
df_final.show(5)

# COMMAND ----------

df_final.printSchema()

# COMMAND ----------

# DBTITLE 1,Normalizando os dados
#normalizando os dados
from pyspark.ml.feature import MinMaxScaler  #biblioteca para colocar os valores entre 0 e 1
scaler = MinMaxScaler( inputCol="features", outputCol="scaledFeatures")  #criando o objeto para a escala
scalerModel = scaler.fit(df_final)

# COMMAND ----------

df_final = scalerModel.transform(df_final).drop('features').withColumnRenamed('scaledFeatures', 'features')
df_final.show()

# COMMAND ----------

# DBTITLE 1,Modificando o Dataset Para a Classifcação Binária
import pyspark.sql.functions as F
df_SVM=df_final.where((F.col("labelEncoder") == 0) | (F.col("labelEncoder") == 1)) 
#Transforma o dataset em um problema de classificação binária
df_SVM.show(50)

# COMMAND ----------

#removendo as colunas que não serão utilizadas
df_SVM = df_SVM.drop('label')
df_SVM.show(50)

# COMMAND ----------

df_SVM=df_SVM.selectExpr('features',"labelEncoder as label")
df_SVM.show(150)

# COMMAND ----------

# DBTITLE 1,Dividindo o dataset entre treinamento e teste
#dividindo entre dados de treinamento e teste
(train, test) = df_SVM.randomSplit([0.7, 0.3])
train.show(100)

# COMMAND ----------

print("Dados para treinamento: ", train.count())
print("Dados para teste: ", test.count())

# COMMAND ----------

# DBTITLE 1,Definindo o Algoritmo
from pyspark.mllib.classification import SVMWithSGD, SVMModel  #modelo de svm
from pyspark.ml.evaluation import MulticlassClassificationEvaluator  #utilizada para encontrar as métricas de desempenho
from pyspark.mllib.linalg import Vectors  #vetores densos
from pyspark.mllib.util import MLUtils

df_train = MLUtils.convertVectorColumnsFromML(train, "features")
df_test = MLUtils.convertVectorColumnsFromML(test, "features")

# COMMAND ----------

df_train.show(5,False)

# COMMAND ----------

from pyspark.mllib.regression import LabeledPoint  
#cria a "linha" (características e label) a ser utilizada

trainingData = df_train.rdd.map(lambda row:LabeledPoint(row.label,row.features))  
#aplica o label ao treinamento
testingData = df_test.rdd.map(lambda row:LabeledPoint(row.label,row.features))  
#aplica o label ao teste

for xs in trainingData.take(10):
        print(xs)

# COMMAND ----------

#contrução do modelo
modelSVM = SVMWithSGD.train(trainingData, iterations=100)

labelsAndPreds = testingData.map(lambda p: (p.label, modelSVM.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda lp: lp[0] != lp[1]).count() / float(testingData.count())
print("Erro na previsão: ",trainErr)