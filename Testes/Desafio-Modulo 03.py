# Databricks notebook source
# MAGIC %md
# MAGIC código para o desafio do terceiro modulo - Spark Avançado

# COMMAND ----------

# DBTITLE 1,Primeiro passo - Seção Spark
from pyspark.sql import SparkSession #importa a biblioteca que cria a seção do spark

# COMMAND ----------

#inicia a seção para a utilização do spark
spark = SparkSession.builder.appName("desafio_tpd").getOrCreate() #cria a seção caso não exista ou obtém a já criada

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables    

# COMMAND ----------

diretorioDatasetDiabets="/FileStore/tables/Mall_Customers.csv"  #diretório que contém o arquivo a ser utilizado

# COMMAND ----------

data = spark.read.format("csv").options(header="true", inferschema="true").load(diretorioDatasetDiabets)  #realiza a leitura do dataset

# COMMAND ----------

data.show(5,False)  #mostrando as 5 primeiras instâncias do dataset

# COMMAND ----------

# DBTITLE 1,Questão 01 - Identificando colunas e linhas
data.columns #mostra as colunas do dataset

# COMMAND ----------

data.count()  #conta a quantidade de registros - linhas

# COMMAND ----------

# DBTITLE 1,Questão 02 - Variaveis tipo String
data.printSchema()  #mostra o esquema inferido pelas variáveis

# COMMAND ----------

#verificando a existência ou não de valores nulos
from pyspark.sql.functions import isnan, when, count, col

data.select([count(when(isnan(c), c)).alias(c) for c in data.columns]).show()

# COMMAND ----------

# DBTITLE 1,Questões de 03 a 05 
# Média dos consumidores
# e desvio padrão
# Outlier

data.describe().show() #mostrando as estatísticas do dataset

# COMMAND ----------

# DBTITLE 1,Questão 06 - Relação sexo e salário anual
# Verificando se existem outliers (anomalias) nos dados. Para isso, será utilizado o boxplot.
# selecionando apenas algumas colunas
data_outliers=data.select([c for c in data.columns if c in ['Age','Annual Income (k$)','Spending Score (1-100)']])
data_outliers.show()

# COMMAND ----------

#boxplot
display(data_outliers)

# COMMAND ----------

#histograma
display(data_outliers)

# COMMAND ----------

# MAGIC %md
# MAGIC Conhecendo o dataset

# COMMAND ----------

display(data.select(["Genre","Annual Income (k$)"]))

# COMMAND ----------

#distribuição dos consumidores pela idade
display(data.select(["Age"]))

# COMMAND ----------

#encontrando a distribuição da idade x sexo
display(data.select(["Age","Genre"]))

# COMMAND ----------

#encontrando a distribuição da idade x sexo
display(data.select(["Age","Genre"]))

# COMMAND ----------

# DBTITLE 1,Questã 07 - Relação entre sexo e salário anual
# A variável "Spending Score (1-100)" ou pontuação de consumo, corresponde à nota atribuída pelo shopping aos consumidores. Quanto mais próxima de 100 indica que o consumidor é mais "lucrativo" para o shopping.
#encontrando as notas atribuidas por gênero 
display(data.select(["Spending Score (1-100)","Genre"]))

# COMMAND ----------

# DBTITLE 1,Questão 08 - Correlação de Pearson
# Analisando a correlação entre Idade e Nota Atribuída
#Analisando as correlações
from pyspark.mllib.stat import Statistics

# COMMAND ----------

rdd_1=data.select("Spending Score (1-100)").rdd.flatMap(lambda x:x)  #utilizada para transforma das colunas do dataframe em colunas para serem empregadas na análise de correlaçoes
rdd_2=data.select("Age").rdd.flatMap(lambda x:x)

# COMMAND ----------

Statistics.corr(rdd_1,rdd_2,method="pearson") 
#definindo a coeficiente de correlação de pearson 

# COMMAND ----------

data.stat.corr("Age","Annual Income (k$)",method="pearson")

# COMMAND ----------

# DBTITLE 1,Questão 09 - Dataset por gênero
#Selecionando os homens
homens=data[data.Genre=='Male']
display(homens)

# COMMAND ----------

#selecionando mulheres
mulheres=data[data.Genre=='Female']
display(homens)

# COMMAND ----------

#correlação entre Idade e o Salário Anual - Homens
homens.stat.corr("Age","Spending Score (1-100)",method="pearson")

# COMMAND ----------

#correlação entre Idade e o Salário Anual - Homens
mulheres.stat.corr("Age","Spending Score (1-100)",method="pearson")

# COMMAND ----------

# DBTITLE 1,Questão de 10 a 15 - Kmeans e clusterização
from pyspark.ml.evaluation import ClusteringEvaluator  #biblioteca utilizada para a avaliação em cada um dos clusters
from pyspark.ml.clustering import KMeans #biblioteca utilizada para a criação do modelo de clusterização utilizando o K-means

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler  #transformando os dados em vetores de características

vecAssembler = VectorAssembler(inputCols=["Spending Score (1-100)","Annual Income (k$)"], outputCol="features") #utilizada para transformar os dados em um vetor (define o objeto)
new_df = vecAssembler.transform(data) #Aplico a transformação
new_df.show()

# COMMAND ----------

#aplica o processo de clusterização
kmeans = KMeans(k=5, seed=1)  # declara o objeto - 3 clusters 
model = kmeans.fit(new_df.select('features')) #aplica o treinamento

# COMMAND ----------

#cria o dataset com a indicação sobre qual cluster cada conjunto de dados foi adicionado
df_final = model.transform(new_df)
df_final.show() 

# COMMAND ----------

data.describe().show()

# COMMAND ----------

df_final.groupby('prediction').count().show()

# COMMAND ----------

#transformando os dados utilizando o pandas 
data_pandas = df_final.toPandas().set_index('CustomerID')
data_pandas.head()

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(12,10))
plt.scatter(data_pandas['Annual Income (k$)'], data_pandas['Spending Score (1-100)'], c=data_pandas.prediction)
plt.xlabel('Annual Income (k$)')
plt.ylabel('Spending Score (1-100)')
plt.show()
