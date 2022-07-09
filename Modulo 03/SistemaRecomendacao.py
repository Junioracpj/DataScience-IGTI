# Databricks notebook source
# MAGIC %md
# MAGIC Notebook utilizado para a aula sobre sistemas de recomendação

# COMMAND ----------

# MAGIC %md
# MAGIC 1) Carregar os dados
# MAGIC 2) Pré-processamento dos dados
# MAGIC 3) Divisão dos dados entre treinamento e teste
# MAGIC 4) Criar o sistema de recomendação através do ALS
# MAGIC 5) Realizar previsões
# MAGIC 6) Testar o modelo

# COMMAND ----------

from pyspark.sql import SparkSession
#importa a biblioteca que cria a seção do spark

#inicia a seção para a utilização do spark
spark = SparkSession.builder.appName("recomendacao").getOrCreate()
#cria a seção caso não exista ou obtém a já criada

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

diretorioRecomendacao="/FileStore/tables/u.data"  #diretório que contém o arquivo a ser utilizado

# COMMAND ----------

# DBTITLE 1,Carregando o arquivo
#lendo arquivos armazenados através da função genérica
rdd_movies = spark.sparkContext.textFile(diretorioRecomendacao)
rdd_movies.take(10)  #user id | item id | rating | timestamp

# COMMAND ----------

#definindo as bibliotecas a serem utilizadas
from pyspark.mllib.recommendation import ALS, Rating  
#MLlib utilizada para implementar os algoritmos ALS e Rating

# COMMAND ----------

# DBTITLE 1,Pré-processamento dos dados  e Separando os dados para treinamento e teste
#dividindo os dados entre treinamento e teste
(trainRatings, testRatings) = rdd_movies.randomSplit([0.7, 0.3])
trainRatings.take(5)

# COMMAND ----------

testRatings.first()  #print da primeira linha do RDD

# COMMAND ----------

trainingData = trainRatings.map(lambda l: l.split('\t')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))) 
#aplicando a função Rating

# COMMAND ----------

trainingData.first()  
#print do RDD criado através da função Rating

# COMMAND ----------

#mesmo procedimento para os dados de teste
testData = testRatings.map(lambda l: l.split('\t')).map(lambda l: (int(l[0]), int(l[1])))
testData.first()  #id do usuário | id do filme

# COMMAND ----------

# DBTITLE 1,Construindo o modelo ALS
#definindo as variáveis do modelo
rank = 10  # número de fatores latentes do modelo  R->P (usuários)*Q (itens) => R_mxn = P_mxrank * Q_rankxm  (em que m= numero de usuário e n= numero de itens)
numIterations = 50 #número de iterações realizadas pelo modelo
model = ALS.train(trainingData, rank, numIterations) # treina o modelo

# COMMAND ----------

# DBTITLE 1,Previsão do modelo
model.predict(253, 465)  # entrada (usuário,produto)

# COMMAND ----------

previsao = model.predictAll(testData)  #previsão para todos os dados de teste
previsao.first()

# COMMAND ----------

previsao = previsao.map(lambda l: ((l[0], l[1]), l[2])) #mapea para a exibição
previsao.take(5)

# COMMAND ----------

testRating2 = testRatings.map(lambda l: l.split('\t')).map(lambda l: ((int(l[0]), int(l[1])), float(l[2]))) # mapea para exibição e utilização na análise

# COMMAND ----------

testRating2.first()

# COMMAND ----------

ratingsAndPredictions = testRating2.join(previsao)
ratingsAndPredictions.take(5)

# COMMAND ----------

# DBTITLE 1,Avaliação do modelo
MSE = ratingsAndPredictions.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print ("Erro médio quadrado = " + str(MSE))

# COMMAND ----------

#dado um usuário -> recomenda os top 5 produtos
model.recommendProducts(105, 5) 

# COMMAND ----------

#dado um produto -> recomenda top 5 usuários
model.recommendUsers(1, 5)  #filme Toy Story (1995)

# COMMAND ----------

#mostrando o vetor de características referentes usuários (coluna - P)
model.userFeatures().take(1)[0]

# COMMAND ----------

#mostra o vetor de características referente a um produto (linha - Q)
model.productFeatures().take(1)[0]