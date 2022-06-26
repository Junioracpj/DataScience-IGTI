# Databricks notebook source
Por Antonio Carlos

# COMMAND ----------

# Tem a função de trazer implementações distribuidas dos principais algoritmos
# dos aprendizados de maquina
# scikit-learn  - bibilioteca para analise preditiva de dados

# Principais passos antes de realizar algo em ML :
# limpeza dos dados
# extração dos atributos
# ajuste dos parametros do modelo
# avaliação do modelo



# COMMAND ----------

# Pipeline -  transformação de textos complexos em numeros
# texto > words > vetores > modelo de regressão logistica

transfor() -  
# transforma um DF de texto em um outro DF com vetores que representam palavras
# pode transformar um DF de vetores em vetores de previsão do modelo
fit() - 
# produz um modelo a partir de um DF, tambem 


# COMMAND ----------

# Classificação
regresão logistica
arvore de decisão
SVM
Naive Bayes
Multilayer Perceptron
# Regressão 
Regressão linear
Regressão por arvore de decisão
modelo linear generalizado
# Agrupamento - Aprendizado nao supervisionado
K-Means
LDA
Modelo de mistura Gaussiana
# Filtragem Colaborativa - sistema de recomendação
# Itemsets frequentes - mineração de dados
FP-growth



# COMMAND ----------

# k-means Exemplo
# site = spark.apache.org/docs/latest/ml-clustering.html#k-means

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import CLuesteringEvaluator

# loads data
dataseet = spark.read.format('libsvm').load('data/mllib/sample_kmeans_data.txt')

# trains a k-means model
kmeans = KMeans.().setK(2).setSeed(1)
model = kmeans.fit(dataset)

#evaluate clustering by computing silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print('Silhouette with squared euclidean distance = ' + str(silhouette))

#show the result
centers = model.clusterCenters()
print('Cluster Center: ')
for center in centers:
    print(center)