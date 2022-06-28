# Databricks notebook source
# DBTITLE 1,Desafio Módulo 2 IGTI Ciência de dados
Objetivos
-------
Exercitar os seguintes conceitos trabalhados no Módulo:
✔ Exercitar o módulo Spark SQL do Apache Spark.
✔ Exercitar o módulo Spark MLLib do Apache Spark
-------
Como cientista de dados, você foi contratado para criar um modelo preditivo que, a partir de dados de pacientes como idade, gênero, nível de glicose, se o paciente fuma ou não, vai prever se aquele paciente terá um derrame cerebral ou não.

# COMMAND ----------

# importando bibliotecas relevantes  e criando a Spark Session
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext

spark = SparkSession \
        .builder\
        .appName('Desafio')\
        .getOrCreate()

# COMMAND ----------

#Lendo o arquivo
df = spark.read.csv('/FileStore/tables/stroke_data.csv', header='True', inferSchema='True')
display(df)
# df.printSchema()


# COMMAND ----------

# Verificando a quantidade de linhas e colunas
df.count()
len(df.columns)

# COMMAND ----------

# verificando estatisticas básicas a respeito da base de dados
display(df.summary())

# COMMAND ----------

# DBTITLE 1,Questão 01 - Quantos Registros existem?
df.count()

# COMMAND ----------

# DBTITLE 1,Questão 02 - Quantas colunas existem no arquivo?
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Questão 3 -  Quantos pacientes não sofreram e sofreram derrame?
print(f'Sofreram enfarto: {df.filter(df.stroke == 1).count()}')
print(f'Nao sofreram enfarto: {df.filter(df.stroke != 1).count()}')


# COMMAND ----------

# DBTITLE 1,Questão 04 - Quantos pacientes sofreram derrame e trabalhavam respectivamente, no setor privado, de forma independente, no governo e quantas são crianças?
df.createOrReplaceTempView('strokes')
df.filter('stroke == 1').groupBy('work_type').count().sort('count').show()

# COMMAND ----------

# DBTITLE 1,Questão 5 - proproção por genero
df.groupBy('gender').count().sort('count', ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Questão 06 -Hipertensos
df.filter('stroke == 1').groupby('hypertension').count().sort('count', ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Questão 7 - Idade de pacientes com hipertensão
df.filter('stroke == 1').groupby('age').count().sort('count', ascending=False).show(5)

# COMMAND ----------

# DBTITLE 1,Questão 8 - Pessoas hipertensas maiores de 50 anos
df.filter('age > 50').filter('stroke == 1').count()

# COMMAND ----------

# DBTITLE 1,Questão 9 - Media de glicose de hipertensos e não hipertensos
df.groupby('stroke').agg({'avg_glucose_level' : 'avg'}).show()

# COMMAND ----------

# DBTITLE 1,Questão 10 - Qual é o IMC medio de quem sofreu e não sofreu derrame

df.groupby('stroke').agg({'bmi' : 'avg'}).show()
# sintaxes importes =  avg - media, sum - soma, count, contabiliza a quantidade

# COMMAND ----------

# DBTITLE 1,Questão 11 - criando um modelo para prever a chance de derrame cmo as seguintes variaveis:
# Idade
# BMI
# Hipertensão
# Doença do coração
# glicose

# Importando as bibliotecas para trabalhar com machine learning.
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import VectorAssembler

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# COMMAND ----------

# Variáveis preditores numéricas
predictions_columns = ['age', 'bmi', 'hypertension', 'heart_disease', 'avg_glucose_level']

# Transformando as colunas preditoras em um único vetor chamando features
vector_assembler = VectorAssembler(inputCols=predictions_columns, outputCol='features')

# Criando a coluna features com união dos elementos preditores.
df_transform = vector_assembler.transform(df)

# COMMAND ----------

# Visualizando a variáveis resposta e as variáveis preditoras
df_model = df_transform.select('stroke', 'features')
df_model.show(10, truncate=False)

# COMMAND ----------

# Criando os dataframes de treino (75% das informações) e teste (25% das informações)
df_training, df_test = df_model.randomSplit([.75, .25])

# Tamanho do dataframe de treino
print(df_training.count())
# Tamanho do dataframe de teste
print(df_test.count())

# COMMAND ----------

# Aplicando o algorítimo de classificação
df_classifier = DecisionTreeClassifier(labelCol='stroke').fit(df_training)
df_prediction = df_classifier.transform(df_test)
df_prediction.show(truncate=False)

# COMMAND ----------

# Calculando a acurácia do modelo
accuracy = MulticlassClassificationEvaluator(labelCol='stroke', metricName='accuracy').evaluate(df_prediction)

# Calculando a precisão do modelo
precision = MulticlassClassificationEvaluator(labelCol='stroke', metricName='weightedPrecision').evaluate(df_prediction)

print(f'Acurácia do modelo: {accuracy:.4f}')
print(f'Precisão do modelo: {precision:.4f}')

# COMMAND ----------

# DBTITLE 1,# Questão 12 - Adicione ao modelo as variáveis categóricas: 
# gênero 
# status de fumante

category_variables = ['gender', 'smoking_status']
category_variables_indexer = StringIndexer(inputCols=category_variables,                                       outputCols=[column + '_Indexer' for column in category_variables])

# Transformando as variáveis categóricas gender e smoking_status em numéricas
df_transform_category = category_variables_indexer.fit(df).transform(df)
all_predictions_columns = predictions_columns + ['gender_Indexer', 'smoking_status_Indexer']
vector_assembler_category = VectorAssembler(inputCols=all_predictions_columns, outputCol='features')
df_transform_category = vector_assembler_category.transform(df_transform_category)
df_transform_category.show()

# COMMAND ----------

df_model_category = df_transform_category.select('stroke', 'features')

# Criando os dataframes de treino (75% das informações) e teste (25% das informações).
df_training_category, df_test_category = df_model_category.randomSplit([.75, .25])

# Tamanho do dataframe de treino.
print(df_training_category.count())
# Tamanho do dataframe de teste.
print(df_test_category.count())

# COMMAND ----------

df_classifier_category = DecisionTreeClassifier(labelCol='stroke').fit(df_training_category)
df_prediction_category = df_classifier_category.transform(df_test_category)

# Calculando a acurácia do modelo.
accuracy_category = MulticlassClassificationEvaluator(labelCol='stroke', metricName='accuracy').evaluate(df_prediction_category)

# Calculando a precisão do modelo.
precision_category = MulticlassClassificationEvaluator(labelCol='stroke', metricName='weightedPrecision').evaluate(df_prediction_category)
print(f'Acurácia do modelo: {accuracy_category:.4f}')
print(f'Precisão do modelo: {precision_category:.4f}')

# COMMAND ----------

# DBTITLE 1,Questão 13 - Qual a variavel mais importante do modelo que você construiu?
df_classifier_category.featureImportances
for feature in zip(all_predictions_columns, df_classifier_category.featureImportances):
    print(f'Variável: {feature[0].upper()}\tContribuição: {feature[1]:.4f}')

# COMMAND ----------

# DBTITLE 1,Questão 14 - Qual a profundidade da árvore de decisão?
df_classifier_category.depth

# COMMAND ----------

# DBTITLE 1,Questão 14 - Qual a quantidade de nós da árvore de decisão?
df_classifier_category.numNodes