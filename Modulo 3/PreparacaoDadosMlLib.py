# Databricks notebook source
# MAGIC %md
# MAGIC Mllib para correção de dados

# COMMAND ----------

from pyspark.sql import SparkSession #importa a biblioteca que cria a seção do spark

# COMMAND ----------

spark = SparkSession.builder.appName("PreparacaoDados").getOrCreate() 
#cria a seção caso não exista ou obtém a já criada

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

diretorioDatasetOcup="/FileStore/tables/designation.json" 
diretorioDatasetSala="/FileStore/tables/salary.json" 
#diretório que contém o arquivo a ser utilizado

# COMMAND ----------

#criando um RDD e convertendo em Dataframe
empregados=spark.sparkContext.parallelize([(1, "Joao", 25), (2, "Ricardo", 35), (3, "Marcio", 24), \
                           (4, "Janete", 28), (5, "Kely", 26), (6, "Vicente", 35), \
                           (7, "Jander", 38), (8, "Maria", 32), (9, "Gabriel", 29), \
                           (10, "Kimberly", 29), (11, "Alex", 28), (12, "Gustavo", 25), \
                           (13, "Rafael", 31)]).toDF(["emp_id","nome","idade"])

# COMMAND ----------

empregados.show(5)

# COMMAND ----------

#lendo arquivos armazenados em um JSON
salario = spark.read.json(diretorioDatasetSala) 

# COMMAND ----------

salario.show(5)

# COMMAND ----------

ocupacao = spark.read.json(diretorioDatasetOcup) #lê os dados do diretório

# COMMAND ----------

ocupacao.show(5)

# COMMAND ----------

# consolidação dos dados
df_final = empregados.join(salario, empregados.emp_id == salario.e_id).join(ocupacao, empregados.emp_id == ocupacao.id).select("e_id", "nome", "idade", "cargo", "salario")

# COMMAND ----------

df_final.show()

# COMMAND ----------

# DBTITLE 1,Retirando valores nulos 
clean_data = df_final.na.drop()
clean_data.show()

# COMMAND ----------

# DBTITLE 1,Substituindo os valores NaN pela media
import math  #utilizado para aplicar algumas funções matematicas
from pyspark.sql import functions as F  #contem as funções da linguagem SQL

#encontrando a média dos salários
salario_medio = math.floor(salario.select(F.mean('salario')).collect()[0][0])
print(salario_medio)

# COMMAND ----------

clean_data = df_final.na.fill({'salario' : salario_medio}) 
# substitui valores de uma coluna por outra
clean_data.show()

# COMMAND ----------

# DBTITLE 1,Retirando linhas duplicadas
autores = [['Thomas','Hardy','June 2, 1840'],\
       ['Charles','Dickens','7 February 1812'],\
        ['Mark','Twain',None],\
        ['Jane','Austen','16 December 1775'],\
      ['Emily',None,None]]
df_autores = spark.sparkContext.parallelize(autores).toDF(
       ["PrimeiroNome","UltimoNome","Dob"])

df_autores.show()

# COMMAND ----------

autores = [['Thomas','Hardy','June 2,1840'],\
    ['Thomas','Hardy','June 2,1840'],\
    ['Thomas','H',None],\
    ['Jane','Austen','16 December 1775'],\
    ['Emily',None,None]]

df_autores = spark.sparkContext.parallelize(autores).toDF(
      ["PrimeiroNome","UltimoNome","Dob"])

# COMMAND ----------

df_autores.show()

# COMMAND ----------

# retirando as linhas duplicadas
df_autores.dropDuplicates().show()

# COMMAND ----------

# DBTITLE 1,Transformando dados
#utiliza a diretriz UDF para criar a função a ser aplicada a cada uma das celulas selecionadas
concat_func = F.udf(lambda nome, idade: nome + "_" + str(idade))

# COMMAND ----------

#aplica a função UDF (concat_func) para criar o novo dataframe
concat_df = df_final.withColumn("nome_idade", concat_func(df_final.nome, df_final.idade))

concat_df.show()

# COMMAND ----------

#cria a função que transforma o salario de reais para dólares
from pyspark.sql.types import LongType
def realDolar(salario):
  return salario*0.25
real_dolar = F.udf(lambda salario: realDolar(salario),LongType())

# COMMAND ----------

#aplica a função UDF (real_dolar) para criar o novo dataframe
df_real_dolar = df_final.withColumn("salario_dolar", real_dolar(df_final.salario))

# COMMAND ----------

# DBTITLE 1,Correlação
from pyspark.mllib.stat import Statistics

# COMMAND ----------

#cria duas series para encontrar a correlação
import random #utilizada pra gerar valores randomicos
serie_1 = spark.sparkContext.parallelize(random.sample(range(1,101),10)) #cria valores randomicos
serie_2 = spark.sparkContext.parallelize(random.sample(range(1,101),10)) #cria valores randômicos
serie_3=serie_1.map(realDolar)  #aplica a transformação (realDolar) sobre a serie 1

# COMMAND ----------

#aplicando a correlação
correlacao = Statistics.corr(serie_1, serie_2, method = "pearson")
print(correlacao)

# COMMAND ----------

#aplicando a correlação
correlacao = Statistics.corr(serie_1, serie_3, method = "pearson")
print(correlacao)

# COMMAND ----------

# DBTITLE 1,Redução da Dimensionalidade
from pyspark.ml.feature import PCA  #define a utilização do PCA
from pyspark.ml.linalg import Vectors #Utilizada para criar vetores com os dados

datasetDigitsDire="/FileStore/tables/digitsNew.csv"

# COMMAND ----------

data = spark.read.csv(datasetDigitsDire, header=True, inferSchema=True) #carrega o arquivo
data.show(5)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler  #cria o vetor de características
assembler = VectorAssembler(inputCols=data.columns[1:], outputCol='features')  
#define as colunas a serem utilizadas como características

data_2 = assembler.transform(data)  #aplica a transformação - Vetores-Características
data_2.show()  #label - dígitos /  features -> 28*28 pixels =  784

# COMMAND ----------

data_2.select("features").show()

# COMMAND ----------


from pyspark.ml.feature import PCA  #importa o PCA
pca = PCA(k=2, inputCol='features', outputCol='features_pca')  #define que queremos 2 dimensões 

# COMMAND ----------

pca_model = pca.fit(data_2)  #aplica o PCA
pca_data = pca_model.transform(data_2).select('features_pca')  #encontra os autovetores de duas dimensões 
pca_data.show(truncate=False)