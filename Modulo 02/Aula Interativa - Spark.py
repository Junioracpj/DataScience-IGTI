# Databricks notebook source
# DBTITLE 1,Aula Interativa - Spark
Por Antonio Carlos

# COMMAND ----------

from pyspark.sql import SparkSession
from operator import add

spark =  SparkSession \
        .builder \
        .appName("Aula interativa 1 - Numeros") \
        .master("local[*]") \ 
        .getOrCreate()

filename = "C:\Users\junio\Downloads\numbers.txt"
linesRdd = spark.read.text(filename).rdd.map(lambda r:r[0])
countsRdd = linesRdd.flatMap(lambda linha : linha.split(' ')) \
            .filter(lambda numer: int(numer) % 2 == 0) \
            .map(number : numer, 1) \
            .reducebykey(add)

output = countsRdd.collect()
for (number, count)
    PRINT(number, count)
print('The end')


# COMMAND ----------

from pyspark.sql import SparkSession
from operator import add
from pyspark.sql.functions import col
from pyspark.sql.functions import desc

spark =  SparkSession \
        .builder \
        .appName("Aula interativa 1 - Titanic") \
        .master("local[*]") \ 
        .getOrCreate()

titanic_df = spark.read.csv("C:\Users\junio\Downloads\titanic.csv", header='True', inferSchema='True')

titanic_df.printSchema()
titanic_df.count()
titanic_df.show(3)
titanic_df.select('Name', 'Sex','Age').show(2)
titanic_df.describe('Age', 'Fare').show()
titanic_df.select('Name', 'Sex','Age').summary().show(2)
titanic_df.groupBy('Survived').count().show()
titanic_df.groupBy('Sex', 'Survived').count().show()
titanic_df.groupBy('Embarked').count().show()
titanic_df = titanic_df.na.fill('Embarked: 'S') # completa dados faltantes
titanic_df.describe('Cabin').show()
titanic_df = titanic_df.drop('Cabin')
titanic_df = titanic_df.withColumn('FamilySize', col('SibSp')+ col('Parch'))
titanic_df.select('Name', 'FamilySize').orderBy(desc('FamilySize')).show(20)
titanic_df.stat.corr('age','fare') #correlacao de person
titanic_df.groupBy('pclass').agg({'fare': 'avg'}).show()
                                