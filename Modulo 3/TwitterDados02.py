# Databricks notebook source
# importante bibliotecas base e criando secao spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark  = SparkSession.builder.appName('TwitterDados').getOrCreate()

# COMMAND ----------

# bibliotecas utilizadas para relizar a analise dos textos
from textblob import TextBlob # utilizada para realizar o processamento de texto e analise de sentimento
from googletrans import Translator # tradutor
from unidecode import undecode # decodifica caracteres nao textuais

# COMMAND ----------

# cria o dataframe que sera responsavel por ler cada linha recebida
# define a fonte (source) de dados
files_dir = spark.readStream \
            .format('socket') \ 
            .option('host', 'localhost') \
            .option('port', 9995) \
            .load()

# COMMAND ----------

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, FloatType

# COMMAND ----------

# funcao para realizar a traducao para o ingles
def translate_udf(col):
    trans_obj = Translator().translate(col)
    return trans_obj.text

# funcao para realizar a analise de sentimento
def sentinment_udf(col):
    sentiment_text = TextBlob(col)
    return sentiment_text.polarity

# COMMAND ----------

# definicao da funcao como user-defined-function
unicode_udf_string = udf(lambda z: unicode(z), StringType()) # define a funcao de decode para ser utilizada na data
group_by_sentiment = udf(lambda x: 'negativo' if x < -0.1 else 'positivo' if x > 0.1 else 'neutro', StringType())
translate_udf_string = udf(translate_udf, StringType()) # define a funcao de traducao
sentiment_udf_float = udf(sentiment_udf, FloatType()) # define a funcao de sentimento

# COMMAND ----------

# aplica um teste
teste = 'eu <3 amo meu cachorro, ele e o meu melhor amigo'
decode = unidecode(teste)
print(decode)
decodeEN = Translator().translate(decode)
print(decodeEN.text)
a = str(decodeEN)
sentiment = TextBlob(a)
print(sentiment.polarity)

# COMMAND ----------

# aplica as funcoes udf para a selecao de colunas
twitter_unicode = twitter \ 
        .select('value', unicode_udf_string(twitters.value) \ 
        .alias('unicoded')) # decodifica
twitters_uni_trans = twitters_unicode.select('value', 'unicoded',
                                            translate_udf_string(col('unicoded')) \
                                             .alias('twitteer_EN')) # traduz
twitters_uni_trans_sent = twitters_uni_trans.select('value', 'unicoded', 'twitteer_EN',
                                            sentiment_udf_float(col('twitteer_EN')) \
                                             .alias('analise')) # analisa o sentimento
t_sent_label = twitters_uni_trans_sent.select('value', 'unicoded',
                                              'twitteer_EN', 'analise',
                                            group_bu_sentiment(col('analise')) \
                                             .alias('classificacao')) 

# COMMAND ----------

t_sent_count = t_sent_label.groupBy('classificacao').count()

# COMMAND ----------

# define a consulta (query) e como deve ser realizada a saida (sink) para o stream criado
query = windowedCounts \
    .writeStream \
    .outputModel('update') \
    .format('console') \
    .start()
query.awaitTermination() # aguarda ate que a streaming query termine