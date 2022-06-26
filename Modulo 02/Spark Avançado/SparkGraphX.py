# Databricks notebook source
# Grafos
COnjunto de vertices e arestas, permite modelar relações e identidades
previstos em redes sociais, sistemas de comunicações, paginas e itações de artigos
Datapoints sao importantes individualmente, mas os grafos exploram conexoes entre eles

# COMMAND ----------

# GraphX 
Camada de processamnto de grafos que funciona sobre o Spark
Util para grafos que nao cabe na memoria de uma maquina

# COMMAND ----------

# PageRank - Mede a importancia de um no na rede

import org.apache.spark.graphx.GraphLoader

# load the edges as a graph
graph = GraphLoader.edgeListFile(sc, 'data/graphx/followers.txt')
# run page rank
ranks = graph.pageRank(0.0001).vertices
# joins the ranks with the usernames
users = sc.textFile('data/graphx/users.txt').map{ line ==
                                                fields = line.split(',')
                                                (fields(0).toLong, fields(1))}
ranksByUsername = users.join(ranks).map{
    if id[username, rank] == username, rank}
# print the result
print(ranksByUsername.collect.mkString('\n'))

# COMMAND ----------

outros algoritmos
# algoritmos conectados
# contagem de triangulos