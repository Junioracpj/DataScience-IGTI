# Databricks notebook source
# Big Data - PySpark - Gerar 2 Bilhões de Números Aleatórios

from random import randint

f_contagem_de_numeros= open("numeros.txt", 'a')

### Gerar 2 Bilhões de Números Aleatórios

total_de_rodadas = 1000000
#print("Números Aleatórios:  %8.2f " % total_de_rodadas, sep=",")
print('Total de Números Aleatórios: {:,.0f}'.format(total_de_rodadas))

de_10000_em_10000 = 0

incrementa = 0

for i in range(total_de_rodadas):
  # Gera números aleatórios entre 0 e 10
  numero_aleatorio = randint(0, 10)

  incrementa = incrementa + 1
  de_10000_em_10000 =  de_10000_em_10000 + 1

  if incrementa == 10000:
    print('Total Processado: {:,.0f}'.format(de_10000_em_10000))
    incrementa = 0

  if numero_aleatorio != 5: # Se for diferetne de 5, imprime na mesma linha
    print(numero_aleatorio, end = " ")
    f_contagem_de_numeros.write(str(numero_aleatorio))
  else: # se for 5, imprime na próxinma linha
    #print(numero_aleatorio)
    f_contagem_de_numeros.write(str(numero_aleatorio) + '\n')
f_contagem_de_numeros.close()   