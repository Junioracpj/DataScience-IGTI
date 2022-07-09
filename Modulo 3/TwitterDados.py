# Databricks notebook source
#Spyder Editor

# COMMAND ----------

# import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

# COMMAND ----------

# set up dos credenciais
consumer_key = 'RDAvfQkiyUQz9Kw73czkabuFT'
consumer_secret = 'jEBHVZuADtKLMHMzLLFhaCC6sNtw88AY6L1Do9yMkhKf2q8ZXt'
acess_token = 'AAAAAAAAAAAAAAAAAAAAAIgweQEAAAAAmPwjULSqeZQrzJ0WmCuSvan6rUs%3DQyBtfUk4XZ1q3AaGuuEQQcGQgCyyBFTx7kQ58KEt2fUHGSiwFn'
acess_secret =

# COMMAND ----------

class TweetsListener(StreamListener):
    def __init__(self, csoket):
        self.client_socket = csocket
        self.count = 0
        self.limit = 30
        
    defon_data(self, data):
        try:
            msg = json.loads(data) # le os twitters
            self.count += 1 # incrementa o contador
            if self.count <= self.limit:
                print(msg['text'].endcode('utf-8')) # verifica a quantidade de twitters
                self.client_socket.send(msg['text'].endcode('utf-8')) # envia a mensagem
            return True
        except BaseException as e:
            print('Error on_data %s' % str(e))
        return True
    
    def on_error(self, status):
        print(status)
        return True

def sendData(c_socket): # define como os dados devem ser enviados
    auth = OAuthHandler(consumer_key, consumer_secret) # autentica no site
    auth.set_acess_token(acess_token, acess_secret) # obtem o token
    twitter_stream = Stream(auth, TweetsListener(c_socket)) # tipo de conexao
    twitter_stream.filter(track=['Bolsonaro']) # define o filtro utilizado

if __name__ == '__main__':
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # cria o objeto socket
    host = '127.0.0.1'
    port = '9995'
    s.bind((host, port))
    print('Listening on port: %s' % str(port))
    s.listen(5) # tempo de aguardo da conexao
    c, addr = s.accept() # estabelece a conexao
    print('Received request from: ' + str(addr))
    sendData(c)