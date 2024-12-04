import random
import datetime
import pickle
import time

ids = 0
class Mensagem:

    def __init__(self, conteudo="", port=3000, total_parts=1):
        global ids
        ids = ids + 1
        self.id: int = ids # Identificador único
        self.port = port
        self.conteudo = conteudo                   # Conteúdo da mensagem (ou parte dela)
        self.total_parts = total_parts             # Número total de partes (para mensagens fragmentadas)
        
    def get_id(self):
        return self.id
    
    def get_conteudo(self):
        return self.conteudo
    
    def set_conteudo(self, conteudo):
        self.conteudo = conteudo
    
    def get_total_parts(self):
        return self.total_parts
        
    def serialize(self):
        return pickle.dumps(self)
    
    @staticmethod
    def deserialize(bytes):
        return pickle.loads(bytes)

    def __str__(self):
        return (
            f"{{\n\tId: {self.id},\n\t"
            f"Conteúdo: {self.conteudo},\n\t"
            f"Parte:{self.total_parts}\n}}"
        )
    
    def __repr__(self):
        return self.__str__()

#
# best routes will be of the following type
# [(access_point_ip, time, number of alternative Routes)]
#
#
class MetricMessage:
    def __init__(self, bestRoutes = []):
        self.bestRoutes = bestRoutes
        self.timestamp = time.time_ns() // 1_000_000
    def getbestRoutes(self):
        return self.bestRoutes

    
    def getTimestamp(self):
        return self.timestamp
        
    def serialize(self):
        return pickle.dumps(self)
    
    @staticmethod
    def deserialize(bytes):
        return pickle.loads(bytes)

    def __str__(self):
        return (
            f"{{"
            f"Conteúdo: {self.bestRoutes},\n\t"
            f"Parte:{self.timestamp}\n}}"
        )
        

    def update_and_set_timestamp(self):
        self.timestamp = datetime.datetime.now()
        
        
class videoMessage:
    # destinations is a set with the ips of the access_points
    # payload is the video itself
    def __init__(self, destinations, payload):
        self.destinations = destinations
        self.payload = payload
        
    def serialize(self):
        return pickle.dumps(self)
    
    def deserialize(bytes):
        return pickle.loads(bytes)