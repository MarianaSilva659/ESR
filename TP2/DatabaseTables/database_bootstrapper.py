import threading
import json
import datetime


class Database_bootstrapper:
    def __init__(self):
        # Info acerca dos meus vizinhos de cada nó da topologia -> chave (Nó) info (VIZINHOS)
        self.vizinhos = {}
        self.servidores = {}
        self.pontosAcesso = {}
    
    def load_database(self, ficheiro):
        with open(ficheiro, 'r') as arquivo:
            data = json.load(arquivo)
            self.vizinhos = data     
            
    def load_servidore(self, ficheiro):
        with open(ficheiro, 'r') as arquivo:
            data = json.load(arquivo)
            self.servidores = data  
            
    def load_PA(self, ficheiro):
        with open(ficheiro, 'r') as arquivo:
            data = json.load(arquivo)
            self.pontosAcesso = data  
    
    # Get vizinhos de um nó
    def get_vizinhos(self, no):
        return self.vizinhos[no]["vizinhos"]
    
    def get_servidores(self):
        return self.servidores["servidores"]
    
    def get_PA(self):
        return self.pontosAcesso["access_point"]
    
    # Get IPIdentificador
    def get_IPIdentificador(self, no):
        return self.vizinhos[no]["ipIdentificador"]
    
    def __str__(self):
            return (
                f"\nDatabase:\n"
                f"\t{self.vizinhos}\n"
            )
    
    def __repr__(self):
        return self.__str__()
            
    