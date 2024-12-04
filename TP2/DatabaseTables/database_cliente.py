import threading
import json
import datetime

class Database_cliente:
    def __init__(self):

        self.melhor_vizinho = None  # Melhor vizinho baseado nos tempos
        self.tempos = {}  # Dicionário para armazenar os tempos de resposta

    def set_melhor_vizinho(self, vizinho):
        self.melhor_vizinho = vizinho

    def get_melhor_vizinho(self):
        return self.melhor_vizinho

    def set_tempo_vizinho(self, vizinho, tempo):
        self.tempos[vizinho] = tempo

    def get_tempos(self):
        return self.tempos