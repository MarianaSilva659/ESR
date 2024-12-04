import threading

class Database_NODOS:

    def __init__(self):

        # Info acerca dos nós para os quais estou a fazer streaming
        # a chave é o nome do video 
        # cada vídeo tem um tuplo de interessados no vídeo (ip, port)
        self.streaming = dict()
        self.streamingLock = threading.Lock()

    def video_a_ser_transmitido(self, video:str):
        with self.streamingLock:
            return video in self.streaming
        
    def get_clientes_que_querem_streaming(self, video:str):
        with self.streamingLock:
            if video not in self.streaming:
                return []
            tuples = self.streaming[video].copy()
        return tuples
    
    def add_cliente_streaming(self, video:str, addr:tuple):
        with self.streamingLock:
            if video in self.streaming:
                self.streaming[video].append(addr)
            else:
                self.streaming[video] = [addr]

    def remove_cliente_streaming(self, video:str, addr:tuple):
        with self.streamingLock:
            try:
                self.streaming[video] = [x for x in self.streaming[video] if x != addr] 
                if len(self.streaming[video]) == 0:
                    del self.streaming[video]
                print(f"Streaming do vídeo '{video}' para {addr[0]} removido com sucesso")
            except KeyError:
                print("Streaming não existia")

#Remove todas as entradas do endereço 'ip' da lista de endereços a enviar videos
    def remove_streaming_for_ip(self, ip:str):
        with self.streamingLock:
            new_streaming = dict()
            for video in self.streaming:
                new_streaming[video] = [x for x in self.streaming[video] if x[0] != ip]
                if len(new_streaming[video]) == 0:
                    del new_streaming[video]
            self.streaming = new_streaming

    def __repr__(self):
        return self.__str__()
