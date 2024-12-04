from collections import defaultdict
import json
import threading
import os


class Database_Server:

    def __init__(self, diretoria = "./videos/"):
        # Dicionário de vídeos no formato {video_name: video_id}
        self.videos = dict()
        self.videosLock = threading.Lock()
        self.next_id = 0 # Controlador para IDs numéricos

        # {"video": (set(access_points))}
        self.streams = defaultdict(set)
        self.streamsLock = threading.Lock()

        self.videos_dir = diretoria
        
    def read_config_file(self):
        self.load_id_Video()
        if not os.path.exists(self.videos_dir):
            print(f"Erro: A diretoria {self.videos_dir} não existe.")
            return

        # Filtrar apenas arquivos com extensão .Mjpeg
        video_files = [
            file for file in os.listdir(self.videos_dir)
            if os.path.isfile(os.path.join(self.videos_dir, file)) and file.lower().endswith(".mjpeg")
        ]

        with self.videosLock:
            for file in video_files:
                if file not in self.videos:  # Adiciona apenas novos vídeos
                    self.videos[file] = self.next_id
                    self.next_id += 1

        print(f"Vídeos encontrados na diretoria {self.videos_dir}: {self.videos}")


    def load_id_Video(self):
        with open(self.videos_dir + "id.json", 'r') as arquivo:
            data = json.load(arquivo)
            self.next_id = int(data["idStart"][0])
            
    def has_video(self, video):
        with self.videosLock:
            return video in self.videos

    def get_videos(self):
        with self.videosLock:
            return self.videos.copy()

    def add_stream(self, video, clientID):
        with self.streamsLock:
            self.streams[video].add(clientID)

    def remove_stream(self, video, clientID):
        with self.streamsLock:
            try:
                self.streams[video].discard(clientID)
                if len(self.streams[video]) == 0:
                # self.streams[video].stop_serving()  # Faz com que o worker que está emitindo o vídeo termine
                 del self.streams[video]  # Retira a stream da base de dados
                return "Streaming removido com sucesso"
            except KeyError:
                print(self.streams)
                return "Streaming não existia"

    def is_streaming(self, video):
        with self.streamsLock:
            return video in self.streams.keys()

    def get_videos_dir(self):
        return self.videos_dir
    
    def get_video_id(self, video):
        with self.videosLock:
            return self.videos[video]
    
    def get_available_ids(self):
        with self.videosLock:
            return list(self.videos.values())
        
    def getAccessPoints(self, video):
        with self.streamsLock:
            return self.streams[video]