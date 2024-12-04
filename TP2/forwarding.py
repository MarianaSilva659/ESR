from collections import defaultdict
from functools import partial
import math
import pickle
import socket
import signal
import sys
import threading
import time
import queue
from typing import OrderedDict
from Mensagem.mensagem import Mensagem, MetricMessage
from utils import get_vizinhos_bootstrapper,change_terminal_title

myIP = ""



# Dictionary to store neighbor IPs and the last time they comunicated with us
neighbors = {}
neighbor_lock = threading.Lock()
neighborsAlive = 0
# in ms
timeForTimeout = 400
timeToSendMetrics = 275
metric_updater_q = queue.Queue()
CHECK_VIDEOS_PORT = 3001 
KEEPALIVE = True

#
# the forwarding table will have the following format:
# video_id : OrderedByBestTimeDict
# where the dict has the following format:
# video_id : ip { 
# {
#     'bestTime': float, #in ms
#     'count': int, 
#     'n_fails': int,
#     'n_alternative_paths': int
#     'score' : float
#   }
#}
# The dict will be ordered by the score parameter in descending order
#

def ctrlc_handler(sig, frame):
    global KEEPALIVE
    KEEPALIVE = False
    time.sleep(1)
    sys.exit(0)
    
def ctrl_slash_handler(sig, frame):
    global KEEPALIVE
    KEEPALIVE = False
    time.sleep(1)
    sys.exit(0)

def scoreCalculator(tableEntry):
    score = 0
    score += 1000 - tableEntry['bestTime']
    score += tableEntry['count'] * 0.01
    score -= 10 *  tableEntry['n_fails']
    score += 20 * tableEntry['n_alternative_paths']
    return score

# The first key is the ip of the access point, the second is the ip of the nodes
# This class aims to have fast lookups while maintaining the order of what the best route is
class NestedOrderedDict:
    def __init__(self):
        self.data = defaultdict(OrderedDict)  # Nested dictionary structure
        self.locks = defaultdict(threading.Lock)  # Locks for each outer key

    def add_entry(self, outer_key, inner_key, bestTime, n_alternative_paths):
        """
        Adds an entry to the nested dictionary.
        Uses a lock per entry to ensure thread-safety in the corresponding inner dictionary.
        """
        
        # Acquire the lock for the specific outer key
        with self.locks[outer_key]:
            if inner_key not in self.data[outer_key]:
                entry = {
                    'bestTime': bestTime,
                    'count': 1,
                    'n_fails': 0,
                    'n_alternative_paths': n_alternative_paths,
                    'score': 0
                 }
                entry["score"] = scoreCalculator(entry)
                self.data[outer_key][inner_key] = entry
            else:
                    self.data[outer_key][inner_key]['bestTime'] = bestTime
                    self.data[outer_key][inner_key]['count'] += 1
                    self.data[outer_key][inner_key]['n_alternative_paths'] = n_alternative_paths
                    self.data[outer_key][inner_key]['score'] = scoreCalculator( self.data[outer_key][inner_key])
            
            # Reorder the inner dictionary by 'score'
            self.data[outer_key] = OrderedDict(
                        sorted(
                            self.data[outer_key].items(),
                            key=lambda x: (x[1]["score"] is None, x[1]["score"]),
                            reverse=True
                        )
            )
    
    def addFailToEntry(self, ip):
        for access_point in self.data.keys():
            with self.locks[access_point]:
                    self.data[access_point][ip]['n_fails'] += 1
                    self.data[access_point][ip]['bestTime'] = math.inf
                    self.data[access_point][ip]['score'] = scoreCalculator(self.data[access_point][ip])
                    self.data[access_point] = OrderedDict(
                        sorted(
                            self.data[access_point].items(),
                            key=lambda x: (x[1]["score"] is None, x[1]["score"]),
                            reverse=True
                        )
                    )
                    
    

    def read_inner_dict(self, outer_key):
        """
        Reads the inner dictionary for a specific outer key.
        Uses a lock to ensure thread-safety.
        """
        with self.locks[outer_key]:
            # Return a copy to avoid external modification
            return dict(self.data[outer_key])
        
    def getBestRoutes(self, requester_ip, access_points):
        """
        Returns the best route for a given video_id and the requester's IP. The return value is always the best route that doesn't directly go through the sender
        Uses a lock to ensure thread-safety.
        """
        routes =  defaultdict(list)
        for access_point in access_points:
            with self.locks[access_point]:
                # Return the best route 
                iterator = iter(self.data[access_point].keys())
                size = len(self.data[access_point].keys())
                while size > 0:
                    ip = next(iterator)
                    if ip != requester_ip:
                        routes[ip].append(access_point) 
                        break
                    size -= 1
        return routes
    
    # Grabs the best and second best routes for each access point
    # It grabs the second best to avoid having to make another function call when
    # the best route ip matches the ip of the node I'm trying to send
    # which will always happen at least once per metric sending
    def getMetrics(self):
        global neighborsAlive
        routes = defaultdict(list)
        for key in self.data.keys():
            with self.locks[key]:
                iterator = iter(self.data[key].keys())
                #print(f"aaaaaaaaaaaaaaaaaaaaaaaaaaaaa {self.data[key].keys()}")
                #print(f"aaaaaaaaaaaaaaaaaaaaaaaaaaaaa {neighborsAlive}")
                ip = next(iterator, None)
                routes[key].append((ip, self.data[key][ip]['bestTime']))
                ip = next(iterator, None)
                if ip is None: 
                    return None
                routes[key].append((ip, self.data[key][ip]['bestTime']))
        return routes
        
                
        

    def get_data(self):
        """Returns the entire nested structure. Not thread safe, but it's only here for debugging"""
        return self.data
    
    def get_access_points(self):
        return list(self.data.keys())

forwardingTable = NestedOrderedDict()
# [(access_point_ip, time, number of alternative Routes)]
def send_metrics():
    global KEEPALIVE 
    global neighborsAlive
    global neighbors
    global neighbor_lock
    global forwardingTable
    socket_channel = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    socket_channel.bind((myIP, 2998))
    while KEEPALIVE:
        with neighbor_lock: 
            if neighborsAlive < 2:
                accessPoints = forwardingTable.get_access_points()
                message = []
                for accessPoint in accessPoints:
                    message.append((accessPoint, math.inf, 0))
                message = MetricMessage(message)
                data = pickle.dumps(message)
                for neighbor in neighbors.keys():
                    socket_channel.sendto(data, (neighbor, 2999))
            else :
                routes = forwardingTable.getMetrics()
                if routes is None:
                    accessPoints = forwardingTable.get_access_points()
                    message = []
                    for accessPoint in accessPoints:
                        message.append((accessPoint, math.inf, 0))
                    message = MetricMessage(message)
                    data = pickle.dumps(message)
                    for neighbor in neighbors.keys():
                        socket_channel.sendto(data, (neighbor, 2999))
                else :
                    alternativeRoutes = neighborsAlive - 1
                    for neighbor in neighbors.keys():
                        message = []
                        for accessPoint, route in routes.items():
                            if route[0][0] != neighbor:
                                message.append((accessPoint, route[0][1], alternativeRoutes))
                            else:
                                message.append((accessPoint, route[1][1], alternativeRoutes))
                        message = MetricMessage(message)
                        socket_channel.sendto(pickle.dumps(message), (neighbor, 2999))
        time.sleep(timeToSendMetrics / 1000)
            
    socket_channel.close()

def metric_updater(channel):
    global KEEPALIVE
    global neighborsAlive
    global neighbors
    global neighbor_lock
    while KEEPALIVE:
        data = channel.get()
        if data is None:
            break
        metrics, addr = data
        metrics = pickle.loads(metrics)
        # metrics' type is MetricMessage
        ip = addr[0]
        current_time =  time.time_ns() // 1_000_000
        tripTime = current_time - metrics.timestamp
        with neighbor_lock: 
            if neighbors[ip] is None: 
                neighborsAlive += 1
            neighbors[ip] = current_time 
            for entry in metrics.bestRoutes:
                forwardingTable.add_entry(entry[0], ip, tripTime + entry[1], entry[2])  
 
    channel.task_done() 
    
# all packets received will be of type 
#  {
#   destinations
#   payload
#  }
# where payload is the actual video
# and destination is a list of the IPs of the acess points
#
def video_handler(video_id):
    global myIP
    global KEEPALIVE
    port = 3004 + video_id
    socket_channel = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    socket_channel.bind((myIP, port))
    while KEEPALIVE:
        data, addr = socket_channel.recvfrom(65535)
        sender_ip = addr[0]
        data = pickle.loads(data)
        routes = forwardingTable.getBestRoutes(sender_ip, data['destinations'])
        for ip, dests in routes.items():
            data['destinations'] = dests 
           # print(f"dest{dests} ip {ip}")
            socket_channel.sendto(pickle.dumps(data), (ip, port))
    socket_channel.close()
        
        
def metric_receiver():
    global metric_updater_q
    global myIP
    global KEEPALIVE
    port = 2999
    socket_channel = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    socket_channel.bind((myIP, port))
    thread = threading.Thread(target=metric_updater, args=(metric_updater_q,))
    thread.start()
    while KEEPALIVE:
        data, addr = socket_channel.recvfrom(65535)
        metric_updater_q.put((data, addr))
    socket_channel.close()
    return

# "Kills" the neighbors that haven't sent metrics in more than the specified timeout
# It kills them by setting their bestTime to math.inf which will put them at the bottom of the forwarding list
# Dead neighbors will only receive video packages if all other neighbors are dead
# This function won't overwrite messages that were received at the exact same time as the thread is waking up do the lock
def neighborSlayer():
    global forwardingTable
    global neighborsAlive
    global neighbors
    global neighbor_lock
    global KEEPALIVE
    while KEEPALIVE:
        with neighbor_lock:
            for ip, last_message in neighbors.items():
                if last_message is not None and last_message + timeForTimeout < time.time_ns() // 1_000_000:
                    neighbors[ip] = None
                    neighborsAlive -= 1
                    forwardingTable.addFailToEntry(ip)
        time.sleep(timeForTimeout)

def get_videos_servidor(ip_server):
    attempts=3
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sckt.settimeout(6)
        server = (ip_server, CHECK_VIDEOS_PORT)
        msg = Mensagem(conteudo="1").serialize()
        print(f"A enviar pedido de vídeos ao servidor {server}")
        
        retries = 0
        while retries < attempts:
            try:
                sckt.sendto(msg, server)
                data, _ = sckt.recvfrom(1024)
                
                if data:
                    # videos is a list of all the video ids 
                    videos = Mensagem.deserialize(data).get_conteudo()
                    #print(f"Vídeo {videos}")
                    for video in videos:
                        thread = threading.Thread(target=video_handler, args=(video,))
                        thread.daemon = True
                        thread.start()
                        
                    
                    print(f"Recebido vídeos do servidor {ip_server}: {videos}")
                    return  
                else:
                    print(f"Resposta vazia do servidor {ip_server}")
                    return
            
            except socket.timeout:
                retries += 1
                print(f"Timeout ao receber resposta do servidor {server}. Retentando...")
        
        # Se chegar aqui, todas as tentativas falharam
        print(f"Falha após {attempts} tentativas. Servidor {ip_server} indisponível.")
    
    finally:
        sckt.close()


def comunicacao_get_videos_servidores(server_ips):
    threads = []
    
    for server_ip in server_ips:
        thread = threading.Thread(target=get_videos_servidor, args=(server_ip,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

def main():
    change_terminal_title()
    global neighbors
    global myIP
    default_value = None
    msgPedido = 3
    respostaBootstrapper = get_vizinhos_bootstrapper(msgPedido)
    temp = respostaBootstrapper["vizinhos"]
    neighbors = {key: default_value for key in temp}
    myIP = respostaBootstrapper["ipIdentificador"]
    ip_servers = respostaBootstrapper["servidores"]
    access_points = respostaBootstrapper["access_points"]
    for access_point in access_points:
        forwardingTable.data[access_point]
    print(f"Vizinhos: {temp}\n\n")
    print(f"MY_IP: {myIP}\n\n")
    print(f"Servidores: {ip_servers}\n\n")
    print(f"Pontos de acesso: {access_points}\n\n")
    print("A pedir aos servidores os seus vídeos...")
    comunicacao_get_videos_servidores(ip_servers)
    # Registra o sinal para encerrar o servidor no momento do CTRL+C
    signal.signal(signal.SIGINT, partial(ctrlc_handler))
    # Registra o sinal para simular o encerramento repentino do servidor no momento do CTRL+\
    signal.signal(signal.SIGQUIT, ctrl_slash_handler)
    thread1 = threading.Thread(target=metric_receiver, args=())
    thread1.daemon = True
    thread1.start()
    thread2 = threading.Thread(target=neighborSlayer, args=())
    thread2.daemon = True
    thread2.start()
    thread3 = threading.Thread(target=send_metrics, args=())
    thread3.daemon = True
    thread3.start()
    thread1.join()
    thread2.join()
    thread3.join()
    
    
    
    
main()