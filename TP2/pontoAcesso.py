import pickle
import socket
import threading
import signal
import sys
import time
from DatabaseTables.database_nodos import Database_NODOS
from Mensagem.mensagem import Mensagem, MetricMessage
from functools import partial
from utils import get_vizinhos_bootstrapper,change_terminal_title

CHECK_METRICAS_PORT = 3000
CHECK_VIDEOS_PORT = 3001 
START_VIDEOS_PORT = 3002 
STOP_VIDEOS_PORT  = 3003 

# chave -> nome do video
# valor -> (id, ip_do_servidor)
VIDEOS = {}
CLIENTS_per_Stream = Database_NODOS()

# Função para encerrar o servidor e as suas threads no momento do CTRL+C
def ctrlc_handler(sig, frame):
    print("A encerrar o servidor e as threads...")
    sys.exit(0)

# Função para encerrar repentinamente no momento do CTRL+\
def ctrl_slash_handler(sig, frame):
    print("A simular encerramento repentino...")
    sys.exit(0)


###########################################################################
#        Descubrir qual dos pontos de acesso responde mais rápido
###########################################################################
def comunicacao_socket_verificar_metricas():
	canal_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	canal_socket.bind(("0.0.0.0", CHECK_METRICAS_PORT))
	print(f"Serviço de PARA CALCULAR MÉTRICAS está pronto para receber conexões na porta {CHECK_METRICAS_PORT}")

	while True:
		try:
			dados, addr = canal_socket.recvfrom(1024)
			print(f"O ponto de acesso receber um pedido para verificar métricas do cliente -> {addr[0]}")
			informar_metricas_ponto_acesso(dados, canal_socket, addr)
		except Exception as e:
			print(f"Erro svc_answer_requests: {e}")
			break
	canal_socket.close()
	

def informar_metricas_ponto_acesso(dados, socket, addr: tuple):
    msg = Mensagem.deserialize(dados)
    print(msg)
    
    if msg.get_conteudo() == "ping":  # Verifica se o conteúdo é "ping"
        msg = Mensagem(conteudo="").serialize()
        socket.sendto(msg, addr)
        print(f"Ponto de acesso a mandar métricas para cliente {addr[0]}")


#######################################################################
#       Pedir aos servidores para saber quais videos eles têm
#######################################################################

def get_videos_servidor(ip_server: str):
    attempts=3
    global VIDEOS
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sckt.settimeout(6)
        server = (ip_server, CHECK_VIDEOS_PORT)
        msg = Mensagem(conteudo="2").serialize()
        print(f"A enviar pedido de vídeos ao servidor {server}")
        
        retries = 0
        while retries < attempts:
            try:
                sckt.sendto(msg, server)
                data, _ = sckt.recvfrom(1024)
                
                if data:
                    aux  = Mensagem.deserialize(data).get_conteudo()
                    for k in aux:
                        aux[k] = (aux[k], ip_server)
                    VIDEOS.update(aux)
                    print(f"Recebido vídeos do servidor {ip_server}: {VIDEOS}")
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


def comunicacao_get_videos_servidores(servers_ips):
    threads = []
    for server_ip in servers_ips:
        thread = threading.Thread(target=get_videos_servidor, args=(server_ip,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

######################################################
#               Verificar se vídeo existe 
######################################################

def comunicacao_socket_verificar_videos(myIP):
	canal_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	canal_socket.bind(("0.0.0.0", CHECK_VIDEOS_PORT))
	print(f"Serviço de verificação SE VÍDEO EXISTE está pronto para receber conexões na porta {CHECK_VIDEOS_PORT}")

	while True:
		try:
			dados, addr = canal_socket.recvfrom(1024)
			print(f"O ponto de acesso receber um pedido para verificar vídeo do cliente -> {addr[0]}")
			threading.Thread(target=informar_se_video_existe, args=(dados, canal_socket, addr, myIP,)).start()
		except Exception as e:
			print(f"Erro svc_answer_requests: {e}")
			break
	canal_socket.close()
	

def informar_se_video_existe(dados, socket, addr: tuple, myIP):
    global VIDEOS
    msg = Mensagem.deserialize(dados)
    video = msg.get_conteudo()
    
    print(f"Mensagem VERIFICA VÍDEO {msg}")
    
    if video in VIDEOS:
        print(f"Vídeo {video} existe!!\n")
        msg = Mensagem(conteudo=True)  # Mensagem afirmativa
        print(f"Man{msg}")
        socket.sendto(msg.serialize(), addr)
    else:
        print("O vídeo não existe nos servidores!!\n")
        msg = Mensagem(conteudo=False)  # Mensagem negativa
        socket.sendto(msg.serialize(), addr)


######################################################
#               Começar vídeo 
######################################################


def comunicacao_socket_transmitir_videos(myIP):
	canal_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	canal_socket.bind(("0.0.0.0", START_VIDEOS_PORT))
	print(f"Serviço de pedido do envio do vídeo está pronto para receber conexões na porta {START_VIDEOS_PORT}")
	while True:
		try:
			dados, addr = canal_socket.recvfrom(1024)
			print(f"O ponto de acesso receber um pedido para transmitir do cliente para visualização do vídeo-> {addr[0]}")
			threading.Thread(target=pedido_transmiçao_video, args=(dados, canal_socket, addr, myIP,)).start()
		except Exception as e:
			print(f"Erro svc_answer_requests: {e}")
			break
	canal_socket.close()
	


def pedido_transmiçao_video(dados, sockett, addr: tuple, myIP):
    global CLIENTS_per_Stream
    global VIDEOS
    msg = Mensagem.deserialize(dados)
    video = msg.get_conteudo()
    print(f"Mensagem PEDIDO PARA INICIAR TRANSMIÇÃO {msg}")
    
    if video in VIDEOS:
        if CLIENTS_per_Stream.video_a_ser_transmitido(video):
            print(f"O vídeo {video} já está sendo transmitido. Adicionando cliente {addr}.")
            CLIENTS_per_Stream.add_cliente_streaming(video, addr)
            print(f"add cliente stream {CLIENTS_per_Stream.streaming}")
        else:
            _, server_ip = VIDEOS[video]
            print(f"Solicitando vídeo ao melhor vizinho: {server_ip}")
            
            msg = Mensagem(conteudo=(video, myIP)).serialize()
            canal_socket_servidor = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            canal_socket_servidor.settimeout(3)
            endereço = (server_ip, START_VIDEOS_PORT)
            
            max_retries = 3
            attempts = 0
            sucesso = False
            
            while attempts < max_retries and not sucesso:
                try:
                    canal_socket_servidor.sendto(msg, endereço)
                    sucesso = True  
                except socket.timeout:
                    attempts += 1
                    print(f"Tentativa {attempts} falhou (timeout). Retentando...")
                except Exception as e:
                    print(f"Erro ao tentar enviar pedido: {e}")
                    break  
            
            if not sucesso:
                print(f"Máximo de tentativas alcançado. Não foi possível solicitar o vídeo {video}.")
                canal_socket_servidor.close()
            else:
                print(f"Pedido enviado com sucesso para {server_ip}.")
                CLIENTS_per_Stream.add_cliente_streaming(video, addr)
                canal_socket_servidor.close()
                transmitir_video_cliente(video, myIP, server_ip)
               # db.add_streaming_from(melhor_vizinho, video) # ip_fornecedor_video: video
            
    else:
        print(f"Vídeo {video} não está disponível nos servidores.")

#todo: mandar metricas para os vizinhos   

def transmitir_video_cliente(video, myIP, server_ip):
    # temo um while True assim a função fica a correr até que não sejam recebidos frames
    global CLIENTS_per_Stream
    global VIDEOS
    video_id, _ = VIDEOS[video]
    socket_channel = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    socket_channel.bind((myIP, 3004 + video_id))
    socket_channel.settimeout(2)
    while True:
        try:
            clients = CLIENTS_per_Stream.get_clientes_que_querem_streaming(video) 
            #enquanto existirem clientes a quererem o vídeo
            if len(clients) > 0: 
                packet, _ = socket_channel.recvfrom(65535) 
                info = pickle.loads(packet)
                for dest in clients: # envia o frame recebido do servidor para todos os dispositivos a ver o vídeo
                    socket_channel.sendto(info["payload"], dest)
            else:
                break 
        except socket.timeout:
            stop_video_msg = Mensagem(conteudo=(video, myIP)).serialize()
            socket_channel.sendto(stop_video_msg, (server_ip, STOP_VIDEOS_PORT))
            socket_channel.close() 
            print("Nenhum pacote recebido. Assumindo fim da transmissão.")
            return
        
    stop_video_msg = Mensagem(conteudo=(video)).serialize()
    socket_channel.sendto(stop_video_msg, (server_ip, STOP_VIDEOS_PORT))
    socket_channel.close() 
    print(f"A transmição do vídeo foi terminada")
      
####################################################
#           Parar transmição do vídeo
####################################################       


def comunicacao_socket_parar_transmiçao_video(myIP):
	canal_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	canal_socket.bind(("0.0.0.0", STOP_VIDEOS_PORT))
	print(f"Serviço de PARA CALCULAR MÉTRICAS está pronto para receber conexões na porta {STOP_VIDEOS_PORT}")

	while True:
		try:
			dados, addr = canal_socket.recvfrom(1024)
			print(f"O ponto de acesso receber um pedido do cliente para parar vídeo -> {addr[0]}")
			threading.Thread(target=para_stream_parar_cliente, args=(dados, addr,myIP,)).start()
		except Exception as e:
			print(f"Erro svc_answer_requests: {e}")
			break
	canal_socket.close()
	

def para_stream_parar_cliente(dados, addr: tuple, myIP):
    global CLIENTS_per_Stream
    msg = Mensagem.deserialize(dados)
    video = msg.get_conteudo()
    CLIENTS_per_Stream.remove_cliente_streaming(video, addr)
    
    clients = CLIENTS_per_Stream.get_clientes_que_querem_streaming(video) 
    if len(clients) == 0: 
        msgServer = Mensagem(conteudo=(video, myIP)).serialize()
        _, server_ip = VIDEOS[video]
        socket_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        endereço = (server_ip, STOP_VIDEOS_PORT)
        socket_server.sendto(msgServer, endereço)
   
    
neighbors = []

def metric_sender(myIP):
    global neighbors
    socket_channel = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    socket_channel.bind((myIP, 2998))
    while True:
        msg = MetricMessage([(myIP, 0, 0)])
        for neighbor in neighbors:
            socket_channel.sendto(msg.serialize(), (neighbor, 2999))
    socket_channel.close()
        
        
# Função principal do ponto de acesso
def main():
    change_terminal_title()
    # Cria a base de dados
    db = Database_NODOS()
    
    # Registra o sinal para encerrar o servidor no momento do CTRL+C
    signal.signal(signal.SIGINT, partial(ctrlc_handler))
    # Registra o sinal para simular o encerramento repentino do servidor no momento do CTRL+\
    signal.signal(signal.SIGQUIT, ctrl_slash_handler)

    print("Ponto de acesso a iniciar...")

    global neighbors
    msgPedido = 2
    respostaBootstrapper = get_vizinhos_bootstrapper(msgPedido)
    neighbors = respostaBootstrapper["vizinhos"]
    myIP = respostaBootstrapper["ipIdentificador"]
    ip_servers = respostaBootstrapper["servidores"]
    print(f"Vizinhos: {neighbors}\n\n")
    print(f"MY_IP: {myIP}\n\n")
    print(f"Servidores: {ip_servers}\n\n")
    print("A pedir aos servidores os seus vídeos...")
    comunicacao_get_videos_servidores(ip_servers)

    # Inicia os serviços de verificação de vídeos e iniciar vídeo em threads separadas
    thread1 = threading.Thread(target=comunicacao_socket_verificar_metricas, args=())
    thread2 = threading.Thread(target=comunicacao_socket_verificar_videos, args=(myIP,))
    thread3 = threading.Thread(target=comunicacao_socket_transmitir_videos, args=(myIP,))
    thread4 = threading.Thread(target=comunicacao_socket_parar_transmiçao_video, args=(myIP,))
    thread5 = threading.Thread(target=metric_sender, args=(myIP,)) 

    threads = [
        thread1,
        thread2,
        thread3,
        thread4,
        thread5,
    ]

    for t in threads:
        t.daemon = True
    
    for t in threads:
        t.start()

    for t in threads:
        t.join()

if __name__ == '__main__':
    main()
