import pickle
from random import randint
import sys, traceback, threading, socket
from ServerWorker import ServerWorker
from DatabaseTables.database_server import Database_Server
from Mensagem.mensagem import Mensagem
import signal
from utils import get_vizinhos_bootstrapper, change_terminal_title
from functools import partial
from VideoStream import VideoStream
from RtpPacket import RtpPacket


CHECK_VIDEOS_PORT = 3001 
START_VIDEOS_PORT = 3002 
STOP_VIDEOS_PORT = 3003 

 
# Função para encerrar o servidor e as suas threads no momento do CTRL+C
def ctrlc_handler(db: Database_Server, sig, frame):
    print("A encerrar o servidor e as threads...")
    sys.exit(0)

# Função para encerrar repentinamente no momento do CTRL+\
def ctrl_slash_handler(sig, frame):
    print("A simular encerramento repentino...")
    sys.exit(0)

#############################################################
# 		PEDIDO PARA CONHECER OS VIDEOS DISPONÍVEIS
#############################################################

def comunicacao_socket_verificar_videos(db: Database_Server):
	canal_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	canal_socket.bind(("0.0.0.0", CHECK_VIDEOS_PORT))
	print(f"Serviço de VERIFICAÇÃO DE VÍDEOS está pronto para receber conexões na porta {CHECK_VIDEOS_PORT}")

	while True:
		try:
			dados, addr = canal_socket.recvfrom(1024)
			print(f"O servidorr receber um pedido do cliente -> {addr[0]}")
			threading.Thread(target=informar_videos_disponiveis, args=(dados, canal_socket, addr, db)).start()
		except Exception as e:
			print(f"Erro svc_answer_requests: {e}")
			break
	canal_socket.close()
	

def informar_videos_disponiveis(dados, socket, addr:tuple, db: Database_Server):
	msg = Mensagem.deserialize(dados) 
	print(f"VERIFICAÇÃO QUE VÍDEOS TEM: Received from {addr}")
	# print(msg)

	if (msg.get_conteudo() == "1"):
		msg = Mensagem(conteudo=db.get_available_ids()).serialize()
	else :
		videos = db.get_videos()
		msg = Mensagem(conteudo=videos).serialize()
		print(f"VERIFICAÇÃO QUE VÍDEOS TEM: respondido com os videos {videos} para {addr}")
	socket.sendto(msg, addr)
	


########################################################
#			PEDIDO PARA COMEÇAR VÍDEO
########################################################

def comunicacao_socket_comecar_video(db: Database_Server, vizinho):
	server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	server_socket.bind(("0.0.0.0", START_VIDEOS_PORT))
	print(f"Serviço PARA COMEÇAR VÍDEO pronto para receber conexões na porta {START_VIDEOS_PORT}")

	while True:
		try:
			dados, addr = server_socket.recvfrom(1024)
			threading.Thread(target=começa_stream_video, args=(dados, server_socket, addr, db, vizinho)).start()
		except Exception as e:
			print(f"Erro comunicacao_socket_comecar_video: {e}")
			break

def começa_stream_video(dados, socket, addr:tuple, db: Database_Server, vizinho):
	msg = Mensagem.deserialize(dados)
	print(f"COMEÇAR VÍDEO: no endereço {addr}")
	
	video, ipAcessPoint = msg.get_conteudo()
	# O video não existe
	if not db.has_video(video):
		print(f"COMEÇAR_VIDEO: Não tenho o video '{video}'!!!")
		raise Exception(f"Servidor não tem video '{video}'!!!")
	# caso o video não esteja a ser emitido é criado e começa a mandar para o ponto de acesso
	dest_addr = vizinho[0]
	dest_port = str(db.get_video_id(video) + 3004)
	if not db.is_streaming(video): 
		threading.Thread(target=serve_movie, args=(db, dest_addr, dest_port, video, db.get_videos_dir())).start()
		db.add_stream(video, ipAcessPoint)        
		print(f"COMEÇAR_VIDEO: A enviar o fluxo do video '{video}' para {dest_addr}:{dest_port}")
	else:
		db.add_stream(video, ipAcessPoint)
  
######################################################
#			PEDIDO PARA PARAR O STREAM
######################################################

def comunicacao_socket_para_stream_video(db: Database_Server):
	service_name = "comunicacao_socket_para_stream_video"
	server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	endereco = "" # Listen on all interfaces
	server_socket.bind((endereco, STOP_VIDEOS_PORT))
	print(f"Serviço '{service_name}' pronto para receber conexões na porta {STOP_VIDEOS_PORT}")

	while True:
		try:
			dados, addr = server_socket.recvfrom(1024)
			threading.Thread(target=informa_que_quer_parar_stream_video, args=(dados, server_socket, addr, db)).start()
		except Exception as e:
			print(f"Erro comunicacao_socket_para_stream_video: {e}")
			break

def informa_que_quer_parar_stream_video(dados, socket, addr:tuple, db: Database_Server):
	msg = Mensagem.deserialize(dados)
	video, ip_PA =  msg.get_conteudo()
	print(f"STOP_VIDEO: {addr} pediu para parar a transmissão do vídeo {video}")
	db.remove_stream(video, ip_PA)

    
###############################################
#	Enviar pacotes RTP de vídeo para pedinte
###############################################

def sendRtp(clientInfo, db, movie_name):
		"""Send RTP packets over UDP."""
		while True:
			clientInfo['event'].wait(0.05)
			
			access_points = db.getAccessPoints(movie_name)
			if len(access_points) == 0:
						break
				
			data = clientInfo['videoStream'].nextFrame()
			if data:
				frameNumber = clientInfo['videoStream'].frameNbr()
				try:
					address = clientInfo['rtpAddr']
					port = int(clientInfo['rtpPort'])
					packet =  ServerWorker.makeRtp(data, frameNumber)

					msg = {'destinations': access_points, 'payload': packet}

					clientInfo['rtpSocket'].sendto(pickle.dumps(msg),(address,port))
				except:
					print("Connection Error")
					print('-'*60)
					traceback.print_exc(file=sys.stdout)
					print('-'*60)
		# Close the RTP socket
		clientInfo['rtpSocket'].close()
		# print("All done!")

#########################################################################
#		Construção de um pacote RTP contendo os dados do vídeo
#########################################################################

def makeRtp(payload, frameNbr):
		"""RTP-packetize the video data."""
		version = 2
		padding = 0
		extension = 0
		cc = 0
		marker = 0
		pt = 26 # MJPEG type
		seqnum = frameNbr
		ssrc = 0
		
		rtpPacket = RtpPacket()
		
		rtpPacket.encode(version, padding, extension, cc, seqnum, marker, pt, ssrc, payload)
		# print("Encoding RTP Packet: " + str(seqnum))
		
		return rtpPacket.getPacket()

#################################
#		Inciar stream
#################################

def serve_movie(db: Database_Server, dest_addr:str, dest_port:int, movie_name:str, videos_dir:str="./videos/"):
		
		filename = f"{videos_dir}{movie_name}" 

		clientInfo = dict()

		# videoStream
		clientInfo['videoStream'] = VideoStream(filename)
		# socket de quem pede o vídeo
		# rtpPort -> A porta UDP do pedinte
		# rtpAddr -> ip do pedinte
		clientInfo['rtpPort'] = dest_port
		clientInfo['rtpAddr'] = dest_addr
		print("Mandando vídeo para:" + clientInfo['rtpAddr'] + ":" + str(clientInfo['rtpPort']))
		# Create a new thread and start sending RTP packets
		clientInfo["rtpSocket"] = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		clientInfo['event'] = threading.Event()
		clientInfo['worker']= threading.Thread(target=sendRtp, args=(clientInfo, db, movie_name))
		clientInfo['worker'].start()


def main():
    change_terminal_title()
    diretoria = "./videos/"
    if len(sys.argv) > 2:
        print("Uso: python servidor.py <nome_do_video> || python servidor.py")
        sys.exit(1)
    elif len(sys.argv) == 2:
        diretoria = sys.argv[1]
        
    db = Database_Server(diretoria)
    # Registra o sinal para encerrar o servidor no momento do CTRL+C
    signal.signal(signal.SIGINT, partial(ctrlc_handler, db))
    # Registra o sinal para simular o encerramento repentino do servidor no momento do CTRL+\
    signal.signal(signal.SIGQUIT, ctrl_slash_handler)
    
    print("O cliente está a ligar-se ao bootstrapper")
    
    msgPedido = 1
    respostaBootstrapper = get_vizinhos_bootstrapper(msgPedido)
    
    vizinhos = respostaBootstrapper["vizinhos"]
    my_IP_Identificador = respostaBootstrapper["ipIdentificador"]
    print(f"Vizinhos deste nodo {vizinhos}\n\n")
    print(f"MY_IP: {my_IP_Identificador}\n\n")

    db.read_config_file()
    
    print(db.videos)

    # Criação de threads para diferentes serviços
    thread_notifica_videos_disponiveis = threading.Thread(target=comunicacao_socket_verificar_videos, args=(db,))
    thread_notifica_comecar_video = threading.Thread(target=comunicacao_socket_comecar_video, args=(db, vizinhos,))
    thread_notifica_para_parar_stream = threading.Thread(target=comunicacao_socket_para_stream_video, args=(db,))

    thread_notifica_videos_disponiveis.daemon = True
    thread_notifica_comecar_video.daemon = True
    thread_notifica_para_parar_stream.daemon = True

    thread_notifica_videos_disponiveis.start()
    thread_notifica_comecar_video.start()
    thread_notifica_para_parar_stream.start()

    thread_notifica_videos_disponiveis.join()
    thread_notifica_comecar_video.join()
    thread_notifica_para_parar_stream.join()
    
    signal.signal(signal.SIGINT, ctrlc_handler)





if __name__ == '__main__':
	main()
