import socket
import signal
import sys
import time
from utils import get_vizinhos_bootstrapper, change_terminal_title
from Mensagem.mensagem import Mensagem
from tkinter import Tk
from ClienteStream import ClienteStream
from DatabaseTables.database_cliente import Database_cliente

CHECK_METRICAS_PORT = 3000
CHECK_VIDEOS_PORT = 3001 
START_VIDEOS_PORT = 3002 
STOP_VIDEOS_PORT  = 3003 


# Função para lidar com sinais de encerramento, como CTRL+C
def signal_handler(sig, frame):
    print("\nEncerrando o cliente...")
    sys.exit(0)

#################################################################
#   Função para avaliar os tempos de resposta dos vizinhos
#################################################################
def avaliar_vizinhos(vizinhos, db_cliente):
    print("Avaliando conexões com os pontos de acesso...")
    for vizinho in vizinhos:
        retries = 3
        for attempt in range(retries):
            try:
                sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sckt.settimeout(2)
                start_time = time.time()
                msg = Mensagem(conteudo="ping").serialize()
                sckt.sendto(msg, (vizinho, CHECK_METRICAS_PORT))
                dados, addr = sckt.recvfrom(1024)
                
                # Calcula o tempo de resposta
                elapsed_time = time.time() - start_time
                db_cliente.set_tempo_vizinho(vizinho, elapsed_time)
                print(f"Tempo de resposta do ponto de acesso {vizinho}: {elapsed_time:.4f} segundos")
                break  
            except socket.timeout:
                if attempt == retries - 1:
                    print(f"Falha após {retries} tentativas com o ponto de acesso {vizinho}. Definindo tempo como infinito.")
                    db_cliente.set_tempo_vizinho(vizinho, float('inf'))
            except Exception as e:
                print(f"Erro ao tentar se conectar ao ponto de acesso {vizinho}: {e}")
                if attempt == retries - 1:
                    print(f"Falha após {retries} tentativas com o ponto de acesso {vizinho}.")
                    db_cliente.set_tempo_vizinho(vizinho, float('inf'))
            finally:
                sckt.close()
    tempos = db_cliente.get_tempos()
    melhor_vizinho = min(tempos, key=tempos.get)
    if tempos[melhor_vizinho] == float('inf'):
        print("Nenhum vizinho respondeu a tempo. Encerrando...")
        sys.exit(1)
    db_cliente.set_melhor_vizinho(melhor_vizinho)
    print(f"Melhor ponto de acesso selecionado: {melhor_vizinho}")


def enviar_com_retries(socket, mensagem, destino, max_retries=3):
        for attempt in range(max_retries):
            try:
                socket.sendto(mensagem, destino)
                return True  
            except Exception as e:
                print(f"Erro no envio: {e}. Retentando...")
                if attempt == max_retries - 1:
                    print(f"Falha após {max_retries} tentativas.")
                    return False

def main():
    root = Tk()
    my_name = socket.gethostname()
    change_terminal_title()

    if len(sys.argv) < 2:
        print("Uso: python cliente.py <nome_do_video>")
        sys.exit(1)

    video = sys.argv[1]

    print("O cliente está a ligar-se ao bootstrapper")

    msgPedido = 1
    respostaBootstrapper = get_vizinhos_bootstrapper(msgPedido)
    
    vizinhos_client = respostaBootstrapper["vizinhos"]
    my_IP_Identificador = respostaBootstrapper["ipIdentificador"]
    print(f"Pontos de acesso deste cliente: {vizinhos_client}\n\n")
    print(f"Vizinhos: {vizinhos_client}\n\n")
    print(f"MY_IP: {my_IP_Identificador}\n\n")

    # Criar instância do Database_cliente para armazenar informações
    db_cliente = Database_cliente()
    melhor_vizinho = ""
    if len(vizinhos_client) > 1:
        avaliar_vizinhos(vizinhos_client, db_cliente)
    elif len(vizinhos_client) == 1:
        db_cliente.set_melhor_vizinho(vizinhos_client[0])
    
    melhor_vizinho = db_cliente.get_melhor_vizinho()
        
    print(f"\nA enviar pedido para o melhor vizinho {melhor_vizinho}\n")
    print(f"Cliente pedir o vídeo: {video}")

    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sckt.settimeout(5)

    ########################################
    #   Verificação da existência do vídeo
    #########################################
    msg = Mensagem(conteudo=video).serialize()
    verificar_video = (melhor_vizinho, CHECK_VIDEOS_PORT)

    if not enviar_com_retries(sckt, msg, verificar_video):
        print("Não foi possível verificar a existência do vídeo. Encerrando...")
        sys.exit(1)

    resposta, addr = sckt.recvfrom(1024)
    msg = Mensagem.deserialize(resposta)
    print(f"Resposta do Ponto de acesso a dizer se existe ou não o vídeo: {msg.get_conteudo()}")

    if msg.get_conteudo() == True:
        ####################################
        #       Iniciar o vídeo
        ####################################
        msg = Mensagem(conteudo=video).serialize()
        pedir_video = (melhor_vizinho, START_VIDEOS_PORT)

        if not enviar_com_retries(sckt, msg, pedir_video):
            print("Não foi possível iniciar a transmissão do vídeo. Encerrando...")
            sys.exit(1)

        try:
            #! Verificar timeouts e assim lá dentro do ClienteGUI
            # Create a new client
            app = ClienteStream(root, sckt)
            app.master.title(f"{my_name}({my_IP_Identificador}) - {video} ")
            root.mainloop()
        finally:
            ##################################
            #       Terminar vídeo
            ##################################
            print("A terminar vídeo...")
            msg = Mensagem(conteudo=video).serialize()
            parar_video = (melhor_vizinho, STOP_VIDEOS_PORT)

            if not enviar_com_retries(sckt, msg, parar_video):
                print("Não foi possível enviar mensagem de término do vídeo.")

            sys.exit(0)
    else:
        print("O vídeo não existe!!")
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    main()