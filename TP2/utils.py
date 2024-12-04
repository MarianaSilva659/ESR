import subprocess
import socket
import signal
import sys
import time
from Mensagem.mensagem import Mensagem

CHECK_VIZINHOS_PORT_VIZINHOS = 3008
IP_bootstrapper = "10.0.1.1"


def get_vizinhos_bootstrapper(msgPedido):
    max_retries=3
    timeout=5
    # Inicializa o socket UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", CHECK_VIZINHOS_PORT_VIZINHOS))  # Associa a uma porta local aleatória
    sock.settimeout(timeout)  # Define o timeout para receber dados

    pedido = Mensagem(conteudo=msgPedido)  # Mensagem de pedido vazia
    bootstrapper_address = (IP_bootstrapper, CHECK_VIZINHOS_PORT_VIZINHOS)

    tentativa = 0

    while tentativa < max_retries:
        try:
            # Envia o pedido ao bootstrapper
            sock.sendto(pedido.serialize(), bootstrapper_address)

            # Aguarda a primeira parte da resposta
            data, _ = sock.recvfrom(1024)  # Recebe até 1024 bytes
            primeira_mensagem = Mensagem.deserialize(data)

            if primeira_mensagem.total_parts == 1:
                # Mensagem única recebida
                #print(f"Mensagem completa recebida: {primeira_mensagem}")
                return primeira_mensagem.conteudo
            else:
                # Mensagem fragmentada
                print(f"Mensagem fragmentada detectada. Total de partes: {primeira_mensagem.total_parts}")
                partes = [primeira_mensagem.conteudo]

                # Recebe as partes restantes
                for _ in range(1, primeira_mensagem.total_parts):
                    data, _ = sock.recvfrom(1024)
                    mensagem = Mensagem.deserialize(data)
                    #print(f"Parte recebida: {len(partes) + 1}/{primeira_mensagem.total_parts}")
                    partes.append(mensagem.conteudo)

                # Reconstrói a mensagem completa
                conteudo_final = b"".join(partes)
                mensagem_final = Mensagem.deserialize(conteudo_final)
                print(f"Mensagem reconstruída: {mensagem_final}")
                sock.close()
                return mensagem_final.conteudo

        except socket.timeout:
            # Timeout: Nenhuma mensagem recebida
            tentativa += 1
            print(f"Nenhuma resposta recebida após {timeout} segundos. Reenviando pedido...")

    # Após o número máximo de tentativas, lança um erro
    print("Número máximo de tentativas atingido. Não foi possível obter uma resposta do bootstrapper.")
    sock.close()
    return None


def hostname():
    return socket.gethostname()

def change_terminal_title():
    sys.stdout.write(f"\033]0;{hostname()}\007")
    sys.stdout.flush()
