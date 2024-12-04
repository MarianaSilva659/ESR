import subprocess
import socket
import signal
import sys
import time
from Mensagem.mensagem import Mensagem
port = 3008
bootstrapper_ip = "10.0.1.1"
def get_vizinhos_bootstrapper():
    max_retries=3
    timeout=5
    # Inicializa o socket UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", 0))  # Associa a uma porta local aleatória
    sock.settimeout(timeout)  # Define o timeout para receber dados

    pedido = Mensagem(conteudo='')  # Mensagem de pedido vazia
    bootstrapper_address = (bootstrapper_ip, port)

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
                return mensagem_final

        except socket.timeout:
            # Timeout: Nenhuma mensagem recebida
            tentativa += 1
            print(f"Nenhuma resposta recebida após {timeout} segundos. Reenviando pedido...")

    # Após o número máximo de tentativas, lança um erro
    print("Número máximo de tentativas atingido. Não foi possível obter uma resposta do bootstrapper.")
    return None



# obtém uma lista dos endereços IP IPv4 das interfaces de rede ativas na máquina onde o código está sendo executado
def get_ips():
    command = "ip -4 addr show | grep inet | awk '{print $2}' | cut -d'/' -f1"
    
    # subprocess.Popen -> para executar o comando no shell
    processo = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Chama processo.communicate() para obter a saída do comando em output e qualquer mensagem de erro em erro.
    output, erro = processo.communicate()
    
    if erro:
        print(f"Ocorreu um erro ao buscar os ips: {erro.decode('utf-8')}")
    
    # Tratamento do output
    lista = output.decode('utf-8').split('\n')
    
    ips = []
    for ip in lista:
        if ip and ip != '127.0.0.1':
            ips.append(ip)
            
    return ips

