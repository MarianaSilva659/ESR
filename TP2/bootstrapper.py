import socket
import threading
import signal
import sys
import time
from DatabaseTables.database_bootstrapper import Database_bootstrapper
from Mensagem.mensagem import Mensagem
import signal
from functools import partial



# Porta de atendimento do bootstrapper de atendimento de vizinhos
CHECK_VIZINHOS_PORT_VIZINHOS = 3008
# Porta de atendimento do bootstrapper de pedido de IPIdentificador
CHECK_VIZINHOS_PORT_IPIDENTIFICADOR = 3009

MAX_BYTES_MESSAGE = 1024

interrompe_threads = False

def ctrlc_handler(db: Database_bootstrapper, sig, frame):
    print("A encerrar o servidor e as threads...")
    sys.exit(0)

def ctrl_slash_handler(sig, frame):
    print("A simular encerramento repentino...")
    sys.exit(0)

###################################################
#        INFORMAR NODOS DOS SEU VIZINHOS
###################################################

# Esta função ira receber e processar as mensagens enviadas para a porta de um determinado endereço
def comunicacao_socker_get_info(db: Database_bootstrapper):
    global interrompe_threads
    # Comunicação por UDP
    canal_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #  Sssocia o socket a um endereço IP específico e a uma porta
    canal_socket.bind(( "0.0.0.0", CHECK_VIZINHOS_PORT_VIZINHOS))
    
    while not interrompe_threads:
        try:
            dados, addr = canal_socket.recvfrom(1024)
            print(f"O bootstrapper receber um pedido do cliente -> {addr[0]}")
            threading.Thread(target=informar_vizinhos_ipIdentificador_servidores, args=(dados, canal_socket, addr, db)).start()
        
        except Exception as e:
            print(f"Erro no handler {informar_vizinhos_ipIdentificador_servidores.__name__} com o endereço 0.0.0.0:{CHECK_VIZINHOS_PORT_VIZINHOS}")
            print(e)
            break
    canal_socket.close()
 

def informar_vizinhos_ipIdentificador_servidores(dados, socket, addr_pedinte: tuple, db: Database_bootstrapper):
    # Cria a mensagem de resposta
    msg = Mensagem.deserialize(dados)
    # pedido pelo servidor e clientes
    if msg.conteudo == 1:
        respostaConteudo = {"vizinhos": db.get_vizinhos(addr_pedinte[0]), "ipIdentificador": db.get_IPIdentificador(addr_pedinte[0])}
        resposta = Mensagem(conteudo=respostaConteudo)
        print(f"Mensagem com IP Identificador {resposta}")
        dados_serializados = resposta.serialize()

        # Verifica se o tamanho é maior que o limite
        if len(dados_serializados) > MAX_BYTES_MESSAGE:
            print("A resposta é maior que 1024 bytes. Enviando em partes.")
            total_parts = (len(dados_serializados) // MAX_BYTES_MESSAGE) + 1
            for i in range(total_parts):
                start = i * MAX_BYTES_MESSAGE
                end = start + MAX_BYTES_MESSAGE
                part_data = dados_serializados[start:end]

                parte_mensagem = Mensagem(
                    conteudo=part_data,  
                    total_parts=total_parts 
                )
                socket.sendto(parte_mensagem.serialize(), addr_pedinte)
                print(f"Parte {i + 1}/{total_parts} enviada para {addr_pedinte}")

        else:
            socket.sendto(dados_serializados, addr_pedinte)
            print("A resposta foi enviada em uma única parte.\n")
    # pedido pelos nodos
    elif msg.conteudo == 2:
        respostaConteudo = {"vizinhos": db.get_vizinhos(addr_pedinte[0]), "ipIdentificador": db.get_IPIdentificador(addr_pedinte[0]), "servidores": db.get_servidores()}
        resposta = Mensagem(conteudo=respostaConteudo)
        print(f"Mensagem com IP Identificador {resposta}")
        dados_serializados = resposta.serialize()

        # Verifica se o tamanho é maior que o limite
        if len(dados_serializados) > MAX_BYTES_MESSAGE:
            print("A resposta é maior que 1024 bytes. Enviando em partes.")
            total_parts = (len(dados_serializados) // MAX_BYTES_MESSAGE) + 1
            for i in range(total_parts):
                start = i * MAX_BYTES_MESSAGE
                end = start + MAX_BYTES_MESSAGE
                part_data = dados_serializados[start:end]

                parte_mensagem = Mensagem(
                    conteudo=part_data,  
                    total_parts=total_parts 
                )
                socket.sendto(parte_mensagem.serialize(), addr_pedinte)
                print(f"Parte {i + 1}/{total_parts} enviada para {addr_pedinte}")

        else:
            socket.sendto(dados_serializados, addr_pedinte)
            print("A resposta foi enviada em uma única parte.\n")
    elif msg.conteudo == 3:
        respostaConteudo = {"vizinhos": db.get_vizinhos(addr_pedinte[0]), "ipIdentificador": db.get_IPIdentificador(addr_pedinte[0]), "servidores": db.get_servidores(), "access_points": db.get_PA()}
        resposta = Mensagem(conteudo=respostaConteudo)
        print(f"Mensagem com IP Identificador {resposta}")
        dados_serializados = resposta.serialize()

        # Verifica se o tamanho é maior que o limite
        if len(dados_serializados) > MAX_BYTES_MESSAGE:
            print("A resposta é maior que 1024 bytes. Enviando em partes.")
            total_parts = (len(dados_serializados) // MAX_BYTES_MESSAGE) + 1
            for i in range(total_parts):
                start = i * MAX_BYTES_MESSAGE
                end = start + MAX_BYTES_MESSAGE
                part_data = dados_serializados[start:end]

                parte_mensagem = Mensagem(
                    conteudo=part_data,  
                    total_parts=total_parts 
                )
                socket.sendto(parte_mensagem.serialize(), addr_pedinte)
                print(f"Parte {i + 1}/{total_parts} enviada para {addr_pedinte}")

        else:
            socket.sendto(dados_serializados, addr_pedinte)
            print("A resposta foi enviada em uma única parte.\n")
      
        

def main():
    print("Inicializando o Bootstrapper.")
    db = Database_bootstrapper()
    
    signal.signal(signal.SIGINT, partial(ctrlc_handler, db))
    signal.signal(signal.SIGQUIT, ctrl_slash_handler)
    
    if len(sys.argv) < 3:
        print(f"Uso: python3 {sys.argv[0]} <config_file.json>")
        sys.exit(1)
    
    
    config_file1 = sys.argv[1]
    config_file2 = sys.argv[2]
    config_file3 = sys.argv[3]
    print(f"Carregando arquivo de configuração: {config_file1}")
    db.load_database(config_file1)
    db.load_servidore(config_file2)
    db.load_PA(config_file3)
    print(db.servidores)
    print("Configuração carregada com sucesso\n")
    

    thread_notifica_clientes_vizinhos = threading.Thread(target=comunicacao_socker_get_info, args=(db,))
    
    # Iniciar as threads
    thread_notifica_clientes_vizinhos.start()
    
        
    # Aguardar pelas threads antes de sair
    thread_notifica_clientes_vizinhos.join()
    

if __name__ == "__main__":
    main()