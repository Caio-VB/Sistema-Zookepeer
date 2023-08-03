# IMPORTAÇÕES NESCESSÁRIAS
import socket, threading, json, time

# DEFININDO A CLASSE MENSSAGEM
class Message:
    def __init__(self, command, timestamp, key, value, host, port):
        self.command = command
        self.timestamp = timestamp
        self.key = key
        self.value = value
        self.host = host
        self.port = port

    # MÉTODO PARA TRANSFORMAR O OBJETO MENSSAGEM EM JSON
    def to_json(self):
        return json.dumps({
            "command": self.command,
            "timestamp": self.timestamp,
            "key": self.key,
            "value": self.value,
            "host": self.host,
            "port": self.port
        })

    # MÉTODO PARA O JSON EM OBJETO MENSSAGEM
    @staticmethod
    def from_json(json_str):
        data = json.loads(json_str)
        return Message(data["command"], data["timestamp"], data["key"], data["value"], data.get("host", ""), data.get("port", ""))


# DEFININDO A CLASSE SERVER
class Server:
    def __init__(self):
        self.timestamp = 0
        self.messages = []
        self.host = ""
        self.port = 0
        self.host_leader = ""
        self.port_leader = 0
        self.host_follower1 = ""
        self.port_follower1 = 0
        self.host_follower2 = ""
        self.port_follower2 = 0

    # MÉTODO PARA ATUALIZAR A TABELA DOS SEGUDORES COM A CHAVE E VALOR VINDA DO REPLICATION
    def update(self, connection, req):
        self.timestamp = self.timestamp + 1
        # SALVANDO OU ATUALIZANDO CHAVE E VALOR
        update = False
        for i, message in enumerate(self.messages):
            parts = message.split(":")
            if parts[1] == req.key:
                update = True
                parts[2] = req.value
                parts[0] = str(req.timestamp)
                self.messages[i] = ":".join(parts)
                break
        if not update:
            self.messages.append(f"{req.timestamp}:{req.key}:{req.value}")

        print(f"REPLICATION key:{req.key} value:{req.value} ts:{req.timestamp}")

        # RESPONDENDO PARA O LÍDER COM O REPLICATION_OK
        res = Message("REPLICATION_OK", "", "", "", "", "").to_json()
        connection.send(res.encode())
        connection.close()

    # MÉTODO PARA ENVIAR O REPLICATION AOS SERVIDORES SEGUIDORES
    def replication(self, host, port, replication):
        # ADICIONANDO DELAY DE 10 SEGUNDOS PARA SIMULAR A LATENCIA E RETORNAR TRY_OTHER_SERVER_OR_LATER
        time.sleep(25)
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.connect((host, port))
        connection.sendall(replication.encode("utf-8"))
        res = Message.from_json(connection.recv(1024).decode("utf-8"))
        connection.close() 
        return res

    # MÉTODO PARA REALIZAR O PUT
    def put(self, connection, req, addr):
        # SE FOR O LÍDER
        if self.host == self.host_leader and self.port == self.port_leader:
            # ATUALIZANDO O TIMESTAMP
            self.timestamp = self.timestamp + 1

            # SALVANDO OU ATUALIZANDO CHAVE E VALOR
            update = False
            for i, message in enumerate(self.messages):
                parts = message.split(":")
                if parts[1] == req.key:
                    update = True
                    parts[2] = req.value
                    parts[0] = str(self.timestamp)
                    self.messages[i] = ":".join(parts)
                    break
            if not update:
                self.messages.append(f"{self.timestamp}:{req.key}:{req.value}")

            print(f"Cliente {addr[0]}:{addr[1]} PUT key:{req.key} value:{req.value}")

            # CRIANDO A REQUISIÇÃO DE REPLICAÇÃO
            replication = Message("REPLICATION", self.timestamp, req.key, req.value, "", "").to_json()

            # REPLICANDO PARA OS SERVIDORES SEGUIDORES
            res_follower1 = self.replication(self.host_follower1, self.port_follower1, replication)
            res_follower2 = self.replication(self.host_follower2, self.port_follower2, replication)

            # RESPONDENDO PARA O CLIENTE
            if res_follower1.command == "REPLICATION_OK" and res_follower2.command == "REPLICATION_OK":
                print(f"Enviando PUT_OK ao Cliente {addr[0]}:{addr[1]} da key:{req.key} ts:{self.timestamp}")
                res = Message("PUT_OK", self.timestamp, "", "", self.host_leader, self.port_leader).to_json()
            else:
                res = Message("Não foi possível fazer o PUT, tente mais tarde.", "", "", "", "", "").to_json()
            connection.send(res.encode())
            connection.close()

        # SE NÃO FOR O LÍDER   
        else:
            # CRIANDO REPASSE
            pass_along = Message("PUT", "", req.key, req.value, "", "").to_json()
            print(f"Encaminhando PUT key:{req.key} value:{req.value}")

            # REPASSANDO PARA O LÍDER
            connection_leader = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connection_leader.connect((self.host_leader, self.port_leader))
            connection_leader.sendall(pass_along.encode("utf-8"))

            # RECEBENDO O PUT_OK DO LÍDER
            res = connection_leader.recv(1024).decode("utf-8")
            connection_leader.close()

            # REPASSANDO O PUT_OK PARA O CLIENTE
            connection.send(res.encode())
            connection.close()

    # MÉTODO PARA REALIZAR O GET
    def get(self, connection, req, addr):
        update = False
        # BUSCANDO A CHAVE NA TABELA DO SERVIDOR
        for i, message in enumerate(self.messages):
            parts = message.split(":")
            if parts[1] == req.key:
                update = True
                # COMPARANDO O TIMESTAMP DO CLIENTE COM O DO SERVIDOR
                if int(parts[0]) >= req.timestamp:
                    # ENVIANDO O GET_OK COM A CHAVE, VALOR E TIMESTAMP PARA O CLIENTE
                    res = Message("GET_OK", parts[0], parts[1], parts[2], "", "").to_json()
                    print(f"Cliente {addr[0]}:{addr[1]} GET key:{parts[1]} ts:{req.timestamp}. Meu ts é {parts[0]}, portanto devolvendo {parts[2]}")
                    connection.send(res.encode())
                else:
                    # ENVIANDO O TRY_OTHER_SERVER_OR_LATER PARA O CLIENTE
                    res = Message("TRY_OTHER_SERVER_OR_LATER", "", "", "", "", "").to_json()
                    print(f"Cliente {addr[0]}:{addr[1]} GET key:{parts[1]} ts:{req.timestamp}. Meu ts é {parts[0]}, portanto devolvendo TRY_OTHER_SERVER_OR_LATER")
                    connection.send(res.encode())
                break
        
        # ENVIANDO O NULL SE A CHAVE NÃO FOR ENCONTRADA
        if not update:
            res = Message("NULL", "", "", "", "", "").to_json()
            connection.send(res.encode())
        connection.close()

    # MÉTODO QUE DIRECIONA O SERVIÇO
    def thread(self, addr, connection):
        req = connection.recv(1024).decode()
        req = Message.from_json(req)

        if req.command == "PUT":
            self.put(connection, req, addr)
        elif req.command == "GET":
            self.get(connection, req, addr)
        elif req.command == "REPLICATION":
            self.update(connection, req)

    # MÉTODO QUE INICIA O SERVER
    def run(self):
        # PEDINDO INFORMAÇÕES DO ENDEREÇO DO SERVIDOR E DO SERVIDOR LÍDER
        self.host = input("Digite o IP desse servidor: ")
        self.port = int(input("Digite a porta desse servidor: "))
        self.host_leader = input("Digite o IP do líder: ")
        self.port_leader = int(input("Digite a porta do líder: "))

        # SE O ENDEREÇO DO SERVIDOR E DO SERVIDOR LÍDER FOREM IGUAIS, PEDE O ENDEREÇO DOS DEMAIS SERVIDORES
        if self.host == self.host_leader and self.port == self.port_leader:
            self.host_follower1 = input("Digite o IP do seguidor 1: ")
            self.port_follower1 = int(input("Digite a porta do seguidor 1: "))
            self.host_follower2 = input("Digite o IP do seguidor 2: ")
            self.port_follower2 = int(input("Digite a porta do seguidor 2: "))

        # INICIANDO O SOCKET PARA OUVIR OS CLIENTES
        connection_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection_socket.bind((self.host, self.port))
        connection_socket.listen(20)

        while True:
            # CONECTANDO COM O CLINETE
            connection, addr = connection_socket.accept()

            # INICIANDO UMA THREAD PARA LIDAR COM A REQUISIÇÃO
            client_thread = threading.Thread(target=self.thread, args=(addr, connection))
            client_thread.start()

server = Server()
server.run()
