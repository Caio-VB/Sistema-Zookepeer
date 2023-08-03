# IMPORTAÇÕES NESCESSÁRIAS
import socket, random, json

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


# DEFININDO A CLASSE CLIENTE
class Client:
    def __init__(self):
        self.timestamp = 0
        self.servers = []
        self.messages = []
        self.connection = None

    # MÉTODO PARA INICIALIZAR O CLIENTE INFORMANDO OS ENDEREÇOS DOS SERVIDORES
    def init(self):
        for i in range(1, 4):
            host = input(f"Digite o IP do servidor {str(i)}: ")
            port = input(f"Digite a porta do servidor {str(i)}: ")
            self.servers.append(f"{host}:{port}")

    # MÉTODO PARA REALIZAR UM PUT EM UM DOS SERVIDORES
    def put(self):
        key = input("Digite a chave a ser inserida: ")
        value = input("Digite o valor a ser inserido: ")

        # CRIANDO A MENSAGEM DE REQUISIÇÃO
        req = Message("PUT", "", key, value,  "", "").to_json()

        # CONECTANDO COM UM SERVIDOR ALEATÓRIO PARA O PUT
        server = random.choice(self.servers)
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connection.connect((server.split(":")[0], int(server.split(":")[1])))

        # ENVIANDO A MENSAGEM DE REQUISIÇÃO
        self.connection.sendall(req.encode("utf-8"))

        # RECEBENDO A MENSAGEM DE RESPOSTA
        res = Message.from_json(self.connection.recv(1024).decode("utf-8"))

        if res.command != "Não foi possível fazer o PUT, tente mais tarde.":
            # SALVANDO O PUT NA TABELA DO CLIENTE
            self.messages.append(f"{res.timestamp}:{key}:{value}")
            print(f"PUT_OK key: {key} value: {value} timestamp: {res.timestamp} realizada no servidor {server}")

        self.connection.close()

    # MÉTODO PARA REALIZAR UM GET EM UM DOS SERVIDORES
    def get(self):
        key = input("Digite a chave a ser obtida: ")

        # VERIFICANDO SE JÁ POSSUI ALGUM TIMESTAMP ASSOSSIADO A ESSA CHAVE
        for i, message in enumerate(self.messages):
            parts = message.split(":")
            if parts[1] == key:
                self.timestamp = int(parts[0])
                break
        
        # CRIANDO A MENSAGEM DE REQUISIÇÃO
        req = Message("GET", self.timestamp, key, "", "", "").to_json()

        # CONECTANDO COM UM SERVIDOR ALEATÓRIO PARA O GET
        server = random.choice(self.servers)
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connection.connect((server.split(":")[0], int(server.split(":")[1])))

        # ENVIANDO A MENSAGEM DE REQUISIÇÃO
        self.connection.sendall(req.encode("utf-8"))

        # RECEBENDO A MENSAGEM DE RESPOSTA
        res = Message.from_json(self.connection.recv(1024).decode("utf-8"))

        if res.command == "GET_OK":
            # SALVANDO OU ATUALIZANDO NA O GET TABELA DO CLIENTE
            update = False
            for i, message in enumerate(self.messages):
                parts = message.split(":")
                if parts[1] == res.key:
                    update = True
                    parts[0] = str(res.timestamp)
                    parts[2] = res.value
                    self.messages[i] = ":".join(parts)
                    break
            if not update:
                self.messages.append(f"{res.timestamp}:{res.key}:{res.value}")
            print(f"GET key: {res.key} value: {res.value} obtido do servidor {server}, meu timestamp {self.timestamp} e do servidor {res.timestamp}")
        elif res.command == "TRY_OTHER_SERVER_OR_LATER":
            print("TRY_OTHER_SERVER_OR_LATER")

        self.connection.close()

    # MÉTODO PARA EXIBIR O MENU
    def menu(self):
        print("Este é o menu interativo:")
        print("1. INIT")
        print("2. PUT")
        print("3. GET")

    # MÉTODO PARA EXECUTAR O CLIENT
    def run(self):
        while True:
            self.menu()
            opcao = input("Digite o número da opção desejada: ")
            # DIRECIONANDO PARA O SERVIÇO SOLICITADO
            if opcao == "1":
                self.init()
            elif opcao == "2":
                self.put()
            elif opcao == "3":
                self.get()


client = Client()
client.run()
