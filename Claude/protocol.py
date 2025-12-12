"""
protocol.py - Definição do Protocolo de Controlo OTT Overlay
"""

import json
from enum import Enum
from typing import Dict, List, Any

class MessageType(Enum):
    """Tipos de mensagens do protocolo"""
    # Fase 1: Descoberta e manutenção
    REGISTER = "REGISTER"           # Nó regista-se no bootstrapper
    NEIGHBOR_LIST = "NEIGHBOR_LIST" # Bootstrapper responde com vizinhos
    HELLO = "HELLO"                 # Keepalive entre vizinhos
    
    # Fase 2: Construção de rotas
    ANNOUNCE = "ANNOUNCE"           # Servidor anuncia stream
    ACTIVATE = "ACTIVATE"           # Cliente pede ativação de rota
    DEACTIVATE = "DEACTIVATE"       # Cliente pede desativação de rota
    
    # Fase 3: Streaming
    STREAM_DATA = "STREAM_DATA"     # Dados multimédia
    
    # Fase 4: Testes e debug
    PING = "PING"                   # Teste de caminho
    PONG = "PONG"                   # Resposta ao ping

    LATENCY_PING = "LATENCY_PING"   # Ping para medir RTT 
    LATENCY_PONG = "LATENCY_PONG"   # Pong para medir resposta ao PING

class Message:
    """Classe base para mensagens do protocolo"""
    
    def __init__(self, msg_type: MessageType, **kwargs):
        self.type = msg_type.value
        self.data = kwargs
    
    def to_json(self) -> str:
        """Serializa mensagem para JSON"""
        msg = {"type": self.type}
        msg.update(self.data)
        return json.dumps(msg)
    
    @staticmethod
    def from_json(json_str: str) -> 'Message':
        """Deserializa JSON para Message"""
        data = json.loads(json_str)
        msg_type = MessageType(data.pop("type"))
        return Message(msg_type, **data)
    
    def __str__(self):
        return f"Message({self.type}, {self.data})"

# ============================================================
# NOVAS MENSAGENS DE LATÊNCIA
# ============================================================

def create_latency_ping_message(from_node: str, ping_id: str, 
                                timestamp: float) -> Message:
    """
    LATENCY_PING - Ping para medir RTT entre vizinhos
    
    Diferente do PING de teste de caminho!
    Este é apenas para medir latência ponto-a-ponto.
    
    Enviado por: Qualquer nó
    Destinatário: Vizinho direto
    Periodicidade: A cada X segundos (ex: 2s)
    
    Exemplo:
    {
        "type": "LATENCY_PING",
        "from_node": "n10",
        "ping_id": "abc123",
        "timestamp": 1234567890.123
    }
    """
    return Message(
        MessageType.LATENCY_PING,
        from_node=from_node,
        ping_id=ping_id,
        timestamp=timestamp
    )


def create_latency_pong_message(from_node: str, ping_id: str, 
                                original_timestamp: float) -> Message:
    """
    LATENCY_PONG - Resposta ao LATENCY_PING
    
    Enviado por: Vizinho que recebeu o PING
    Destinatário: Quem enviou o PING
    
    Exemplo:
    {
        "type": "LATENCY_PONG",
        "from_node": "n16",
        "ping_id": "abc123",
        "original_timestamp": 1234567890.123
    }
    """
    return Message(
        MessageType.LATENCY_PONG,
        from_node=from_node,
        ping_id=ping_id,
        original_timestamp=original_timestamp
    )



# ============================================================
# MENSAGENS DE DESCOBERTA E REGISTO
# ============================================================

def create_register_message(node_id: str, ip: str, port: int) -> Message:
    """
    REGISTER - Nó regista-se no bootstrapper
    
    Enviado por: Nó overlay ao arrancar
    Destinatário: Bootstrapper (n16)
    
    Exemplo:
    {
        "type": "REGISTER",
        "node_id": "n10",
        "ip": "10.0.16.1",
        "port": 5000
    }
    """
    return Message(
        MessageType.REGISTER,
        node_id=node_id,
        ip=ip,
        port=port
    )


def create_neighbor_list_message(neighbors: List[Dict[str, Any]]) -> Message:
    """
    NEIGHBOR_LIST - Bootstrapper envia lista de vizinhos
    
    Enviado por: Bootstrapper
    Destinatário: Nó que se registou
    
    Exemplo:
    {
        "type": "NEIGHBOR_LIST",
        "neighbors": [
            {"id": "O1", "node_id": "n16", "ip": "10.0.16.10", "port": 5000},
            {"id": "O5", "node_id": "n5", "ip": "10.0.20.1", "port": 5000}
        ]
    }
    """
    return Message(
        MessageType.NEIGHBOR_LIST,
        neighbors=neighbors
    )


def create_hello_message(from_node: str, timestamp: float = None) -> Message:
    """
    HELLO - Keepalive entre vizinhos
    
    Enviado por: Qualquer nó overlay
    Destinatário: Vizinhos diretos
    Periodicidade: A cada X segundos (ex: 10s)
    
    Exemplo:
    {
        "type": "HELLO",
        "from": "n10",
        "timestamp": 1234567890.123
    }
    """
    import time
    return Message(
        MessageType.HELLO,
        from_node=from_node,
        timestamp=timestamp or time.time()
    )


# ============================================================
# MENSAGENS DE CONSTRUÇÃO DE ROTAS
# ============================================================

def create_announce_message(
    flow_id: str,
    from_node: str,
    metric: float,
    msg_id: str = None
) -> Message:
    """
    ANNOUNCE - Servidor anuncia disponibilidade de stream
    
    MUDANÇA: metric agora é float (latência em ms) em vez de int (saltos)
    
    Exemplo com latência (novo):
    {
        "type": "ANNOUNCE",
        "flow_id": "stream1",
        "from_node": "n16",
        "metric": 45.3,  # 45.3ms de latência acumulada
        "msg_id": "announce_123456"
    }
    """
    import uuid
    return Message(
        MessageType.ANNOUNCE,
        flow_id=flow_id,
        from_node=from_node,
        metric=metric,
        msg_id=msg_id or str(uuid.uuid4())
    )


def create_activate_message(
    flow_id: str,
    from_node: str,
    to_node: str
) -> Message:
    """
    ACTIVATE - Cliente pede ativação de rota
    
    Enviado por: Cliente que quer receber stream
    Direção: Segue o caminho inverso da melhor rota
    Efeito: Cada nó intermédio ativa a rota (adiciona destino)
    
    Exemplo:
    Cliente n17 quer stream1:
    {
        "type": "ACTIVATE",
        "flow_id": "stream1",
        "from_node": "n17",
        "to_node": "n16"
    }
    """
    return Message(
        MessageType.ACTIVATE,
        flow_id=flow_id,
        from_node=from_node,
        to_node=to_node
    )

def create_deactivate_message(
    flow_id: str,
    from_node: str,
    to_node: str
) -> Message:
    """
    DEACTIVATE - Cliente pára de receber stream
    
    Enviado por: Cliente que quer parar de receber
    Direção: Segue o caminho inverso do ACTIVATE
    Efeito: Cada nó removes o cliente dos destinos
    
    Exemplo:
    Cliente n25 quer parar stream1:
    {
        "type": "DEACTIVATE",
        "flow_id": "stream1",
        "from_node": "n25",
        "to_node": "n16"
    }
    """
    return Message(
        MessageType.DEACTIVATE,
        flow_id=flow_id,
        from_node=from_node,
        to_node=to_node
    )


# ============================================================
# MENSAGENS DE STREAMING
# ============================================================

def create_stream_data_message(
    flow_id: str,
    sequence: int,
    data: bytes,
    timestamp: float = None
) -> bytes:
    """
    STREAM_DATA - Pacote de dados multimédia
    
    Enviado por: Servidor (n16)
    Destino: Todos os clientes com rota ativa
    Reencaminhamento: Cada nó replica para os destinos na tabela
    
    NOTA: Para eficiência, usa formato binário:
    [HEADER (JSON)] + [DATA (bytes)]
    
    Header:
    {
        "type": "STREAM_DATA",
        "flow_id": "stream1",
        "sequence": 42,
        "timestamp": 1234567890.123,
        "data_length": 1024
    }
    """
    import time
    header = {
        "type": MessageType.STREAM_DATA.value,
        "flow_id": flow_id,
        "sequence": sequence,
        "timestamp": timestamp or time.time(),
        "data_length": len(data)
    }
    
    # Formato: [tamanho_header (4 bytes)][header JSON][data]
    header_json = json.dumps(header).encode('utf-8')
    header_length = len(header_json).to_bytes(4, byteorder='big')
    
    return header_length + header_json + data


def parse_stream_data_message(raw_bytes: bytes) -> tuple:
    """
    Faz parse de mensagem STREAM_DATA
    
    Returns:
        (header_dict, data_bytes)
    """
    # Lê tamanho do header (primeiros 4 bytes)
    header_length = int.from_bytes(raw_bytes[:4], byteorder='big')
    
    # Extrai header JSON
    header_json = raw_bytes[4:4+header_length]
    header = json.loads(header_json.decode('utf-8'))
    
    # Extrai dados
    data = raw_bytes[4+header_length:]
    
    return header, data


# ============================================================
# MENSAGENS DE TESTE E DEBUG
# ============================================================

def create_ping_message(from_node: str, path: List[str] = None) -> Message:
    """
    PING - Teste de caminho overlay
    
    Enviado por: Qualquer nó (normalmente servidor)
    Comportamento: Cada nó adiciona-se ao path e reencaminha
    
    Exemplo:
    {
        "type": "PING",
        "from_node": "n16",
        "path": ["n16", "n10", "n5", "n2"]
    }
    """
    return Message(
        MessageType.PING,
        from_node=from_node,
        path=path or []
    )


def create_pong_message(from_node: str, to_node: str, path: List[str]) -> Message:
    """
    PONG - Resposta ao PING
    
    Enviado por: Nó destino do PING
    Conteúdo: Caminho completo percorrido
    """
    return Message(
        MessageType.PONG,
        from_node=from_node,
        to_node=to_node,
        path=path
    )


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def send_message(sock, message: Message):
    """Envia mensagem através de um socket"""
    data = message.to_json().encode('utf-8')
    # Formato: [tamanho (4 bytes)][mensagem JSON]
    length = len(data).to_bytes(4, byteorder='big')
    sock.sendall(length + data)


def send_raw_bytes(sock, data: bytes):
    """Envia bytes raw (para STREAM_DATA)"""
    sock.sendall(data)


def receive_message(sock) -> Message:
    """Recebe e faz parse de mensagem"""
    # Lê tamanho (4 bytes)
    length_bytes = sock.recv(4)
    if not length_bytes:
        return None
    
    length = int.from_bytes(length_bytes, byteorder='big')
    
    # Lê mensagem completa
    data = b''
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            return None
        data += chunk
    
    return Message.from_json(data.decode('utf-8'))


def receive_raw_bytes(sock, length: int) -> bytes:
    """Recebe exatamente N bytes"""
    data = b''
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            return None
        data += chunk
    return data


# ============================================================
# EXEMPLO DE USO
# ============================================================

if __name__ == "__main__":
    print("=== Exemplos de Mensagens do Protocolo ===\n")
    
    # 1. REGISTER
    msg = create_register_message("n10", "10.0.16.1", 5000)
    print("1. REGISTER:")
    print(f"   {msg.to_json()}\n")
    
    # 2. NEIGHBOR_LIST
    neighbors = [
        {"id": "O1", "node_id": "n16", "ip": "10.0.16.10", "port": 5000},
        {"id": "O5", "node_id": "n5", "ip": "10.0.20.1", "port": 5000}
    ]
    msg = create_neighbor_list_message(neighbors)
    print("2. NEIGHBOR_LIST:")
    print(f"   {msg.to_json()}\n")
    
    # 3. ANNOUNCE
    msg = create_announce_message("stream1", "n16", 0)
    print("3. ANNOUNCE:")
    print(f"   {msg.to_json()}\n")
    
    # 4. ACTIVATE
    msg = create_activate_message("stream1", "n17", "n16")
    print("4. ACTIVATE:")
    print(f"   {msg.to_json()}\n")
    
    # 5. STREAM_DATA
    data = b"Frame data aqui..."
    stream_msg = create_stream_data_message("stream1", 42, data)
    print("5. STREAM_DATA:")
    print(f"   Tamanho total: {len(stream_msg)} bytes")
    header, payload = parse_stream_data_message(stream_msg)
    print(f"   Header: {header}")
    print(f"   Data: {payload[:20]}... (truncated)\n")
    
    # 6. PING
    msg = create_ping_message("n16", ["n16", "n10", "n5"])
    print("6. PING:")
    print(f"   {msg.to_json()}\n")