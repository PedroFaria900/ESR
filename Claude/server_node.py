"""
server_node.py - Servidor de Streaming + Bootstrapper (CORRIGIDO)

Funções:
1. Bootstrapper: Regista nós e fornece lista de vizinhos
2. Servidor de streaming: Envia ANNOUNCEs e dados multimédia
"""

import socket
import threading
import json
import time
import sys
import cv2
from typing import Dict, List

from protocol import *
from routing_table import RoutingTable
from latency_monitor import LatencyMonitor


class StreamingServer:
    """Servidor de streaming (também é bootstrapper)"""
    
    def __init__(self, node_id: str, my_ip: str, my_port: int, 
                 topology_file: str = 'topology_overlay.json'):
        self.node_id = node_id
        self.my_ip = my_ip
        self.my_port = my_port
        
        # Carrega topologia completa (para bootstrapper)
        with open(topology_file, 'r') as f:
            self.topology = json.load(f)
        
        # Estruturas de dados
        self.registered_nodes: Dict[str, Dict] = {}  # Nós que se registaram
        self.neighbors: Dict[str, socket.socket] = {}  # Vizinhos conectados

        self.routing_table = RoutingTable(
            node_id=node_id,
            hysteresis_threshold=0.15,  # 15% de melhoria mínima
            jitter_tolerance=5.0        # 5ms de tolerância
        )

        self.routing_table = RoutingTable(node_id)
        
        # Monitor de Latência
        self.latency_monitor = LatencyMonitor(
            node_id=node_id,
            ping_interval=2.0,
            window_size=5,
            timeout=5.0
        )

        # Socket UDP de envio
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Streaming
        self.flow_id = self.topology['config']['stream_id']
        self.streaming = False
        
        # Controlo
        self.running = False
        self.server_socket = None
        
        print(f"[{self.node_id}] Servidor criado")
        print(f"[{self.node_id}] IP: {self.my_ip}:{self.my_port}")
        print(f"[{self.node_id}] Flow ID: {self.flow_id}")
    
    def start(self):
        """Inicia servidor"""
        self.running = True

        self.latency_monitor.start()

        # === ADICIONAR ISTO: Inicializa a rota local ===
        self.routing_table.update_route(
            flow_id=self.flow_id,
            origin=self.node_id,
            metric=0.0,
            via_neighbor=self.node_id
        )
        # ===============================================

        # 1. Servidor (bootstrapper + streaming)
        threading.Thread(target=self._server_thread, daemon=True).start()
        time.sleep(1.0)  # Aguarda servidor arrancar
        
        print(f"[{self.node_id}] Aguardando 10 segundos para vizinhos iniciarem...")
        time.sleep(10)  # ✅ AGUARDA os vizinhos estarem prontos
        
        # 2. Conecta aos vizinhos diretos (da topologia)
        self._connect_to_neighbors()
        
        # 3. Thread de ANNOUNCEs
        threading.Thread(target=self._announce_thread, daemon=True).start()
        
        threading.Thread(target=self._latency_ping_thread, daemon=True).start()

        # 4. Thread de streaming (inicia depois)
        threading.Thread(target=self._streaming_thread, daemon=True).start()
        
        # 5. Thread de estatísticas
        threading.Thread(target=self._stats_thread, daemon=True).start()
        
        print(f"[{self.node_id}] Servidor iniciado!")
    
    def stop(self):
        """Para servidor"""
        print(f"[{self.node_id}] A parar...")
        self.running = False
        self.streaming = False
        
        for sock in self.neighbors.values():
            try:
                sock.close()
            except:
                pass
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        print(f"[{self.node_id}] Parado.")
    
    # ========================================================
    # SERVIDOR - Escuta registos e conexões
    # ========================================================
    
    def _server_thread(self):
        """Thread do servidor"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.my_ip, self.my_port))
            self.server_socket.listen(10)
            print(f"[{self.node_id}] Bootstrapper a escutar em {self.my_ip}:{self.my_port}")
            
            while self.running:
                try:
                    self.server_socket.settimeout(1.0)
                    conn, addr = self.server_socket.accept()
                    print(f"[{self.node_id}] Conexão de {addr}")
                    
                    threading.Thread(
                        target=self._handle_connection,
                        args=(conn, addr),
                        daemon=True
                    ).start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        print(f"[{self.node_id}] Erro: {e}")
                    break
        except Exception as e:
            print(f"[{self.node_id}] ERRO ao iniciar servidor: {e}")
    
    def _handle_connection(self, conn: socket.socket, addr):
        """Trata conexão mantendo vizinhos ativos"""
        node_id = None
        try:
            msg = receive_message(conn)
            if not msg:
                return
            
            msg_type = MessageType(msg.type)
            
            if msg_type == MessageType.REGISTER:
                node_id = msg.data['node_id']
                self._handle_register(msg, conn)
                
                # Lógica de Persistência: Se é vizinho, mantém conectado
                if self.node_id in self.topology['nodes']:
                    my_neighbors = self.topology['nodes'][self.node_id].get('neighbors', [])
                    if node_id in my_neighbors:
                        print(f"[{self.node_id}] Mantendo conexão com vizinho {node_id}")
                        self.neighbors[node_id] = conn
                        # Passa a escutar
                        threading.Thread(
                            target=self._listen_to_neighbor, 
                            args=(node_id, conn), 
                            daemon=True
                        ).start()
                        return # NÃO FECHA O SOCKET

            else:
                self._process_message(msg, conn, None)
        
        except Exception as e:
            print(f"[{self.node_id}] Erro ao processar: {e}")
        
        # Só fecha se não foi guardado como vizinho
        if node_id is None or node_id not in self.neighbors:
            try:
                conn.close()
            except:
                pass
    
    # ========================================================
    # BOOTSTRAPPER - Registo de nós
    # ========================================================
    
    def _handle_register(self, msg: Message, conn: socket.socket):
        """
        Processa REGISTER
        
        1. Regista nó
        2. Envia lista de vizinhos (da topologia)
        """
        node_id = msg.data['node_id']
        ip = msg.data['ip']
        port = msg.data['port']
        
        print(f"[{self.node_id}] REGISTER de {node_id} ({ip}:{port})")
        
        # Regista nó
        self.registered_nodes[node_id] = {
            'node_id': node_id,
            'ip': ip,
            'port': port,
            'timestamp': time.time()
        }
        
        # Obtém vizinhos da topologia
        if node_id in self.topology['nodes']:
            node_config = self.topology['nodes'][node_id]
            neighbor_ids = node_config['neighbors']
            
            # Prepara lista de vizinhos
            neighbors = []
            for neighbor_id in neighbor_ids:
                if neighbor_id in self.topology['nodes']:
                    neighbor_config = self.topology['nodes'][neighbor_id]
                    neighbors.append({
                        'id': neighbor_config['id'],
                        'node_id': neighbor_id,
                        'ip': neighbor_config['ip'],
                        'port': neighbor_config['port']
                    })
            
            # Envia NEIGHBOR_LIST
            response = create_neighbor_list_message(neighbors)
            send_message(conn, response)
            
            print(f"[{self.node_id}]   → Enviados {len(neighbors)} vizinhos para {node_id}")
        else:
            print(f"[{self.node_id}]   ! {node_id} não encontrado na topologia!")
    
    # ========================================================
    # CONEXÕES AOS VIZINHOS
    # ========================================================
    
    def _connect_to_neighbors(self):
        """Conecta aos vizinhos diretos do servidor"""
        if self.node_id not in self.topology['nodes']:
            return
        
        my_config = self.topology['nodes'][self.node_id]
        neighbor_ids = my_config.get('neighbors', [])
        
        print(f"[{self.node_id}] A conectar a {len(neighbor_ids)} vizinhos...")
        
        for neighbor_id in neighbor_ids:
            if neighbor_id not in self.topology['nodes']:
                continue
            
            neighbor = self.topology['nodes'][neighbor_id]
            
            # Aguarda um pouco (para vizinho arrancar servidor)
            time.sleep(2)
            
            try:
                print(f"[{self.node_id}]   Conectando a {neighbor_id}...")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5.0)
                sock.connect((neighbor['ip'], neighbor['port']))
                sock.settimeout(None)
                
                self.neighbors[neighbor_id] = sock

                self.latency_monitor.add_neighbor(neighbor_id)

                print(f"[{self.node_id}]   ✓ Conectado a {neighbor_id}")
                
                # Thread para escutar
                threading.Thread(
                    target=self._listen_to_neighbor,
                    args=(neighbor_id, sock),
                    daemon=True
                ).start()
                
            except Exception as e:
                print(f"[{self.node_id}]   ✗ Erro ao conectar a {neighbor_id}: {e}")
    
    def _listen_to_neighbor(self, neighbor_id: str, sock: socket.socket):
        """Escuta mensagens de vizinho"""
        
        # Envia HELLO ao conectar
        try:
            msg = create_hello_message(self.node_id)
            send_message(sock, msg)
            print(f"[{self.node_id}] → HELLO enviado para {neighbor_id}")
        except Exception as e:
            print(f"[{self.node_id}] Erro ao enviar HELLO: {e}")
            if neighbor_id in self.neighbors:
                del self.neighbors[neighbor_id]
            sock.close()
            return
        
        time.sleep(2.0)
        
        while self.running:
            try:
                msg = receive_message(sock)
                if not msg:
                    break
                
                self._process_message(msg, sock, neighbor_id)
                
            except Exception as e:
                print(f"[{self.node_id}] Conexão perdida com {neighbor_id}: {e}")
                break
        
        if neighbor_id in self.neighbors:
            del self.neighbors[neighbor_id]
            self.latency_monitor.remove_neighbor(neighbor_id)
            print(f"[{self.node_id}] Vizinho {neighbor_id} desconectado")
        
        try:
            sock.close()
        except:
            pass
    
    def _process_message(self, msg: Message, conn: socket.socket, 
                        from_neighbor: str = None):
        """Processa mensagens"""
        msg_type = MessageType(msg.type)
        
        if msg_type == MessageType.ACTIVATE:
            self._handle_activate(msg, from_neighbor)

        elif msg_type == MessageType.DEACTIVATE: 
            self._handle_deactivate(msg, from_neighbor)
        
        elif msg_type == MessageType.LATENCY_PING:
            self._handle_latency_ping(msg, conn, from_neighbor)
        
        elif msg_type == MessageType.LATENCY_PONG:
            self._handle_latency_pong(msg, from_neighbor)

        elif msg_type == MessageType.HELLO:
            pass  # Keepalive
        
        else:
            print(f"[{self.node_id}] Mensagem: {msg_type}")
    
    def _handle_activate(self, msg: Message, from_neighbor: str):
        """
        Processa ACTIVATE
        
        Quando chega ao servidor, ativa a rota para começar streaming
        """
        flow_id = msg.data['flow_id']
        from_node = msg.data['from_node']
        
        print(f"[{self.node_id}] ACTIVATE recebido: {flow_id} de {from_node} "
              f"via {from_neighbor}")
        
        # Ativa rota (adiciona from_neighbor como destino)
        self.routing_table.activate_route(flow_id, from_neighbor)
        
        # Inicia streaming (se ainda não estiver ativo)
        if not self.streaming:
            self.streaming = True
            print(f"[{self.node_id}] *** STREAMING ATIVADO ***")

    def _handle_deactivate(self, msg: Message, from_neighbor: str):
        """
        Processa DEACTIVATE no servidor
        
        Quando um cliente pára, remove-o dos destinos.
        Se nenhum cliente estiver interessado, desativa o streaming.
        """
        flow_id = msg.data['flow_id']
        from_node = msg.data['from_node']
        
        print(f"[{self.node_id}] DEACTIVATE recebido: {flow_id} de {from_node} "
            f"via {from_neighbor}")
        
        # Remove este cliente dos destinos
        removed = self.routing_table.deactivate_route(flow_id, from_neighbor)
        
        if removed:
            print(f"[{self.node_id}]   ✓ Removido {from_neighbor} dos destinos")
        
        # Verifica se ainda há destinos ativos
        destinations = self.routing_table.get_destinations(flow_id)
        
        if not destinations:
            self.streaming = False
            print(f"[{self.node_id}]   SEM DESTINOS - STREAMING DESATIVADO")
        else:
            print(f"[{self.node_id}]   Ainda há {len(destinations)} destino(s) ativo(s)")
    
    def _handle_latency_ping(self, msg: Message, conn: socket.socket, 
                            from_neighbor: str):
        """Responde a LATENCY_PING"""
        ping_id = msg.data['ping_id']
        original_timestamp = msg.data['timestamp']
        
        try:
            pong = create_latency_pong_message(
                from_node=self.node_id,
                ping_id=ping_id,
                original_timestamp=original_timestamp
            )
            send_message(conn, pong)
        except:
            pass
    
    def _handle_latency_pong(self, msg: Message, from_neighbor: str):
        """Processa LATENCY_PONG"""
        ping_id = msg.data['ping_id']
        original_timestamp = msg.data['original_timestamp']
        
        rtt_ms = self.latency_monitor.process_pong(
            from_neighbor, ping_id, original_timestamp
        )

    # ========================================================
    # ANÚNCIOS PERIÓDICOS
    # ========================================================
    
    def _announce_thread(self):
        """Envia ANNOUNCEs periodicamente"""
        interval = self.topology['config']['route_announce_interval']
        
        print(f"[{self.node_id}] A enviar ANNOUNCEs a cada {interval}s")
        
        # Aguarda um pouco antes de começar
        time.sleep(10.0)
        
        while self.running:
            # Cria ANNOUNCE
            msg = create_announce_message(self.flow_id, self.node_id, 0.0)
            
            # Envia a todos os vizinhos
            sent_count = 0
            for neighbor_id, sock in list(self.neighbors.items()):
                try:
                    send_message(sock, msg)
                    sent_count += 1
                except Exception as e:
                    print(f"[{self.node_id}] Erro ao enviar ANNOUNCE para {neighbor_id}: {e}")
            
            if sent_count > 0:
                print(f"[{self.node_id}] ANNOUNCE enviado para {sent_count} vizinho(s)")
            
            time.sleep(interval)
    
    # ========================================================
    # STREAMING DE DADOS
    # ========================================================
    
    def _streaming_thread(self):
        """Envia dados de streaming via UDP"""
        print(f"[{self.node_id}] Thread de streaming UDP iniciada...")

        cap = cv2.VideoCapture('movie.Mjpeg') 
        
        if not cap.isOpened():
            print(f"[{self.node_id}] ERRO CRÍTICO: Não encontrei 'movie.Mjpeg'!")
            return

        sequence = 0

        while self.running:
            # Pausa se não houver clientes ativos
            if not self.streaming:
                time.sleep(0.5)
                continue
            
            destinations = self.routing_table.get_destinations(self.flow_id)
            if not destinations:
                time.sleep(0.5)
                continue

            # 1. Lê Frame do ficheiro
            ret, frame = cap.read()
            if not ret:
                # Fim do vídeo -> Reinicia (Loop infinito)
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                continue

            # 2. Reduz resolução (CRÍTICO para UDP!)
            # O UDP tem limite de ~65KB. 320x240 gera frames de 15-20KB, o que é seguro.
            frame = cv2.resize(frame, (320, 240))

            # 3. Comprime para JPEG
            # Qualidade 50 é suficiente para testes e poupa banda
            _, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 50])
            jpg_bytes = buffer.tobytes()
            
            # Segurança: Se o frame for gigante, salta-o para não partir o UDP
            if len(jpg_bytes) > 60000:
                print(f"[{self.node_id}] Frame muito grande ({len(jpg_bytes)} bytes), ignorado.")
                continue

            # 4. Cria Pacote (Usa a função existente)
            # [Tamanho 4B] + [Header JSON] + [Bytes JPEG]
            udp_packet = create_stream_data_message(self.flow_id, sequence, jpg_bytes)

            # 5. Envia via UDP para os vizinhos
            sent_count = 0
            for dest_id in destinations:
                if dest_id in self.neighbors:
                    try:
                        # Descobre IP do vizinho pelo socket TCP
                        tcp_sock = self.neighbors[dest_id]
                        dest_ip, _ = tcp_sock.getpeername()
                        
                        # Envia para a porta 5000 UDP
                        self.udp_socket.sendto(udp_packet, (dest_ip, 5000))
                        sent_count += 1
                    except Exception as e:
                        print(f"Erro UDP: {e}")

            if sequence % 20 == 0:
                print(f"[{self.node_id}] Enviado Frame {sequence} ({len(jpg_bytes)} bytes) para {sent_count} vizinhos")

            sequence += 1
            time.sleep(0.04) # ~25 FPS
    
    # ========================================================
    # Envia LATENCY_PING
    # ========================================================
    
    def _latency_ping_thread(self):
        """Thread que envia LATENCY_PING para medir RTT"""
        print(f"[{self.node_id}] Thread de latência iniciada")
        
        while self.running:
            time.sleep(2.0)
            
            self.latency_monitor.cleanup_timeouts()
            
            for neighbor_id, sock in list(self.neighbors.items()):
                try:
                    ping_id, timestamp = self.latency_monitor.create_ping_message(neighbor_id)
                    msg = create_latency_ping_message(
                        from_node=self.node_id,
                        ping_id=ping_id,
                        timestamp=timestamp
                    )
                    send_message(sock, msg)
                except:
                    pass

    # ========================================================
    # ESTATÍSTICAS
    # ========================================================
    
    def _stats_thread(self):
        """Imprime estatísticas"""
        while self.running:
            time.sleep(30)
            
            print(f"\n[{self.node_id}] === STATUS SERVIDOR ===")
            print(f"  Nós registados: {len(self.registered_nodes)}")
            print(f"  Vizinhos conectados: {list(self.neighbors.keys())}")
            print(f"  Streaming: {'SIM' if self.streaming else 'NÃO'}")
            print(f"  Rotas: {self.routing_table.get_stats()}")
            self.latency_monitor.print_stats()
            self.routing_table.print_table()


# ============================================================
# MAIN
# ============================================================

def main():
    if len(sys.argv) < 2:
        print("Uso: python server_node.py <node_id>")
        print("Exemplo: python server_node.py n16")
        sys.exit(1)
    
    node_id = sys.argv[1]
    
    # Carrega topologia
    try:
        with open('topology_overlay.json', 'r') as f:
            topology = json.load(f)
    except FileNotFoundError:
        print("ERRO: Ficheiro topology_overlay.json não encontrado!")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERRO: Ficheiro topology_overlay.json inválido: {e}")
        sys.exit(1)
    
    if node_id not in topology['nodes']:
        print(f"ERRO: Nó {node_id} não encontrado!")
        sys.exit(1)
    
    node_info = topology['nodes'][node_id]
    
    # Se passarmos um 3º argumento, usamos como ID da stream
    # Caso contrário, usa o do config.json
    if len(sys.argv) >= 3:
        custom_stream_id = sys.argv[2]
    else:
        custom_stream_id = topology['config']['stream_id']

    # Cria e inicia servidor
    server = StreamingServer(
        node_id=node_id,
        my_ip=node_info['ip'],
        my_port=node_info['port']
    )

    # FORÇAR O ID DA STREAM
    server.flow_id = custom_stream_id
    print(f"[{node_id}] A servir fluxo: {server.flow_id}") # Debug
    
    server.start()
    
    try:
        print(f"\n[{node_id}] Servidor a correr... (Ctrl+C para parar)")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{node_id}] Interrompido")
        server.stop()


if __name__ == "__main__":
    main()