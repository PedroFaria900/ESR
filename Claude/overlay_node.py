"""
overlay_node.py - Nó da Rede Overlay (VERSÃO FINAL CORRIGIDA)

Correções aplicadas:
1. Tolerância a Falhas TOTAL: A limpeza de rotas (process_neighbor_down) 
   agora ocorre dentro de _listen_to_connection, garantindo que funciona
   tanto para conexões recebidas como iniciadas.
2. Unificação de Processamento: Usa _process_message para tudo.
"""

import socket
import threading
import json
import time
import sys
from typing import Dict, List, Any

from protocol import *
from routing_table import RoutingTable
from latency_monitor import LatencyMonitor

class OverlayNode:
    """Nó da rede overlay (relay)"""
    
    def __init__(self, node_id: str, my_ip: str, my_port: int, 
                 bootstrap_ip: str, bootstrap_port: int):
        self.node_id = node_id
        self.my_ip = my_ip
        self.my_port = my_port
        self.bootstrap_ip = bootstrap_ip
        self.bootstrap_port = bootstrap_port
        
        # Estruturas de dados
        self.neighbors: Dict[str, socket.socket] = {}
        self.neighbor_info: Dict[str, Dict] = {}

        self.routing_table = RoutingTable(
            node_id=node_id,
            hysteresis_threshold=0.15,  # 15% de melhoria mínima
            jitter_tolerance=5.0        # 5ms de tolerância
        )

        self.routing_table = RoutingTable(node_id)

        # Monitor de Latência
        self.latency_monitor = LatencyMonitor(
            node_id=node_id,
            ping_interval=2.0,    # Ping a cada 2s
            window_size=5,       # Média de 10 amostras
            timeout=5.0           # Timeout de 5s
        )        

        # --- NOVO: Socket UDP para Dados ---
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Locks
        self.neighbors_lock = threading.Lock()
        # Vincula à mesma porta do TCP (ex: 5000)
        self.udp_socket.bind(('0.0.0.0', self.my_port))

        # Controlo
        self.running = False
        self.server_socket = None
        
        print(f"[{self.node_id}] Nó overlay criado")
        print(f"[{self.node_id}] IP: {self.my_ip}:{self.my_port}")
        print(f"[{self.node_id}] Canal UDP pronto em port {self.my_port}")
    
    def start(self):
        """Inicia o nó overlay"""
        self.running = True
        
        # Inicia monitor de latência
        self.latency_monitor.start()

        # Inicia thread UDP (NOVO)
        threading.Thread(target=self._udp_server_thread, daemon=True).start()

        # 1. Inicia servidor (escuta conexões)
        threading.Thread(target=self._server_thread, daemon=True).start()
        time.sleep(1)
        
        # 2. Carrega vizinhos esperados
        self._load_expected_neighbors()
        
        # 3. Regista-se no bootstrapper
        time.sleep(2)
        self._register_to_bootstrap()
        
        # 4. Threads de manutenção
        threading.Thread(target=self._keepalive_thread, daemon=True).start()
        threading.Thread(target=self._reconnect_thread, daemon=True).start()
        threading.Thread(target=self._latency_ping_thread, daemon=True).start()
        threading.Thread(target=self._stats_thread, daemon=True).start()
        
        print(f"[{self.node_id}] Nó iniciado!")
    
    def stop(self):
        """Para o nó"""
        print(f"[{self.node_id}] A parar...")
        self.running = False
        
        with self.neighbors_lock:
            for sock in list(self.neighbors.values()):
                try:
                    sock.close()
                except:
                    pass
        
        if self.server_socket:
            self.server_socket.close()
        
        print(f"[{self.node_id}] Parado.")
    
    # ========================================================
    # SERVIDOR - Escuta conexões de entrada
    # ========================================================
    
    def _server_thread(self):
        """Thread que aceita conexões"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind(('0.0.0.0', self.my_port))
            self.server_socket.listen(10)
            print(f"[{self.node_id}] Servidor a escutar em 0.0.0.0:{self.my_port}")
            
            while self.running:
                try:
                    self.server_socket.settimeout(1.0)
                    conn, addr = self.server_socket.accept()
                    
                    threading.Thread(
                        target=self._handle_incoming_connection,
                        args=(conn, addr),
                        daemon=True
                    ).start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        print(f"[{self.node_id}] Erro no servidor: {e}")
                    break
        except Exception as e:
            print(f"[{self.node_id}] ERRO ao iniciar servidor: {e}")
    
    def _handle_incoming_connection(self, conn: socket.socket, addr):
        """
        Trata handshake inicial de conexão de entrada.
        Se bem sucedido, passa para _listen_to_connection.
        """
        from_neighbor = None
        
        try:
            # 1. Handshake inicial (apenas lê a primeira mensagem)
            length_bytes = conn.recv(4)
            if not length_bytes or len(length_bytes) < 4:
                conn.close()
                return
                
            length = int.from_bytes(length_bytes, byteorder='big')
            header_data = self._recv_exact(conn, length)
            if not header_data:
                conn.close()
                return
            
            # Tenta identificar o vizinho (HELLO/ACTIVATE)
            try:
                msg_json = json.loads(header_data.decode('utf-8'))
                msg_type = MessageType(msg_json.get('type'))
                msg = Message(msg_type, **msg_json)

                if msg.type == MessageType.HELLO.value or msg.type == MessageType.ACTIVATE.value:
                    from_neighbor = msg.data.get('from_node') or msg.data.get('to_node')
                    
                    if from_neighbor:
                        print(f"[{self.node_id}] Nova conexão de {from_neighbor} ({addr})")
                        
                        # Adiciona vizinho
                        with self.neighbors_lock:
                            self.neighbors[from_neighbor] = conn
                        
                        # ADICIONAR AO MONITOR ==========
                        self.latency_monitor.add_neighbor(from_neighbor)

                        # Processa a primeira mensagem
                        self._process_message(msg, conn, addr, from_neighbor)

                        # Entra no loop principal de escuta (que gere a desconexão)
                        self._listen_to_connection(conn, from_neighbor)
                        
                    else:
                        print(f"[{self.node_id}] Erro: Mensagem inicial sem ID.")
                        conn.close()
                else:
                    print(f"[{self.node_id}] Handshake inválido: {msg_type}")
                    conn.close()
                    
            except json.JSONDecodeError:
                print(f"[{self.node_id}] Erro de protocolo no handshake.")
                conn.close()
                
        except Exception as e:
            print(f"[{self.node_id}] Erro na conexão inicial: {e}")
            try:
                conn.close()
            except:
                pass

    def _listen_to_connection(self, conn: socket.socket, from_node: str):
        """
        Loop principal de escuta.
        CRÍTICO: Gere a limpeza da tabela de rotas quando a conexão cai.
        """
        try:
            while self.running:
                # 1. Lê tamanho (4 bytes)
                length_bytes = conn.recv(4)
                if not length_bytes or len(length_bytes) < 4:
                    break
                
                length = int.from_bytes(length_bytes, byteorder='big')
                
                # 2. Lê Header/Mensagem JSON
                header_data = self._recv_exact(conn, length)
                if not header_data:
                    break
                
                # 3. Processamento de mensagens
                try:
                    msg_json = json.loads(header_data.decode('utf-8'))
                    msg_type = MessageType(msg_json.get('type'))
                    
                    if msg_type == MessageType.STREAM_DATA:
                        # === DADO DE VÍDEO ===
                        data_length = msg_json['data_length']
                        payload = self._recv_exact(conn, data_length)
                        if payload:
                            full_packet = length_bytes + header_data + payload
                            self._handle_stream_data(msg_json, full_packet, from_node)
                    
                    else:
                        # === MENSAGEM DE CONTROLO ===
                        msg = Message.from_json(header_data.decode('utf-8'))
                        self._process_message(msg, conn, None, from_node) 
                        
                except json.JSONDecodeError:
                    print(f"[{self.node_id}] Lixo recebido de {from_node}")
                    continue
                    
        except Exception as e:
            if self.running:
                print(f"[{self.node_id}] Conexão perdida com {from_node}: {e}")
        
        finally:
            # === ZONA DE LIMPEZA E RECUPERAÇÃO DE FALHAS ===
            # Este bloco corre SEMPRE que a conexão termina, não importa quem iniciou.
            if from_node:
                # print(f"[{self.node_id}] A limpar conexão de {from_node}...")
                
                affected_flows = []
                # Como a tabela de rotas não tem lookup inverso fácil, procuramos manualmente
                if hasattr(self, 'routing_table'):
                    for flow_id, route in self.routing_table.routes.items():
                        if from_node in route.destinations:
                            affected_flows.append(flow_id)

                with self.neighbors_lock:
                    # Verifica se este ainda é o socket ativo (para evitar remover reconexões novas)
                    if from_node in self.neighbors and self.neighbors[from_node] == conn:
                        del self.neighbors[from_node]
                        print(f"[{self.node_id}] Vizinho {from_node} desconectado.")
                        
                        # REMOVER DO MONITOR
                        self.latency_monitor.remove_neighbor(from_node)

                        # AVISA A TABELA DE ROTAS PARA RECALCULAR
                        self.routing_table.process_neighbor_down(from_node)
                
                for flow_id in affected_flows:
                    remaining_dests = self.routing_table.get_destinations(flow_id)
                    
                    if not remaining_dests:
                        # Ficámos sem ninguém a ouvir este fluxo. Cancelar a montante!
                        next_hop = self.routing_table.get_next_hop(flow_id)
                        
                        if next_hop:
                            print(f"[{self.node_id}] Rota {flow_id} ficou vazia após saída de {from_node}. Enviando DEACTIVATE auto...")
                            
                            try:
                                # Cria mensagem DEACTIVATE em nosso nome
                                msg = create_deactivate_message(flow_id, self.node_id, next_hop)
                                
                                # Envia para o próximo salto (em direção ao servidor)
                                with self.neighbors_lock:
                                    if next_hop in self.neighbors:
                                        send_message(self.neighbors[next_hop], msg)
                            except Exception as e:
                                print(f"[{self.node_id}] Erro ao enviar DEACTIVATE auto: {e}")

            try:
                conn.close()
            except:
                pass

    def _recv_exact(self, sock: socket.socket, length: int) -> bytes:
        """Recebe exatamente N bytes"""
        data = b''
        while len(data) < length:
            try:
                chunk = sock.recv(length - len(data))
                if not chunk: return None
                data += chunk
            except:
                return None
        return data
    
    def _udp_server_thread(self):
        """Escuta pacotes UDP e reencaminha"""
        print(f"[{self.node_id}] Thread UDP iniciada...")
        while self.running:
            try:
                # Recebe pacote (60KB buffer seguro)
                data, addr = self.udp_socket.recvfrom(60000)
                # Reencaminha
                self._handle_udp_packet(data, addr)
            except Exception as e:
                if self.running:
                    print(f"[{self.node_id}] Erro UDP: {e}")

    def _handle_udp_packet(self, data: bytes, source_addr):
        """Lê header e reencaminha baseado no flow_id"""
        try:
            # 1. Parse do cabeçalho (Formato: [Len][JSON][Data])
            if len(data) < 4: return
            header_len = int.from_bytes(data[:4], 'big')
            if len(data) < 4 + header_len: return
            
            header_json = data[4:4+header_len]
            header = json.loads(header_json.decode('utf-8'))
            
            flow_id = header.get('flow_id')
            if not flow_id: return
            
            # 2. Consulta Tabela de Rotas
            destinations = self.routing_table.get_destinations(flow_id)
            if not destinations:
                return

            # 3. Encaminha para cada destino
            for dest_id in destinations:
                dest_ip = None
                dest_port = 5000 # Porta padrão para nós overlay

                # Caso A: É um vizinho conhecido da topologia (Relay)
                if dest_id in self.neighbor_info:
                    dest_ip = self.neighbor_info[dest_id]['ip']
                    dest_port = self.neighbor_info[dest_id]['port']
                
                # Caso B: É um cliente dinâmico (não está no neighbor_info)
                elif dest_id in self.neighbors:
                    try:
                        # Truque: Buscar IP ao socket TCP conectado
                        tcp_sock = self.neighbors[dest_id]
                        dest_ip, _ = tcp_sock.getpeername()
                        dest_port = 5001 # Clientes usam 5001 (ver topology.json)
                    except:
                        continue

                if dest_ip:
                    # Split Horizon: Não enviar de volta para a origem (pelo IP)
                    if dest_ip != source_addr[0]:
                        self.udp_socket.sendto(data, (dest_ip, dest_port))
                        
        except Exception:
            pass
            
    # ========================================================
    # REGISTO E VIZINHOS
    # ========================================================
    
    def _load_expected_neighbors(self):
        """Carrega vizinhos esperados"""
        try:
            with open('topology_overlay.json', 'r') as f:
                topology = json.load(f)
            
            if self.node_id in topology['nodes']:
                node_config = topology['nodes'][self.node_id]
                neighbor_ids = node_config.get('neighbors', [])
                
                for neighbor_id in neighbor_ids:
                    if neighbor_id in topology['nodes']:
                        neighbor_info = topology['nodes'][neighbor_id]
                        self.neighbor_info[neighbor_id] = {
                            'node_id': neighbor_id,
                            'ip': neighbor_info['ip'],
                            'port': neighbor_info['port']
                        }
                
                print(f"[{self.node_id}] Vizinhos esperados: {neighbor_ids}")
        except Exception as e:
            print(f"[{self.node_id}] Erro ao carregar topologia: {e}")
    
    def _register_to_bootstrap(self):
        """Regista-se no bootstrapper"""
        print(f"[{self.node_id}] A registar no bootstrapper...")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5.0)
                sock.connect((self.bootstrap_ip, self.bootstrap_port))
                
                msg = create_register_message(self.node_id, self.my_ip, self.my_port)
                send_message(sock, msg)
                
                response = receive_message(sock)
                if response and response.type == MessageType.NEIGHBOR_LIST.value:
                    neighbors = response.data['neighbors']
                    print(f"[{self.node_id}] Recebi {len(neighbors)} vizinhos")
                    
                    for neighbor in neighbors:
                        self._connect_to_neighbor(neighbor)
                
                sock.close()
                return
                
            except Exception as e:
                print(f"[{self.node_id}] ERRO no registo (tentativa {attempt+1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
    
    def _connect_to_neighbor(self, neighbor: Dict):
        """Conecta-se a um vizinho"""
        neighbor_id = neighbor['node_id']
        neighbor_ip = neighbor['ip']
        neighbor_port = neighbor['port']
        
        if neighbor_id == self.node_id:
            return
        
        with self.neighbors_lock:
            if neighbor_id in self.neighbors:
                return
        
        try:
            print(f"[{self.node_id}] A conectar a {neighbor_id}...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((neighbor_ip, neighbor_port))
            sock.settimeout(None)
            
            with self.neighbors_lock:
                self.neighbors[neighbor_id] = sock
            
            self.neighbor_info[neighbor_id] = neighbor

            # ADICIONAR AO MONITOR
            self.latency_monitor.add_neighbor(neighbor_id)
            
            print(f"[{self.node_id}] ✓ Conectado a {neighbor_id}")
            
            # Envia HELLO inicial
            msg = create_hello_message(self.node_id)
            send_message(sock, msg)
            
            # Thread para escutar
            threading.Thread(
                target=self._listen_to_connection,
                args=(sock, neighbor_id),
                daemon=True
            ).start()
            
        except Exception as e:
            print(f"[{self.node_id}] ✗ Erro ao conectar a {neighbor_id}: {e}")
    
    # ========================================================
    # PROCESSAMENTO UNIFICADO
    # ========================================================
    
    def _process_message(self, msg: Message, conn: socket.socket, addr=None, from_neighbor: str = None):
        """Processa todas as mensagens de controlo"""
        msg_type = MessageType(msg.type)
        
        if msg_type == MessageType.HELLO:
            pass # Keepalive
        
        elif msg_type == MessageType.ANNOUNCE:
            self._handle_announce(msg, from_neighbor)
        
        elif msg_type == MessageType.ACTIVATE:
            self._handle_activate(msg, from_neighbor)

        elif msg_type == MessageType.DEACTIVATE:
            self._handle_deactivate(msg, from_neighbor)

        elif msg_type == MessageType.LATENCY_PING:
            self._handle_latency_ping(msg, conn, from_neighbor)
        
        elif msg_type == MessageType.LATENCY_PONG:
            self._handle_latency_pong(msg, from_neighbor)

        elif msg_type == MessageType.PING:
            self._handle_ping(msg, from_neighbor)
        
        else:
            print(f"[{self.node_id}] Mensagem desconhecida: {msg_type}")
    
    def _handle_announce(self, msg: Message, from_node: str):
        """Processa ANNOUNCE e propaga"""
        flow_id = msg.data['flow_id']
        origin = msg.data['from_node']
        received_metric = msg.data['metric']
        msg_id = msg.data.get('msg_id')
        
        link_latency = self.latency_monitor.get_latency(from_node)

        # Obtém estatísticas da medição
        latency_stats = self.latency_monitor.get_stats()
        neighbor_stats = latency_stats.get('latencies', {}).get(from_node, {})

        if link_latency is None:
            # Ainda não temos medida - usa estimativa conservadora
            link_latency = 10.0  # 10ms default
            samples_info = "sem dados"
        else:
            n_samples = neighbor_stats.get('samples', 0)
            samples_info = f"n={n_samples}"

        print(f"[{self.node_id}] ANNOUNCE de {origin} via {from_node}:")
        print(f"[{self.node_id}]   recebido={received_metric:.2f}ms")
        print(f"[{self.node_id}]   link={link_latency:.2f}ms ({samples_info})")

        my_metric = received_metric + link_latency

        print(f"[{self.node_id}]   total={my_metric:.2f}ms")

        # Verifica se tínhamos destinos à espera (antes do update)
        was_active_with_destinations = False
        old_route = self.routing_table.get_route(flow_id)
        if old_route and old_route.destinations:
            was_active_with_destinations = True

        # === 1. GUARDAR ESTADO ANTERIOR ===
        old_route = self.routing_table.get_route(flow_id)
        old_via = old_route.via_neighbor if old_route else None
        was_active = old_route.active if old_route else False

        # Atualiza tabela
        should_forward = self.routing_table.update_route(
            flow_id=flow_id,
            origin=origin,
            metric=my_metric,      # ← USA MÉTRICA TOTAL, NÃO A RECEBIDA!
            via_neighbor=from_node,
            msg_id=msg_id
        )

        # === 2. VERIFICAR SE MUDOU DE VIZINHO (NOVO) ===
        if was_active and should_forward:
            new_route = self.routing_table.get_route(flow_id)
            new_via = new_route.via_neighbor
            
            # Se mudámos de fornecedor (Ex: de n6 para n7)
            if old_via and new_via and old_via != new_via:
                print(f"[{self.node_id}] 🔀 Troca de Rota: {old_via} -> {new_via}. Cancelando anterior...")
                
                # A. Envia DEACTIVATE para o vizinho VELHO
                try:
                    deact_msg = create_deactivate_message(flow_id, self.node_id, old_via)
                    with self.neighbors_lock:
                        if old_via in self.neighbors:
                            send_message(self.neighbors[old_via], deact_msg)
                            print(f"[{self.node_id}]   🚫 DEACTIVATE enviado para antigo pai {old_via}")
                except Exception as e:
                    print(f"[{self.node_id}] Erro ao limpar rota antiga: {e}")

                # B. Envia ACTIVATE para o vizinho NOVO (Isto tu já tinhas, mas confirma)
                try:
                    act_msg = create_activate_message(flow_id, self.node_id, origin)
                    with self.neighbors_lock:
                        if new_via in self.neighbors:
                            send_message(self.neighbors[new_via], act_msg)
                            print(f"[{self.node_id}]   ✅ ACTIVATE enviado para novo pai {new_via}")
                except Exception as e:
                    print(f"[{self.node_id}] Erro ao ativar nova rota: {e}")

        # --- LÓGICA DE RECUPERAÇÃO AUTOMÁTICA ---
        # Se a rota foi atualizada e nós temos clientes à espera, 
        # temos de garantir que pedimos o stream ao novo vizinho!
        if should_forward and was_active_with_destinations:
            route = self.routing_table.get_route(flow_id)
            # Se a rota agora é válida e temos destinos, enviamos ACTIVATE para montante
            if route.via_neighbor and route.via_neighbor == from_node:
                print(f"[{self.node_id}] Rota recuperada via {from_node}! Reenviando ACTIVATE...")
                
                # Garante que está ativa na tabela
                route.active = True 
                
                # Envia ACTIVATE para o novo "pai"
                activate_msg = create_activate_message(flow_id, self.node_id, origin)
                try:
                    # Envia apenas para o vizinho de onde veio este ANNOUNCE
                    with self.neighbors_lock:
                        if from_node in self.neighbors:
                            send_message(self.neighbors[from_node], activate_msg)
                            print(f"[{self.node_id}]   → ACTIVATE enviado para {from_node}")
                except Exception as e:
                    print(f"[{self.node_id}] Erro ao recuperar stream: {e}")
        
        if should_forward:
            print(f"[{self.node_id}] Reencaminhando ANNOUNCE (métrica={my_metric:.2f}ms)")
        
            # Cria ANNOUNCE com a métrica TOTAL até este nó
            forward_msg = create_announce_message(
                flow_id=flow_id,
                from_node=origin,      # Mantém origem original
                metric=my_metric,      # ← Métrica total até aqui
                msg_id=msg_id          # Mantém ID (evita loops)
            )
            
            # Envia para todos os vizinhos EXCETO de onde veio
            with self.neighbors_lock:
                neighbors_copy = dict(self.neighbors)
            
            sent_count = 0
            for neighbor_id, sock in neighbors_copy.items():
                if neighbor_id != from_node:  # NÃO envia de volta
                    try:
                        send_message(sock, forward_msg)
                        sent_count += 1
                    except Exception as e:
                        print(f"[{self.node_id}] Erro ao enviar para {neighbor_id}: {e}")
            
            if sent_count > 0:
                print(f"[{self.node_id}]   → Encaminhado para {sent_count} vizinho(s)")

    
    def _handle_activate(self, msg: Message, from_node: str):
        """Processa ACTIVATE"""
        flow_id = msg.data['flow_id']
        
        print(f"[{self.node_id}] ACTIVATE recebido via {from_node}")
        
        # Ativa rota
        self.routing_table.activate_route(flow_id, from_node)
        
        # Reencaminha
        next_hop = self.routing_table.get_next_hop(flow_id)
        if next_hop:
            with self.neighbors_lock:
                if next_hop in self.neighbors:
                    try:
                        send_message(self.neighbors[next_hop], msg)
                        print(f"[{self.node_id}]   → ACTIVATE para {next_hop}")
                    except Exception as e:
                        print(f"[{self.node_id}]   ✗ Erro envio ACTIVATE: {e}")
        else:
            print(f"[{self.node_id}]   ! Sem rota para servidor")

    def _handle_deactivate(self, msg: Message, from_node: str):
        """
        Processa DEACTIVATE
        
        Lógica:
        1. Remove o cliente (from_node) dos destinos
        2. Se rota ficar sem destinos, desativa
        3. Reencaminha para próximo hop (back towards servidor)
        """
        flow_id = msg.data['flow_id']
        from_client = msg.data['from_node']
        
        print(f"[{self.node_id}] DEACTIVATE recebido: {flow_id} de {from_client} via {from_node}")
        
        # 1. Remove cliente dos destinos
        self.routing_table.deactivate_route(flow_id, from_node)

        remaining_dests = self.routing_table.get_destinations(flow_id)
        
        if remaining_dests:
            # Ainda temos gente a ouvir!
            # NÃO enviamos DEACTIVATE para cima. O fluxo continua a chegar até nós
            # e nós apenas paramos de enviar para o 'from_node' que saiu.
            print(f"[{self.node_id}]   ✋ DEACTIVATE absorvido (ainda restam {len(remaining_dests)} destinos)")
            return

        # Se não sobrou ninguém, então sim, avisamos o servidor para parar
        print(f"[{self.node_id}]   Destinos vazios. Reencaminhando DEACTIVATE para montante...")
        
        # 2. Reencaminha para próximo hop (em direcção ao servidor)
        next_hop = self.routing_table.get_next_hop(flow_id)
        
        if next_hop:
            with self.neighbors_lock:
                if next_hop in self.neighbors:
                    try:
                        send_message(self.neighbors[next_hop], msg)
                        print(f"[{self.node_id}]   → DEACTIVATE encaminhado para {next_hop}")
                    except Exception as e:
                        print(f"[{self.node_id}]   ✗ Erro envio DEACTIVATE: {e}")
        else:
            print(f"[{self.node_id}]   ! Sem rota para servidor (somos o destino?)")
    
    def _handle_stream_data(self, header: dict, raw_packet: bytes, from_node: str):
        """Reencaminha STREAM_DATA"""
        flow_id = header['flow_id']
        destinations = self.routing_table.get_destinations(flow_id)
        
        if not destinations:
            return
            
        with self.neighbors_lock:
            for dest_id in destinations:
                if dest_id != from_node and dest_id in self.neighbors:
                    try:
                        self.neighbors[dest_id].sendall(raw_packet)
                    except:
                        pass
    
    def _handle_latency_ping(self, msg: Message, conn: socket.socket, 
                            from_neighbor: str):
        """
        Processa LATENCY_PING e responde com PONG imediatamente
        """
        ping_id = msg.data['ping_id']
        original_timestamp = msg.data['timestamp']
        
        # Responde imediatamente com PONG
        try:
            pong = create_latency_pong_message(
                from_node=self.node_id,
                ping_id=ping_id,
                original_timestamp=original_timestamp
            )
            send_message(conn, pong)
        except Exception as e:
            pass  # Silencioso
    
    def _handle_latency_pong(self, msg: Message, from_neighbor: str):
        """
        Processa LATENCY_PONG e atualiza monitor
        """
        ping_id = msg.data['ping_id']
        original_timestamp = msg.data['original_timestamp']
        
        # Processa no monitor
        rtt_ms = self.latency_monitor.process_pong(
            from_neighbor, ping_id, original_timestamp
        )
        
        # Debug (opcional, comentar para produção)
        # if rtt_ms:
        #     print(f"[LATENCY] {from_neighbor}: {rtt_ms:.2f}ms")

    def _handle_ping(self, msg: Message, from_node: str):
        """Processa PING"""
        path = msg.data.get('path', [])
        path.append(self.node_id)
        
        print(f"[{self.node_id}] PING: {' → '.join(path)}")
        
        with self.neighbors_lock:
            neighbors_copy = dict(self.neighbors)
        
        for neighbor_id, sock in neighbors_copy.items():
            if neighbor_id != from_node:
                forward_msg = create_ping_message(msg.data['from_node'], path.copy())
                try:
                    send_message(sock, forward_msg)
                except:
                    pass
    
    # ========================================================
    # THREADS DE MANUTENÇÃO
    # ========================================================
    
    def _reconnect_thread(self):
        """Tenta reconectar periodicamente"""
        while self.running:
            time.sleep(15)
            for neighbor_id, info in list(self.neighbor_info.items()):
                with self.neighbors_lock:
                    is_connected = neighbor_id in self.neighbors
                if not is_connected:
                    self._connect_to_neighbor(info)
    
    def _keepalive_thread(self):
        """Envia HELLO periodicamente"""
        while self.running:
            time.sleep(20)
            with self.neighbors_lock:
                neighbors_copy = dict(self.neighbors)
            for neighbor_id, sock in neighbors_copy.items():
                try:
                    msg = create_hello_message(self.node_id)
                    send_message(sock, msg)
                except:
                    pass

    def _latency_ping_thread(self):
        """
        Thread que envia LATENCY_PING para todos os vizinhos
        periodicamente para medir RTT
        """
        print(f"[{self.node_id}] Thread de latência iniciada")
        
        while self.running:
            time.sleep(2.0)  # A cada 2 segundos
            
            # Cleanup de timeouts
            self.latency_monitor.cleanup_timeouts()
            
            # Envia PING para todos os vizinhos
            with self.neighbors_lock:
                neighbors_copy = dict(self.neighbors)
            
            for neighbor_id, sock in neighbors_copy.items():
                try:
                    # Cria PING
                    ping_id, timestamp = self.latency_monitor.create_ping_message(neighbor_id)
                    
                    # Envia
                    msg = create_latency_ping_message(
                        from_node=self.node_id,
                        ping_id=ping_id,
                        timestamp=timestamp
                    )
                    send_message(sock, msg)
                    
                except Exception as e:
                    # Silencioso para não spammar
                    pass

    def _stats_thread(self):
        """Imprime estatísticas periodicamente"""
        while self.running:
            time.sleep(30)
            with self.neighbors_lock:
                neighbor_list = list(self.neighbors.keys())
            print(f"\n[{self.node_id}] === STATUS ===")
            print(f"  Vizinhos conectados: {neighbor_list}")
            print(f"  Rotas: {self.routing_table.get_stats()}")
            self.latency_monitor.print_stats()
            self.routing_table.print_table()


# ============================================================
# MAIN
# ============================================================

def main():
    if len(sys.argv) < 2:
        print("Uso: python overlay_node.py <node_id>")
        sys.exit(1)
    
    node_id = sys.argv[1]
    
    with open('topology_overlay.json', 'r') as f:
        topology = json.load(f)
    
    if node_id not in topology['nodes']:
        print(f"ERRO: Nó {node_id} não encontrado!")
        sys.exit(1)
    
    node_info = topology['nodes'][node_id]
    config = topology['config']
    
    node = OverlayNode(
        node_id=node_id,
        my_ip=node_info['ip'],
        my_port=node_info['port'],
        bootstrap_ip=config['bootstrap_ip'],
        bootstrap_port=config['bootstrap_port']
    )
    
    node.start()
    
    try:
        print(f"\n[{node_id}] A correr... (Ctrl+C para parar)")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{node_id}] Interrompido")
        node.stop()


if __name__ == "__main__":
    main()
