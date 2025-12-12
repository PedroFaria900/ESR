"""
client_node.py - Cliente de Streaming (VERSÃO COMPLETA CORRIGIDA)

Mudanças aplicadas:
1. Envia HELLO ao conectar para se identificar ao overlay
2. Timeout de 5s ao conectar (depois remove)
3. Melhor handling de diferentes tipos de mensagem
4. Gestão robusta de erros na recepção
"""

import socket
import threading
import json
import time
import sys
import cv2
import numpy as np

from protocol import *


class StreamingClient:
    """Cliente que recebe e reproduz stream"""
    
    def __init__(self, client_id: str, my_ip: str, overlay_ip: str, 
                 overlay_port: int, flow_id: str):
        self.client_id = client_id
        self.my_ip = my_ip
        self.overlay_ip = overlay_ip
        self.overlay_port = overlay_port
        self.flow_id = flow_id
        
        self.overlay_socket = None
        self.running = False
        self.receiving = False
        
        # Estatísticas
        self.frames_received = 0
        self.last_sequence = -1
        self.frames_lost = 0

        # Socket UDP (Porta 5001)
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind(('0.0.0.0', 5001))
        
        print(f"[{self.client_id}] Cliente criado")
        print(f"[{self.client_id}] Overlay: {self.overlay_ip}:{self.overlay_port}")
        print(f"[{self.client_id}] Flow: {self.flow_id}")
    
    def start(self):
        """Inicia cliente"""
        self.running = True
        
        # Inicia thread de recepção UDP
        threading.Thread(target=self._receive_thread_udp, daemon=True).start()

        # 1. Conecta ao nó overlay
        if not self._connect_to_overlay():
            print(f"[{self.client_id}] ERRO: Não conseguiu conectar ao overlay!")
            return
        
        # 2. Envia ACTIVATE para começar a receber
        self._request_stream()
        
        # 3. Thread para receber dados
        threading.Thread(target=self._receive_thread, daemon=True).start()
        
        # 4. Thread de estatísticas
        threading.Thread(target=self._stats_thread, daemon=True).start()
        
        print(f"[{self.client_id}] Cliente iniciado!")
    
    def stop(self):
        """Para cliente"""
        print(f"[{self.client_id}] A parar...")

        self._send_deactivate()

        self.running = False
        self.receiving = False
        
        if self.overlay_socket:
            try:
                self.overlay_socket.close()
            except:
                pass
        
        print(f"[{self.client_id}] Parado.")

    def _send_deactivate(self):
        """Envia DEACTIVATE para notificar que vai desconectar"""
        try:
            msg = create_deactivate_message(
                self.flow_id,
                self.client_id,
                "unknown_server"  # Será preenchido pelo overlay
            )
            
            send_message(self.overlay_socket, msg)
            print(f"[{self.client_id}] DEACTIVATE enviado")
            time.sleep(0.2)  # Pequeno delay para entregar
            
        except Exception as e:
            print(f"[{self.client_id}] Erro ao enviar DEACTIVATE: {e}")
    
    # ========================================================
    # CONEXÃO AO OVERLAY
    # ========================================================
    
    def _connect_to_overlay(self) -> bool:
        """Conecta ao nó overlay mais próximo"""
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                print(f"[{self.client_id}] A conectar ao overlay (tentativa {attempt+1}/{max_retries})...")
                self.overlay_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                
                # 🔥 MUDANÇA 1: Timeout de 5s ao conectar
                self.overlay_socket.settimeout(5.0)
                self.overlay_socket.connect((self.overlay_ip, self.overlay_port))
                
                # 🔥 MUDANÇA 2: Remove timeout após conectar (importante para STREAM_DATA)
                self.overlay_socket.settimeout(None)
                
                print(f"[{self.client_id}] ✓ Conectado!")
                
                # 🔥 MUDANÇA 3: Envia HELLO para se identificar ao overlay
                try:
                    msg = create_hello_message(self.client_id)
                    send_message(self.overlay_socket, msg)
                    print(f"[{self.client_id}] → HELLO enviado")
                    time.sleep(0.1)  # Pequeno delay para processar
                except Exception as e:
                    print(f"[{self.client_id}] Erro ao enviar HELLO: {e}")
                
                return True
                
            except socket.timeout:
                print(f"[{self.client_id}] ✗ Timeout na conexão")
                if attempt < max_retries - 1:
                    print(f"[{self.client_id}] A tentar novamente em {retry_delay}s...")
                    time.sleep(retry_delay)
                    
            except Exception as e:
                print(f"[{self.client_id}] ✗ Erro: {e}")
                if attempt < max_retries - 1:
                    print(f"[{self.client_id}] A tentar novamente em {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    print(f"[{self.client_id}] Desistindo após {max_retries} tentativas")
                    return False
        
        return False
    
    def _request_stream(self):
        """Envia ACTIVATE para começar a receber stream"""
        try:
            # NOTA: Como cliente, não sabemos quem é o servidor,
            # mas o overlay tratará de reencaminhar o ACTIVATE
            msg = create_activate_message(
                self.flow_id,
                self.client_id,
                "unknown_server"  # Será preenchido pelo overlay
            )
            
            send_message(self.overlay_socket, msg)
            print(f"[{self.client_id}] ACTIVATE enviado para {self.flow_id}")
            
        except Exception as e:
            print(f"[{self.client_id}] Erro ao enviar ACTIVATE: {e}")
    
    # ========================================================
    # RECEPÇÃO DE DADOS
    # ========================================================
    
    def _receive_thread(self):
        """Thread que recebe frames"""
        self.receiving = True
        
        print(f"[{self.client_id}] A aguardar frames...")
        
        while self.running and self.receiving:
            try:
                # Tenta receber mensagem
                # Primeiro verifica se é controlo ou dados
                
                # Lê os primeiros 4 bytes (tamanho)
                length_bytes = self.overlay_socket.recv(4)
                if not length_bytes or len(length_bytes) < 4:
                    print(f"[{self.client_id}] Conexão fechada pelo overlay")
                    break
                
                length = int.from_bytes(length_bytes, byteorder='big')
                
                # Lê o header
                header_data = self._recv_exact(length)
                if not header_data:
                    print(f"[{self.client_id}] Erro ao receber header")
                    break
                
                # Tenta fazer parse como JSON
                try:
                    header = json.loads(header_data.decode('utf-8'))
                    msg_type = header.get('type')
                    
                    #  MUDANÇA 4: Processa diferentes tipos de mensagem
                    if msg_type == MessageType.STREAM_DATA.value:
                        # É STREAM_DATA - lê o payload
                        data_length = header['data_length']
                        data = self._recv_exact(data_length)
                        
                        if data:
                            self._process_frame(header, data)
                    
                    elif msg_type == MessageType.HELLO.value:
                        # Keepalive do overlay - ignora
                        pass
                    
                    else:
                        # Outra mensagem de controlo
                        print(f"[{self.client_id}] Mensagem recebida: {msg_type}")
                
                except json.JSONDecodeError:
                    # Não é JSON válido
                    print(f"[{self.client_id}] Dados inválidos recebidos")
                    continue
                
            except socket.timeout:
                continue
                
            except Exception as e:
                if self.running:
                    print(f"[{self.client_id}] Erro na recepção: {e}")
                break
        
        print(f"[{self.client_id}] Recepção terminada - enviando DEACTIVATE")
        self._send_deactivate()
        
        self.receiving = False
    
    def _receive_thread_udp(self):
        """Recebe pacotes UDP e imprime animação"""
        print(f"[{self.client_id}] À espera de dados UDP na porta 5001...")
        
        while self.running:
            try:
                # 1. Recebe pacote
                packet, addr = self.udp_socket.recvfrom(60000)
                
                # 2. Parse do Cabeçalho
                head_len = int.from_bytes(packet[:4], 'big')
                
                # (Opcional) Ler JSON para ver número de sequência
                # header_json = packet[4:4+head_len]
                # header = json.loads(header_json)
                
                # 3. Extrair Payload (JPEG)
                jpg_data = packet[4+head_len:]
                
                # 4. Descodificar Imagem
                # Converte bytes para array numpy
                nparr = np.frombuffer(jpg_data, np.uint8)
                # Descodifica
                img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                
                if img is not None:
                    # 5. Mostrar na Janela
                    window_name = f"Cliente {self.client_id} ({self.flow_id})"
                    cv2.imshow(window_name, img)
                    
                    # Necessário para o OpenCV atualizar a janela (1ms)
                    # Pressionar 'q' fecha a janela (opcional)
                    if cv2.waitKey(1) & 0xFF == ord('q'):
                        break
                    
                    self.frames_received += 1
                else:
                    print(f"[{self.client_id}] Frame corrompido/incompleto")
                
            except Exception as e:
                print(f"Erro RX UDP: {e}")
                pass 
                
        cv2.destroyAllWindows()

    def _recv_exact(self, length: int) -> bytes:
        """Recebe exatamente N bytes"""
        data = b''
        while len(data) < length:
            try:
                chunk = self.overlay_socket.recv(length - len(data))
                if not chunk:
                    return None
                data += chunk
            except Exception as e:
                print(f"[{self.client_id}] Erro ao receber dados: {e}")
                return None
        return data
    
    def _process_frame(self, header: dict, data: bytes):
        """Processa frame recebido"""
        sequence = header['sequence']
        timestamp = header['timestamp']
        
        # Atualiza estatísticas
        self.frames_received += 1
        
        # Detecta perda de frames
        if self.last_sequence != -1:
            expected = self.last_sequence + 1
            if sequence > expected:
                lost = sequence - expected
                self.frames_lost += lost
                print(f"[{self.client_id}] ⚠ Perdidos {lost} frames!")
        
        self.last_sequence = sequence
        
        # "Reproduz" frame (neste caso, apenas imprime)
        latency = time.time() - timestamp
        
        # 🔥 MUDANÇA 5: Print reduzido para não spammar (só a cada 10 frames)
        if sequence % 10 == 0:
            print(f"[{self.client_id}] Frame {sequence}: {data.decode('utf-8')} "
                  f"(latência: {latency*1000:.1f}ms)")
    
    # ========================================================
    # ESTATÍSTICAS
    # ========================================================
    
    def _stats_thread(self):
        """Imprime estatísticas periodicamente"""
        last_frames = 0
        
        while self.running:
            time.sleep(10)
            
            if not self.receiving:
                continue
            
            frames_now = self.frames_received
            fps = (frames_now - last_frames) / 10.0
            last_frames = frames_now
            
            loss_rate = 0
            if self.frames_received > 0:
                total = self.frames_received + self.frames_lost
                loss_rate = (self.frames_lost / total) * 100
            
            print(f"\n[{self.client_id}] === ESTATÍSTICAS ===")
            print(f"  Frames recebidos: {self.frames_received}")
            print(f"  Frames perdidos: {self.frames_lost}")
            print(f"  Taxa de perda: {loss_rate:.2f}%")
            print(f"  FPS atual: {fps:.1f}")
            print()


# ============================================================
# MAIN
# ============================================================

def main():
    if len(sys.argv) < 2:
        print("Uso: python client_node.py <client_id> [stream_id_to_watch]")
        print("Exemplo: python client_node.py n17")
        sys.exit(1)
    
    client_id = sys.argv[1]
    
    # Carrega topologia
    try:
        with open('topology_overlay.json', 'r') as f:
            topology = json.load(f)
    except FileNotFoundError:
        print("ERRO: topology_overlay.json não encontrado!")
        sys.exit(1)
    except json.JSONDecodeError:
        print("ERRO: topology_overlay.json inválido!")
        sys.exit(1)
    
    if client_id not in topology['clients']:
        print(f"ERRO: Cliente {client_id} não encontrado!")
        print(f"Clientes disponíveis: {list(topology['clients'].keys())}")
        sys.exit(1)
    
    client_info = topology['clients'][client_id]
    config = topology['config']

    if len(sys.argv) >= 3:
        target_stream = sys.argv[2]
    else:
        target_stream = config['stream_id']

    # Cria e inicia cliente
    client = StreamingClient(
        client_id=client_id,
        my_ip=client_info['ip'],
        overlay_ip=client_info['overlay_node_ip'],
        overlay_port=config['overlay_port'],
        flow_id=target_stream
    )
    
    client.start()
    
    try:
        print(f"\n[{client_id}] Cliente a correr... (Ctrl+C para parar)")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{client_id}] Interrompido")
        client.stop()


if __name__ == "__main__":
    main()