"""
latency_monitor.py - Monitorização de Latência entre Vizinhos

Este módulo mede continuamente a latência (RTT) para cada vizinho
através de mensagens PING/PONG dedicadas.
"""

import time
import threading
from typing import Dict, Optional
from collections import deque


class LatencyMonitor:
    """
    Monitor de latência para vizinhos overlay
    
    Funcionalidades:
    - Envia PINGs periódicos para cada vizinho
    - Calcula RTT médio com janela deslizante
    - Deteta timeout/falhas de vizinhos
    """
    
    def __init__(self, node_id: str, ping_interval: float = 2.0, 
                 window_size: int = 10, timeout: float = 5.0):
        """
        Args:
            node_id: ID deste nó
            ping_interval: Intervalo entre PINGs (segundos)
            window_size: Tamanho da janela para média móvel
            timeout: Tempo máximo de espera por PONG (segundos)
        """
        self.node_id = node_id
        self.ping_interval = ping_interval
        self.window_size = window_size
        self.timeout = timeout
        
        # {neighbor_id: deque([rtt1, rtt2, ...])}
        self.latencies: Dict[str, deque] = {}
        
        # {neighbor_id: {ping_id: timestamp}}
        self.pending_pings: Dict[str, Dict[str, float]] = {}
        
        # Lock para thread-safety
        self.lock = threading.Lock()
        
        self.running = False
        
        print(f"[LATENCY] Monitor criado (intervalo={ping_interval}s, "
              f"janela={window_size}, timeout={timeout}s)")
    
    def start(self):
        """Inicia monitorização (não faz nada por si, precisa de callbacks)"""
        self.running = True
        print(f"[LATENCY] Monitor ativo")
    
    def stop(self):
        """Para monitorização"""
        self.running = False
    
    def add_neighbor(self, neighbor_id: str):
        """Adiciona vizinho para monitorizar"""
        with self.lock:
            if neighbor_id not in self.latencies:
                self.latencies[neighbor_id] = deque(maxlen=self.window_size)
                self.pending_pings[neighbor_id] = {}
                print(f"[LATENCY] A monitorizar {neighbor_id}")
    
    def remove_neighbor(self, neighbor_id: str):
        """Remove vizinho"""
        with self.lock:
            if neighbor_id in self.latencies:
                del self.latencies[neighbor_id]
                del self.pending_pings[neighbor_id]
                print(f"[LATENCY] Removido {neighbor_id} da monitorização")
    
    def create_ping_message(self, neighbor_id: str) -> tuple:
        """
        Cria mensagem PING para enviar
        
        Returns:
            (ping_id, timestamp) - para incluir na mensagem
        """
        import uuid
        
        ping_id = str(uuid.uuid4())[:8]  # ID curto
        timestamp = time.time()
        
        with self.lock:
            if neighbor_id in self.pending_pings:
                self.pending_pings[neighbor_id][ping_id] = timestamp
        
        return ping_id, timestamp
    
    def process_pong(self, neighbor_id: str, ping_id: str, 
                     original_timestamp: float) -> Optional[float]:
        """
        Processa PONG recebido e calcula RTT
        
        Args:
            neighbor_id: ID do vizinho que respondeu
            ping_id: ID do PING original
            original_timestamp: Timestamp do PING original
        
        Returns:
            RTT em milissegundos (ou None se inválido)
        """
        now = time.time()
        
        with self.lock:
            # Verifica se estamos à espera deste PONG
            if neighbor_id not in self.pending_pings:
                return None
            
            if ping_id not in self.pending_pings[neighbor_id]:
                return None
            
            # Calcula RTT
            sent_time = self.pending_pings[neighbor_id].pop(ping_id)
            rtt_seconds = now - sent_time
            rtt_ms = rtt_seconds * 1000
            
            # Adiciona à janela de latências
            if neighbor_id in self.latencies:
                self.latencies[neighbor_id].append(rtt_ms)
            
            return rtt_ms
    
    def get_latency(self, neighbor_id: str) -> Optional[float]:
        """
        Obtém latência média para um vizinho
        
        Returns:
            Latência em milissegundos (ou None se sem dados)
        """
        with self.lock:
            if neighbor_id not in self.latencies:
                return None
            
            latency_list = list(self.latencies[neighbor_id])
            
            if not latency_list:
                return None
            
            # Retorna média da janela
            return sum(latency_list) / len(latency_list)
    
    def get_all_latencies(self) -> Dict[str, float]:
        """
        Obtém latências para todos os vizinhos
        
        Returns:
            {neighbor_id: latency_ms}
        """
        with self.lock:
            result = {}
            for neighbor_id in self.latencies:
                lat = self.get_latency(neighbor_id)
                if lat is not None:
                    result[neighbor_id] = lat
            return result
    
    def cleanup_timeouts(self):
        """
        Remove PINGs que expiraram (timeout)
        
        Deve ser chamado periodicamente
        """
        now = time.time()
        
        with self.lock:
            for neighbor_id in list(self.pending_pings.keys()):
                expired = []
                
                for ping_id, timestamp in self.pending_pings[neighbor_id].items():
                    if now - timestamp > self.timeout:
                        expired.append(ping_id)
                
                # Remove expirados
                for ping_id in expired:
                    del self.pending_pings[neighbor_id][ping_id]
                    print(f"[LATENCY] PING timeout para {neighbor_id} (id={ping_id})")
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas"""
        with self.lock:
            stats = {
                "neighbors": len(self.latencies),
                "latencies": {}
            }
            
            for neighbor_id, lat_deque in self.latencies.items():
                if lat_deque:
                    lats = list(lat_deque)
                    stats["latencies"][neighbor_id] = {
                        "avg_ms": sum(lats) / len(lats),
                        "min_ms": min(lats),
                        "max_ms": max(lats),
                        "samples": len(lats)
                    }
            
            return stats
    
    def print_stats(self):
        """Imprime estatísticas formatadas"""
        stats = self.get_stats()
        
        print(f"\n{'='*60}")
        print(f"LATÊNCIAS - Node {self.node_id}")
        print(f"{'='*60}")
        
        if not stats["latencies"]:
            print("  (sem dados)")
        else:
            for neighbor_id, data in stats["latencies"].items():
                print(f"  {neighbor_id:8s}: {data['avg_ms']:6.2f}ms "
                      f"(min={data['min_ms']:.2f}, max={data['max_ms']:.2f}, "
                      f"n={data['samples']})")
        
        print(f"{'='*60}\n")


# ============================================================
# EXEMPLO DE USO
# ============================================================

if __name__ == "__main__":
    print("=== Teste do Monitor de Latência ===\n")
    
    monitor = LatencyMonitor("n10", ping_interval=2.0)
    monitor.start()
    
    # Simula adicionar vizinhos
    monitor.add_neighbor("n16")
    monitor.add_neighbor("n5")
    
    # Simula envio de PING
    print("1. Enviando PING para n16...")
    ping_id, ts = monitor.create_ping_message("n16")
    print(f"   PING ID: {ping_id}, Timestamp: {ts}")
    
    # Simula espera
    time.sleep(0.05)  # 50ms
    
    # Simula receção de PONG
    print("\n2. Recebendo PONG de n16...")
    rtt = monitor.process_pong("n16", ping_id, ts)
    print(f"   RTT: {rtt:.2f}ms")
    
    # Adiciona mais amostras
    print("\n3. Adicionando mais amostras...")
    for i in range(5):
        ping_id, ts = monitor.create_ping_message("n16")
        time.sleep(0.03 + i * 0.01)  # Variação 30-70ms
        monitor.process_pong("n16", ping_id, ts)
    
    # Mostra latência média
    print("\n4. Latência média:")
    lat = monitor.get_latency("n16")
    print(f"   n16: {lat:.2f}ms")
    
    # Mostra todas
    print("\n5. Todas as latências:")
    all_lats = monitor.get_all_latencies()
    print(f"   {all_lats}")
    
    # Estatísticas
    print("\n6. Estatísticas completas:")
    monitor.print_stats()