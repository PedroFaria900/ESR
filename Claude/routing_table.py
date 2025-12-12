"""
routing_table.py - Tabela de Rotas para Overlay
"""

from typing import Dict, List, Optional
from dataclasses import dataclass, field
import time


@dataclass
class RouteEntry:
    """
    Entrada na tabela de rotas
    
    Campos conforme especificação do enunciado:
    - flow_id: Identificador do fluxo (ex: "stream1")
    - origin: Nó de origem do fluxo (ex: "n16")
    - metric: Custo do caminho (ex: latencia)
    - via_neighbor: Vizinho pelo qual recebi o ANNOUNCE
    - destinations: Lista de vizinhos para onde reencaminhar
    - active: Se a rota está ativa (cliente pediu)
    """
    flow_id: str                    # ID do fluxo
    origin: str                     # Nó origem (servidor)
    metric: float                     # Custo (latência em ms)
    via_neighbor: str               # De onde veio (IP ou node_id)
    destinations: List[str] = field(default_factory=list)  # Para onde enviar
    active: bool = False            # Se está ativa
    last_update: float = field(default_factory=time.time)  # Timestamp
    
    def __str__(self):
        status = "ACTIVE" if self.active else "INACTIVE"
        dests = ', '.join(self.destinations) if self.destinations else "none"
        return (f"[{status}] {self.flow_id}: from {self.origin} "
                f"via {self.via_neighbor} (metric={self.metric:.2f}ms) -> [{dests}]")


class RoutingTable:
    """
    Tabela de rotas do nó overlay
    
    Mantém uma entrada por fluxo, com informação de:
    - Melhor caminho para a origem
    - Vizinhos para onde reencaminhar dados
    """
    
    def __init__(self, node_id: str, hysteresis_threshold: float = 0.15,  # 15%
                 jitter_tolerance: float = 5.0):
        self.node_id = node_id
        self.routes: Dict[str, RouteEntry] = {}  # {flow_id: RouteEntry}
        self.seen_announces: set = set()  # Para evitar loops
    

        self.hysteresis_threshold = hysteresis_threshold
        self.jitter_tolerance = jitter_tolerance

        print(f"[ROUTING] Tabela criada com histerese de {hysteresis_threshold*100:.0f}% "
            f"e tolerância a jitter de {jitter_tolerance}ms")

    def update_route(
        self,
        flow_id: str,
        origin: str,
        metric: float,
        via_neighbor: str,
        msg_id: str = None
    ) -> bool:
        """
        Atualiza rota se for melhor ou igual (refresh)
        Não apaga destinos quando a rota muda de fornecedor.
        """
        # Evita processar ANNOUNCE duplicado exato
        if msg_id and msg_id in self.seen_announces:
            return False
        
        if msg_id:
            self.seen_announces.add(msg_id)
        
        # Se não existe rota, cria
        if flow_id not in self.routes:
            self.routes[flow_id] = RouteEntry(
                flow_id=flow_id,
                origin=origin,
                metric=metric,
                via_neighbor=via_neighbor
            )
            print(f"[ROUTING] Nova rota: {self.routes[flow_id]}")
            return True
        
        current = self.routes[flow_id]
        
        # 1. Se a nova rota é MELHOR (menor latência) -> Atualiza tudo
        if metric < current.metric:
            current.origin = origin
            current.metric = metric
            current.via_neighbor = via_neighbor
            current.last_update = time.time()
            print(f"[ROUTING] Rota melhorada: {current}")
            return True
            
        # 2. SMesma rota, mesmo vizinho (REFRESH)
        if via_neighbor == current.via_neighbor:
            # Aceita como refresh se a variação for pequena (jitter)
            latency_diff = abs(metric - current.metric)
            
            if latency_diff <= self.jitter_tolerance:
                # Variação aceitável - apenas refresh
                current.metric = metric  # Atualiza para novo valor
                current.last_update = time.time()
                return True  # Reencaminha para propagar atualização
            
            elif metric < current.metric:
                # Melhoria significativa da mesma rota
                improvement_pct = (current.metric - metric) / current.metric * 100
                current.metric = metric
                current.last_update = time.time()
                print(f"[ROUTING] ✓ Rota via {via_neighbor} melhorou "
                      f"{improvement_pct:.1f}% ({current.metric:.2f}ms)")
                return True
            
            else:
                # Pioria da mesma rota (congestionamento?)
                degradation_pct = (metric - current.metric) / current.metric * 100
                current.metric = metric
                current.last_update = time.time()
                print(f"[ROUTING] ⚠️ Rota via {via_neighbor} piorou "
                      f"{degradation_pct:.1f}% ({current.metric:.2f}ms)")
                return True
        
        # 3. Rota alternativa (vizinho diferente)
        
        # Calcula melhoria necessária (threshold)
        improvement_needed = current.metric * self.hysteresis_threshold
        threshold_latency = current.metric - improvement_needed
        
        if metric < threshold_latency:
            # Nova rota é SIGNIFICATIVAMENTE melhor
            improvement_pct = (current.metric - metric) / current.metric * 100
            
            print(f"[ROUTING] 🔄 TROCA DE ROTA: {current.via_neighbor} → {via_neighbor}")
            print(f"[ROUTING]    Antes: {current.metric:.2f}ms")
            print(f"[ROUTING]    Agora: {metric:.2f}ms")
            print(f"[ROUTING]    Melhoria: {improvement_pct:.1f}% "
                  f"(threshold: {self.hysteresis_threshold*100:.0f}%)")
            
            # ⚠️ IMPORTANTE: NÃO apaga destinos!
            # Guarda destinos antes de atualizar
            old_destinations = current.destinations.copy()
            old_active = current.active
            
            # Atualiza rota
            current.origin = origin
            current.metric = metric
            current.via_neighbor = via_neighbor
            current.last_update = time.time()
            
            # Restaura destinos e estado
            current.destinations = old_destinations
            current.active = old_active
            
            return True
        
        else:
            # Nova rota não é suficientemente melhor
            improvement_pct = (current.metric - metric) / current.metric * 100
            
            # Log apenas se a diferença for notável (> 5%)
            if improvement_pct > 5:
                print(f"[ROUTING] ⏸️ Rota via {via_neighbor} ignorada "
                      f"({metric:.2f}ms, melhoria {improvement_pct:.1f}% < "
                      f"{self.hysteresis_threshold*100:.0f}%)")
            
            return False
    
    def activate_route(self, flow_id: str, destination: str) -> bool:
        """
        Ativa rota e adiciona destino
        
        Chamado quando:
        1. Cliente local pede para receber stream
        2. Mensagem ACTIVATE passa por aqui
        
        Args:
            flow_id: ID do fluxo
            destination: Vizinho para onde enviar dados
        
        Returns:
            True se rota foi ativada/atualizada
        """
        if flow_id not in self.routes:
            print(f"[ROUTING] ERRO: Tentou ativar rota inexistente: {flow_id}")
            return False
        
        route = self.routes[flow_id]
        
        # Adiciona destino se ainda não existe
        if destination not in route.destinations:
            route.destinations.append(destination)
            print(f"[ROUTING] Destino adicionado: {destination} para {flow_id}")
        
        # Ativa rota
        if not route.active:
            route.active = True
            print(f"[ROUTING] Rota ativada: {route}")
        
        return True
    
    def deactivate_route(self, flow_id: str, destination: str = None):
        """
        Desativa rota ou remove destino específico
        
        Args:
            flow_id: ID do fluxo
            destination: Se especificado, remove apenas este destino
        """
        if flow_id not in self.routes:
            return
        
        route = self.routes[flow_id]
        
        if destination:
            # Remove destino específico
            if destination in route.destinations:
                route.destinations.remove(destination)
                print(f"[ROUTING] Destino removido: {destination} de {flow_id}")
            
            # Se não há mais destinos, desativa
            if not route.destinations:
                route.active = False
                print(f"[ROUTING] Rota desativada (sem destinos): {flow_id}")
        else:
            # Desativa completamente
            route.active = False
            route.destinations = []
            print(f"[ROUTING] Rota desativada: {flow_id}")

    def get_next_hop_for_deactivate(self, flow_id: str) -> str:
        """
        Obtém próximo hop para enviar DEACTIVATE de volta
        
        Nota: É o MESMO que get_next_hop (via_neighbor)
        """
        route = self.get_route(flow_id)
        if route:
            return route.via_neighbor
        return None
    
    def get_route(self, flow_id: str) -> Optional[RouteEntry]:
        """Obtém entrada de rota"""
        return self.routes.get(flow_id)
    
    def get_next_hop(self, flow_id: str) -> Optional[str]:
        """
        Obtém próximo hop para chegar à origem (servidor)
        
        Usado para enviar ACTIVATE de volta ao servidor
        """
        route = self.get_route(flow_id)
        if route:
            return route.via_neighbor
        return None
    
    def get_destinations(self, flow_id: str) -> List[str]:
        """
        Obtém lista de destinos para reencaminhar dados
        
        Usado ao receber STREAM_DATA
        """
        route = self.get_route(flow_id)
        if route and route.active:
            return route.destinations.copy()
        return []
    
    def is_route_active(self, flow_id: str) -> bool:
        """Verifica se rota está ativa"""
        route = self.get_route(flow_id)
        return route is not None and route.active
    
    def cleanup_old_routes(self, max_age_seconds: int = 60):
        """
        Remove rotas antigas (não atualizadas há muito tempo)
        
        Chamado periodicamente para limpar entradas obsoletas
        """
        now = time.time()
        to_remove = []
        
        for flow_id, route in self.routes.items():
            age = now - route.last_update
            if age > max_age_seconds:
                to_remove.append(flow_id)
                print(f"[ROUTING] Removendo rota antiga: {flow_id} (idade: {age:.1f}s)")
        
        for flow_id in to_remove:
            del self.routes[flow_id]
    
    def print_table(self):
        """Imprime tabela de rotas (debug)"""
        print(f"\n{'='*70}")
        print(f"ROUTING TABLE - Node {self.node_id}")
        print(f"  (Histerese: {self.hysteresis_threshold*100:.0f}%, "
              f"Jitter: ±{self.jitter_tolerance}ms)")
        print(f"{'='*75}")
        
        if not self.routes:
            print("  (vazia)")
        else:
            for flow_id, route in self.routes.items():
                print(f"  {route}")
        
        print(f"{'='*75}\n")
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas da tabela"""
        total = len(self.routes)
        active = sum(1 for r in self.routes.values() if r.active)
        
        return {
            "total_routes": total,
            "active_routes": active,
            "inactive_routes": total - active,
            "hysteresis_pct": self.hysteresis_threshold * 100,
            "jitter_tolerance_ms": self.jitter_tolerance
        }

    def process_neighbor_down(self, neighbor_id: str):
        """
        CORRIGIDO: Invalida a rota mas NÃO apaga os destinos.
        Isto permite que, quando a rota recuperar por outro lado, 
        o nó saiba que tem de voltar a pedir o stream.
        """
        # print(f"[ROUTING] Processando queda do vizinho: {neighbor_id}")
        
        # Não removemos chaves do dicionário durante a iteração,
        # por isso não precisamos de lista temporária para apagar rotas completas.
        
        for flow_id, route in self.routes.items():
            # 1. Se o vizinho era o nosso fornecedor (UPSTREAM)
            if route.via_neighbor == neighbor_id:
                print(f"[ROUTING] Rota para {flow_id} QUEBRADA (era via {neighbor_id}). Aguardando novo caminho...")
                # Não apagamos (del). Apenas marcamos como inválida/infinita
                route.metric = 9999.0  # Infinito
                route.via_neighbor = None # Sem fornecedor
                # route.active mantemos como True se tiver destinos
                # route.destinations MANTÉM-SE INTACTO!
            
            # 2. Se o vizinho era um cliente (DOWNSTREAM)
            elif neighbor_id in route.destinations:
                route.destinations.remove(neighbor_id)
                print(f"[ROUTING] {neighbor_id} removido dos destinos de {flow_id}")
                
                if not route.destinations:
                    route.active = False
# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def format_latency(latency_ms: float) -> str:
    """
    Formata latência para display
    
    Args:
        latency_ms: Latência em milissegundos
    
    Returns:
        String formatada (ex: "12.5ms", "150ms", "1.2s")
    """
    if latency_ms < 1000:
        return f"{latency_ms:.1f}ms"
    else:
        return f"{latency_ms/1000:.2f}s"

# ============================================================
# EXEMPLO DE USO
# ============================================================

if __name__ == "__main__":
    print("=== Teste da Tabela de Rotas ===\n")
    
    # Simula nó n10 recebendo ANNOUNCEs
    table = RoutingTable("n10")
    
    print("1. n10 recebe ANNOUNCE do servidor n16 (métrica 1)")
    table.update_route("stream1", "n16", 1, "n16", "msg1")
    table.print_table()
    
    print("2. n10 recebe ANNOUNCE duplicado (não processa)")
    updated = table.update_route("stream1", "n16", 1, "n16", "msg1")
    print(f"   Atualizado? {updated}\n")
    
    print("3. n10 recebe ANNOUNCE via n5 (métrica 2 - pior)")
    updated = table.update_route("stream1", "n16", 2, "n5", "msg2")
    print(f"   Atualizado? {updated}")
    table.print_table()
    
    print("4. Cliente n25 pede stream - ativa rota")
    table.activate_route("stream1", "n25")
    table.print_table()
    
    print("5. Cliente n5 também pede stream")
    table.activate_route("stream1", "n5")
    table.print_table()
    
    print("6. Obter destinos para reencaminhamento:")
    dests = table.get_destinations("stream1")
    print(f"   Destinos: {dests}\n")
    
    print("7. Cliente n25 desconecta")
    table.deactivate_route("stream1", "n25")
    table.print_table()
    
    print("8. Estatísticas:")
    stats = table.get_stats()
    print(f"   {stats}\n")