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
    - metric: Custo do caminho (ex: número de saltos)
    - via_neighbor: Vizinho pelo qual recebi o ANNOUNCE
    - destinations: Lista de vizinhos para onde reencaminhar
    - active: Se a rota está ativa (cliente pediu)
    """
    flow_id: str                    # ID do fluxo
    origin: str                     # Nó origem (servidor)
    metric: int                     # Custo (nº saltos, latência, etc)
    via_neighbor: str               # De onde veio (IP ou node_id)
    destinations: List[str] = field(default_factory=list)  # Para onde enviar
    active: bool = False            # Se está ativa
    last_update: float = field(default_factory=time.time)  # Timestamp
    
    def __str__(self):
        status = "ACTIVE" if self.active else "INACTIVE"
        dests = ', '.join(self.destinations) if self.destinations else "none"
        return (f"[{status}] {self.flow_id}: from {self.origin} "
                f"via {self.via_neighbor} (metric={self.metric}) -> [{dests}]")


class RoutingTable:
    """
    Tabela de rotas do nó overlay
    
    Mantém uma entrada por fluxo, com informação de:
    - Melhor caminho para a origem
    - Vizinhos para onde reencaminhar dados
    """
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.routes: Dict[str, RouteEntry] = {}  # {flow_id: RouteEntry}
        self.seen_announces: set = set()  # Para evitar loops
    
    # Em Claude/routing_table.py

    def update_route(
        self,
        flow_id: str,
        origin: str,
        metric: int,
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
        
        # 1. Se a nova rota é MELHOR (menor métrica) -> Atualiza tudo
        if metric < current.metric:
            current.origin = origin
            current.metric = metric
            current.via_neighbor = via_neighbor
            current.last_update = time.time()
            print(f"[ROUTING] Rota melhorada: {current}")
            return True
            
        # 2. Se é a MESMA rota vinda do MESMO vizinho -> Refresh (CRÍTICO PARA PROPAGAÇÃO)
        elif metric == current.metric and via_neighbor == current.via_neighbor:
            current.last_update = time.time()
            return True 
        
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
        print(f"{'='*70}")
        
        if not self.routes:
            print("  (vazia)")
        else:
            for flow_id, route in self.routes.items():
                print(f"  {route}")
        
        print(f"{'='*70}\n")
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas da tabela"""
        total = len(self.routes)
        active = sum(1 for r in self.routes.values() if r.active)
        
        return {
            "total_routes": total,
            "active_routes": active,
            "inactive_routes": total - active
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
                route.metric = 9999  # Infinito
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