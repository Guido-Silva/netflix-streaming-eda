"""
Cola de Prioridad (Priority Queue) basada en Heap
=================================================
Módulo para gestionar la cola de procesamiento de eventos de usuarios
en una plataforma de streaming tipo Netflix.

Prioridades:
    - PAYMENT (0): Pagos y suscripciones — máxima prioridad
    - PREMIUM (1): Usuarios premium
    - STANDARD (2): Usuarios estándar

Complejidad:
    - push: O(log n)
    - pop: O(log n)
    - peek: O(1)
    - size: O(1)
"""

import heapq
import time
from dataclasses import dataclass, field
from typing import Any, Optional


# Constantes de prioridad
PRIORITY_PAYMENT = 0   # Máxima prioridad (pagos)
PRIORITY_PREMIUM = 1   # Alta prioridad (usuarios premium)
PRIORITY_STANDARD = 2  # Prioridad normal (usuarios estándar)


@dataclass
class UserEvent:
    """
    Representa un evento de usuario en la plataforma de streaming.

    Atributos:
        user_id (str): Identificador único del usuario.
        event_type (str): Tipo de evento ('stream', 'search', 'payment', 'login', etc.).
        priority (int): Prioridad del evento (0=PAYMENT, 1=PREMIUM, 2=STANDARD).
        timestamp (float): Marca de tiempo del evento (para desempate).
        data (dict): Datos adicionales del evento.
    """
    user_id: str
    event_type: str
    priority: int
    timestamp: float = field(default_factory=time.time)
    data: dict = field(default_factory=dict)

    def __lt__(self, other: "UserEvent") -> bool:
        """Comparación para el heap: primero por prioridad, luego por timestamp."""
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.timestamp < other.timestamp

    def __eq__(self, other: "UserEvent") -> bool:
        """Igualdad basada en user_id y timestamp."""
        return self.user_id == other.user_id and self.timestamp == other.timestamp

    def __repr__(self) -> str:
        nombres_prioridad = {0: "PAGO", 1: "PREMIUM", 2: "ESTÁNDAR"}
        return (f"UserEvent(usuario={self.user_id}, tipo={self.event_type}, "
                f"prioridad={nombres_prioridad.get(self.priority, self.priority)})")


class PriorityQueue:
    """
    Cola de prioridad basada en min-heap para procesar eventos de usuarios.

    Los eventos con menor valor de prioridad se procesan primero:
        - 0 (PAYMENT) → procesado primero
        - 1 (PREMIUM) → segunda prioridad
        - 2 (STANDARD) → tercera prioridad

    En caso de empate en prioridad, se usa el timestamp (FIFO dentro de
    la misma prioridad).

    Ejemplo de uso:
        >>> pq = PriorityQueue()
        >>> evento = UserEvent("u1", "payment", PRIORITY_PAYMENT)
        >>> pq.push(evento)
        >>> evento_procesado = pq.pop()
    """

    def __init__(self):
        """Inicializa la cola de prioridad vacía."""
        self._heap = []
        self._contador = 0  # Contador para desempate (garantiza estabilidad)

    def push(self, evento: UserEvent) -> None:
        """
        Inserta un nuevo evento en la cola de prioridad.

        Complejidad: O(log n)

        Args:
            evento (UserEvent): El evento a insertar.
        """
        # Usamos (priority, contador, evento) para garantizar orden estable
        heapq.heappush(self._heap, (evento.priority, evento.timestamp, self._contador, evento))
        self._contador += 1

    def pop(self) -> Optional[UserEvent]:
        """
        Extrae y retorna el evento de mayor prioridad.

        Complejidad: O(log n)

        Returns:
            UserEvent: El evento con mayor prioridad, o None si está vacía.
        """
        if self.is_empty():
            return None
        _, _, _, evento = heapq.heappop(self._heap)
        return evento

    def peek(self) -> Optional[UserEvent]:
        """
        Retorna el evento de mayor prioridad sin extraerlo.

        Complejidad: O(1)

        Returns:
            UserEvent: El evento con mayor prioridad, o None si está vacía.
        """
        if self.is_empty():
            return None
        _, _, _, evento = self._heap[0]
        return evento

    def size(self) -> int:
        """
        Retorna el número de eventos en la cola.

        Complejidad: O(1)

        Returns:
            int: Número de elementos en la cola.
        """
        return len(self._heap)

    def is_empty(self) -> bool:
        """
        Verifica si la cola está vacía.

        Complejidad: O(1)

        Returns:
            bool: True si la cola está vacía, False en caso contrario.
        """
        return len(self._heap) == 0

    def __repr__(self) -> str:
        return f"PriorityQueue(tamaño={self.size()})"


def demo():
    """
    Demostración del uso de la Priority Queue con eventos de Netflix.
    """
    print("=" * 60)
    print("   DEMO: Cola de Prioridad - Plataforma Netflix")
    print("=" * 60)

    pq = PriorityQueue()

    # Simular llegada desordenada de eventos
    print("\n📥 Insertando eventos en orden aleatorio:")

    eventos = [
        UserEvent("u005", "stream", PRIORITY_STANDARD, data={"video": "The Crown"}),
        UserEvent("u001", "payment", PRIORITY_PAYMENT, data={"monto": 15.99}),
        UserEvent("u003", "search", PRIORITY_PREMIUM, data={"query": "action"}),
        UserEvent("u002", "stream", PRIORITY_PREMIUM, data={"video": "Stranger Things"}),
        UserEvent("u006", "login", PRIORITY_STANDARD, data={}),
        UserEvent("u004", "payment", PRIORITY_PAYMENT, data={"monto": 9.99}),
    ]

    for evento in eventos:
        pq.push(evento)
        print(f"  + Insertado: {evento}")

    print(f"\n📊 Total eventos en cola: {pq.size()}")
    print(f"🔍 Próximo a procesar: {pq.peek()}")

    print("\n🔄 Procesando eventos en orden de prioridad:")
    orden = 1
    while not pq.is_empty():
        evento = pq.pop()
        print(f"  {orden}. Procesando: {evento}")
        orden += 1

    print("\n✅ Cola vacía:", pq.is_empty())

    # Experimento de rendimiento
    print("\n" + "=" * 60)
    print("⏱️  Experimento de Rendimiento")
    print("=" * 60)

    import random
    tamaños = [100, 1000, 10000, 100000]

    for n in tamaños:
        pq = PriorityQueue()
        inicio = time.perf_counter()

        for i in range(n):
            prioridad = random.choice([PRIORITY_PAYMENT, PRIORITY_PREMIUM, PRIORITY_STANDARD])
            evento = UserEvent(f"u{i}", "stream", prioridad)
            pq.push(evento)

        tiempo = time.perf_counter() - inicio
        print(f"  n={n:>7}: {tiempo:.4f}s ({tiempo/n*1000:.4f}ms por operación)")


if __name__ == "__main__":
    demo()
