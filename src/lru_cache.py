"""
LRU Cache (Least Recently Used Cache)
======================================
Módulo de caché inteligente para almacenar los videos más recientemente
accedidos en una plataforma de streaming tipo Netflix.

El caché mantiene en memoria los últimos N videos accedidos. Cuando está
lleno y se necesita espacio, evicta el video menos recientemente usado (LRU).

Complejidad:
    - get: O(1)
    - put: O(1)
    - contains: O(1)
    - get_stats: O(1)
    - most_recent: O(n)
"""

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
import time


@dataclass
class VideoContent:
    """
    Representa el contenido de un video en la plataforma.

    Atributos:
        video_id (str): Identificador único del video.
        title (str): Título del video.
        genre (str): Género del video.
        views (int): Número total de reproducciones.
        size_mb (float): Tamaño del video en megabytes.
    """
    video_id: str
    title: str
    genre: str
    views: int = 0
    size_mb: float = 0.0

    def __repr__(self) -> str:
        return f"Video(id={self.video_id}, título='{self.title}', género={self.genre}, vistas={self.views:,})"


class LRUCache:
    """
    Caché LRU (Least Recently Used) para videos de streaming.

    Implementa un caché de capacidad fija usando OrderedDict de Python,
    que mantiene el orden de inserción y permite mover elementos al final
    en O(1) — simulando el comportamiento LRU.

    La política de evicción elimina el elemento que fue accedido hace
    más tiempo (el "menos recientemente usado").

    Args:
        capacity (int): Capacidad máxima del caché. Default: 1000.

    Ejemplo de uso:
        >>> cache = LRUCache(capacity=1000)
        >>> video = VideoContent("v1", "Stranger Things", "Drama", 50000000, 850.0)
        >>> cache.put("v1", video)
        >>> contenido = cache.get("v1")
    """

    def __init__(self, capacity: int = 1000):
        """
        Inicializa el caché LRU.

        Args:
            capacity (int): Número máximo de videos a mantener en caché.
        """
        if capacity <= 0:
            raise ValueError("La capacidad debe ser mayor a 0")

        self.capacity = capacity
        self._cache: OrderedDict = OrderedDict()

        # Métricas de rendimiento
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._total_requests = 0

    def get(self, video_id: str) -> Optional[VideoContent]:
        """
        Obtiene el contenido de un video del caché.

        Si el video está en el caché (hit), lo mueve al final (más reciente)
        y retorna el contenido. Si no está (miss), retorna None.

        Complejidad: O(1)

        Args:
            video_id (str): Identificador del video a buscar.

        Returns:
            VideoContent: El contenido del video, o None si no está en caché.
        """
        self._total_requests += 1

        if video_id in self._cache:
            # Cache hit: mover al final (más recientemente usado)
            self._cache.move_to_end(video_id)
            self._hits += 1
            return self._cache[video_id]
        else:
            # Cache miss
            self._misses += 1
            return None

    def put(self, video_id: str, content: VideoContent) -> None:
        """
        Almacena un video en el caché.

        Si el video ya existe, actualiza su contenido y lo marca como
        el más reciente. Si el caché está lleno, evicta el menos
        recientemente usado antes de insertar el nuevo.

        Complejidad: O(1)

        Args:
            video_id (str): Identificador del video.
            content (VideoContent): Contenido del video a almacenar.
        """
        if video_id in self._cache:
            # Actualizar y marcar como más reciente
            self._cache.move_to_end(video_id)
            self._cache[video_id] = content
        else:
            # Insertar nuevo elemento
            if len(self._cache) >= self.capacity:
                # Evictar el menos recientemente usado (primer elemento)
                self._cache.popitem(last=False)
                self._evictions += 1

            self._cache[video_id] = content

    def contains(self, video_id: str) -> bool:
        """
        Verifica si un video está en el caché sin modificar el orden LRU.

        Complejidad: O(1)

        Args:
            video_id (str): Identificador del video a buscar.

        Returns:
            bool: True si el video está en caché, False en caso contrario.
        """
        return video_id in self._cache

    def get_stats(self) -> Dict[str, Any]:
        """
        Retorna métricas de rendimiento del caché.

        Complejidad: O(1)

        Returns:
            dict: Diccionario con hits, misses, hit_rate, evictions y uso actual.
        """
        hit_rate = (self._hits / self._total_requests * 100) if self._total_requests > 0 else 0.0

        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(hit_rate, 2),
            "evictions": self._evictions,
            "total_requests": self._total_requests,
            "uso_actual": len(self._cache),
            "capacidad": self.capacity,
            "ocupacion_pct": round(len(self._cache) / self.capacity * 100, 2)
        }

    def most_recent(self, n: int = 10) -> List[VideoContent]:
        """
        Retorna los N videos más recientemente accedidos.

        Complejidad: O(n)

        Args:
            n (int): Número de videos a retornar. Default: 10.

        Returns:
            list: Lista de VideoContent ordenada de más a menos reciente.
        """
        items = list(self._cache.values())
        return items[-n:][::-1]  # Últimos n, en orden inverso (más reciente primero)

    def reset_stats(self) -> None:
        """Reinicia las métricas de rendimiento sin vaciar el caché."""
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._total_requests = 0

    def clear(self) -> None:
        """Vacía el caché completamente."""
        self._cache.clear()
        self.reset_stats()

    def __len__(self) -> int:
        return len(self._cache)

    def __repr__(self) -> str:
        stats = self.get_stats()
        return (f"LRUCache(capacidad={self.capacity}, "
                f"uso={stats['uso_actual']}, "
                f"hit_rate={stats['hit_rate']}%)")


def demo():
    """
    Demostración del LRU Cache con simulación de accesos a videos Netflix.
    """
    print("=" * 60)
    print("   DEMO: LRU Cache - Plataforma Netflix")
    print("=" * 60)

    # Crear caché pequeño para visualizar evicción
    cache = LRUCache(capacity=5)

    # Catálogo de videos
    catalogo = [
        VideoContent("v001", "Stranger Things T4", "Drama", 89_000_000, 1200.0),
        VideoContent("v002", "Squid Game", "Thriller", 111_000_000, 850.0),
        VideoContent("v003", "The Crown T5", "Drama", 60_000_000, 1400.0),
        VideoContent("v004", "Narcos", "Drama", 45_000_000, 900.0),
        VideoContent("v005", "La Casa de Papel", "Thriller", 75_000_000, 800.0),
        VideoContent("v006", "Dark", "Sci-Fi", 40_000_000, 950.0),
        VideoContent("v007", "Lupin", "Acción", 35_000_000, 750.0),
    ]

    print("\n📥 Cargando videos al caché (capacidad=5):")
    for video in catalogo[:5]:
        cache.put(video.video_id, video)
        print(f"  + Agregado: {video.title} (ID: {video.video_id})")

    print(f"\n📊 Estado del caché: {len(cache)}/5 videos")

    print("\n🔍 Accediendo a 'Stranger Things T4' (v001):")
    resultado = cache.get("v001")
    print(f"  → {resultado}")

    print("\n📥 Insertando 'Dark' (v006) — debe evictar el LRU:")
    cache.put("v006", catalogo[5])
    print(f"  + Insertado: Dark")
    print(f"  ¿'Squid Game' (v002) sigue en caché? {cache.contains('v002')}")
    print(f"  ¿'Stranger Things' (v001) sigue en caché? {cache.contains('v001')}")

    print("\n📈 Simulación de 10,000 accesos con distribución Zipf:")
    import random

    cache_grande = LRUCache(capacity=100)
    ids_videos = [f"v{i:03d}" for i in range(500)]
    contenidos = {vid: VideoContent(vid, f"Video {vid}", "Drama") for vid in ids_videos}

    # Distribución Zipf: pocos videos muy populares
    pesos_zipf = [1.0 / (i + 1) for i in range(len(ids_videos))]
    suma = sum(pesos_zipf)
    pesos_zipf = [p / suma for p in pesos_zipf]

    for _ in range(10_000):
        vid = random.choices(ids_videos, weights=pesos_zipf)[0]
        if not cache_grande.get(vid):
            cache_grande.put(vid, contenidos[vid])

    stats = cache_grande.get_stats()
    print(f"  Hit Rate: {stats['hit_rate']}%")
    print(f"  Hits: {stats['hits']:,}")
    print(f"  Misses: {stats['misses']:,}")
    print(f"  Evictions: {stats['evictions']:,}")

    print("\n🎬 Videos más recientes en caché:")
    for video in cache_grande.most_recent(5):
        print(f"  - {video}")


if __name__ == "__main__":
    demo()
