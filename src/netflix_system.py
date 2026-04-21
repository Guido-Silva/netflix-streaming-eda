"""
Sistema Integrador Netflix Streaming Platform
==============================================
Clase que integra todas las estructuras de datos en un sistema cohesivo
que simula el backend de una plataforma de streaming tipo Netflix.

Componentes integrados:
    - Priority Queue: Cola de procesamiento de eventos de usuarios
    - LRU Cache: Caché de contenido de video (top 1000)
    - Count-Min Sketch: Estadísticas de popularidad en tiempo real
    - Bloom Filter: Detección de bots y solicitudes duplicadas
    - Trie: Autocompletado de búsqueda
    - LSH + MinHash: Sistema de recomendaciones

Ejemplo de uso:
    >>> plataforma = NetflixStreamingPlatform()
    >>> plataforma.process_event("u001", "stream", "v001", {"quality": "HD"})
    >>> contenido = plataforma.stream_video("u001", "v001")
    >>> sugerencias = plataforma.search("str")
    >>> recomendaciones = plataforma.get_recommendations("v001")
"""

import time
import random
from typing import Dict, List, Optional, Any, Tuple

from .priority_queue import PriorityQueue, UserEvent, PRIORITY_PAYMENT, PRIORITY_PREMIUM, PRIORITY_STANDARD
from .lru_cache import LRUCache, VideoContent
from .count_min_sketch import CountMinSketch
from .bloom_filter import BloomFilter
from .trie import Trie
from .lsh_minhash import LSHMinHash


class NetflixStreamingPlatform:
    """
    Plataforma de streaming integrada que combina múltiples estructuras de datos.

    Simula el backend de Netflix usando cada estructura de datos para su
    caso de uso óptimo.

    Args:
        cache_capacity (int): Capacidad del LRU Cache. Default: 1000.
        cms_width (int): Ancho del Count-Min Sketch. Default: 2000.
        cms_depth (int): Profundidad del Count-Min Sketch. Default: 7.
        bloom_capacity (int): Capacidad del Bloom Filter. Default: 500000.
        bloom_error_rate (float): Tasa de falsos positivos del Bloom Filter. Default: 0.001.
        lsh_hashes (int): Número de hashes para LSH. Default: 100.
        lsh_bands (int): Número de bandas para LSH. Default: 20.
    """

    def __init__(
        self,
        cache_capacity: int = 1000,
        cms_width: int = 2000,
        cms_depth: int = 7,
        bloom_capacity: int = 500000,
        bloom_error_rate: float = 0.001,
        lsh_hashes: int = 100,
        lsh_bands: int = 20,
    ):
        """Inicializa todos los componentes de la plataforma."""
        # Cola de prioridad para eventos de usuarios
        self.cola_eventos = PriorityQueue()

        # Caché LRU para contenido de video
        self.cache = LRUCache(capacity=cache_capacity)

        # Count-Min Sketch para estadísticas de popularidad
        self.sketch_popularidad = CountMinSketch(width=cms_width, depth=cms_depth)

        # Bloom Filter para detección de bots y duplicados
        self.filtro_bots = BloomFilter(capacity=bloom_capacity, error_rate=bloom_error_rate)

        # Trie para autocompletado de búsqueda
        self.trie_busqueda = Trie()

        # LSH + MinHash para recomendaciones
        self.motor_recomendaciones = LSHMinHash(num_hashes=lsh_hashes, num_bands=lsh_bands)

        # Estadísticas globales del sistema
        self._stats_sistema = {
            "total_eventos_procesados": 0,
            "total_reproducciones": 0,
            "total_busquedas": 0,
            "total_bots_detectados": 0,
            "total_recomendaciones_generadas": 0,
            "tiempo_inicio": time.time(),
        }

        # Catálogo de videos (en producción vendría de una BD)
        self._catalogo: Dict[str, VideoContent] = {}

        # Co-watching patterns para LSH (usuario → conjunto de videos vistos)
        self._historial_usuarios: Dict[str, set] = {}

        # Video → conjunto de usuarios (para LSH)
        self._usuarios_por_video: Dict[str, set] = {}

    def cargar_catalogo(self, videos: List[VideoContent]) -> None:
        """
        Carga el catálogo de videos al sistema.

        Indexa cada video en el Trie para autocompletado.

        Args:
            videos (list): Lista de VideoContent a cargar.
        """
        for video in videos:
            self._catalogo[video.video_id] = video
            # Indexar en Trie con popularidad de vistas
            self.trie_busqueda.insert(video.title, popularity=video.views)

    def process_event(
        self,
        user_id: str,
        event_type: str,
        video_id: Optional[str] = None,
        data: Optional[Dict] = None,
        user_type: str = "standard"
    ) -> Dict[str, Any]:
        """
        Procesa un evento de usuario usando la cola de prioridad.

        Determina la prioridad según el tipo de evento y usuario,
        y encola el evento para procesamiento.

        Args:
            user_id (str): ID del usuario.
            event_type (str): Tipo de evento ('stream', 'search', 'payment', 'login').
            video_id (str, optional): ID del video relacionado.
            data (dict, optional): Datos adicionales del evento.
            user_type (str): Tipo de usuario ('premium', 'standard', 'payment').

        Returns:
            dict: Información sobre el evento encolado.
        """
        if data is None:
            data = {}
        if video_id:
            data["video_id"] = video_id

        # Determinar prioridad
        if event_type == "payment":
            prioridad = PRIORITY_PAYMENT
        elif user_type == "premium":
            prioridad = PRIORITY_PREMIUM
        else:
            prioridad = PRIORITY_STANDARD

        evento = UserEvent(
            user_id=user_id,
            event_type=event_type,
            priority=prioridad,
            data=data
        )
        self.cola_eventos.push(evento)
        self._stats_sistema["total_eventos_procesados"] += 1

        return {
            "evento_encolado": str(evento),
            "posicion_estimada": self.cola_eventos.size(),
            "prioridad": prioridad,
        }

    def process_next_event(self) -> Optional[Dict[str, Any]]:
        """
        Procesa el siguiente evento de la cola de prioridad.

        Returns:
            dict: Resultado del procesamiento, o None si la cola está vacía.
        """
        evento = self.cola_eventos.pop()
        if not evento:
            return None

        resultado = {
            "evento": str(evento),
            "procesado_en": time.time(),
        }

        # Enrutar evento según tipo
        if evento.event_type == "stream" and "video_id" in evento.data:
            contenido = self.stream_video(evento.user_id, evento.data["video_id"])
            resultado["contenido"] = str(contenido) if contenido else "No encontrado"

        return resultado

    def stream_video(
        self,
        user_id: str,
        video_id: str
    ) -> Optional[VideoContent]:
        """
        Reproduce un video para un usuario.

        Flujo:
        1. Verificar si la solicitud es de un bot (Bloom Filter)
        2. Buscar el video en el caché LRU
        3. Si no está en caché, buscarlo en el catálogo
        4. Actualizar estadísticas en Count-Min Sketch
        5. Actualizar patrones de co-watching para recomendaciones

        Args:
            user_id (str): ID del usuario que solicita el video.
            video_id (str): ID del video a reproducir.

        Returns:
            VideoContent: El contenido del video, o None si es bot/no existe.
        """
        # 1. Detección de bots (verificar si la IP/usuario ya fue marcado)
        solicitud_clave = f"{user_id}:{video_id}:{int(time.time() // 60)}"  # Por minuto
        if self.filtro_bots.contains(solicitud_clave):
            self._stats_sistema["total_bots_detectados"] += 1
            return None  # Posible bot o solicitud duplicada
        self.filtro_bots.add(solicitud_clave)

        # 2. Buscar en caché LRU
        contenido = self.cache.get(video_id)

        # 3. Si no está en caché, buscar en catálogo
        if contenido is None and video_id in self._catalogo:
            contenido = self._catalogo[video_id]
            self.cache.put(video_id, contenido)

        if contenido is None:
            return None

        # 4. Actualizar estadísticas de popularidad
        self.sketch_popularidad.update(video_id)
        self._stats_sistema["total_reproducciones"] += 1

        # 5. Actualizar co-watching patterns para LSH
        if user_id not in self._historial_usuarios:
            self._historial_usuarios[user_id] = set()
        self._historial_usuarios[user_id].add(video_id)

        if video_id not in self._usuarios_por_video:
            self._usuarios_por_video[video_id] = set()
        self._usuarios_por_video[video_id].add(user_id)

        return contenido

    def search(self, prefix: str, max_results: int = 10) -> List[Tuple[str, int]]:
        """
        Realiza una búsqueda con autocompletado usando el Trie.

        Args:
            prefix (str): Prefijo de búsqueda ingresado por el usuario.
            max_results (int): Número máximo de resultados. Default: 10.

        Returns:
            list: Lista de tuplas (título, popularidad) ordenadas por popularidad.
        """
        self._stats_sistema["total_busquedas"] += 1
        return self.trie_busqueda.autocomplete(prefix, max_suggestions=max_results)

    def get_recommendations(
        self,
        video_id: str,
        k: int = 10
    ) -> List[Tuple[str, float]]:
        """
        Genera recomendaciones de videos similares usando LSH + MinHash.

        Si el video tiene suficiente historial de usuarios, actualiza el
        índice LSH y busca videos con patrones de co-watching similares.

        Args:
            video_id (str): ID del video base para las recomendaciones.
            k (int): Número máximo de recomendaciones. Default: 10.

        Returns:
            list: Lista de tuplas (video_id, similitud) ordenadas de mayor a menor.
        """
        # Actualizar índice LSH con datos recientes
        if video_id in self._usuarios_por_video:
            self.motor_recomendaciones.add_video(
                video_id, self._usuarios_por_video[video_id]
            )

        self._stats_sistema["total_recomendaciones_generadas"] += 1
        similares = self.motor_recomendaciones.find_similar(video_id, threshold=0.1)
        return similares[:k]

    def rebuild_lsh_index(self) -> None:
        """
        Reconstruye el índice LSH con todos los datos de co-watching actuales.

        Se recomienda llamar periódicamente para mantener el índice actualizado.
        """
        for vid_id, usuarios in self._usuarios_por_video.items():
            if len(usuarios) >= 5:  # Solo indexar si tiene suficientes vistas
                self.motor_recomendaciones.add_video(vid_id, usuarios)

    def get_trending(self, k: int = 10) -> List[Tuple[str, int]]:
        """
        Retorna los K videos más populares actualmente.

        Usa el Count-Min Sketch para obtener las estimaciones de frecuencia.

        Args:
            k (int): Número de videos trending a retornar. Default: 10.

        Returns:
            list: Lista de tuplas (video_id, reproducciones_estimadas).
        """
        return self.sketch_popularidad.top_k(k)

    def get_stats(self) -> Dict[str, Any]:
        """
        Retorna métricas completas de todos los componentes del sistema.

        Returns:
            dict: Estadísticas detalladas de cada componente.
        """
        tiempo_activo = time.time() - self._stats_sistema["tiempo_inicio"]

        return {
            "sistema": {
                **self._stats_sistema,
                "tiempo_activo_seg": round(tiempo_activo, 2),
                "eventos_en_cola": self.cola_eventos.size(),
                "videos_en_catalogo": len(self._catalogo),
                "usuarios_activos": len(self._historial_usuarios),
            },
            "cache_lru": self.cache.get_stats(),
            "count_min_sketch": self.sketch_popularidad.get_stats(),
            "bloom_filter": self.filtro_bots.get_info(),
            "trie": {
                "total_titulos": self.trie_busqueda.size(),
            },
            "lsh": self.motor_recomendaciones.get_stats(),
        }

    def __repr__(self) -> str:
        return f"NetflixStreamingPlatform(videos={len(self._catalogo)}, cache={self.cache})"


def demo():
    """
    Demostración completa end-to-end del sistema de streaming Netflix.
    """
    print("=" * 70)
    print("   DEMO: Sistema Completo Netflix Streaming Platform")
    print("=" * 70)

    # Inicializar plataforma
    plataforma = NetflixStreamingPlatform(
        cache_capacity=100,
        cms_width=500,
        cms_depth=5,
        bloom_capacity=10000,
    )

    # Cargar catálogo
    print("\n📚 Cargando catálogo de videos...")
    catalogo = [
        VideoContent("v001", "Stranger Things", "Drama", 89_000_000, 1200.0),
        VideoContent("v002", "Squid Game", "Thriller", 111_000_000, 850.0),
        VideoContent("v003", "La Casa de Papel", "Thriller", 75_000_000, 900.0),
        VideoContent("v004", "The Crown", "Drama", 60_000_000, 1400.0),
        VideoContent("v005", "Dark", "Sci-Fi", 40_000_000, 950.0),
        VideoContent("v006", "Narcos", "Drama", 45_000_000, 800.0),
        VideoContent("v007", "Black Mirror", "Sci-Fi", 32_000_000, 750.0),
        VideoContent("v008", "Bridgerton", "Romance", 55_000_000, 1100.0),
        VideoContent("v009", "Lupin", "Acción", 35_000_000, 700.0),
        VideoContent("v010", "Peaky Blinders", "Drama", 48_000_000, 850.0),
    ]
    plataforma.cargar_catalogo(catalogo)
    print(f"  ✅ {len(catalogo)} videos cargados")

    # Simular eventos de usuarios
    print("\n👥 Procesando eventos de usuarios...")
    usuarios_premium = ["u001", "u002", "u003"]
    usuarios_standard = ["u004", "u005", "u006", "u007", "u008"]
    ids_videos = [v.video_id for v in catalogo]

    # Agregar eventos a la cola
    eventos = [
        ("u001", "payment", None, {"monto": 15.99}, "premium"),
        ("u004", "stream", "v002", {}, "standard"),
        ("u002", "stream", "v001", {}, "premium"),
        ("u005", "search", None, {"query": "dark"}, "standard"),
        ("u003", "stream", "v003", {}, "premium"),
        ("u001", "payment", None, {"monto": 9.99}, "premium"),
    ]

    for user_id, event_type, video_id, data, user_type in eventos:
        resultado = plataforma.process_event(user_id, event_type, video_id, data, user_type)
        print(f"  + Encolado: {resultado['evento_encolado'][:60]}...")

    print(f"\n  📊 Eventos en cola: {plataforma.cola_eventos.size()}")

    # Simular reproducciones
    print("\n🎬 Simulando 500 reproducciones...")
    todos_usuarios = usuarios_premium + usuarios_standard
    import random
    random.seed(42)

    # Distribución Zipf para popularidad
    for _ in range(500):
        user_id = random.choice(todos_usuarios)
        # Videos populares tienen más probabilidad
        pesos = [1/(i+1) for i in range(len(ids_videos))]
        video_id = random.choices(ids_videos, weights=pesos)[0]
        plataforma.stream_video(user_id, video_id)

    # Reconstruir índice LSH
    plataforma.rebuild_lsh_index()

    # Mostrar trending
    print("\n🔥 Top-5 Videos Trending:")
    for i, (vid, freq) in enumerate(plataforma.get_trending(5), 1):
        video_info = plataforma._catalogo.get(vid)
        titulo = video_info.title if video_info else vid
        print(f"  {i}. {titulo}: ~{freq} reproducciones")

    # Demostrar búsqueda
    print("\n🔍 Prueba de Autocompletado:")
    for prefijo in ["str", "the", "bl"]:
        resultados = plataforma.search(prefijo, max_results=3)
        print(f"  '{prefijo}' → {[r[0] for r in resultados]}")

    # Mostrar estadísticas
    print("\n📊 Estadísticas del Sistema:")
    stats = plataforma.get_stats()
    print(f"\n  [Sistema]")
    for k, v in stats["sistema"].items():
        print(f"    {k}: {v}")
    print(f"\n  [Caché LRU]")
    for k, v in stats["cache_lru"].items():
        print(f"    {k}: {v}")
    print(f"\n  [Bloom Filter]")
    for k, v in stats["bloom_filter"].items():
        print(f"    {k}: {v}")


if __name__ == "__main__":
    demo()
