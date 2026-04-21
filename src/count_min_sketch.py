"""
Count-Min Sketch (Conteo Probabilístico)
=========================================
Estructura de datos probabilística para estimar la frecuencia de elementos
en un flujo de datos masivo con consumo de memoria sublineal.

En una plataforma de streaming, se usa para contar reproducciones de videos
en tiempo real sin almacenar el conteo exacto de cada video.

Parámetros:
    - width (w): Número de columnas en la matriz de contadores.
    - depth (d): Número de filas / funciones hash.

Error garantizado: La estimación nunca es menor al valor real.
    P(estimación > real + ε×N) < δ
    donde ε = e/w y δ = e^(-d)

Complejidad:
    - update: O(d)
    - query: O(d)
    - top_k: O(n log k)
    - Espacio: O(w × d)
"""

import hashlib
import heapq
from typing import List, Tuple, Dict


class CountMinSketch:
    """
    Implementación del Count-Min Sketch para conteo de popularidad de videos.

    Usa d funciones hash independientes para actualizar d contadores por elemento.
    La estimación de frecuencia es el mínimo de los d contadores.

    Args:
        width (int): Número de columnas (afecta el error de estimación).
                     Mayor width → menor error. Default: 1000.
        depth (int): Número de filas / funciones hash (afecta la probabilidad de error).
                     Mayor depth → menor probabilidad de error. Default: 5.

    Ejemplo de uso:
        >>> cms = CountMinSketch(width=1000, depth=5)
        >>> cms.update("stranger_things_s1e1")
        >>> cms.update("stranger_things_s1e1")
        >>> cms.query("stranger_things_s1e1")
        2
    """

    def __init__(self, width: int = 1000, depth: int = 5):
        """
        Inicializa la matriz de contadores.

        Args:
            width (int): Número de columnas en la matriz.
            depth (int): Número de filas (funciones hash).
        """
        if width <= 0 or depth <= 0:
            raise ValueError("width y depth deben ser mayores a 0")

        self.width = width
        self.depth = depth

        # Matriz de contadores: depth filas × width columnas
        self._tabla = [[0] * width for _ in range(depth)]

        # Semillas para las funciones hash (una por fila)
        self._semillas = [i * 2654435761 + 1 for i in range(depth)]

        # Diccionario auxiliar para rastrear elementos (necesario para top-k)
        self._elementos: Dict[str, int] = {}

        # Contadores globales
        self._total_actualizaciones = 0
        self._elementos_unicos = 0

    def _hash(self, item: str, semilla: int) -> int:
        """
        Calcula el índice de columna para un elemento dado una semilla.

        Usa SHA-256 combinado con la semilla para generar funciones hash
        independientes.

        Args:
            item (str): El elemento a hashear.
            semilla (int): Semilla para hacer las funciones hash independientes.

        Returns:
            int: Índice de columna (0 a width-1).
        """
        clave = f"{semilla}:{item}".encode("utf-8")
        digest = hashlib.sha256(clave).hexdigest()
        return int(digest[:16], 16) % self.width

    def update(self, item: str, count: int = 1) -> None:
        """
        Incrementa el conteo de un elemento en la estructura.

        Actualiza los d contadores correspondientes al elemento.

        Complejidad: O(d)

        Args:
            item (str): El elemento a contar (ej: ID de video).
            count (int): Cantidad a agregar al conteo. Default: 1.
        """
        for i, semilla in enumerate(self._semillas):
            col = self._hash(item, semilla)
            self._tabla[i][col] += count

        # Actualizar tracking auxiliar para top-k
        self._elementos[item] = self._elementos.get(item, 0) + count
        self._total_actualizaciones += count

    def query(self, item: str) -> int:
        """
        Estima la frecuencia de un elemento.

        Retorna el mínimo de los d contadores correspondientes al elemento.
        La estimación es siempre ≥ al valor real (nunca subestima).

        Complejidad: O(d)

        Args:
            item (str): El elemento a consultar.

        Returns:
            int: Frecuencia estimada del elemento.
        """
        minimo = float('inf')
        for i, semilla in enumerate(self._semillas):
            col = self._hash(item, semilla)
            minimo = min(minimo, self._tabla[i][col])
        return int(minimo) if minimo != float('inf') else 0

    def top_k(self, k: int = 10) -> List[Tuple[str, int]]:
        """
        Retorna los K elementos con mayor frecuencia estimada.

        Usa el diccionario auxiliar para encontrar los más frecuentes.
        Las frecuencias son estimadas (pueden tener pequeño error positivo).

        Complejidad: O(n log k) donde n = elementos únicos vistos

        Args:
            k (int): Número de elementos top a retornar. Default: 10.

        Returns:
            list: Lista de tuplas (elemento, frecuencia) ordenadas de mayor a menor.
        """
        if not self._elementos:
            return []

        # Usar heap para encontrar los k mayores eficientemente
        top = heapq.nlargest(k, self._elementos.items(), key=lambda x: x[1])
        return top

    def reset(self) -> None:
        """
        Reinicia todos los contadores a cero.

        Útil para ventanas de tiempo (ej: reiniciar estadísticas cada hora).
        """
        self._tabla = [[0] * self.width for _ in range(self.depth)]
        self._elementos.clear()
        self._total_actualizaciones = 0
        self._elementos_unicos = 0

    def get_stats(self) -> dict:
        """
        Retorna estadísticas de la estructura.

        Returns:
            dict: Estadísticas incluyendo dimensiones y uso.
        """
        return {
            "width": self.width,
            "depth": self.depth,
            "celdas_totales": self.width * self.depth,
            "total_actualizaciones": self._total_actualizaciones,
            "elementos_unicos": len(self._elementos),
            "error_estimado_max": f"e/{self.width} = {2.718:.3f}/{self.width} ≈ {2.718/self.width:.6f}",
            "prob_error_max": f"e^(-{self.depth}) ≈ {2.718**(-self.depth):.6f}"
        }

    def __repr__(self) -> str:
        return (f"CountMinSketch(width={self.width}, depth={self.depth}, "
                f"elementos={len(self._elementos)})")


def demo():
    """
    Demostración del Count-Min Sketch con simulación de reproducciones en Netflix.
    """
    print("=" * 60)
    print("   DEMO: Count-Min Sketch - Plataforma Netflix")
    print("=" * 60)

    import random

    cms = CountMinSketch(width=500, depth=5)

    # Catálogo de videos con popularidades variables
    videos = {
        "stranger_things_s4": 1000,
        "squid_game_s1": 800,
        "la_casa_papel_s5": 600,
        "the_crown_s5": 400,
        "dark_s1": 200,
        "lupin_s2": 150,
        "narcos_s1": 100,
        "black_mirror_s1": 80,
        "mindhunter_s1": 60,
        "ozark_s4": 50,
    }

    print("\n📺 Simulando reproducciones de videos:")
    reproducciones_reales = {}

    for video, popularidad in videos.items():
        # Simular variación aleatoria en reproducciones
        reps = random.randint(int(popularidad * 0.8), int(popularidad * 1.2))
        reproducciones_reales[video] = reps
        cms.update(video, reps)

    # Agregar ruido con videos menos populares
    videos_extra = [f"video_niche_{i}" for i in range(100)]
    for v in videos_extra:
        reps = random.randint(1, 30)
        reproducciones_reales[v] = reps
        cms.update(v, reps)

    print(f"\n📊 Estadísticas de la estructura:")
    for k, v in cms.get_stats().items():
        print(f"  {k}: {v}")

    print("\n🔍 Comparación: Conteo Real vs Estimado:")
    print(f"  {'Video':<30} {'Real':>8} {'Estimado':>10} {'Error':>8}")
    print("  " + "-" * 60)
    for video, real in list(videos.items())[:10]:
        estimado = cms.query(video)
        error = estimado - reproducciones_reales[video]
        print(f"  {video:<30} {reproducciones_reales[video]:>8} {estimado:>10} {error:>+8}")

    print("\n🏆 Top-5 Videos más populares:")
    for i, (video, freq) in enumerate(cms.top_k(5), 1):
        print(f"  {i}. {video}: {freq:,} reproducciones")


if __name__ == "__main__":
    demo()
