"""
LSH + MinHash (Locality Sensitive Hashing con MinHash)
=======================================================
Sistema de recomendación de videos basado en similitud de patrones de visionado.

MinHash estima la similitud de Jaccard entre conjuntos de usuarios que vieron
cada video. LSH agrupa videos con firmas similares en "buckets" para encontrar
candidatos a similares sin comparar todos los pares.

Aplicación en Netflix:
    Si los usuarios que vieron "Stranger Things" también vieron "Dark",
    ambos videos tienen alta similitud de Jaccard en sus conjuntos de usuarios,
    por lo que el sistema los recomienda juntos.

Similitud de Jaccard:
    J(A, B) = |A ∩ B| / |A ∪ B|

Complejidad:
    - add_video: O(n × h) donde n = usuarios, h = num_hashes
    - find_similar: O(b × r) donde b = bandas, r = filas
    - jaccard_similarity: O(|A| + |B|)
"""

import hashlib
import random
from typing import Dict, List, Set, Tuple, Optional


class MinHash:
    """
    Genera firmas MinHash para estimar similitud de Jaccard entre conjuntos.

    Cada firma es un vector de num_hashes valores, donde cada valor es
    el mínimo de aplicar una función hash a todos los elementos del conjunto.

    La similitud de Jaccard se estima como:
        J(A, B) ≈ (número de posiciones con igual firma) / num_hashes

    Args:
        num_hashes (int): Número de funciones hash (mayor → más preciso). Default: 100.
    """

    def __init__(self, num_hashes: int = 100):
        """
        Inicializa MinHash con num_hashes funciones hash.

        Args:
            num_hashes (int): Número de permutaciones hash a usar.
        """
        self.num_hashes = num_hashes
        # Generar parámetros aleatorios para las funciones hash lineales
        # h(x) = (a*x + b) mod p donde p es primo grande
        self._primo = (1 << 31) - 1  # 2^31 - 1 (Mersenne prime)
        rng = random.Random(42)  # Semilla fija para reproducibilidad
        self._a = [rng.randint(1, self._primo - 1) for _ in range(num_hashes)]
        self._b = [rng.randint(0, self._primo - 1) for _ in range(num_hashes)]

    def _hash_element(self, element: str) -> int:
        """
        Convierte un elemento (string) a un entero para las funciones hash.

        Args:
            element (str): El elemento a hashear.

        Returns:
            int: Valor entero del elemento.
        """
        digest = hashlib.sha256(element.encode()).hexdigest()
        return int(digest[:8], 16)

    def compute_signature(self, element_set: Set[str]) -> List[int]:
        """
        Calcula la firma MinHash de un conjunto de elementos.

        Para cada función hash h_i, calcula el mínimo de h_i(x) para x en el conjunto.

        Complejidad: O(|conjunto| × num_hashes)

        Args:
            element_set (set): Conjunto de elementos (IDs de usuarios).

        Returns:
            list: Vector de num_hashes valores (la firma MinHash).
        """
        # Inicializar firma con infinito
        firma = [float('inf')] * self.num_hashes

        for elemento in element_set:
            x = self._hash_element(elemento)
            for i in range(self.num_hashes):
                valor_hash = (self._a[i] * x + self._b[i]) % self._primo
                if valor_hash < firma[i]:
                    firma[i] = valor_hash

        return [int(v) if v != float('inf') else 0 for v in firma]

    def similarity(self, firma1: List[int], firma2: List[int]) -> float:
        """
        Estima la similitud de Jaccard entre dos firmas MinHash.

        Args:
            firma1 (list): Firma MinHash del primer conjunto.
            firma2 (list): Firma MinHash del segundo conjunto.

        Returns:
            float: Similitud estimada (0.0 a 1.0).
        """
        if len(firma1) != len(firma2):
            raise ValueError("Las firmas deben tener la misma longitud")

        coincidencias = sum(1 for a, b in zip(firma1, firma2) if a == b)
        return coincidencias / self.num_hashes


class LSHMinHash:
    """
    Sistema LSH + MinHash para encontrar videos similares eficientemente.

    Divide la firma MinHash en 'num_bands' bandas de 'rows_per_band' filas cada una.
    Dos videos son candidatos si al menos UNA banda tiene exactamente la misma
    sub-firma (colisión en el mismo bucket).

    La probabilidad de que dos videos con similitud s sean candidatos es:
        P = 1 - (1 - s^r)^b
    donde r = filas por banda, b = número de bandas.

    Args:
        num_hashes (int): Número total de funciones hash. Default: 100.
        num_bands (int): Número de bandas. Default: 20.
    """

    def __init__(self, num_hashes: int = 100, num_bands: int = 20):
        """
        Inicializa LSH con MinHash.

        Args:
            num_hashes (int): Tamaño total de la firma MinHash.
            num_bands (int): Número de bandas para LSH.
        """
        if num_hashes % num_bands != 0:
            # Ajustar num_bands para que divida exactamente a num_hashes
            num_bands = max(1, num_hashes // (num_hashes // num_bands))

        self.num_hashes = num_hashes
        self.num_bands = num_bands
        self.rows_per_band = num_hashes // num_bands

        # Motor MinHash
        self._minhash = MinHash(num_hashes=num_hashes)

        # Almacenamiento de firmas por video
        self._firmas: Dict[str, List[int]] = {}

        # Tabla de bandas: {(banda, hash_subbanda): [video_ids]}
        self._tablas_bandas: List[Dict[int, List[str]]] = [
            {} for _ in range(num_bands)
        ]

        # Almacenamiento del conjunto original (para cálculo exacto)
        self._conjuntos: Dict[str, Set[str]] = {}

    def add_video(self, video_id: str, user_set: Set[str]) -> None:
        """
        Agrega un video al índice LSH con su conjunto de usuarios que lo vieron.

        Complejidad: O(|user_set| × num_hashes)

        Args:
            video_id (str): Identificador único del video.
            user_set (set): Conjunto de IDs de usuarios que vieron el video.
        """
        # Calcular firma MinHash
        firma = self._minhash.compute_signature(user_set)
        self._firmas[video_id] = firma
        self._conjuntos[video_id] = user_set

        # Indexar en bandas LSH
        for banda in range(self.num_bands):
            inicio = banda * self.rows_per_band
            fin = inicio + self.rows_per_band
            sub_firma = tuple(firma[inicio:fin])

            # Hash de la sub-firma para usarla como clave del bucket
            bucket_key = hash(sub_firma)

            if bucket_key not in self._tablas_bandas[banda]:
                self._tablas_bandas[banda][bucket_key] = []

            if video_id not in self._tablas_bandas[banda][bucket_key]:
                self._tablas_bandas[banda][bucket_key].append(video_id)

    def find_similar(self, video_id: str, threshold: float = 0.5) -> List[Tuple[str, float]]:
        """
        Encuentra videos similares al video dado usando LSH.

        Primero usa LSH para encontrar candidatos (O(b×r)), luego verifica
        la similitud exacta solo entre candidatos.

        Complejidad: O(b×r + |candidatos|×h)

        Args:
            video_id (str): ID del video para el cual buscar similares.
            threshold (float): Umbral mínimo de similitud (0.0 a 1.0). Default: 0.5.

        Returns:
            list: Lista de tuplas (video_id, similitud) ordenadas de mayor a menor.
        """
        if video_id not in self._firmas:
            return []

        firma_query = self._firmas[video_id]
        candidatos = set()

        # Encontrar candidatos por colisión de bandas
        for banda in range(self.num_bands):
            inicio = banda * self.rows_per_band
            fin = inicio + self.rows_per_band
            sub_firma = tuple(firma_query[inicio:fin])
            bucket_key = hash(sub_firma)

            if bucket_key in self._tablas_bandas[banda]:
                for vid in self._tablas_bandas[banda][bucket_key]:
                    if vid != video_id:
                        candidatos.add(vid)

        # Verificar similitud exacta (por firma MinHash) entre candidatos
        resultados = []
        for cand_id in candidatos:
            similitud = self._minhash.similarity(firma_query, self._firmas[cand_id])
            if similitud >= threshold:
                resultados.append((cand_id, round(similitud, 4)))

        resultados.sort(key=lambda x: -x[1])
        return resultados

    def jaccard_similarity(self, set1: Set[str], set2: Set[str]) -> float:
        """
        Calcula la similitud de Jaccard exacta entre dos conjuntos.

        J(A, B) = |A ∩ B| / |A ∪ B|

        Complejidad: O(|A| + |B|)

        Args:
            set1 (set): Primer conjunto.
            set2 (set): Segundo conjunto.

        Returns:
            float: Similitud de Jaccard (0.0 a 1.0).
        """
        if not set1 and not set2:
            return 1.0
        interseccion = len(set1 & set2)
        union = len(set1 | set2)
        return interseccion / union if union > 0 else 0.0

    def get_stats(self) -> dict:
        """
        Retorna estadísticas del índice LSH.

        Returns:
            dict: Información sobre el índice.
        """
        total_buckets = sum(len(tabla) for tabla in self._tablas_bandas)
        return {
            "videos_indexados": len(self._firmas),
            "num_hashes": self.num_hashes,
            "num_bands": self.num_bands,
            "rows_per_band": self.rows_per_band,
            "total_buckets": total_buckets,
        }

    def __repr__(self) -> str:
        return (f"LSHMinHash(videos={len(self._firmas)}, "
                f"hashes={self.num_hashes}, bandas={self.num_bands})")


def demo():
    """
    Demostración de LSH + MinHash con patrones de co-visionado en Netflix.
    """
    print("=" * 60)
    print("   DEMO: LSH + MinHash - Recomendaciones Netflix")
    print("=" * 60)

    import random
    random.seed(42)

    lsh = LSHMinHash(num_hashes=100, num_bands=20)

    # Crear usuarios
    todos_usuarios = [f"user_{i}" for i in range(200)]

    # Definir videos con grupos de usuarios superpuestos para simular similitudes
    print("\n📺 Creando catálogo con patrones de co-visionado:")

    videos = {
        "stranger_things_s4": set(random.sample(todos_usuarios, 100)),
        "dark_s1": None,        # Será similar a Stranger Things (sci-fi)
        "squid_game_s1": set(random.sample(todos_usuarios, 90)),
        "alice_in_borderland": None,  # Será similar a Squid Game
        "la_casa_papel_s5": set(random.sample(todos_usuarios, 80)),
        "money_heist_berlin": None,   # Muy similar a La Casa de Papel
        "the_crown_s5": set(random.sample(todos_usuarios, 70)),
        "bridgerton_s2": set(random.sample(todos_usuarios, 65)),
        "narcos_s1": set(random.sample(todos_usuarios, 60)),
    }

    # Crear videos similares compartiendo usuarios base
    base_st = videos["stranger_things_s4"]
    videos["dark_s1"] = base_st.copy()
    # Cambiar 20% de los usuarios (80% similitud)
    quitar = set(random.sample(list(base_st), 20))
    agregar = set(random.sample([u for u in todos_usuarios if u not in base_st], 20))
    videos["dark_s1"] = (base_st - quitar) | agregar

    base_sq = videos["squid_game_s1"]
    videos["alice_in_borderland"] = base_sq.copy()
    quitar = set(random.sample(list(base_sq), 15))
    agregar = set(random.sample([u for u in todos_usuarios if u not in base_sq], 15))
    videos["alice_in_borderland"] = (base_sq - quitar) | agregar

    base_lcdp = videos["la_casa_papel_s5"]
    videos["money_heist_berlin"] = base_lcdp.copy()
    quitar = set(random.sample(list(base_lcdp), 8))
    agregar = set(random.sample([u for u in todos_usuarios if u not in base_lcdp], 8))
    videos["money_heist_berlin"] = (base_lcdp - quitar) | agregar

    # Indexar todos los videos
    for vid_id, usuarios in videos.items():
        lsh.add_video(vid_id, usuarios)
        print(f"  + {vid_id}: {len(usuarios)} usuarios")

    print(f"\n📊 Estadísticas del índice LSH:")
    for k, v in lsh.get_stats().items():
        print(f"  {k}: {v}")

    # Demostrar similitudes exactas
    print("\n🎯 Similitudes de Jaccard exactas:")
    pares = [
        ("stranger_things_s4", "dark_s1"),
        ("squid_game_s1", "alice_in_borderland"),
        ("la_casa_papel_s5", "money_heist_berlin"),
        ("stranger_things_s4", "the_crown_s5"),
    ]
    for v1, v2 in pares:
        j = lsh.jaccard_similarity(videos[v1], videos[v2])
        estimada = lsh._minhash.similarity(lsh._firmas[v1], lsh._firmas[v2])
        print(f"  {v1[:20]:>20} ↔ {v2[:20]:<20} "
              f"Jaccard: {j:.3f} | MinHash: {estimada:.3f}")

    # Demostrar recomendaciones
    print("\n🔮 Recomendaciones para 'Stranger Things S4':")
    similares = lsh.find_similar("stranger_things_s4", threshold=0.3)
    for vid, sim in similares:
        print(f"  → {vid}: similitud={sim:.4f}")


if __name__ == "__main__":
    demo()
