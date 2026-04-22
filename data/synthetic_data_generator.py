"""
Generador de Datos Sintéticos para Netflix Streaming Platform
=============================================================
Genera datos realistas para pruebas y experimentos de las estructuras de datos.

Genera:
    - Usuarios con tipos (premium/standard) y metadata
    - Catálogo de videos con género, duración y popularidad
    - Eventos de usuarios (stream, search, payment, etc.)
    - Matriz de co-watching para LSH + MinHash
"""

import random
import json
import time
from typing import List, Dict, Tuple, Set


# Géneros de contenido Netflix
GENEROS = [
    "Drama", "Comedia", "Acción", "Thriller", "Documental",
    "Anime", "Romance", "Sci-Fi", "Horror", "Crimen",
    "Aventura", "Fantasía", "Biográfico", "Musical", "Infantil"
]

# Tipos de eventos de usuario
TIPOS_EVENTO = [
    "stream", "search", "pause", "resume", "rating",
    "payment", "login", "logout", "browse", "download"
]

# Países de origen del contenido
PAISES = [
    "USA", "España", "México", "Colombia", "Japón",
    "Corea del Sur", "Reino Unido", "Francia", "Alemania", "India"
]

# Nombres de series y películas sintéticas para el catálogo
TITULOS_SERIES = [
    "Stranger Things", "Dark", "Squid Game", "La Casa de Papel",
    "The Crown", "Narcos", "Black Mirror", "Bridgerton", "Lupin",
    "Peaky Blinders", "Ozark", "Mindhunter", "You", "Elite",
    "Money Heist", "Alice in Borderland", "Kingdom", "Hellbound",
    "All of Us Are Dead", "Extraordinary Attorney Woo",
    "Sacred Games", "Suburra", "Baby", "Fauda", "Lilyhammer",
    "Marco Polo", "Sense8", "Altered Carbon", "The OA",
    "Russian Doll", "Dead to Me", "Grace and Frankie",
    "Unorthodox", "Dark Desire", "Club de Cuervos",
    "Ingobernable", "Diablero", "Somos", "Control Z",
    "Who Killed Sara", "Rebelde", "Sky Rojo", "Valeria",
    "Cable Girls", "Grand Army", "The Haunting of Hill House",
    "The Umbrella Academy", "Locke & Key", "The Society"
]

TITULOS_PELICULAS = [
    "Bird Box", "Extraction", "The Gray Man", "Red Notice",
    "Army of the Dead", "6 Underground", "Project Power",
    "The Irishman", "Marriage Story", "Roma", "Cuties",
    "The Power of the Dog", "Munich", "Don't Look Up",
    "The Harder They Fall", "tick, tick... BOOM!", "Mank",
    "Malcolm & Marie", "The White Tiger", "Pieces of a Woman",
    "I Am Mother", "In the Shadow of the Moon", "IO",
    "Tall Girl", "The Perfect Date", "Always Be My Maybe",
    "Set It Up", "To All the Boys I've Loved Before",
    "The Kissing Booth", "A Week Away", "He's All That",
    "Purple Hearts", "The Half of It", "All the Bright Places"
]


def generate_users(n: int = 1000) -> List[Dict]:
    """
    Genera n usuarios sintéticos con metadata realista.

    Args:
        n (int): Número de usuarios a generar. Default: 1000.

    Returns:
        list: Lista de diccionarios con información de usuario.
    """
    random.seed(42)
    usuarios = []

    for i in range(n):
        tipo = random.choices(
            ["premium", "standard"],
            weights=[0.35, 0.65]  # 35% premium, 65% standard
        )[0]

        edad = random.randint(16, 65)

        usuario = {
            "user_id": f"user_{i:04d}",
            "tipo": tipo,
            "edad": edad,
            "pais": random.choice(PAISES),
            "generos_favoritos": random.sample(GENEROS, k=random.randint(2, 5)),
            "fecha_registro": f"2023-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "activo": random.random() < 0.8,  # 80% activos
            "dispositivos": random.choices(
                ["tv", "mobile", "tablet", "desktop"],
                k=random.randint(1, 3)
            ),
        }
        usuarios.append(usuario)

    return usuarios


def generate_videos(n: int = 500) -> List[Dict]:
    """
    Genera un catálogo de n videos sintéticos.

    Args:
        n (int): Número de videos a generar. Default: 500.

    Returns:
        list: Lista de diccionarios con información de video.
    """
    random.seed(123)
    videos = []

    todos_titulos = TITULOS_SERIES + TITULOS_PELICULAS
    # Generar más títulos si n > len(todos_titulos)
    titulos_extra = [f"Producción Original {i}" for i in range(max(0, n - len(todos_titulos)))]
    todos_titulos = todos_titulos + titulos_extra

    for i in range(min(n, len(todos_titulos))):
        es_serie = random.random() < 0.6  # 60% series, 40% películas

        # Popularidad con distribución Pareto (80/20): pocos videos muy populares
        popularidad_base = int(random.paretovariate(1.5) * 1_000_000)
        popularidad = min(popularidad_base, 150_000_000)

        video = {
            "video_id": f"vid_{i:04d}",
            "titulo": todos_titulos[i],
            "genero": random.choice(GENEROS),
            "tipo": "serie" if es_serie else "pelicula",
            "año": random.randint(2015, 2024),
            "pais_origen": random.choice(PAISES),
            "duracion_min": random.randint(20, 180) if not es_serie else random.randint(30, 60),
            "temporadas": random.randint(1, 5) if es_serie else 1,
            "calificacion_imdb": round(random.uniform(5.0, 9.5), 1),
            "vistas_totales": popularidad,
            "tamaño_mb": round(random.uniform(400, 2000), 1),
            "idioma_original": random.choice(["Español", "Inglés", "Coreano", "Japonés", "Francés"]),
            "tags": random.sample(GENEROS, k=random.randint(1, 3)),
        }
        videos.append(video)

    return videos


def generate_events(users: List[Dict], videos: List[Dict], n: int = 10000000) -> List[Dict]:
    """
    Genera n eventos de usuario sintéticos con distribución realista.

    Los eventos siguen una distribución donde:
    - El 70% son reproducciones (stream)
    - Los usuarios premium generan más eventos
    - Los videos populares tienen más reproducciones (distribución Zipf)

    Args:
        users (list): Lista de usuarios generados.
        videos (list): Lista de videos generados.
        n (int): Número de eventos a generar. Default: 10000.

    Returns:
        list: Lista de eventos ordenados por timestamp.
    """
    random.seed(456)
    eventos = []
    ids_usuarios = [u["user_id"] for u in users]
    ids_videos = [v["video_id"] for v in videos]

    # Pesos para videos (distribución Zipf)
    pesos_videos = [1.0 / (i + 1) ** 0.8 for i in range(len(ids_videos))]

    # Timestamp base (últimos 30 días)
    ts_base = time.time() - 30 * 24 * 3600

    for i in range(n):
        usuario = random.choice(users)
        user_id = usuario["user_id"]

        # Determinar tipo de evento con probabilidades
        tipo = random.choices(
            TIPOS_EVENTO,
            weights=[70, 10, 5, 5, 3, 2, 2, 1, 1, 1]
        )[0]

        # Seleccionar video (con distribución Zipf para popularidad)
        video_id = random.choices(ids_videos, weights=pesos_videos)[0]

        evento = {
            "evento_id": f"evt_{i:06d}",
            "user_id": user_id,
            "tipo": tipo,
            "video_id": video_id if tipo in ["stream", "pause", "resume", "rating", "download"] else None,
            "timestamp": ts_base + random.uniform(0, 30 * 24 * 3600),
            "user_type": usuario["tipo"],
            "datos": {}
        }

        # Agregar datos específicos según tipo
        if tipo == "stream":
            evento["datos"] = {
                "calidad": random.choice(["SD", "HD", "4K"]) if usuario["tipo"] == "premium" else random.choice(["SD", "HD"]),
                "dispositivo": random.choice(usuario.get("dispositivos", ["mobile"])),
                "duracion_vista": random.randint(1, 120),
            }
        elif tipo == "payment":
            evento["datos"] = {
                "monto": 15.99 if usuario["tipo"] == "premium" else 9.99,
                "metodo": random.choice(["tarjeta", "paypal", "transferencia"]),
            }
        elif tipo == "search":
            evento["datos"] = {
                "query": random.choice([t.split()[0] for t in TITULOS_SERIES[:20]]),
            }
        elif tipo == "rating":
            evento["datos"] = {
                "calificacion": random.randint(1, 5),
            }

        eventos.append(evento)

    # Ordenar por timestamp
    eventos.sort(key=lambda e: e["timestamp"])
    return eventos


def generate_cowatch_matrix(
    users: List[Dict],
    videos: List[Dict],
    min_common: int = 10
) -> Dict[str, Set[str]]:
    """
    Genera una matriz de co-watching para el sistema de recomendaciones LSH.

    Retorna un diccionario que mapea video_id → conjunto de user_ids que lo vieron.
    Los videos comparten usuarios de forma correlacionada según género.

    Args:
        users (list): Lista de usuarios.
        videos (list): Lista de videos.
        min_common (int): Mínimo de usuarios comunes para videos del mismo género.

    Returns:
        dict: Diccionario {video_id: set(user_ids)}.
    """
    random.seed(789)
    ids_usuarios = [u["user_id"] for u in users]
    cowatch: Dict[str, Set[str]] = {}

    # Agrupar usuarios por géneros favoritos
    usuarios_por_genero: Dict[str, List[str]] = {}
    for usuario in users:
        for genero in usuario.get("generos_favoritos", []):
            if genero not in usuarios_por_genero:
                usuarios_por_genero[genero] = []
            usuarios_por_genero[genero].append(usuario["user_id"])

    for video in videos:
        vid_id = video["video_id"]
        genero = video["genero"]

        # Base de usuarios interesados en el género
        usuarios_interesados = usuarios_por_genero.get(genero, ids_usuarios[:50])

        # Popularidad determina cuántos usuarios lo ven
        popularidad = video.get("vistas_totales", 1_000_000)
        fraccion = min(0.9, popularidad / 10_000_000)  # Máximo 90% de audiencia

        n_espectadores = max(5, int(len(usuarios_interesados) * fraccion))
        n_espectadores = min(n_espectadores, len(ids_usuarios))

        # Seleccionar espectadores (mezcla de usuarios del género + aleatorios)
        espectadores_base = random.sample(
            usuarios_interesados,
            min(n_espectadores, len(usuarios_interesados))
        )

        # Agregar algunos usuarios aleatorios
        extras = random.sample(ids_usuarios, min(10, len(ids_usuarios)))
        todos_espectadores = list(set(espectadores_base + extras))

        cowatch[vid_id] = set(todos_espectadores)

    return cowatch


def save_to_json(data: any, filename: str) -> None:
    """
    Guarda datos en formato JSON.

    Args:
        data: Los datos a guardar.
        filename (str): Nombre del archivo de salida.
    """
    # Convertir sets a listas para serialización JSON
    def convert(obj):
        if isinstance(obj, set):
            return list(obj)
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=convert)
    print(f"  ✅ Guardado: {filename}")


def demo():
    """
    Genera y guarda todos los datasets sintéticos.
    """
    print("=" * 60)
    print("   Generador de Datos Sintéticos - Netflix EDA")
    print("=" * 60)

    print("\n👥 Generando 1,000 usuarios...")
    usuarios = generate_users(1000)
    print(f"  Premium: {sum(1 for u in usuarios if u['tipo'] == 'premium')}")
    print(f"  Standard: {sum(1 for u in usuarios if u['tipo'] == 'standard')}")

    print("\n🎬 Generando 500 videos...")
    videos = generate_videos(500)
    print(f"  Series: {sum(1 for v in videos if v['tipo'] == 'serie')}")
    print(f"  Películas: {sum(1 for v in videos if v['tipo'] == 'pelicula')}")
    generos_count = {}
    for v in videos:
        generos_count[v['genero']] = generos_count.get(v['genero'], 0) + 1
    top_generos = sorted(generos_count.items(), key=lambda x: -x[1])[:5]
    print(f"  Top géneros: {top_generos}")

    print("\n📊 Generando 10,000 eventos...")
    eventos = generate_events(usuarios, videos, 10000)
    tipos_count = {}
    for e in eventos:
        tipos_count[e['tipo']] = tipos_count.get(e['tipo'], 0) + 1
    print(f"  Distribución de eventos: {dict(sorted(tipos_count.items(), key=lambda x: -x[1])[:5])}")

    print("\n🔗 Generando matriz de co-watching...")
    cowatch = generate_cowatch_matrix(usuarios, videos)
    espectadores_promedio = sum(len(v) for v in cowatch.values()) / len(cowatch)
    print(f"  Espectadores promedio por video: {espectadores_promedio:.1f}")

    print("\n✅ Resumen de datos generados:")
    print(f"  - Usuarios: {len(usuarios)}")
    print(f"  - Videos: {len(videos)}")
    print(f"  - Eventos: {len(eventos)}")
    print(f"  - Videos en matriz co-watching: {len(cowatch)}")

    return usuarios, videos, eventos, cowatch


if __name__ == "__main__":
    demo()
