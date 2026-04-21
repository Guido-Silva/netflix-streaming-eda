# 🎬 Plataforma de Streaming de Video - Caso Netflix

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)](https://www.python.org/)
[![Google Colab](https://img.shields.io/badge/Google-Colab-orange?logo=googlecolab)](https://colab.research.google.com/)
[![Estructuras de Datos](https://img.shields.io/badge/EDA-Estructuras%20de%20Datos-green)](https://github.com/Guido-Silva/netflix-streaming-eda)

> **Curso:** Estructuras de Datos y Algoritmos  
> **Institución:** UTEC  
> **Proyecto:** Simulación de plataforma de streaming tipo Netflix con estructuras de datos avanzadas

---

## 📋 Descripción del Proyecto

Este proyecto simula el backend de una plataforma de streaming de video similar a Netflix, implementando estructuras de datos y algoritmos avanzados para resolver problemas reales de escalabilidad, rendimiento y personalización.

El sistema gestiona **colas de procesamiento priorizadas**, **caché inteligente**, **conteo de popularidad**, **detección de bots**, **autocompletado de búsqueda** y **recomendaciones personalizadas** — todo con estructuras de datos diseñadas para alta concurrencia y eficiencia.

---

## 🏗️ Arquitectura del Sistema

| Componente | Estructura de Datos | Uso en Netflix | Complejidad |
|---|---|---|---|
| **Cola de Procesamiento** | Priority Queue (Min-Heap) | Priorizar eventos: pagos > premium > estándar | O(log n) push/pop |
| **Caché Inteligente** | LRU Cache (OrderedDict) | Top 1000 videos en memoria, evicción automática | O(1) get/put |
| **Conteo de Popularidad** | Count-Min Sketch | Estadísticas de reproducción en tiempo real | O(1) update/query |
| **Detección de Bots** | Bloom Filter | Identificar solicitudes duplicadas/maliciosas | O(k) add/lookup |
| **Autocompletado** | Trie (Árbol de Prefijos) | Sugerencias instantáneas al buscar | O(m) búsqueda |
| **Recomendaciones** | LSH + MinHash | Top-K videos similares por patrón de visionado | O(n) aprox. |

---

## 📁 Estructura del Repositorio

```
netflix-streaming-eda/
├── README.md                          # Informe técnico (este archivo)
├── src/
│   ├── priority_queue.py              # Cola de procesamiento priorizada
│   ├── lru_cache.py                   # Caché inteligente LRU
│   ├── count_min_sketch.py            # Conteo probabilístico de popularidad
│   ├── bloom_filter.py                # Filtro de Bloom para detección de bots
│   ├── trie.py                        # Árbol de prefijos para autocompletado
│   ├── lsh_minhash.py                 # LSH + MinHash para recomendaciones
│   └── netflix_system.py             # Sistema integrador
├── data/
│   └── synthetic_data_generator.py   # Generador de datos sintéticos
├── notebooks/
│   ├── 01_priority_queue.ipynb        # Experimentos Priority Queue
│   ├── 02_lru_cache.ipynb             # Experimentos LRU Cache
│   ├── 03_count_min_sketch.ipynb      # Experimentos Count-Min Sketch
│   ├── 04_bloom_filter.ipynb          # Experimentos Bloom Filter
│   ├── 05_trie_autocomplete.ipynb     # Experimentos Trie
│   ├── 06_lsh_minhash.ipynb           # Experimentos LSH + MinHash
│   └── 07_sistema_integrado.ipynb     # Sistema completo end-to-end
└── tests/
    └── test_all.py                    # Tests unitarios de todos los componentes
```

---

## 🚀 Instrucciones de Uso (Google Colab)

### Opción 1: Ejecutar directamente en Colab

1. Abre [Google Colab](https://colab.research.google.com/)
2. Ve a **Archivo → Abrir notebook → GitHub**
3. Ingresa: `Guido-Silva/netflix-streaming-eda`
4. Selecciona el notebook que desees abrir

### Opción 2: Clonar el repositorio en Colab

```python
# En una celda de Colab:
!git clone https://github.com/Guido-Silva/netflix-streaming-eda.git
%cd netflix-streaming-eda
```

### Opción 3: Ejecutar el sistema completo

```python
!git clone https://github.com/Guido-Silva/netflix-streaming-eda.git
%cd netflix-streaming-eda

# Ejecutar demo completo
from src.netflix_system import NetflixStreamingPlatform
plataforma = NetflixStreamingPlatform()
plataforma.demo()
```

---

## 🧩 Descripción de Componentes

### 1. 📥 Priority Queue (Cola de Prioridad)

**Archivo:** `src/priority_queue.py`

Implementa una cola de procesamiento de eventos usando un **min-heap**. Los eventos se procesan según su prioridad:

- **PAYMENT (0):** Pagos y suscripciones — prioridad máxima
- **PREMIUM (1):** Usuarios premium — alta prioridad  
- **STANDARD (2):** Usuarios estándar — prioridad normal

```python
from src.priority_queue import PriorityQueue, UserEvent

pq = PriorityQueue()
pq.push(UserEvent(user_id="u1", event_type="stream", priority=1, data={}))
evento = pq.pop()
```

### 2. 💾 LRU Cache (Caché de Último Uso Reciente)

**Archivo:** `src/lru_cache.py`

Mantiene los **1000 videos más recientemente accedidos** en memoria usando `OrderedDict`. Cuando el caché está lleno, evicta el video menos recientemente usado (LRU).

```python
from src.lru_cache import LRUCache, VideoContent

cache = LRUCache(capacity=1000)
video = VideoContent(video_id="v1", title="Stranger Things", genre="Drama", views=1000000, size_mb=850)
cache.put("v1", video)
content = cache.get("v1")
```

### 3. 📊 Count-Min Sketch (Conteo Probabilístico)

**Archivo:** `src/count_min_sketch.py`

Estima la **frecuencia de reproducción de videos** en tiempo real con memoria sublineal. Usa múltiples funciones hash para minimizar el error de estimación.

```python
from src.count_min_sketch import CountMinSketch

cms = CountMinSketch(width=1000, depth=5)
cms.update("stranger_things_s1e1")
cms.update("stranger_things_s1e1")
freq = cms.query("stranger_things_s1e1")  # ≈ 2
```

### 4. 🔍 Bloom Filter (Filtro de Bloom)

**Archivo:** `src/bloom_filter.py`

Detecta **bots y solicitudes duplicadas** con alta eficiencia de memoria. Garantiza zero falsos negativos con una tasa configurable de falsos positivos.

```python
from src.bloom_filter import BloomFilter

bf = BloomFilter(capacity=100000, error_rate=0.01)
bf.add("user_ip_192.168.1.1")
is_bot = bf.contains("user_ip_192.168.1.1")  # True
```

### 5. 🔤 Trie (Árbol de Prefijos)

**Archivo:** `src/trie.py`

Proporciona **autocompletado instantáneo** para la búsqueda de contenido. Ordena sugerencias por popularidad para mostrar los títulos más relevantes primero.

```python
from src.trie import Trie

trie = Trie()
trie.insert("Stranger Things", popularity=1000000)
trie.insert("Squid Game", popularity=2000000)
sugerencias = trie.autocomplete("str")  # ["Stranger Things"]
```

### 6. 🎯 LSH + MinHash (Recomendaciones)

**Archivo:** `src/lsh_minhash.py`

Sistema de **recomendaciones basado en patrones de co-visionado**. Encuentra videos similares usando Locality Sensitive Hashing y firmas MinHash.

```python
from src.lsh_minhash import LSHMinHash

lsh = LSHMinHash(num_hashes=100, num_bands=20)
lsh.add_video("v1", user_set={"u1", "u2", "u3", "u4"})
lsh.add_video("v2", user_set={"u1", "u2", "u3", "u5"})
similares = lsh.find_similar("v1", threshold=0.5)
```

---

## 📈 Análisis de Complejidad

### Tabla Big-O por Operación

| Estructura | Operación | Tiempo | Espacio | Notas |
|---|---|---|---|---|
| **Priority Queue** | push | O(log n) | O(n) | n = elementos en cola |
| **Priority Queue** | pop | O(log n) | O(n) | heapify hacia abajo |
| **Priority Queue** | peek | O(1) | O(1) | raíz del heap |
| **LRU Cache** | get | O(1) | O(capacity) | HashMap + OrderedDict |
| **LRU Cache** | put | O(1) | O(capacity) | Evicción O(1) con OrderedDict |
| **Count-Min Sketch** | update | O(d) | O(w×d) | d = profundidad, w = ancho |
| **Count-Min Sketch** | query | O(d) | O(1) | mínimo de d contadores |
| **Count-Min Sketch** | top-k | O(n log k) | O(k) | con heap auxiliar |
| **Bloom Filter** | add | O(k) | O(m) | k = núm hash, m = bits |
| **Bloom Filter** | contains | O(k) | O(1) | sin falsos negativos |
| **Trie** | insert | O(m) | O(m×A) | m = longitud, A = alfabeto |
| **Trie** | search | O(m) | O(1) | búsqueda exacta |
| **Trie** | autocomplete | O(m + k) | O(k) | k = sugerencias |
| **LSH + MinHash** | add_video | O(n×h) | O(n×b) | n = usuarios, h = hashes |
| **LSH + MinHash** | find_similar | O(b×r) | O(1) | b = bandas, r = filas |

### Comparativa vs Estructuras Tradicionales

| Problema | Solución Tradicional | Solución Usada | Ventaja |
|---|---|---|---|
| Frecuencia de videos | HashMap exacto O(n) memoria | Count-Min Sketch O(w×d) | ~100x menos memoria |
| Detección de duplicados | HashSet O(n) memoria | Bloom Filter O(m bits) | ~10x menos memoria |
| Búsqueda por prefijo | Lista + búsqueda lineal O(n) | Trie O(m) | O(n/m)x más rápido |
| Videos similares | Comparación par-a-par O(n²) | LSH O(n) aprox | Escalable a millones |

---

## 🧪 Ejecutar Tests

```bash
# Desde la raíz del proyecto
python -m pytest tests/test_all.py -v

# O directamente:
python tests/test_all.py
```

---

## 👥 Integrantes

| Nombre | Componente | Contacto |
|---|---|---|
| Guido Silva | Priority Queue, LRU Cache, Count-Min Sketch | juan.silva@utec.edu.pe |
| Compañero 2 | Bloom Filter | - |
| Compañero 3 | Trie (Autocompletado) | - |
| Compañero 4 | LSH + MinHash | - |

**Docente:** Estructuras de Datos y Algoritmos  
**Universidad:** UTEC  
**Año:** 2024

---

## 📚 Referencias

- Cormen, T. H. et al. (2009). *Introduction to Algorithms* (3rd ed.)
- Cormode, G., & Muthukrishnan, S. (2005). An Improved Data Stream Summary: The Count-Min Sketch and its Applications
- Bloom, B. H. (1970). Space/Time Trade-offs in Hash Coding with Allowable Errors
- Andoni, A., & Indyk, P. (2008). Near-Optimal Hashing Algorithms for Approximate Nearest Neighbor in High Dimensions
- Broder, A. Z. (1997). On the resemblance and containment of documents
