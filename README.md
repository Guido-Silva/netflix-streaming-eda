# Plataforma de Streaming de Video — Caso Netflix

## Integrantes
- Enrique Huasupoma (email)
- Ruben Coquil (email)
- Cristopher Jair (email)
- Alvaro Colquicocha (email)
- Alex Valdez (email)
- Guido Silva (juan.silva@utec.edu.pe)

## Descripción
Proyecto final del curso **Estructuras de Datos y Algoritmos (EDA) — UTEC** que simula el backend de una plataforma de streaming tipo Netflix. Se implementan estructuras de datos avanzadas para resolver problemas reales de rendimiento, escalabilidad y personalización: procesamiento priorizado de eventos, caché inteligente, conteo aproximado de frecuencias, detección de bots, autocompletado de búsqueda y recomendación de contenido similar.

## Estructuras Avanzadas Implementadas
1. **Priority Queue (Min-Heap)** — Priorización de eventos críticos (pagos, usuarios premium, tráfico estándar).
2. **LRU Cache** — Caché de los 1 000 videos más accedidos para reducir latencia.
3. **Count-Min Sketch** — Conteo aproximado de popularidad sobre flujo masivo de eventos.
4. **Bloom Filter** — Detección probabilística eficiente de solicitudes duplicadas y posibles bots.
5. **Trie (Prefix Tree)** — Autocompletado de búsquedas por prefijo con ranking por popularidad.
6. **LSH + MinHash** — Recomendaciones aproximadas de contenido similar (Top-K videos).

## Estructura del Repositorio
```text
netflix-streaming-eda/
├── README.md
├── notebooks/
│   ├── 01_priority_queue.ipynb
│   ├── 02_lru_cache.ipynb
│   ├── 03_count_min_sketch.ipynb
│   ├── 04_bloom_filter.ipynb
│   ├── 05_trie_autocomplete.ipynb
│   ├── 06_lsh_minhash.ipynb
│   └── 07_sistema_integrado.ipynb
├── src/
│   ├── priority_queue.py
│   ├── lru_cache.py
│   ├── count_min_sketch.py
│   ├── bloom_filter.py
│   ├── trie.py
│   ├── lsh_minhash.py
│   └── netflix_system.py
├── data/
│   └── synthetic_data_generator.py
├── tests/
│   └── test_all.py
├── informe/
│   ├── informe_tecnico.md
│   └── Proyecto_Final_NombreGrupo.pdf
└── presentacion/
    └── slides.pdf
```

## Instalación
```bash
pip install -r requirements.txt
```

## Uso
Ejecutar el sistema completo integrado:
```bash
python src/netflix_system.py
```

Correr tests unitarios:
```bash
python -m pytest tests/test_all.py -v
```

Uso programático:
```python
from src.netflix_system import NetflixStreamingPlatform

plataforma = NetflixStreamingPlatform()
plataforma.demo()
```

También puedes explorar cada estructura de forma independiente en los notebooks de Google Colab ubicados en `notebooks/`. Ejecutarlos en orden numérico para reproducir todos los experimentos.

## Benchmarks
Los experimentos y mediciones de rendimiento se encuentran en la carpeta `notebooks/`. Cada notebook incluye:
- Generación de datos sintéticos a escala (hasta 10 M eventos).
- Medición de tiempos de inserción y consulta.
- Comparación de uso de memoria frente a estructuras clásicas.
- Visualizaciones con `matplotlib` y `seaborn`.

Para reproducir todos los benchmarks de forma automática:
```bash
python src/netflix_system.py --dataset data/synthetic_data_generator.py
```

## Resultados Principales
- **LRU Cache**: reducción de latencia promedio en consultas frecuentes (hit rate > 90 % con top-1 000 videos).
- **Count-Min Sketch**: conteo de frecuencias con error relativo < 1 % usando < 5 % de la memoria de un diccionario exacto.
- **Bloom Filter**: tasa de falsos positivos < 0.1 % con una fracción del espacio de un set estándar.
- **Trie**: sugerencias de autocompletado en O(k) donde k es la longitud del prefijo.
- **LSH + MinHash**: recomendaciones Top-K con similitud de Jaccard aproximada en tiempo sub-lineal sobre catálogos de 100 K+ videos.
- **Priority Queue**: procesamiento ordenado de eventos con complejidad O(log n) por operación.

## Informe y Presentación
- Informe técnico editable: `informe/informe_tecnico.md`
- Informe técnico final (PDF): `informe/Proyecto_Final_NombreGrupo.pdf`
- Presentación final (PDF): `presentacion/slides.pdf`

## Referencias
- Cormen, T. H., Leiserson, C. E., Rivest, R. L., & Stein, C. (2009). *Introduction to Algorithms* (3rd ed.). MIT Press.
- Cormode, G., & Muthukrishnan, S. (2005). An improved data stream summary: The count-min sketch and its applications. *Journal of Algorithms, 55*(1), 58–75.
- Bloom, B. H. (1970). Space/time trade-offs in hash coding with allowable errors. *Communications of the ACM, 13*(7), 422–426.
- Andoni, A., & Indyk, P. (2008). Near-optimal hashing algorithms for approximate nearest neighbor in high dimensions. *Communications of the ACM, 51*(1), 117–122.
- Broder, A. Z. (1997). On the resemblance and containment of documents. *Compression and Complexity of Sequences*, 21–29.
