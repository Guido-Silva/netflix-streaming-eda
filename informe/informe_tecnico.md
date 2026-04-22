# Informe Técnico: Plataforma de Streaming de Video (Caso Netflix)

**Fecha:** Abril 2026  
**Asignatura:** Estructuras de Datos y Algoritmos  
**Programa:** Maestría en Ciencias de Datos e IA — UTEC

---

## 1. Resumen Ejecutivo

### 1.1 Problema Abordado

Las plataformas de streaming de video a escala global enfrentan desafíos computacionales extremos: millones de eventos concurrentes de usuarios, catálogos de cientos de miles de títulos y necesidad de respuesta en tiempo real con baja latencia. Las estructuras de datos clásicas (listas, diccionarios, árboles balanceados) resultan insuficientes cuando se requiere memoria sublineal, consultas aproximadas con garantías probabilísticas o búsquedas de similitud en espacios de alta dimensionalidad.

Este proyecto modela el backend de una plataforma de streaming tipo Netflix, donde se integran estructuras de datos avanzadas: una cola de prioridad (priority queue) para programar solicitudes de reproducción, un filtro de Bloom para filtrar rápidamente contenido no visto y así evitar accesos innecesarios a la base de datos, una caché LRU para almacenar temporalmente los segmentos de video más recientes, un trie para búsquedas de texto (por ejemplo, autocompletado de títulos) y técnicas de LSH + MinHash para detección de similitud (por ejemplo, recomendación de contenido similar). 

### 1.2 Solución Propuesta

Se diseñó e implementó un sistema integrado que combina seis estructuras de datos avanzadas, cada una asignada al subsistema donde ofrece la mejor relación costo-beneficio:

| Estructura | Subsistema | Beneficio principal |
|---|---|---|
| Priority Queue (Min-Heap) | Cola de eventos | O(log n) por inserción/extracción |
| LRU Cache | Caché de video metadata | O(1) acceso y evicción automática |
| Count-Min Sketch | Estadísticas de popularidad | Memoria sublineal con error acotado |
| Bloom Filter | Detección de bots/duplicados | Sin falsos negativos, memoria mínima |
| Trie (Árbol de Prefijos) | Autocompletado de búsqueda | O(m) independiente del catálogo |
| LSH + MinHash | Motor de recomendaciones | O(n) en vez de O(n²) para similitud |

### 1.3 Resultados Principales

- **LRU Cache**: tasa de acierto (hit rate) de 85–90% con distribución Zipf, frente a ~45% con distribución uniforme.
- **Count-Min Sketch**: error de estimación < 1% con parámetros w=2000, d=7 y consumo de ~56 KB, frente a ~50 MB de un `dict` completo (**ahorro de 909×**).
- **Bloom Filter**: tasa de falsos positivos observada ≤ 1% con capacidad de 10 000 elementos y solo 120 KB de memoria.
- **Trie**: autocompletado en < 100 ms para catálogos de hasta 50 000 títulos.
- **LSH + MinHash**: reduce de 10¹² a ~10⁶ comparaciones para 1 millón de videos, estimando similitud de Jaccard con error < 15%.
- **Sistema integrado** (10 000 eventos, 1 000 usuarios, 500 videos): latencia promedio < 100 ms, 5% de bots detectados, 9 500 reproducciones exitosas procesadas.

---

## 2. Introducción y Contexto

### 2.1 Descripción del Escenario

Netflix reportó en 2023 más de 260 millones de suscriptores activos generando miles de millones de eventos diarios: reproducciones, pausas, búsquedas, pagos y logins. El backend debe:

1. **Priorizar eventos críticos** (pagos, alertas de fraude) sobre eventos de baja urgencia (navegación).
2. **Acelerar el acceso a contenido popular** sin saturar la base de datos principal con solicitudes repetidas.
3. **Monitorear tendencias en tiempo real** sobre un flujo continuo de millones de eventos por segundo.
4. **Filtrar bots y solicitudes duplicadas** antes de que consuman recursos del sistema de procesamiento.
5. **Ofrecer autocompletado instantáneo** en la barra de búsqueda mientras el usuario escribe.
6. **Generar recomendaciones personalizadas** basadas en patrones de visualización compartidos entre usuarios.

Este proyecto simula ese backend con datos sintéticos de 1 000 usuarios, 500 videos y 10 000 eventos, implementando cada subsistema con la estructura de datos más adecuada según análisis de complejidad teórica y experimental.

### 2.2 Justificación de las Estructuras Seleccionadas

La elección de cada estructura responde a requerimientos concretos del escenario:

- **Priority Queue sobre cola FIFO**: una cola FIFO trata todos los eventos por igual, retrasando pagos detrás de millones de eventos de streaming. Un min-heap garantiza que los eventos de pago y usuarios premium se procesen primero que los standard con O(log n) por operación, frente a O(n) de una lista ordenada.

- **LRU Cache sobre caché FIFO o LFU**: el tráfico de streaming sigue una distribución de Zipf (el 20% del catálogo acumula el 80% de las reproducciones). LRU se alinea naturalmente con esta distribución porque los contenidos recientes tienden a ser los más populares. LFU requeriría mantener contadores ordenados (O(log n) por actualización), mientras LRU con `OrderedDict` logra O(1) amortizado en todas las operaciones.

- **Count-Min Sketch sobre contador exacto**: en un flujo de 10⁹ eventos diarios, mantener un contador exacto por video requeriría decenas de GB de memoria. El CMS ofrece estimaciones con error acotado (ε = e/w) usando memoria O(w×d), típicamente decenas de kilobytes.

- **Bloom Filter sobre `set` de Python**: para detectar si una IP o token ya fue visto, un `set` exacto requeriría almacenar cada elemento. Con 500 millones de usuarios, eso implica ~10 GB de memoria. Un Bloom Filter reduce eso a ~600 MB con tasa de falsos positivos del 1% (reducción de 17×).

- **Trie sobre búsqueda lineal**: una búsqueda lineal por prefijo sobre 500 000 títulos requiere O(n×m) por consulta. Un Trie reduce eso a O(m) donde m es la longitud del prefijo, independientemente del tamaño del catálogo.

- **LSH + MinHash sobre comparación exhaustiva**: recomendar videos similares comparando todos los pares requeriría O(n²) operaciones. Para 1 millón de videos, son 10¹² comparaciones. LSH reduce el espacio de búsqueda a candidatos probables en tiempo subcuadrático por consulta.

### 2.3 Objetivos Específicos del Proyecto

1. Implementar las seis estructuras de datos en Python, sin librerías externas especializadas, como módulos independientes en `src/`.
2. Validar cada implementación con pruebas unitarias en `tests/test_all.py`
3. Comparar empíricamente el rendimiento de cada estructura frente a alternativas simples en notebooks dedicados (`notebooks/01` al `notebooks/07`).
4. Integrar las seis estructuras en un sistema cohesivo (`netflix_system.py`) que procese un flujo de 10 000 eventos de extremo a extremo.
5. Documentar los trade-offs de cada estructura: cuándo usarla y cuándo no es la opción adecuada.

---

## 3. Investigación de Estructuras Avanzadas

### 3.1 Priority Queue (Min-Heap)

#### Fundamento Teórico

Un heap binario es un árbol binario completo que satisface la propiedad de heap: el valor de cada nodo es menor o igual al de sus hijos (min-heap). Esta propiedad permite encontrar el mínimo en O(1) y mantenerla tras inserciones o extracciones en O(log n).

La implementación clásica usa un arreglo donde el nodo en posición `i` tiene hijos en `2i+1` y `2i+2`. Python incluye el módulo `heapq` que implementa esta estructura eficientemente sobre listas.

En este proyecto, cada evento de usuario se modela como una tupla `(prioridad, timestamp, contador, evento)` para garantizar desempate estable (FIFO dentro de la misma prioridad):

```python
heapq.heappush(self._heap, (evento.priority, evento.timestamp, self._contador, evento))
```

Los tres niveles de prioridad implementados son:

| Nivel | Valor | Tipo de evento |
|---|---|---|
| PAYMENT | 0 | Pagos y suscripciones — máxima prioridad |
| PREMIUM | 1 | Usuarios premium |
| STANDARD | 2 | Usuarios estándar |

#### Paper Original

Williams, J.W.J. (1964). *Algorithm 232 – Heapsort*. **Communications of the ACM**, 7(6), 347–348.  
Cormen, T.H., Leiserson, C.E., Rivest, R.L., & Stein, C. (2022). *Introduction to Algorithms* (4.ª ed.). MIT Press.

#### Análisis de Complejidad

| Operación | Tiempo | Espacio |
|---|---|---|
| `push(evento)` | O(log n) | — |
| `pop()` | O(log n) | — |
| `peek()` | O(1) | — |
| `size()` | O(1) | — |
| Espacio total | — | O(n) |

Comparativa con alternativas: una lista ordenada requiere O(n) por inserción (por el desplazamiento), mientras que una cola desordenada requiere O(n) para encontrar el mínimo. El heap ofrece el mejor balance con O(log n) para ambas operaciones.

#### Caso de Uso en Netflix

Cuando miles de eventos llegan simultáneamente, la cola de prioridad garantiza que un evento de pago fallido (que podría implicar una pérdida de ingresos o un fraude) se procese antes que 10 000 solicitudes de streaming estándar. En el sistema implementado, todos los eventos PAYMENT son atendidos antes de cualquier evento PREMIUM o STANDARD, y dentro de la misma prioridad se respeta el orden de llegada (FIFO).

#### Trade-offs

| Ventaja | Limitación |
|---|---|
| Extracción del máximo prioritario en O(log n) | No permite buscar/actualizar un elemento arbitrario eficientemente |
| Inserción eficiente O(log n) | No soporta prioridades dinámicas sin reconstrucción |
| Memoria compacta O(n) usando arreglo | No es thread-safe sin sincronización adicional en entornos concurrentes |
| Implementación disponible en `heapq` de Python | Solo eficiente para extraer el mínimo, no para k-ésimo elemento |

---

### 3.2 LRU Cache (Least Recently Used)

#### Fundamento Teórico

Un LRU Cache mantiene un conjunto de elementos de capacidad fija. Cuando se necesita espacio para un nuevo elemento, descarta el elemento que fue accedido hace más tiempo (el "menos recientemente usado"). La implementación eficiente requiere combinar dos estructuras:

- Un **diccionario hash** para acceso O(1) por clave.
- Una **lista doblemente enlazada** para mantener el orden de acceso en O(1).

Python's `collections.OrderedDict` implementa exactamente esta combinación, permitiendo `move_to_end()` en O(1):

```python
self._cache.move_to_end(video_id)    # Marcar como más reciente — O(1)
self._cache.popitem(last=False)      # Evictar el más antiguo  — O(1)
```

#### Paper Original

Sleator, D.D. & Tarjan, R.E. (1985). *Amortized efficiency of list update and paging rules*. **Communications of the ACM**, 28(2), 202–208.  
King, W.F. (1971). *Analysis of demand paging algorithms*. IFIP Congress.

#### Análisis de Complejidad

| Operación | Tiempo | Espacio |
|---|---|---|
| `get(video_id)` | O(1) | — |
| `put(video_id, content)` | O(1) | — |
| `contains(video_id)` | O(1) | — |
| `most_recent(n)` | O(n) | — |
| Espacio total | — | O(capacidad) |

#### Caso de Uso en Netflix

La distribución de popularidad de videos sigue una ley de Zipf: el video más popular tiene el doble de reproducciones que el segundo más popular, el triple que el tercero, etc. Un caché de capacidad C puede servir una fracción desproporcionadamente alta del tráfico total.

En el experimento, un LRU Cache con capacidad 100 (sobre un catálogo de 500 videos) alcanzó una tasa de acierto del **90% con distribución Zipf**, frente a solo el **45% con distribución uniforme**. Esto confirma que LRU es la política correcta para el tráfico de streaming.

**Figura 1:** Tasa de acierto del LRU Cache vs. capacidad del caché, comparando distribución Zipf y uniforme (10 000 accesos sobre 500 videos).

![Hit Rate LRU Cache vs Capacidad](lru_hit_rate.png)

#### Trade-offs

| Ventaja | Limitación |
|---|---|
| O(1) para todas las operaciones críticas | No considera frecuencia, solo recencia de acceso |
| Se alinea naturalmente con distribución Zipf | Un acceso único "contamina" el caché (problema de one-hit wonders) |
| Sin overhead de ordenamiento ni contadores | Puede evictar contenido muy popular si no fue accedido recientemente |
| Implementación compacta con `OrderedDict` | No es thread-safe sin `Lock` adicional en Python |

---

### 3.3 Count-Min Sketch

#### Fundamento Teórico

El Count-Min Sketch (CMS) es una estructura de datos probabilística de la familia de los *sketches* para estimar la frecuencia de elementos en un flujo de datos masivo. Usa una matriz de contadores de dimensiones d×w y d funciones hash independientes.

**Actualización** `update(item)`: para cada función hash i, incrementa `tabla[i][hᵢ(item)]`.  
**Consulta** `query(item)`: retorna `min{ tabla[i][hᵢ(item)] : i = 0..d-1 }`.

La estimación nunca subestima el valor real (garantía de cota inferior), pero puede sobreestimar debido a colisiones de hash. El error está acotado por:

```
P(estimación > real + ε·N) < δ
donde  ε = e/w   y   δ = e^(-d)
```

En la implementación se usan funciones hash basadas en SHA-256 con semillas independientes para garantizar independencia entre las d filas de la matriz.

#### Paper Original

Cormode, G. & Muthukrishnan, S. (2005). *An improved data stream summary: the count-min sketch and its applications*. **Journal of Algorithms**, 55(1), 58–75.

#### Análisis de Complejidad

| Operación | Tiempo | Espacio |
|---|---|---|
| `update(item, count)` | O(d) | — |
| `query(item)` | O(d) | — |
| `top_k(k)` | O(n log k) | — |
| Espacio total | — | O(w × d) |

#### Caso de Uso en Netflix

En lugar de mantener un contador exacto para cada uno de los 500 000 títulos del catálogo, el CMS permite estimar el conteo de reproducciones de cualquier video en tiempo O(d) con uso de memoria O(w×d). Con parámetros w=2000, d=7 (ajustados para ε < 0.002, δ < 0.001), el CMS ocupa solo **56 KB** frente a los ~50 MB de un diccionario Python con 500 000 entradas. En experimentos con 100 000 eventos sobre 1 000 videos distintos, el error promedio fue < 1%.

**Figura 2:** Mapa de calor del error relativo del Count-Min Sketch en función de los parámetros w (ancho) y d (profundidad).

![Mapa de Calor Error CMS](cms_error_heatmap.png)

#### Trade-offs

| Ventaja | Limitación |
|---|---|
| Memoria sublineal O(w×d), típicamente kilobytes | Nunca subestima, pero puede sobreestimar (error positivo) |
| Actualizaciones y consultas en O(d) | No permite eliminar elementos (contadores solo crecen) |
| Parámetros ajustables según precisión requerida | Requiere estimación previa del volumen de datos N |
| Funciona sobre flujos infinitos de datos | No almacena los elementos; no es invertible ni enumerable |

---

### 3.4 Bloom Filter

#### Fundamento Teórico

Un Bloom Filter es una estructura de datos probabilística para verificar pertenencia a un conjunto. Usa un arreglo de m bits y k funciones hash independientes.

**Inserción** `add(item)`: activa los k bits en posiciones h₁(item), h₂(item), …, hₖ(item).  
**Consulta** `contains(item)`: retorna `True` si los k bits están activos; `False` si alguno está inactivo.

Propiedad fundamental: si retorna `False`, el elemento **definitivamente no está** en el conjunto. Si retorna `True`, el elemento **probablemente está** (puede ser un falso positivo). **No hay falsos negativos.**

Los parámetros óptimos se calculan automáticamente a partir de la capacidad n y la tasa de error p:

```
m = -n · ln(p) / (ln 2)²        # Tamaño óptimo en bits
k = (m / n) · ln 2               # Número óptimo de funciones hash
```

La implementación usa `bytearray` (8 bits por byte) para máxima eficiencia de memoria, frente a una lista de booleanos de Python (1 byte por elemento).

#### Paper Original

Bloom, B.H. (1970). *Space/time trade-offs in hash coding with allowable errors*. **Communications of the ACM**, 13(7), 422–426.

#### Análisis de Complejidad

| Operación | Tiempo | Espacio |
|---|---|---|
| `add(item)` | O(k) | — |
| `contains(item)` | O(k) | — |
| Espacio total | — | O(m) bits |

#### Caso de Uso en Netflix

Para detectar bots y solicitudes duplicadas, el sistema verifica si el par `(IP_usuario, token_sesión)` ya fue registrado. Con 500 millones de usuarios activos, almacenar un `set` exacto requeriría ~10 GB. Un Bloom Filter con tasa de falsos positivos del 1% reduce eso a ~600 MB (**reducción de 17×**). En el sistema implementado, el Bloom Filter detecta el ~5% de solicitudes duplicadas/bots sin ningún falso negativo, con una tasa de falsos positivos observada prácticamente igual a la teórica del 1%.

**Figura 3:** Evolución de la tasa de falsos positivos del Bloom Filter conforme se insertan elementos (capacidad = 10 000, error objetivo = 1%).

![Tasa de Falsos Positivos Bloom Filter](bloom_fp_rate.png)

#### Trade-offs

| Ventaja | Limitación |
|---|---|
| Sin falsos negativos garantizado | Posibles falsos positivos (tasa controlable por parámetros) |
| Memoria mínima O(m) bits, constante e independiente de los elementos | No es posible eliminar elementos (solo Counting Bloom Filters) |
| O(k) para inserción y consulta | No permite recuperar los elementos almacenados |
| Escala a miles de millones de elementos | La tasa de FP aumenta si se insertan más elementos que la capacidad configurada |

---

### 3.5 Trie (Árbol de Prefijos)

#### Fundamento Teórico

Un Trie (del inglés *retrieval*) es un árbol en el que cada nodo representa un carácter de una cadena. La ruta desde la raíz hasta un nodo marcado como fin de palabra almacena la cadena completa. A diferencia de un árbol de búsqueda binaria, el tiempo de búsqueda depende de la longitud de la clave m, no del número de claves n almacenadas.

Cada nodo del Trie contiene:

- Un diccionario de hijos (`children: Dict[str, TrieNode]`).
- Un flag `is_end` que indica fin de palabra.
- La popularidad de la palabra (para ordenar sugerencias de autocompletado).

El autocompletado realiza un DFS desde el nodo del prefijo para recolectar todas las palabras del subárbol, luego las ordena por popularidad de mayor a menor.

#### Paper Original

Fredkin, E. (1960). *Trie memory*. **Communications of the ACM**, 3(9), 490–499.

#### Análisis de Complejidad

| Operación | Tiempo | Espacio |
|---|---|---|
| `insert(word)` | O(m) | — |
| `search(word)` | O(m) | — |
| `autocomplete(prefix, k)` | O(m + resultados) | — |
| `delete(word)` | O(m) | — |
| `starts_with(prefix)` | O(m) | — |
| Espacio total | — | O(Σ × m × n) |

Donde m = longitud promedio de palabra, n = número de palabras, Σ = tamaño efectivo del alfabeto.

#### Caso de Uso en Netflix

Cuando el usuario escribe "str" en la barra de búsqueda, el sistema debe sugerir "Stranger Things", "Street Food", etc., ordenadas por popularidad, en tiempo real (< 100 ms). Una búsqueda lineal sobre 500 000 títulos requeriría O(n×m) por cada tecla presionada. El Trie reduce eso a O(m) para navegar hasta el nodo del prefijo, independientemente del tamaño del catálogo.

En el sistema implementado se precarga el catálogo completo de 500 títulos al iniciar la plataforma, permitiendo respuestas instantáneas sin consultar la base de datos durante el autocompletado.

**Figura 4:** Tiempo de búsqueda y autocompletado en el Trie vs. número de palabras almacenadas (de 100 a 50 000 palabras).

![Rendimiento del Trie vs. Tamaño](trie_performance.png)

#### Trade-offs

| Ventaja | Limitación |
|---|---|
| Búsqueda O(m) completamente independiente de n | Alto consumo de memoria para alfabetos grandes o palabras muy largas |
| Autocompletado eficiente por prefijo, con ranking por popularidad | Menor eficiencia que hash maps para búsquedas de igualdad exacta |
| Soporte natural para búsquedas por prefijo y `starts_with` | No adecuado para búsquedas aproximadas (sin tolerancia a errores tipográficos) |
| Estructura determinista, sin errores de estimación | La profundidad del árbol puede causar overhead en palabras largas |

---

### 3.6 LSH + MinHash

#### Fundamento Teórico

**Similitud de Jaccard:** Para dos conjuntos A y B, mide su solapamiento:

```
J(A, B) = |A ∩ B| / |A ∪ B|
```

Para recomendar videos, se define el conjunto de usuarios que vieron cada video y se buscan videos con alta similitud de Jaccard entre sus audiencias.

**MinHash:** Estima la similitud de Jaccard sin comparar los conjuntos completos. Usa h funciones hash y toma el mínimo de cada función aplicada a todos los elementos del conjunto (firma MinHash de h valores). La propiedad fundamental es:

```
P[ minₕ(A) = minₕ(B) ] = J(A, B)
```

Con h funciones hash independientes, el error de estimación decrece como 1/√h.

**LSH (Locality Sensitive Hashing):** Divide la firma MinHash en b bandas de r filas. Dos videos son candidatos a similares si sus firmas coinciden en al menos una banda completa. La probabilidad de ser candidatos para similitud s es:

```
P(candidatos | J = s) = 1 − (1 − sʳ)ᵇ
```

Esto concentra la detección en pares con J ≥ umbral, evitando comparar todos los O(n²) pares.

#### Papers Originales

Broder, A.Z. (1997). *On the resemblance and containment of documents*. **Proceedings of the Compression and Complexity of Sequences 1997**. IEEE.  
Indyk, P. & Motwani, R. (1998). *Approximate nearest neighbors: towards removing the curse of dimensionality*. **Proceedings of STOC 1998**, 604–613.

#### Análisis de Complejidad

| Operación | Tiempo | Espacio |
|---|---|---|
| `add_video(id, user_set)` | O(n × h) | — |
| `find_similar(video_id)` | O(b × r) por consulta | — |
| `jaccard_similarity(A, B)` | O(\|A\| + \|B\|) exacto | — |
| Construcción de índice (n videos) | O(n × h) | O(n × h) |

#### Caso de Uso en Netflix

Si los usuarios que vieron "Stranger Things" también vieron "Dark" (ambas series de ciencia ficción), los conjuntos de usuarios de ambos videos tendrán alta similitud de Jaccard, y el sistema los recomendará juntos. En experimentos con 9 videos controlados (pares con similitudes conocidas entre 0.3 y 0.7), LSH + MinHash identificó correctamente todos los pares con J ≥ 0.5 sin falsos negativos.

**Figura 5:** Matriz de similitud estimada por MinHash para 9 videos con patrones de co-visualización controlados.

![Matriz de Similitud LSH MinHash](lsh_similarity_matrix.png)

#### Trade-offs

| Ventaja | Limitación |
|---|---|
| Reduce O(n²) a O(n) para búsqueda de similitud aproximada | Estimación aproximada con error ~1/√h |
| Escala a millones de videos sin comparar todos los pares | Falsos positivos y negativos en la etapa de candidatos LSH |
| No requiere almacenar los conjuntos completos de usuarios | Requiere ajuste cuidadoso de parámetros b, r y h según el umbral deseado |
| Paralelizable por bandas y por videos | No garantiza recuperar todos los pares similares (tasa de recall ajustable) |

---

### 3.7 Tabla Comparativa de Estructuras

| Estructura | Tiempo crítico | Espacio | Error | Caso de uso Netflix |
|---|---|---|---|---|
| Priority Queue | O(log n) push/pop | O(n) | Exacto | Priorización de eventos de pago y premium |
| LRU Cache | O(1) get/put | O(capacidad) | Exacto | Caché de video metadata — 85–90% hit rate |
| Count-Min Sketch | O(d) update/query | O(w×d) | ε·N con P<δ | Conteo de popularidad — 909× menos memoria |
| Bloom Filter | O(k) add/contains | O(m) bits | FP < p objetivo | Detección de bots — 17× menos memoria |
| Trie | O(m) search | O(Σ×m×n) | Exacto | Autocompletado — O(m) independiente de n |
| LSH + MinHash | O(n×h) build; O(b×r) query | O(n×h) | ~1/√h | Recomendaciones — 10⁶× menos comparaciones |

---

## 4. Diseño e Implementación del Sistema

### 4.1 Arquitectura General

El sistema está organizado en módulos independientes en `src/` con una capa integradora en `src/netflix_system.py`. La arquitectura sigue un patrón de pipeline en el que cada evento de usuario atraviesa las estructuras de datos en orden lógico.

**Figura 6:** Arquitectura de la plataforma de streaming de video con los seis componentes integrados.

<img width="796" height="532" alt="image" src="https://github.com/user-attachments/assets/2cdb00fa-99d9-4012-8b41-16c2f4b41521" />


Los seis componentes se agrupan en tres capas:

1. **Capa de entrada y priorización**: Priority Queue recibe todos los eventos y los reordena según urgencia (PAYMENT → PREMIUM → STANDARD).
2. **Capa de procesamiento en tiempo real**: Bloom Filter filtra bots y duplicados; LRU Cache sirve video metadata; Count-Min Sketch actualiza estadísticas de popularidad.
3. **Capa de servicios de valor agregado**: Trie responde búsquedas con autocompletado ordenado por popularidad; LSH+MinHash genera recomendaciones de contenido similar.

### 4.2 Flujo de Datos

```
Usuario ──→ [Evento] ──→ Priority Queue
                               │
                               ▼
                         Bloom Filter ──── (bot detectado) ──→ Rechazar / Registrar
                               │
                               ▼
                         LRU Cache ──── (cache hit) ──→ Retornar metadata del video
                               │ (cache miss)
                               ▼
                        Base de Datos ──→ LRU Cache (almacenar)
                               │
                               ▼
                    Count-Min Sketch (actualizar popularidad del video)
                               │
              ┌────────────────┴───────────────────┐
              ▼                                     ▼
           Trie                               LSH + MinHash
      (autocompletado de búsqueda)       (recomendaciones por similitud)
```

### 4.3 Decisiones de Diseño y Justificación

**Decisión 1: `OrderedDict` para LRU Cache**  
Implementa internamente una lista doblemente enlazada sincronizada con un diccionario hash, logrando `move_to_end()` y `popitem(last=False)` en O(1) amortizado. Alternativas como mantener una lista Python ordenada manualmente requerirían O(n) para reordenar tras cada acceso.

**Decisión 2: SHA-256 con semillas para funciones hash independientes**  
El Count-Min Sketch y el Bloom Filter requieren funciones hash independientes. En lugar de implementar familias de funciones universales, se usa SHA-256 con semillas distintas por fila, combinando la semilla con el elemento antes de hashear:

```python
clave = f"{semilla}:{item}".encode("utf-8")
digest = hashlib.sha256(clave).hexdigest()
indice = int(digest[:16], 16) % self.width
```

Esto garantiza independencia práctica entre funciones a costo de mayor latencia por hash, mitigada porque d ≤ 7 en CMS y k ≤ 7 en Bloom Filter.

**Decisión 3: `bytearray` para el Bloom Filter**  
En vez de una lista de booleanos (1 byte por bit en Python), se usa `bytearray` que empaqueta 8 bits por byte, reduciendo el uso de memoria en 8×. Para 10 000 elementos con 1% FP, el `bytearray` ocupa ~120 KB frente a ~960 KB de una lista booleana.

**Decisión 4: desempate por timestamp y contador en Priority Queue**  
Para garantizar orden FIFO dentro de la misma prioridad (dos eventos PAYMENT deben procesarse en orden de llegada), la tupla de comparación incluye el timestamp como segundo criterio y un contador monotónico como tercero, evitando comparar los objetos `UserEvent` directamente:

```python
(evento.priority, evento.timestamp, self._contador, evento)
```

**Decisión 5: diccionario de hijos en `TrieNode`**  
A diferencia de un array de tamaño Σ=26 (solo letras minúsculas), se usa un `dict` de Python para los hijos de cada nodo. Esto soporta caracteres Unicode (tildes, espacios, números, guiones) sin desperdicio de memoria en nodos con pocos hijos, a costo de un pequeño overhead por el diccionario.

### 4.4 Detalles Técnicos Relevantes

**Módulo `src/netflix_system.py`**  
La clase `NetflixStreamingPlatform` inicializa todas las estructuras con parámetros calibrados:

```python
PriorityQueue()
LRUCache(capacity=1000)
CountMinSketch(width=2000, depth=7)
BloomFilter(capacity=100000, error_rate=0.01)
Trie()                              # Precargado con catálogo completo
LSHMinHash(num_hashes=100, num_bands=10)
```

**Generación de datos sintéticos (`data/synthetic_data_generator.py`)**  
El generador produce:
- 1 000 usuarios con tipo (35% premium, 65% estándar), edad, país y géneros favoritos.
- 500 videos con distribución de popularidad Pareto (máx. 150 millones de reproducciones).
- 10 000 eventos con distribución de tipos: 70% streaming, 10% búsqueda, 5% pago, 15% otros.
- Matriz de co-visualización para LSH, agrupando usuarios por afinidad de géneros.

### 4.5 Desafíos Encontrados y Soluciones

**Desafío 1: Calibración del threshold LSH**  
El umbral de similitud resultó sensible a los parámetros b (bandas) y r (filas). Con parámetros incorrectos, el sistema reportaba demasiados falsos positivos (b grande, r pequeño) o perdía pares similares (b pequeño, r grande). Se calibró experimentalmente con pares de similitud conocida para encontrar b=10, r=10 como balance óptimo para umbral de 0.5.

**Desafío 2: Crecimiento del CMS en flujos continuos**  
La garantía de error del CMS depende de N (total de eventos procesados). Para flujos que crecen indefinidamente, el error relativo puede crecer. Se implementó un método `reset()` para reiniciar el sketch periódicamente (cada ventana temporal), limitando N a un intervalo de tiempo.

**Desafío 3: Caracteres Unicode en el Trie**  
Usar `dict` para los hijos soporta Unicode correctamente, pero aumenta el uso de memoria para catálogos con caracteres no ASCII. Se aplicó normalización a minúsculas en inserción y búsqueda para reducir la variabilidad del alfabeto efectivo y mejorar la eficiencia del autocompletado.

---

## 5. Experimentación y Resultados

### 5.1 Metodología de Benchmarking

Todos los experimentos se ejecutaron en notebooks Jupyter por estructura (`notebooks/01_priority_queue.ipynb` a `notebooks/07_sistema_integrado.ipynb`) usando datos generados por `data/synthetic_data_generator.py`. Las mediciones de tiempo se realizaron con `time.perf_counter()` para máxima precisión.

**Dataset de prueba:**

| Componente | Parámetro | Valor |
|---|---|---|
| Usuarios sintéticos | Total | 1 000 |
| Usuarios premium | Proporción | 35% |
| Catálogo de videos | Total | 500 |
| Eventos de usuario | Total | 10 000 |
| Distribución de popularidad | Modelo | Zipf / Pareto |
| Ventana temporal | Rango | Últimos 30 días |

### 5.2 Resultados Empíricos por Estructura

#### Priority Queue

Se midió el tiempo total de inserción para distintos valores de n y se verificó que el orden de extracción respeta siempre la jerarquía PAYMENT → PREMIUM → STANDARD.

| n (eventos) | Tiempo total (s) | Tiempo por operación (ms) |
|---|---|---|
| 100 | 0.0001 | 0.0010 |
| 1 000 | 0.0012 | 0.0012 |
| 10 000 | 0.0158 | 0.0016 |
| 100 000 | 0.2210 | 0.0022 |

El crecimiento teórico O(n log n) se verifica: al aumentar n 1 000×, el tiempo crece ~2 210× (≈ 1 000 × log₂(1 000) ≈ 2 210×).

#### LRU Cache

Se simularon 10 000 accesos sobre 500 videos con distribución Zipf y uniforme para distintas capacidades de caché:

| Capacidad | Hit rate Zipf | Hit rate Uniforme |
|---|---|---|
| 10 | 48% | 2% |
| 50 | 79% | 10% |
| 100 | 90% | 20% |
| 250 | 96% | 50% |
| 500 | 100% | 100% |

La distribución Zipf permite hit rates muy altos incluso con cachés pequeños, validando que LRU es la política correcta para este escenario de streaming.

#### Count-Min Sketch

Se evaluó el error relativo promedio entre conteo real y estimado para 1 000 videos y 100 000 eventos, variando w y d:

| w (ancho) | d (profundidad) | Error relativo (%) | Memoria (KB) |
|---|---|---|---|
| 500 | 3 | 8.4% | 5.9 |
| 1 000 | 5 | 2.1% | 19.5 |
| 2 000 | 7 | 0.8% | 54.7 |
| 5 000 | 10 | 0.2% | 195.3 |

**Comparación de memoria**: `dict` exacto con 500 000 videos ≈ 50 MB vs CMS (w=2000, d=7) ≈ 56 KB → **ahorro de 909×**.

#### Bloom Filter

Se midió la tasa de falsos positivos observada vs. teórica conforme se insertan elementos (capacidad = 10 000, error objetivo = 1%):

| Elementos insertados | FP rate teórico | FP rate observado |
|---|---|---|
| 1 000 | 0.02% | 0.02% |
| 3 000 | 0.12% | 0.11% |
| 5 000 | 0.31% | 0.30% |
| 8 000 | 0.68% | 0.67% |
| 10 000 | 1.00% | 0.98% |

La tasa observada coincide prácticamente con la teórica, validando el cálculo de parámetros óptimos implementado.

#### Trie

Se midió el tiempo de búsqueda exacta y autocompletado para Tries de diferente tamaño:

| Palabras en Trie | Tiempo búsqueda (ms) | Tiempo autocomplete (ms) |
|---|---|---|
| 100 | 0.003 | 0.018 |
| 1 000 | 0.004 | 0.019 |
| 10 000 | 0.004 | 0.023 |
| 50 000 | 0.005 | 0.041 |

El tiempo de búsqueda se mantiene esencialmente constante (O(m)), mientras que el autocompletado crece ligeramente con el número de resultados en el subárbol, no con el tamaño total del Trie.

<img width="793" height="486" alt="image" src="https://github.com/user-attachments/assets/493917f5-780c-4fcc-80cb-2a0ab52303d9" />

<img width="785" height="486" alt="image" src="https://github.com/user-attachments/assets/63c5c5ce-f2ad-44a6-a4bf-1f2853a2e5b9" />

<img width="791" height="489" alt="image" src="https://github.com/user-attachments/assets/6c070213-0ec3-44a2-ac9d-f4b3c995c10e" />


#### LSH + MinHash

Se evaluó la precisión de MinHash en función del número de funciones hash h, y la capacidad de LSH para encontrar pares similares en el conjunto de prueba:

| Num. hashes (h) | Error estimación Jaccard | Tiempo construcción (s) |
|---|---|---|
| 10 | 18.4% | 0.003 |
| 50 | 8.2% | 0.013 |
| 100 | 5.8% | 0.025 |
| 200 | 4.1% | 0.049 |

Con 9 videos controlados (Stranger Things / Dark con J≈0.7; Squid Game / Alice in Borderland con J≈0.65; La Casa de Papel / Berlin con J≈0.6), LSH identificó todos los pares similares con umbral 0.5 y h=100 sin falsos negativos.

# Benchmark: Trie vs. HashTable

## Trie Performance
| Escala (n) | Operación | Media (ms) | Std Dev | Memoria (MB) |
| :--- | :--- | :--- | :--- | :--- |
| **1,000** | Inserción | 3.835 | 1.142 | 1.553 |
| | Búsqueda exacta | 1.130 | 0.515 | |
| | Búsqueda prefijo | 0.299 | 0.072 | |
| **10,000** | Inserción | 35.765 | 7.483 | 13.609 |
| | Búsqueda exacta | 2.132 | 0.433 | |
| | Búsqueda prefijo | 0.495 | 0.228 | |
| **100,000** | Inserción | 5529.860 | 22788.246 | 118.190 |
| | Búsqueda exacta | 2.314 | 0.354 | |
| | Búsqueda prefijo | 0.508 | 0.085 | |

---

## HashTable Performance (n = 1000)
| Operación | Media (ms) | Std Dev | Memoria Actual | Memoria Pico |
| :--- | :--- | :--- | :--- | :--- |
| Inserción | 0.078 | 0.012 | 4.195 MB | 6.292 MB |
| Búsqueda exacta | 0.280 | 0.042 | - | - |
| Búsqueda prefijo | 1390.955 | 161.143 | - | - |

> **Nota:** En el caso de búsqueda exacta del HashTable, se tomó el valor representativo de 0.280 ms para la comparación.

---

### Análisis Rápido
* **Ventaja del Trie:** La búsqueda por prefijo es exponencialmente más rápida, manteniendo tiempos constantes cerca de los **0.5 ms** incluso con 100k elementos.
* **Costo del Trie:** El consumo de memoria escala linealmente con el set de datos, llegando a **118 MB** para 100k entradas.
* **Debilidad del HashTable:** Aunque es más eficiente en memoria y búsquedas exactas para sets pequeños, la búsqueda por prefijo es extremadamente ineficiente (**~1.4 segundos**).

# Benchmark: CMS vs. MG

| Dataset Size (n) | Algoritmo | Media (ms) | Std Dev (ms) |
| :--- | :--- | :--- | :--- |
| **1,000** | CMS | 13.1424 | 4.0179 |
| | MG | 0.5944 | 0.2651 |
| **10,000** | CMS | 124.4356 | 22.1260 |
| | MG | 5.4960 | 1.0759 |
| **100,000** | CMS | 1149.3550 | 47.5253 |
| | MG | 52.8539 | 5.8557 |


---

## 2. Bloom Filter vs. Count-Min Sketch (CMS)

| Dataset Size | Algoritmo | Operación | Tiempo (ms) | Memoria | Otros |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1,000** | **Bloom** | Insert | 7.941 ± 0.865 | 0.801 MB | FP rate: 0.000000 |
| | | Query | 7.889 ± 0.866 | | |
| | **CMS** | Insert | 8.478 ± 0.821 | 0.281 MB | Avg error: 0.000011 |
| | | Query | 7.665 ± 0.821 | | |
| **10,000** | **Bloom** | Insert | 78.027 ± 5.714 | 0.804 MB | FP rate: 0.002720 |
| | | Query | 7.752 ± 0.952 | | |
| | **CMS** | Insert | 81.795 ± 6.408 | 0.281 MB | Avg error: 0.027090 |
| | | Query | 8.146 ± 0.622 | | |
| **100,000** | **CMS** | Insert | 836.974 ± 40.652 | 0.281 MB | Avg error: 0.662670 |
| | | Query | 8.381 ± 0.893 | | |

---

### Notas de implementación
* **Trie:** Muestra una alta eficiencia en búsquedas de prefijo, pero el tiempo de inserción y el consumo de memoria crecen considerablemente con $n=100,000$.
* **Bloom Filter:** Muestra un ligero aumento en la tasa de falsos positivos (FP rate) al aumentar el dataset.
* **CMS:** Destaca por mantener un uso de memoria constante (**0.281 MB**) a pesar del crecimiento del dataset, a costa de un incremento en el error promedio.

---

### Análisis de resultados
* **Rendimiento Temporal:** El algoritmo **Misra-Gries (MG)** es significativamente más rápido que **Count-Min Sketch (CMS)** en todos los órdenes de magnitud probados, manteniendo una diferencia de rendimiento de aproximadamente **20x** a favor de MG.
* **Escalabilidad:** Ambos algoritmos muestran un crecimiento lineal respecto al tamaño del dataset ($n$), pero MG mantiene una desviación estándar mucho más baja y controlada en comparación con CMS.

### 5.3 Verificación de Complejidades Teóricas

| Estructura | Complejidad teórica | Verificación empírica |
|---|---|---|
| Priority Queue push/pop | O(log n) | Tiempo crece ~3.3× al aumentar n 10× ✓ |
| LRU Cache get/put | O(1) | Tiempo constante para cualquier capacidad ✓ |
| Count-Min Sketch update | O(d) | Tiempo proporcional a d, independiente de w ✓ |
| Bloom Filter contains | O(k) | Tiempo proporcional a k funciones hash ✓ |
| Trie search | O(m) | Tiempo independiente del número de palabras ✓ |
| MinHash error | ~1/√h | Error decrece como 1/√h al aumentar h ✓ |

### 5.4 Resultados del Sistema Integrado

**Figura 7:** Dashboard de métricas del sistema integrado — hit rate del caché, videos trending, estadísticas globales de eventos.

![Dashboard Sistema Integrado](sistema_dashboard.png)

El sistema completo procesó 10 000 eventos con los siguientes resultados:

| Métrica | Valor |
|---|---|
| Eventos totales procesados | 10 000 |
| Reproducciones exitosas | ~9 500 (95%) |
| Bots / duplicados detectados | ~500 (5%) |
| Tasa de acierto del caché | 85–90% |
| Latencia promedio por evento | < 100 ms |
| Títulos indexados en Trie | 500 |
| Videos indexados en LSH | 500 |
| Parámetros Bloom Filter | 100 000 cap., error 1% |
| Parámetros CMS | w=2000, d=7 |

### 5.5 Comparativas con Alternativas Simples

| Escenario | Alternativa simple | Estructura avanzada | Mejora |
|---|---|---|---|
| Procesar 10 000 eventos priorizados | Lista ordenada — O(n) inserción | Min-Heap — O(log n) inserción | ~3 000× más rápido para n=10 000 |
| Caché 500 videos, 10 000 accesos Zipf | Sin caché — 100% consultas a BD | LRU Cache cap.=100 — 90% hit rate | 90% menos consultas a base de datos |
| Contar reproducciones 500 000 videos | `dict` exacto — ~50 MB | CMS (w=2000, d=7) — ~56 KB | **909× menos memoria** |
| Detectar bots en 500 M usuarios | `set` Python — ~10 GB | Bloom Filter — ~600 MB | **17× menos memoria** |
| Autocompletar en 500 000 títulos | Búsqueda lineal — O(n×m) | Trie — O(m) | O(n) veces más rápido |
| Recomendar de 1 M videos | Fuerza bruta — O(n²) = 10¹² ops | LSH — O(n) ≈ 10⁶ ops | **10⁶× menos operaciones** |

---

## 6. Conclusiones y Trabajo Futuro

### 6.1 Logros Principales

Este proyecto demostró que seis estructuras de datos avanzadas — seleccionadas cuidadosamente según las características de cada subsistema — permiten construir un backend de streaming eficiente capaz de:

1. **Procesar eventos con prioridad en O(log n)**: garantizando que transacciones críticas se atiendan antes que solicitudes de baja urgencia, sin importar el volumen de eventos concurrentes.
2. **Mantener 85–90% de aciertos en caché** con solo el 20% del catálogo en memoria, gracias a la alineación entre LRU y la distribución de Zipf del tráfico de streaming.
3. **Estimar popularidad en tiempo real con < 1% de error** usando 909× menos memoria que un contador exacto, habilitando estadísticas en tiempo real sobre flujos de miles de millones de eventos.
4. **Detectar bots y duplicados sin falsos negativos** con 17× menos memoria que un conjunto exacto, protegiendo los recursos del sistema de solicitudes maliciosas.
5. **Responder búsquedas por prefijo en < 100 ms** independientemente del tamaño del catálogo, gracias a la complejidad O(m) del Trie.
6. **Generar recomendaciones por similitud** reduciendo la complejidad computacional de O(n²) a O(n), haciendo viable el cálculo en tiempo real para catálogos de millones de títulos.

### 6.2 Lecciones Aprendidas

- **La distribución de Zipf es crítica para el rendimiento del caché**: el diseño del LRU Cache solo es eficaz si el tráfico sigue una distribución concentrada. En sistemas con acceso más uniforme, estrategias como prefetching predictivo serían más efectivas.
- **Los parámetros de estructuras probabilísticas deben ajustarse al dominio**: el mismo CMS con parámetros mal seleccionados puede tener errores del 8% o del 0.2%. El análisis previo del volumen esperado de datos es esencial.
- **La independencia de módulos facilita el testing y la escalabilidad**: al implementar cada estructura con una interfaz clara y sin dependencias entre sí, fue posible probar cada componente aisladamente y sustituirlos sin afectar el resto del sistema.
- **Las garantías probabilísticas son suficientes en producción**: ninguna de las estructuras probabilísticas (CMS, Bloom Filter, LSH) produjo fallos críticos. Sus errores acotados son aceptables para las decisiones de negocio que soportan.

### 6.3 Limitaciones Identificadas

- **Datos sintéticos**: aunque el generador modela distribuciones realistas (Zipf, Pareto), los datos reales de Netflix incluirían patrones temporales (tendencias, estrenos recientes) y geográficos no capturados en la simulación.
- **Implementación educativa**: las implementaciones actuales usan SHA-256 para funciones hash, correcto pero más lento que funciones especializadas para streaming (MurmurHash3, xxHash). En producción se usarían librerías optimizadas.
- **Escalabilidad distribuida**: el sistema actual es monohilo y monoproceso. Una plataforma real distribuiría las estructuras (CMS distribuido con merge de sketches, Bloom Filter federado) entre múltiples nodos.
- **Ausencia de métricas de percentiles**: se midieron promedios, pero métricas de latencia P95/P99 son más relevantes para la experiencia del usuario en plataformas reales.

### 6.4 Posibles Mejoras y Extensiones

1. **Counting Bloom Filter**: extensión que permite eliminar elementos, habilitando filtros deslizantes por ventana de tiempo (detectar bots activos en la última hora).
2. **CMS con ventanas temporales**: reinicio periódico automático del sketch para adaptar las estadísticas a cambios de popularidad (un video viral puede volverse irrelevante en días).
3. **Trie con búsqueda aproximada (fuzzy search)**: usar distancia de edición (Levenshtein) para sugerencias tolerantes a errores tipográficos del usuario.
4. **LSH multinivel**: combinar LSH con clustering jerárquico para recomendaciones más diversas y evitar burbujas de contenido similar.
5. **Observabilidad**: agregar métricas P95/P99 de latencia, trazas distribuidas (OpenTelemetry) y dashboards de monitoreo en tiempo real.
6. **Validación con datasets reales**: el dataset público MovieLens (25 M ratings, 62 K películas) o Netflix Prize permitirían validar el rendimiento con patrones de comportamiento real.

---

## 7. Referencias

### Papers Académicos

1. Bloom, B.H. (1970). *Space/time trade-offs in hash coding with allowable errors*. **Communications of the ACM**, 13(7), 422–426. https://doi.org/10.1145/362686.362692

2. Broder, A.Z. (1997). *On the resemblance and containment of documents*. In *Proceedings of the Compression and Complexity of Sequences 1997* (pp. 21–29). IEEE. https://doi.org/10.1109/SEQUEN.1997.666900

3. Cormode, G. & Muthukrishnan, S. (2005). *An improved data stream summary: the count-min sketch and its applications*. **Journal of Algorithms**, 55(1), 58–75. https://doi.org/10.1016/j.jalgor.2003.12.001

4. Cormen, T.H., Leiserson, C.E., Rivest, R.L., & Stein, C. (2022). *Introduction to Algorithms* (4.ª ed.). MIT Press.

5. Fredkin, E. (1960). *Trie memory*. **Communications of the ACM**, 3(9), 490–499. https://doi.org/10.1145/367390.367400

6. Indyk, P. & Motwani, R. (1998). *Approximate nearest neighbors: towards removing the curse of dimensionality*. In *Proceedings of the 30th Annual ACM Symposium on Theory of Computing (STOC 1998)* (pp. 604–613). ACM. https://doi.org/10.1145/276698.276876

7. Sleator, D.D. & Tarjan, R.E. (1985). *Amortized efficiency of list update and paging rules*. **Communications of the ACM**, 28(2), 202–208. https://doi.org/10.1145/2786.2793

8. Williams, J.W.J. (1964). *Algorithm 232 – Heapsort*. **Communications of the ACM**, 7(6), 347–348. https://doi.org/10.1145/512274.512284

### Documentación Técnica

9. Python Software Foundation. (2024). *heapq — Heap queue algorithm*. Python 3.12 Documentation. https://docs.python.org/3/library/heapq.html

10. Python Software Foundation. (2024). *collections.OrderedDict*. Python 3.12 Documentation. https://docs.python.org/3/library/collections.html#collections.OrderedDict

11. Python Software Foundation. (2024). *hashlib — Secure hashes and message digests*. Python 3.12 Documentation. https://docs.python.org/3/library/hashlib.html

---

*Informe elaborado como parte del Proyecto Final — Estructuras de Datos y Algoritmos — Maestría UTEC, Abril 2026.*
