"""
Tests unitarios para todas las estructuras de datos del proyecto Netflix EDA.

Ejecutar:
    python -m pytest tests/test_all.py -v
    python tests/test_all.py
"""

import sys
import os
import unittest
import time

# Agregar el directorio raíz al path para importar los módulos
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.priority_queue import PriorityQueue, UserEvent, PRIORITY_PAYMENT, PRIORITY_PREMIUM, PRIORITY_STANDARD
from src.lru_cache import LRUCache, VideoContent
from src.count_min_sketch import CountMinSketch
from src.bloom_filter import BloomFilter
from src.trie import Trie
from src.lsh_minhash import LSHMinHash, MinHash


class TestPriorityQueue(unittest.TestCase):
    """Tests para la Cola de Prioridad basada en heap."""

    def setUp(self):
        """Crear una cola de prioridad para cada test."""
        self.pq = PriorityQueue()

    def test_cola_inicial_vacia(self):
        """La cola debe estar vacía al inicializarse."""
        self.assertTrue(self.pq.is_empty())
        self.assertEqual(self.pq.size(), 0)

    def test_push_aumenta_tamaño(self):
        """Insertar elementos debe aumentar el tamaño de la cola."""
        evento = UserEvent("u1", "stream", PRIORITY_STANDARD)
        self.pq.push(evento)
        self.assertEqual(self.pq.size(), 1)
        self.assertFalse(self.pq.is_empty())

    def test_pop_retorna_evento(self):
        """Pop debe retornar el evento insertado."""
        evento = UserEvent("u1", "stream", PRIORITY_STANDARD)
        self.pq.push(evento)
        resultado = self.pq.pop()
        self.assertIsNotNone(resultado)
        self.assertEqual(resultado.user_id, "u1")

    def test_pop_cola_vacia_retorna_none(self):
        """Pop en cola vacía debe retornar None."""
        resultado = self.pq.pop()
        self.assertIsNone(resultado)

    def test_peek_no_modifica_cola(self):
        """Peek no debe modificar el tamaño de la cola."""
        evento = UserEvent("u1", "stream", PRIORITY_STANDARD)
        self.pq.push(evento)
        tamaño_antes = self.pq.size()
        self.pq.peek()
        self.assertEqual(self.pq.size(), tamaño_antes)

    def test_orden_prioridad_payment_primero(self):
        """Eventos de pago deben procesarse antes que premium y standard."""
        self.pq.push(UserEvent("u3", "stream", PRIORITY_STANDARD))
        self.pq.push(UserEvent("u1", "payment", PRIORITY_PAYMENT))
        self.pq.push(UserEvent("u2", "stream", PRIORITY_PREMIUM))

        primero = self.pq.pop()
        self.assertEqual(primero.priority, PRIORITY_PAYMENT)
        self.assertEqual(primero.user_id, "u1")

    def test_orden_prioridad_premium_antes_standard(self):
        """Eventos premium deben procesarse antes que standard."""
        self.pq.push(UserEvent("u2", "stream", PRIORITY_STANDARD))
        self.pq.push(UserEvent("u1", "stream", PRIORITY_PREMIUM))

        primero = self.pq.pop()
        self.assertEqual(primero.priority, PRIORITY_PREMIUM)

    def test_orden_completo_prioridades(self):
        """Los tres niveles de prioridad deben salir en orden correcto."""
        self.pq.push(UserEvent("u3", "stream", PRIORITY_STANDARD))
        self.pq.push(UserEvent("u1", "payment", PRIORITY_PAYMENT))
        self.pq.push(UserEvent("u2", "stream", PRIORITY_PREMIUM))

        orden = []
        while not self.pq.is_empty():
            orden.append(self.pq.pop().priority)

        self.assertEqual(orden, [PRIORITY_PAYMENT, PRIORITY_PREMIUM, PRIORITY_STANDARD])

    def test_multiples_misma_prioridad_fifo(self):
        """Eventos con misma prioridad deben salir en orden FIFO (timestamp)."""
        for i in range(5):
            evento = UserEvent(f"u{i}", "stream", PRIORITY_STANDARD)
            self.pq.push(evento)
            time.sleep(0.001)  # Garantizar timestamps diferentes

        primer_id = self.pq.pop().user_id
        self.assertEqual(primer_id, "u0")  # El primero insertado sale primero

    def test_push_pop_100_eventos(self):
        """La cola debe manejar correctamente 100 eventos."""
        import random
        for i in range(100):
            prioridad = random.choice([PRIORITY_PAYMENT, PRIORITY_PREMIUM, PRIORITY_STANDARD])
            self.pq.push(UserEvent(f"u{i}", "stream", prioridad))

        self.assertEqual(self.pq.size(), 100)

        prioridad_anterior = -1
        while not self.pq.is_empty():
            evento = self.pq.pop()
            self.assertGreaterEqual(evento.priority, prioridad_anterior)
            prioridad_anterior = evento.priority


class TestLRUCache(unittest.TestCase):
    """Tests para el LRU Cache."""

    def setUp(self):
        """Crear un caché pequeño para cada test."""
        self.cache = LRUCache(capacity=3)
        self.video1 = VideoContent("v001", "Stranger Things", "Drama", 89000000, 1200.0)
        self.video2 = VideoContent("v002", "Squid Game", "Thriller", 111000000, 850.0)
        self.video3 = VideoContent("v003", "Dark", "Sci-Fi", 40000000, 950.0)
        self.video4 = VideoContent("v004", "Narcos", "Drama", 45000000, 800.0)

    def test_cache_inicial_vacio(self):
        """El caché debe estar vacío al inicializarse."""
        self.assertEqual(len(self.cache), 0)

    def test_get_elemento_inexistente(self):
        """Get de elemento inexistente debe retornar None y contar como miss."""
        resultado = self.cache.get("v999")
        self.assertIsNone(resultado)
        self.assertEqual(self.cache.get_stats()["misses"], 1)

    def test_put_y_get_basico(self):
        """Insertar y recuperar un elemento básico."""
        self.cache.put("v001", self.video1)
        resultado = self.cache.get("v001")
        self.assertIsNotNone(resultado)
        self.assertEqual(resultado.title, "Stranger Things")

    def test_hit_contado_correctamente(self):
        """Los hits deben contarse correctamente."""
        self.cache.put("v001", self.video1)
        self.cache.get("v001")
        self.cache.get("v001")
        stats = self.cache.get_stats()
        self.assertEqual(stats["hits"], 2)

    def test_miss_contado_correctamente(self):
        """Los misses deben contarse correctamente."""
        self.cache.get("v999")
        self.cache.get("v998")
        stats = self.cache.get_stats()
        self.assertEqual(stats["misses"], 2)

    def test_eviccion_lru(self):
        """El elemento menos recientemente usado debe ser evictado cuando el caché está lleno."""
        # Llenar el caché
        self.cache.put("v001", self.video1)
        self.cache.put("v002", self.video2)
        self.cache.put("v003", self.video3)

        # El caché está lleno. Agregar v004 debe evictar v001 (el LRU)
        self.cache.put("v004", self.video4)

        # v001 debe haber sido evictado
        self.assertFalse(self.cache.contains("v001"))
        # v004 debe estar en caché
        self.assertTrue(self.cache.contains("v004"))

    def test_acceso_actualiza_orden_lru(self):
        """Acceder a un elemento debe actualizarlo como el más reciente."""
        self.cache.put("v001", self.video1)
        self.cache.put("v002", self.video2)
        self.cache.put("v003", self.video3)

        # Acceder a v001 lo hace el más reciente
        self.cache.get("v001")

        # Insertar v004 debe evictar v002 (ahora el LRU, no v001)
        self.cache.put("v004", self.video4)

        self.assertTrue(self.cache.contains("v001"))
        self.assertFalse(self.cache.contains("v002"))

    def test_capacidad_respetada(self):
        """El caché nunca debe exceder su capacidad."""
        for i in range(10):
            video = VideoContent(f"v{i:03d}", f"Video {i}", "Drama")
            self.cache.put(f"v{i:03d}", video)

        self.assertLessEqual(len(self.cache), self.cache.capacity)

    def test_contains_no_modifica_lru(self):
        """Contains no debe modificar el orden LRU."""
        self.cache.put("v001", self.video1)
        self.cache.put("v002", self.video2)
        self.cache.put("v003", self.video3)

        # contains no debe actualizar el orden LRU
        self.cache.contains("v001")

        # Insertar v004 debe evictar v001 (sigue siendo el LRU)
        self.cache.put("v004", self.video4)
        self.assertFalse(self.cache.contains("v001"))

    def test_hit_rate_calculado(self):
        """El hit rate debe calcularse correctamente."""
        self.cache.put("v001", self.video1)
        self.cache.get("v001")  # hit
        self.cache.get("v001")  # hit
        self.cache.get("v999")  # miss
        stats = self.cache.get_stats()
        self.assertAlmostEqual(stats["hit_rate"], 66.67, places=1)

    def test_most_recent(self):
        """Most_recent debe retornar los elementos más recientemente accedidos."""
        self.cache.put("v001", self.video1)
        self.cache.put("v002", self.video2)
        self.cache.put("v003", self.video3)

        recientes = self.cache.most_recent(2)
        self.assertEqual(len(recientes), 2)
        # v003 fue el último insertado, debe ser el más reciente
        self.assertEqual(recientes[0].video_id, "v003")

    def test_capacidad_invalida(self):
        """Capacidad <= 0 debe lanzar ValueError."""
        with self.assertRaises(ValueError):
            LRUCache(capacity=0)
        with self.assertRaises(ValueError):
            LRUCache(capacity=-1)


class TestCountMinSketch(unittest.TestCase):
    """Tests para el Count-Min Sketch."""

    def setUp(self):
        """Crear un CMS para cada test."""
        self.cms = CountMinSketch(width=1000, depth=5)

    def test_query_elemento_no_visto(self):
        """Un elemento no visto debe tener frecuencia 0."""
        self.assertEqual(self.cms.query("video_inexistente"), 0)

    def test_update_y_query_basico(self):
        """Update y query básicos deben funcionar."""
        self.cms.update("video1")
        self.assertEqual(self.cms.query("video1"), 1)

    def test_update_multiple_veces(self):
        """Múltiples updates deben acumularse."""
        self.cms.update("video1", 5)
        self.cms.update("video1", 3)
        resultado = self.cms.query("video1")
        # Puede ser ligeramente sobreestimado pero nunca menos que 8
        self.assertGreaterEqual(resultado, 8)

    def test_no_subestima(self):
        """El CMS NUNCA debe subestimar la frecuencia real."""
        items = ["video1", "video2", "video3"]
        conteos_reales = {"video1": 100, "video2": 50, "video3": 200}

        for item, count in conteos_reales.items():
            self.cms.update(item, count)

        for item, count_real in conteos_reales.items():
            estimado = self.cms.query(item)
            self.assertGreaterEqual(estimado, count_real,
                f"{item}: estimado={estimado} < real={count_real}")

    def test_top_k_retorna_correctos(self):
        """Top-k debe retornar los k elementos más frecuentes."""
        videos = {"v1": 100, "v2": 50, "v3": 200, "v4": 30, "v5": 150}
        for vid, count in videos.items():
            self.cms.update(vid, count)

        top_3 = self.cms.top_k(3)
        self.assertEqual(len(top_3), 3)

        # Los top 3 deben ser v3, v5, v1 (orden por frecuencia)
        top_ids = [item[0] for item in top_3]
        self.assertIn("v3", top_ids)  # El más frecuente debe estar
        self.assertIn("v5", top_ids)

    def test_reset_limpia_contadores(self):
        """Reset debe limpiar todos los contadores."""
        self.cms.update("video1", 100)
        self.cms.reset()
        self.assertEqual(self.cms.query("video1"), 0)
        self.assertEqual(len(self.cms.top_k(10)), 0)

    def test_parametros_invalidos(self):
        """Width o depth <= 0 debe lanzar ValueError."""
        with self.assertRaises(ValueError):
            CountMinSketch(width=0, depth=5)
        with self.assertRaises(ValueError):
            CountMinSketch(width=100, depth=0)

    def test_get_stats(self):
        """Get stats debe retornar información correcta."""
        self.cms.update("v1", 10)
        self.cms.update("v2", 20)
        stats = self.cms.get_stats()
        self.assertEqual(stats["width"], 1000)
        self.assertEqual(stats["depth"], 5)
        self.assertEqual(stats["total_actualizaciones"], 30)
        self.assertEqual(stats["elementos_unicos"], 2)


class TestBloomFilter(unittest.TestCase):
    """Tests para el Filtro de Bloom."""

    def setUp(self):
        """Crear un Bloom Filter para cada test."""
        self.bf = BloomFilter(capacity=1000, error_rate=0.01)

    def test_contains_elemento_no_agregado(self):
        """Un elemento no agregado usualmente no debe estar en el filtro."""
        # Nota: Puede haber falsos positivos, pero con alta probabilidad no
        self.assertFalse(self.bf.contains("elemento_nuevo"))

    def test_add_y_contains(self):
        """Un elemento agregado siempre debe ser encontrado (sin falsos negativos)."""
        self.bf.add("usuario_legit_001")
        self.assertTrue(self.bf.contains("usuario_legit_001"))

    def test_sin_falsos_negativos(self):
        """No debe haber falsos negativos — elemento agregado siempre detectado."""
        elementos = [f"user_{i}" for i in range(100)]
        for elem in elementos:
            self.bf.add(elem)

        for elem in elementos:
            self.assertTrue(self.bf.contains(elem),
                f"Falso negativo para '{elem}'")

    def test_tasa_falsos_positivos_aceptable(self):
        """La tasa de falsos positivos debe estar cerca del objetivo."""
        # Agregar 1000 elementos
        for i in range(1000):
            self.bf.add(f"legit_{i}")

        # Verificar 1000 elementos que NO están
        falsos_positivos = sum(
            1 for i in range(10000, 11000)
            if self.bf.contains(f"nuevo_{i}")
        )
        tasa_fp = falsos_positivos / 1000
        # La tasa debe ser razonablemente cercana al objetivo (error_rate=0.01)
        self.assertLess(tasa_fp, 0.05)  # Tolerancia generosa

    def test_parametros_invalidos(self):
        """Parámetros inválidos deben lanzar ValueError."""
        with self.assertRaises(ValueError):
            BloomFilter(capacity=0)
        with self.assertRaises(ValueError):
            BloomFilter(capacity=1000, error_rate=0)
        with self.assertRaises(ValueError):
            BloomFilter(capacity=1000, error_rate=1.0)

    def test_tamaño_optimo_calculado(self):
        """El tamaño de bits debe ser mayor que la capacidad."""
        self.assertGreater(self.bf.size, self.bf.capacity)

    def test_num_hashes_positivo(self):
        """El número de funciones hash debe ser al menos 1."""
        self.assertGreaterEqual(self.bf.num_hashes, 1)

    def test_get_info_retorna_datos(self):
        """get_info debe retornar información sobre el filtro."""
        info = self.bf.get_info()
        self.assertIn("capacidad", info)
        self.assertIn("num_hashes", info)
        self.assertIn("tamaño_bits", info)
        self.assertEqual(info["capacidad"], 1000)


class TestTrie(unittest.TestCase):
    """Tests para el Trie (Árbol de Prefijos)."""

    def setUp(self):
        """Crear un Trie con datos de prueba."""
        self.trie = Trie()
        titulos = [
            ("Stranger Things", 89),
            ("Squid Game", 111),
            ("Dark", 40),
            ("Daredevil", 44),
            ("The Crown", 60),
            ("The Witcher", 50),
        ]
        for titulo, pop in titulos:
            self.trie.insert(titulo, popularity=pop)

    def test_insert_y_search_basico(self):
        """Insert y search básicos deben funcionar."""
        self.trie.insert("Test Show", popularity=100)
        resultado = self.trie.search("Test Show")
        self.assertEqual(resultado, 100)

    def test_search_inexistente_retorna_none(self):
        """Buscar una palabra inexistente debe retornar None."""
        self.assertIsNone(self.trie.search("Titulo Inexistente"))

    def test_size_correcto(self):
        """El tamaño debe reflejar el número de palabras insertadas."""
        self.assertEqual(self.trie.size(), 6)

    def test_autocomplete_con_prefijo(self):
        """Autocomplete debe retornar títulos que comienzan con el prefijo."""
        sugerencias = self.trie.autocomplete("str")
        titulos = [s[0] for s in sugerencias]
        self.assertIn("Stranger Things", titulos)

    def test_autocomplete_prefijo_comun(self):
        """Autocomplete con prefijo 'the' debe retornar títulos con The."""
        sugerencias = self.trie.autocomplete("the")
        titulos = [s[0].lower() for s in sugerencias]
        self.assertTrue(any(t.startswith("the") for t in titulos))

    def test_autocomplete_ordenado_por_popularidad(self):
        """Autocomplete debe ordenar resultados por popularidad (mayor primero)."""
        # Dark (40) y Daredevil (44) comparten prefijo "da"
        sugerencias = self.trie.autocomplete("da")
        if len(sugerencias) >= 2:
            self.assertGreaterEqual(sugerencias[0][1], sugerencias[1][1])

    def test_autocomplete_prefijo_inexistente(self):
        """Autocomplete con prefijo inexistente debe retornar lista vacía."""
        sugerencias = self.trie.autocomplete("xyz123")
        self.assertEqual(sugerencias, [])

    def test_starts_with_existente(self):
        """starts_with debe retornar True para prefijos que existen."""
        self.assertTrue(self.trie.starts_with("str"))
        self.assertTrue(self.trie.starts_with("the"))

    def test_starts_with_inexistente(self):
        """starts_with debe retornar False para prefijos que no existen."""
        self.assertFalse(self.trie.starts_with("xyz"))

    def test_delete_elimina_palabra(self):
        """Delete debe eliminar una palabra del Trie."""
        self.trie.insert("Temporal Show", popularity=1)
        self.assertTrue(self.trie.search("Temporal Show") is not None)
        self.trie.delete("Temporal Show")
        self.assertIsNone(self.trie.search("Temporal Show"))

    def test_delete_reduce_tamaño(self):
        """Delete debe reducir el tamaño del Trie."""
        tamaño_antes = self.trie.size()
        self.trie.delete("Dark")
        self.assertEqual(self.trie.size(), tamaño_antes - 1)

    def test_delete_inexistente_retorna_false(self):
        """Delete de palabra inexistente debe retornar False."""
        resultado = self.trie.delete("No Existe")
        self.assertFalse(resultado)

    def test_insert_actualiza_popularidad(self):
        """Insertar la misma palabra dos veces debe acumular popularidad."""
        self.trie.insert("Nueva Serie", popularity=100)
        self.trie.insert("Nueva Serie", popularity=50)
        pop = self.trie.search("Nueva Serie")
        self.assertEqual(pop, 150)

    def test_autocomplete_max_sugerencias(self):
        """Autocomplete debe respetar el límite de sugerencias."""
        # Agregar muchos títulos con 'a'
        for i in range(20):
            self.trie.insert(f"Action Movie {i}", popularity=i)
        sugerencias = self.trie.autocomplete("ac", max_suggestions=5)
        self.assertLessEqual(len(sugerencias), 5)


class TestLSH(unittest.TestCase):
    """Tests para LSH + MinHash."""

    def setUp(self):
        """Crear LSH con datos de prueba."""
        self.lsh = LSHMinHash(num_hashes=100, num_bands=20)
        self.minhash = MinHash(num_hashes=100)

        # Videos con diferentes niveles de similitud
        self.usuarios_base = {f"u{i}" for i in range(100)}

        # v1 y v2 son muy similares (80% usuarios en común)
        v1_users = set(list(self.usuarios_base)[:80])
        v2_users = set(list(self.usuarios_base)[:70]) | {f"extra_{i}" for i in range(10)}

        # v3 es muy diferente
        v3_users = {f"nuevo_{i}" for i in range(80)}

        self.lsh.add_video("v1", v1_users)
        self.lsh.add_video("v2", v2_users)
        self.lsh.add_video("v3", v3_users)

        self.v1_users = v1_users
        self.v2_users = v2_users
        self.v3_users = v3_users

    def test_jaccard_identicos(self):
        """Conjuntos idénticos deben tener similitud de Jaccard 1.0."""
        s = {"a", "b", "c"}
        sim = self.lsh.jaccard_similarity(s, s)
        self.assertEqual(sim, 1.0)

    def test_jaccard_disjuntos(self):
        """Conjuntos disjuntos deben tener similitud de Jaccard 0.0."""
        s1 = {"a", "b", "c"}
        s2 = {"d", "e", "f"}
        sim = self.lsh.jaccard_similarity(s1, s2)
        self.assertEqual(sim, 0.0)

    def test_jaccard_parcial(self):
        """Conjuntos con intersección parcial deben tener similitud entre 0 y 1."""
        s1 = {"a", "b", "c", "d"}
        s2 = {"c", "d", "e", "f"}
        sim = self.lsh.jaccard_similarity(s1, s2)
        self.assertAlmostEqual(sim, 0.333, places=2)

    def test_minhash_estima_similitud(self):
        """MinHash debe estimar la similitud de Jaccard razonablemente."""
        s1 = {f"u{i}" for i in range(100)}
        s2 = {f"u{i}" for i in range(80)} | {f"n{i}" for i in range(20)}

        firma1 = self.minhash.compute_signature(s1)
        firma2 = self.minhash.compute_signature(s2)

        sim_estimada = self.minhash.similarity(firma1, firma2)
        sim_real = self.lsh.jaccard_similarity(s1, s2)

        # La estimación debe estar dentro de ±0.15 de la real
        self.assertAlmostEqual(sim_estimada, sim_real, delta=0.15)

    def test_find_similar_video_inexistente(self):
        """find_similar para video inexistente debe retornar lista vacía."""
        resultado = self.lsh.find_similar("v_inexistente")
        self.assertEqual(resultado, [])

    def test_find_similar_retorna_resultados(self):
        """find_similar debe retornar videos similares."""
        similares = self.lsh.find_similar("v1", threshold=0.1)
        # Debería encontrar v2 como similar
        ids_similares = [s[0] for s in similares]
        self.assertIn("v2", ids_similares)

    def test_similitudes_entre_0_y_1(self):
        """Las similitudes retornadas deben estar entre 0 y 1."""
        similares = self.lsh.find_similar("v1", threshold=0.0)
        for _, sim in similares:
            self.assertGreaterEqual(sim, 0.0)
            self.assertLessEqual(sim, 1.0)

    def test_get_stats(self):
        """get_stats debe retornar información correcta."""
        stats = self.lsh.get_stats()
        self.assertEqual(stats["videos_indexados"], 3)
        self.assertEqual(stats["num_hashes"], 100)

    def test_add_video_multiples_veces(self):
        """Agregar el mismo video múltiples veces no debe causar errores."""
        s = {"u1", "u2", "u3"}
        self.lsh.add_video("v_test", s)
        self.lsh.add_video("v_test", s)  # No debe lanzar excepción
        stats = self.lsh.get_stats()
        self.assertIn("v_test", self.lsh._firmas)


if __name__ == "__main__":
    unittest.main(verbosity=2)
