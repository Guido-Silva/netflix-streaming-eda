"""
Trie (Árbol de Prefijos)
=========================
Estructura de datos para autocompletado eficiente de búsquedas en la plataforma.

El Trie almacena cadenas de texto de forma que se pueden buscar todos los strings
que comparten un prefijo común en O(m) donde m es la longitud del prefijo.

Uso en Netflix: Cuando el usuario escribe "str", el sistema sugiere
"Stranger Things", "Stranger" etc., ordenados por popularidad.

Complejidad:
    - insert: O(m) donde m = longitud de la palabra
    - search: O(m)
    - autocomplete: O(m + k) donde k = número de sugerencias
    - delete: O(m)
    - Espacio: O(SIGMA × m × n) donde SIGMA = tamaño del alfabeto
"""

from typing import List, Optional, Dict
import heapq


class TrieNode:
    """
    Nodo del Trie que representa un carácter en la estructura.

    Atributos:
        children (dict): Diccionario de caracteres hijos → TrieNode.
        is_end (bool): Indica si este nodo marca el final de una palabra.
        popularity (int): Popularidad/frecuencia de la palabra (si es fin de palabra).
        word (str): La palabra completa almacenada en este nodo (si es fin).
    """

    def __init__(self):
        """Inicializa un nodo vacío del Trie."""
        self.children: Dict[str, "TrieNode"] = {}
        self.is_end: bool = False
        self.popularity: int = 0
        self.word: str = ""

    def __repr__(self) -> str:
        return f"TrieNode(hijos={list(self.children.keys())}, fin={self.is_end})"


class Trie:
    """
    Árbol de prefijos para autocompletado de búsquedas en Netflix.

    Permite insertar títulos de películas/series con su popularidad y
    recuperar sugerencias ordenadas por popularidad dado un prefijo.

    Ejemplo de uso:
        >>> trie = Trie()
        >>> trie.insert("Stranger Things", popularity=1000000)
        >>> trie.insert("Squid Game", popularity=2000000)
        >>> trie.autocomplete("Str")
        [("Stranger Things", 1000000)]
    """

    def __init__(self):
        """Inicializa el Trie con un nodo raíz vacío."""
        self._raiz = TrieNode()
        self._total_palabras = 0

    def insert(self, word: str, popularity: int = 1) -> None:
        """
        Inserta una palabra en el Trie con su popularidad.

        Si la palabra ya existe, actualiza su popularidad sumando el nuevo valor.

        Complejidad: O(m) donde m = longitud de la palabra.

        Args:
            word (str): La palabra/título a insertar.
            popularity (int): Popularidad o número de reproducciones. Default: 1.
        """
        nodo = self._raiz
        word_lower = word.lower()

        for char in word_lower:
            if char not in nodo.children:
                nodo.children[char] = TrieNode()
            nodo = nodo.children[char]

        if not nodo.is_end:
            self._total_palabras += 1

        nodo.is_end = True
        nodo.popularity += popularity
        nodo.word = word  # Guardar la forma original (con mayúsculas)

    def search(self, word: str) -> Optional[int]:
        """
        Busca una palabra exacta en el Trie.

        Complejidad: O(m)

        Args:
            word (str): La palabra a buscar.

        Returns:
            int: La popularidad de la palabra si existe, None si no existe.
        """
        nodo = self._raiz
        word_lower = word.lower()

        for char in word_lower:
            if char not in nodo.children:
                return None
            nodo = nodo.children[char]

        return nodo.popularity if nodo.is_end else None

    def autocomplete(self, prefix: str, max_suggestions: int = 10) -> List[tuple]:
        """
        Retorna sugerencias de autocompletado para un prefijo dado.

        Recorre el Trie hasta el nodo que representa el fin del prefijo,
        luego realiza un DFS para recolectar todas las palabras del subárbol,
        y las ordena por popularidad de mayor a menor.

        Complejidad: O(m + k) donde m = longitud del prefijo, k = nodos visitados

        Args:
            prefix (str): El prefijo a buscar.
            max_suggestions (int): Número máximo de sugerencias a retornar. Default: 10.

        Returns:
            list: Lista de tuplas (palabra_original, popularidad) ordenadas por popularidad.
        """
        nodo = self._raiz
        prefix_lower = prefix.lower()

        # Navegar hasta el nodo del prefijo
        for char in prefix_lower:
            if char not in nodo.children:
                return []  # No hay palabras con este prefijo
            nodo = nodo.children[char]

        # DFS para recolectar todas las palabras del subárbol
        sugerencias = []
        self._dfs_recolectar(nodo, sugerencias)

        # Ordenar por popularidad (mayor a menor) y retornar top N
        sugerencias.sort(key=lambda x: -x[1])
        return sugerencias[:max_suggestions]

    def _dfs_recolectar(self, nodo: TrieNode, resultado: list) -> None:
        """
        DFS recursivo para recolectar todas las palabras del subárbol.

        Args:
            nodo (TrieNode): Nodo actual.
            resultado (list): Lista donde se acumulan las palabras encontradas.
        """
        if nodo.is_end:
            resultado.append((nodo.word, nodo.popularity))

        for hijo in nodo.children.values():
            self._dfs_recolectar(hijo, resultado)

    def delete(self, word: str) -> bool:
        """
        Elimina una palabra del Trie.

        Complejidad: O(m)

        Args:
            word (str): La palabra a eliminar.

        Returns:
            bool: True si la palabra fue eliminada, False si no existía.
        """
        word_lower = word.lower()
        return self._delete_recursivo(self._raiz, word_lower, 0)

    def _delete_recursivo(self, nodo: TrieNode, word: str, depth: int) -> bool:
        """
        Eliminación recursiva de una palabra.

        Args:
            nodo (TrieNode): Nodo actual.
            word (str): Palabra a eliminar (en minúsculas).
            depth (int): Profundidad actual (índice del carácter).

        Returns:
            bool: True si el nodo puede ser eliminado (no tiene otros hijos).
        """
        if depth == len(word):
            if not nodo.is_end:
                return False
            nodo.is_end = False
            nodo.popularity = 0
            nodo.word = ""
            self._total_palabras -= 1
            return len(nodo.children) == 0

        char = word[depth]
        if char not in nodo.children:
            return False

        puede_eliminar = self._delete_recursivo(nodo.children[char], word, depth + 1)

        if puede_eliminar:
            del nodo.children[char]
            return not nodo.is_end and len(nodo.children) == 0

        return False

    def starts_with(self, prefix: str) -> bool:
        """
        Verifica si existe alguna palabra que empiece con el prefijo dado.

        Complejidad: O(m)

        Args:
            prefix (str): El prefijo a verificar.

        Returns:
            bool: True si existe al menos una palabra con ese prefijo.
        """
        nodo = self._raiz
        for char in prefix.lower():
            if char not in nodo.children:
                return False
            nodo = nodo.children[char]
        return True

    def size(self) -> int:
        """Retorna el número total de palabras en el Trie."""
        return self._total_palabras

    def __repr__(self) -> str:
        return f"Trie(palabras={self._total_palabras})"


def demo():
    """
    Demostración del Trie con títulos de películas y series de Netflix.
    """
    print("=" * 60)
    print("   DEMO: Trie - Autocompletado de Búsqueda Netflix")
    print("=" * 60)

    trie = Trie()

    # Catálogo de Netflix con popularidades (en millones de vistas)
    catalogo_netflix = [
        ("Stranger Things", 89),
        ("Squid Game", 111),
        ("La Casa de Papel", 75),
        ("The Crown", 60),
        ("Narcos", 45),
        ("Dark", 40),
        ("Lupin", 35),
        ("Black Mirror", 32),
        ("Mindhunter", 28),
        ("Ozark", 25),
        ("Bridgerton", 55),
        ("The Witcher", 50),
        ("Emily in Paris", 42),
        ("You", 38),
        ("Cobra Kai", 35),
        ("Peaky Blinders", 48),
        ("The Last Kingdom", 30),
        ("Daredevil", 44),
        ("Breaking Bad", 70),
        ("Better Call Saul", 52),
        ("Money Heist", 75),
        ("Sacred Games", 20),
        ("Suburra", 15),
        ("Elite", 38),
        ("Suburra Blood on Rome", 18),
        ("Stranger", 10),
    ]

    print(f"\n📚 Insertando {len(catalogo_netflix)} títulos en el Trie:")
    for titulo, popularidad in catalogo_netflix:
        trie.insert(titulo, popularity=popularidad)

    print(f"  Total palabras en el Trie: {trie.size()}")

    # Demostrar búsqueda exacta
    print("\n🔍 Búsqueda exacta:")
    titulos_buscar = ["Stranger Things", "Dark", "Título No Existe"]
    for titulo in titulos_buscar:
        pop = trie.search(titulo)
        if pop:
            print(f"  '{titulo}' → encontrado, popularidad: {pop}M vistas")
        else:
            print(f"  '{titulo}' → no encontrado")

    # Demostrar autocompletado
    print("\n✨ Autocompletado por prefijos:")
    prefijos = ["str", "the", "b", "la", "su", "el"]
    for prefijo in prefijos:
        sugerencias = trie.autocomplete(prefijo, max_suggestions=3)
        if sugerencias:
            print(f"\n  Prefijo: '{prefijo}'")
            for titulo, pop in sugerencias:
                print(f"    → {titulo} ({pop}M vistas)")
        else:
            print(f"\n  Prefijo: '{prefijo}' → sin sugerencias")

    # Demostrar starts_with
    print("\n🔎 ¿Existe algún título que empiece con...?")
    for prefijo in ["ne", "bla", "xyz"]:
        existe = trie.starts_with(prefijo)
        print(f"  '{prefijo}' → {'Sí' if existe else 'No'}")

    # Demostrar eliminación
    print("\n🗑️ Eliminando 'Dark' del catálogo:")
    eliminado = trie.delete("Dark")
    print(f"  Eliminado: {eliminado}")
    print(f"  ¿'Dark' sigue en el Trie? {trie.search('Dark') is not None}")
    print(f"  Total palabras ahora: {trie.size()}")


if __name__ == "__main__":
    demo()
