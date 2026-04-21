"""
Filtro de Bloom (Bloom Filter)
================================
Estructura de datos probabilística para verificar pertenencia de conjuntos.
Se usa para detectar bots y solicitudes duplicadas en la plataforma.

Características:
    - Sin falsos negativos: Si un elemento NO está, seguro no lo está.
    - Posibles falsos positivos: Un elemento PUEDE parecer que está sin estarlo.
    - Memoria muy eficiente: Usa un arreglo de bits en lugar de almacenar valores.

Complejidad:
    - add: O(k) donde k = número de funciones hash
    - contains: O(k)
    - Espacio: O(m) bits donde m se calcula según capacidad y error rate
"""

import math
import hashlib
from typing import Optional


class BloomFilter:
    """
    Filtro de Bloom para detectar bots y solicitudes duplicadas en Netflix.

    Calcula automáticamente el tamaño óptimo del arreglo de bits (m) y el
    número óptimo de funciones hash (k) basándose en la capacidad esperada
    y la tasa de falsos positivos deseada.

    Fórmulas:
        m = -n * ln(p) / (ln(2))²
        k = (m/n) * ln(2)

    donde:
        n = capacidad esperada
        p = tasa de falsos positivos
        m = número de bits
        k = número de funciones hash

    Args:
        capacity (int): Número esperado de elementos a insertar. Default: 100000.
        error_rate (float): Tasa máxima de falsos positivos (0 < error_rate < 1). Default: 0.01.

    Ejemplo de uso:
        >>> bf = BloomFilter(capacity=100000, error_rate=0.01)
        >>> bf.add("192.168.1.100:user123")
        >>> bf.contains("192.168.1.100:user123")
        True
        >>> bf.contains("192.168.1.999:user999")
        False  # (usualmente)
    """

    def __init__(self, capacity: int = 100000, error_rate: float = 0.01):
        """
        Inicializa el Filtro de Bloom con parámetros óptimos.

        Args:
            capacity (int): Número esperado de elementos únicos.
            error_rate (float): Tasa de falsos positivos aceptable.
        """
        if capacity <= 0:
            raise ValueError("La capacidad debe ser mayor a 0")
        if not (0 < error_rate < 1):
            raise ValueError("error_rate debe estar entre 0 y 1 (exclusivo)")

        self.capacity = capacity
        self.error_rate = error_rate

        # Calcular tamaño óptimo del arreglo de bits
        self.size = self._calcular_tamaño_bits(capacity, error_rate)

        # Calcular número óptimo de funciones hash
        self.num_hashes = self._calcular_num_hashes(self.size, capacity)

        # Arreglo de bits implementado con bytearray (8 bits por byte)
        self._bits = bytearray(math.ceil(self.size / 8))

        # Contador de elementos insertados
        self._count = 0

    def _calcular_tamaño_bits(self, n: int, p: float) -> int:
        """
        Calcula el tamaño óptimo del arreglo de bits.

        Fórmula: m = -n * ln(p) / (ln(2))²

        Args:
            n (int): Capacidad esperada.
            p (float): Tasa de falsos positivos.

        Returns:
            int: Tamaño óptimo en bits.
        """
        m = -n * math.log(p) / (math.log(2) ** 2)
        return int(math.ceil(m))

    def _calcular_num_hashes(self, m: int, n: int) -> int:
        """
        Calcula el número óptimo de funciones hash.

        Fórmula: k = (m/n) * ln(2)

        Args:
            m (int): Tamaño del arreglo de bits.
            n (int): Capacidad esperada.

        Returns:
            int: Número óptimo de funciones hash.
        """
        k = (m / n) * math.log(2)
        return max(1, int(round(k)))

    def _get_bit_indices(self, item: str):
        """
        Calcula los índices de bits para un elemento.

        Genera num_hashes índices únicos usando diferentes semillas SHA-256.

        Args:
            item (str): El elemento a hashear.

        Yields:
            int: Índice de bit (0 a size-1).
        """
        item_bytes = item.encode("utf-8")
        for semilla in range(self.num_hashes):
            clave = f"{semilla}:{item}".encode("utf-8")
            digest = hashlib.sha256(clave).hexdigest()
            indice = int(digest[:16], 16) % self.size
            yield indice

    def _set_bit(self, index: int) -> None:
        """Activa el bit en la posición dada."""
        byte_index = index // 8
        bit_offset = index % 8
        self._bits[byte_index] |= (1 << bit_offset)

    def _get_bit(self, index: int) -> bool:
        """Verifica si el bit en la posición dada está activo."""
        byte_index = index // 8
        bit_offset = index % 8
        return bool(self._bits[byte_index] & (1 << bit_offset))

    def add(self, item: str) -> None:
        """
        Agrega un elemento al filtro.

        Activa los k bits correspondientes al elemento.

        Complejidad: O(k)

        Args:
            item (str): El elemento a agregar (ej: IP de usuario, token).
        """
        for indice in self._get_bit_indices(item):
            self._set_bit(indice)
        self._count += 1

    def contains(self, item: str) -> bool:
        """
        Verifica si un elemento podría estar en el filtro.

        Si retorna False: el elemento DEFINITIVAMENTE no está en el filtro.
        Si retorna True: el elemento PROBABLEMENTE está en el filtro
                        (puede ser un falso positivo).

        Complejidad: O(k)

        Args:
            item (str): El elemento a verificar.

        Returns:
            bool: True si el elemento posiblemente está, False si definitivamente no.
        """
        for indice in self._get_bit_indices(item):
            if not self._get_bit(indice):
                return False
        return True

    def false_positive_rate(self) -> float:
        """
        Calcula la tasa actual de falsos positivos basada en cuántos elementos
        se han insertado.

        Fórmula: (1 - e^(-k*n/m))^k

        Returns:
            float: Tasa estimada actual de falsos positivos.
        """
        if self._count == 0:
            return 0.0
        import math as m
        tasa = (1 - m.exp(-self.num_hashes * self._count / self.size)) ** self.num_hashes
        return round(tasa, 6)

    def get_info(self) -> dict:
        """
        Retorna información sobre la configuración del filtro.

        Returns:
            dict: Parámetros del filtro y estadísticas actuales.
        """
        return {
            "capacidad": self.capacity,
            "error_rate_objetivo": self.error_rate,
            "tamaño_bits": self.size,
            "tamaño_bytes": math.ceil(self.size / 8),
            "num_hashes": self.num_hashes,
            "elementos_insertados": self._count,
            "tasa_fp_actual": self.false_positive_rate(),
            "memoria_KB": round(math.ceil(self.size / 8) / 1024, 2)
        }

    def __repr__(self) -> str:
        return (f"BloomFilter(capacity={self.capacity}, "
                f"error_rate={self.error_rate}, "
                f"bits={self.size}, "
                f"hashes={self.num_hashes})")


def demo():
    """
    Demostración del Bloom Filter para detección de bots en Netflix.
    """
    print("=" * 60)
    print("   DEMO: Bloom Filter - Detección de Bots Netflix")
    print("=" * 60)

    import random
    import string

    bf = BloomFilter(capacity=10000, error_rate=0.01)

    print(f"\n📋 Configuración del Bloom Filter:")
    for k, v in bf.get_info().items():
        print(f"  {k}: {v}")

    # Simular IPs legítimas registradas
    print("\n✅ Registrando 1000 IPs/usuarios legítimos:")
    usuarios_legit = [f"user_{i}_ip_192.168.{i//256}.{i%256}" for i in range(1000)]
    for usuario in usuarios_legit:
        bf.add(usuario)

    # Verificar que todos los registrados son reconocidos
    falsos_negativos = sum(1 for u in usuarios_legit if not bf.contains(u))
    print(f"  Falsos negativos (NUNCA debería haber): {falsos_negativos}")
    print(f"  Tasa FP actual: {bf.false_positive_rate():.4%}")

    # Simular IPs nuevas (posibles bots)
    print("\n🤖 Detectando posibles bots (1000 IPs no registradas):")
    nuevos = [f"bot_{i}_ip_10.0.{i//256}.{i%256}" for i in range(1000)]
    falsos_positivos = sum(1 for b in nuevos if bf.contains(b))
    tasa_real_fp = falsos_positivos / len(nuevos)
    print(f"  Falsos positivos detectados: {falsos_positivos}/{len(nuevos)}")
    print(f"  Tasa FP real: {tasa_real_fp:.4%} (objetivo: {bf.error_rate:.4%})")

    print("\n🔍 Consultas individuales:")
    print(f"  ¿'user_0_ip_192.168.0.0' es legítimo? {bf.contains('user_0_ip_192.168.0.0')}")
    print(f"  ¿'bot_999_ip_10.0.3.231' es legítimo? {bf.contains('bot_999_ip_10.0.3.231')}")


if __name__ == "__main__":
    demo()
