from __future__ import annotations

import bisect
import collections
import dataclasses
import hashlib
import heapq
import math
import random
import statistics
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np

try:
    import pandas as pd
except ImportError:  # pragma: no cover - Colab and the local notebook env include pandas.
    pd = None


def _display_table(data: Any) -> None:
    if pd is not None and isinstance(data, pd.DataFrame):
        try:
            from IPython.display import display

            display(data)
            return
        except Exception:
            pass
    print(data)


def stable_hash(text: str, seed: int = 0) -> int:
    payload = f"{text}|{seed}".encode("utf-8")
    return int(hashlib.blake2b(payload, digest_size=8).hexdigest(), 16)


def zipf_sample(size: int, universe: Sequence[str], skew: float, seed: int) -> List[str]:
    rng = np.random.default_rng(seed)
    probs = np.array([1.0 / ((idx + 1) ** skew) for idx in range(len(universe))], dtype=float)
    probs /= probs.sum()
    return list(rng.choice(np.array(universe), size=size, p=probs))


def deep_size(obj: Any, seen: Optional[set[int]] = None) -> int:
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)

    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        size += sum(deep_size(key, seen) + deep_size(value, seen) for key, value in obj.items())
    elif isinstance(obj, (list, tuple, set, frozenset, collections.deque)):
        size += sum(deep_size(item, seen) for item in obj)
    elif hasattr(obj, "__dict__"):
        size += deep_size(vars(obj), seen)
    elif hasattr(obj, "__slots__"):
        for slot in obj.__slots__:
            if hasattr(obj, slot):
                size += deep_size(getattr(obj, slot), seen)
    return size


@dataclasses.dataclass(frozen=True)
class StreamEvent:
    seq: int
    user_id: str
    user_type: str
    event_type: str
    content_id: str
    priority: int

    @property
    def key(self) -> Tuple[int, int]:
        return (-self.priority, self.seq)


def priority_level(user_type: str, event_type: str) -> int:
    if event_type in {"payment", "subscription", "chargeback"}:
        return 4
    if user_type == "premium" and event_type in {"play", "download"}:
        return 3
    if event_type in {"search", "resume"}:
        return 2
    return 1


def generate_stream_events(
    n: int,
    seed: int = 42,
    user_count: int = 2500,
    catalog_size: int = 5000,
) -> List[StreamEvent]:
    rng = random.Random(seed)
    users = [f"user_{idx:05d}" for idx in range(user_count)]
    content_ids = [f"title_{idx:05d}" for idx in range(catalog_size)]
    user_types = ["premium", "standard"]
    event_types = ["play", "search", "resume", "download", "payment", "subscription", "browse"]
    user_weights = [0.3, 0.7]
    event_weights = [0.34, 0.2, 0.14, 0.08, 0.06, 0.03, 0.15]

    content_stream = zipf_sample(n, content_ids, skew=1.15, seed=seed)
    events = []
    for seq in range(n):
        user_type = rng.choices(user_types, weights=user_weights, k=1)[0]
        event_type = rng.choices(event_types, weights=event_weights, k=1)[0]
        events.append(
            StreamEvent(
                seq=seq,
                user_id=rng.choice(users),
                user_type=user_type,
                event_type=event_type,
                content_id=content_stream[seq],
                priority=priority_level(user_type, event_type),
            )
        )
    return events


class HeapPriorityQueue:
    def __init__(self) -> None:
        self.heap: List[Tuple[Tuple[int, int], StreamEvent]] = []

    def insert(self, event: StreamEvent) -> None:
        heapq.heappush(self.heap, (event.key, event))

    def pop(self) -> Optional[StreamEvent]:
        if not self.heap:
            return None
        return heapq.heappop(self.heap)[1]

    def peek(self) -> Optional[StreamEvent]:
        if not self.heap:
            return None
        return self.heap[0][1]


class SkipListNode:
    __slots__ = ("key", "value", "forward")

    def __init__(self, key: Any = None, value: Any = None, level: int = 1) -> None:
        self.key = key
        self.value = value
        self.forward: List[Optional["SkipListNode"]] = [None] * level


class SkipList:
    def __init__(self, max_level: int = 16, probability: float = 0.5, seed: int = 42) -> None:
        self.max_level = max_level
        self.probability = probability
        self.level = 1
        self.header = SkipListNode(level=max_level)
        self.length = 0
        self.random = random.Random(seed)

    def random_level(self) -> int:
        level = 1
        while self.random.random() < self.probability and level < self.max_level:
            level += 1
        return level

    def insert(self, key: Any, value: Any) -> None:
        update: List[SkipListNode] = [self.header] * self.max_level
        current = self.header

        for idx in reversed(range(self.level)):
            while current.forward[idx] is not None and current.forward[idx].key < key:
                current = current.forward[idx]
            update[idx] = current

        node_level = self.random_level()
        if node_level > self.level:
            for idx in range(self.level, node_level):
                update[idx] = self.header
            self.level = node_level

        node = SkipListNode(key, value, node_level)
        for idx in range(node_level):
            node.forward[idx] = update[idx].forward[idx]
            update[idx].forward[idx] = node
        self.length += 1

    def pop_min(self) -> Optional[Tuple[Any, Any]]:
        node = self.header.forward[0]
        if node is None:
            return None

        for idx in range(len(node.forward)):
            if self.header.forward[idx] is node:
                self.header.forward[idx] = node.forward[idx]

        while self.level > 1 and self.header.forward[self.level - 1] is None:
            self.level -= 1
        self.length -= 1
        return node.key, node.value

    def peek_min(self) -> Optional[Tuple[Any, Any]]:
        node = self.header.forward[0]
        if node is None:
            return None
        return node.key, node.value


class SkipListPriorityQueue:
    def __init__(self) -> None:
        self.index = SkipList()

    def insert(self, event: StreamEvent) -> None:
        self.index.insert(event.key, event)

    def pop(self) -> Optional[StreamEvent]:
        item = self.index.pop_min()
        if item is None:
            return None
        return item[1]

    def peek(self) -> Optional[StreamEvent]:
        item = self.index.peek_min()
        if item is None:
            return None
        return item[1]


class BPlusLeaf:
    __slots__ = ("keys", "values", "next", "is_leaf")

    def __init__(self) -> None:
        self.keys: List[Any] = []
        self.values: List[Any] = []
        self.next: Optional["BPlusLeaf"] = None
        self.is_leaf = True


class BPlusInternal:
    __slots__ = ("keys", "children", "is_leaf")

    def __init__(self) -> None:
        self.keys: List[Any] = []
        self.children: List[Any] = []
        self.is_leaf = False


class BPlusTree:
    def __init__(self, order: int = 16) -> None:
        self.order = order
        self.root: Any = BPlusLeaf()
        self.first_leaf: BPlusLeaf = self.root

    def insert(self, key: Any, value: Any) -> None:
        split = self._insert(self.root, key, value)
        if split is not None:
            separator, right_node = split
            new_root = BPlusInternal()
            new_root.keys = [separator]
            new_root.children = [self.root, right_node]
            self.root = new_root

    def _insert(self, node: Any, key: Any, value: Any) -> Optional[Tuple[Any, Any]]:
        if node.is_leaf:
            index = bisect.bisect_left(node.keys, key)
            node.keys.insert(index, key)
            node.values.insert(index, value)
            if len(node.keys) <= self.order:
                return None

            mid = len(node.keys) // 2
            right = BPlusLeaf()
            right.keys = node.keys[mid:]
            right.values = node.values[mid:]
            node.keys = node.keys[:mid]
            node.values = node.values[:mid]
            right.next = node.next
            node.next = right
            return right.keys[0], right

        child_index = bisect.bisect_right(node.keys, key)
        split = self._insert(node.children[child_index], key, value)
        if split is None:
            return None

        separator, right_node = split
        node.keys.insert(child_index, separator)
        node.children.insert(child_index + 1, right_node)
        if len(node.keys) <= self.order:
            return None

        mid = len(node.keys) // 2
        promoted = node.keys[mid]
        right = BPlusInternal()
        right.keys = node.keys[mid + 1 :]
        right.children = node.children[mid + 1 :]
        node.keys = node.keys[:mid]
        node.children = node.children[: mid + 1]
        return promoted, right

    def pop_min(self) -> Optional[Tuple[Any, Any]]:
        while self.first_leaf is not None and not self.first_leaf.keys:
            self.first_leaf = self.first_leaf.next
        if self.first_leaf is None:
            return None
        key = self.first_leaf.keys.pop(0)
        value = self.first_leaf.values.pop(0)
        while self.first_leaf is not None and not self.first_leaf.keys:
            self.first_leaf = self.first_leaf.next
        return key, value

    def peek_min(self) -> Optional[Tuple[Any, Any]]:
        leaf = self.first_leaf
        while leaf is not None and not leaf.keys:
            leaf = leaf.next
        if leaf is None:
            return None
        return leaf.keys[0], leaf.values[0]


class IndexedLRUCache:
    def __init__(self, capacity: int, index: Any) -> None:
        self.capacity = capacity
        self.index = index
        self.entries: Dict[str, int] = {}
        self.clock = 0
        self.hits = 0
        self.misses = 0

    def access(self, content_id: str) -> None:
        self.clock += 1
        timestamp = self.clock

        if content_id in self.entries:
            self.hits += 1
        else:
            self.misses += 1

        self.entries[content_id] = timestamp
        self.index.insert((timestamp, content_id), content_id)
        self._evict_if_needed()

    def _evict_if_needed(self) -> None:
        while len(self.entries) > self.capacity:
            item = self.index.pop_min()
            if item is None:
                break
            (timestamp, content_id), _ = item
            if self.entries.get(content_id) == timestamp:
                del self.entries[content_id]

    def stats(self) -> Dict[str, float]:
        total = self.hits + self.misses
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": self.hits / total if total else 0.0,
            "size": len(self.entries),
        }


class HashMapSkipListLRUCache(IndexedLRUCache):
    def __init__(self, capacity: int = 1000) -> None:
        super().__init__(capacity=capacity, index=SkipList())


class HashMapBPlusTreeLRUCache(IndexedLRUCache):
    def __init__(self, capacity: int = 1000) -> None:
        super().__init__(capacity=capacity, index=BPlusTree(order=24))


class TrieNode:
    __slots__ = ("children", "is_end", "values")

    def __init__(self) -> None:
        self.children: Dict[str, "TrieNode"] = {}
        self.is_end = False
        self.values: List[str] = []


class Trie:
    def __init__(self) -> None:
        self.root = TrieNode()

    def insert(self, word: str) -> None:
        node = self.root
        for char in word:
            node = node.children.setdefault(char, TrieNode())
        node.is_end = True
        node.values.append(word)

    def contains(self, word: str) -> bool:
        node = self.root
        for char in word:
            node = node.children.get(char)
            if node is None:
                return False
        return node.is_end

    def autocomplete(self, prefix: str, limit: int = 10) -> List[str]:
        node = self.root
        for char in prefix:
            node = node.children.get(char)
            if node is None:
                return []

        results: List[str] = []
        stack: List[Tuple[str, TrieNode]] = [(prefix, node)]
        while stack and len(results) < limit:
            current_prefix, current_node = stack.pop()
            if current_node.is_end:
                results.extend(current_node.values[: limit - len(results)])
            for char in sorted(current_node.children.keys(), reverse=True):
                stack.append((current_prefix + char, current_node.children[char]))
        return results[:limit]


class BloomFilter:
    def __init__(self, size_bits: int = 200_000, num_hashes: int = 7) -> None:
        self.size_bits = size_bits
        self.num_hashes = num_hashes
        self.bits = np.zeros(size_bits, dtype=np.bool_)

    def _indexes(self, item: str) -> Iterable[int]:
        for seed in range(self.num_hashes):
            yield stable_hash(item, seed) % self.size_bits

    def add(self, item: str) -> None:
        for index in self._indexes(item):
            self.bits[index] = True

    def __contains__(self, item: str) -> bool:
        return all(self.bits[index] for index in self._indexes(item))

    @property
    def memory_bytes(self) -> int:
        return int(self.bits.nbytes)


class CountMinSketch:
    def __init__(self, width: int = 5000, depth: int = 7) -> None:
        self.width = width
        self.depth = depth
        self.table = np.zeros((depth, width), dtype=np.int32)
        self.seeds = [17 + idx * 131 for idx in range(depth)]

    def add(self, item: str, count: int = 1) -> None:
        for row, seed in enumerate(self.seeds):
            index = stable_hash(item, seed) % self.width
            self.table[row, index] += count

    def estimate(self, item: str) -> int:
        return min(self.table[row, stable_hash(item, seed) % self.width] for row, seed in enumerate(self.seeds))

    @property
    def memory_bytes(self) -> int:
        return int(self.table.nbytes)


class MisraGries:
    def __init__(self, k: int = 50) -> None:
        self.k = k
        self.counters: Dict[str, int] = {}

    def add(self, item: str) -> None:
        if item in self.counters:
            self.counters[item] += 1
            return
        if len(self.counters) < self.k - 1:
            self.counters[item] = 1
            return

        to_remove = []
        for key in list(self.counters.keys()):
            self.counters[key] -= 1
            if self.counters[key] == 0:
                to_remove.append(key)
        for key in to_remove:
            del self.counters[key]

    def top_candidates(self, k: int = 10) -> List[Tuple[str, int]]:
        return sorted(self.counters.items(), key=lambda item: (-item[1], item[0]))[:k]


class MinHash:
    def __init__(self, num_hashes: int = 64, seed: int = 42) -> None:
        self.num_hashes = num_hashes
        rng = random.Random(seed)
        self.prime = 4_294_967_311
        self.params = [(rng.randint(1, self.prime - 1), rng.randint(0, self.prime - 1)) for _ in range(num_hashes)]

    def signature(self, tokens: Iterable[str]) -> List[int]:
        signature = [sys.maxsize] * self.num_hashes
        for token in tokens:
            token_value = stable_hash(token)
            for index, (alpha, beta) in enumerate(self.params):
                hashed = (alpha * token_value + beta) % self.prime
                if hashed < signature[index]:
                    signature[index] = hashed
        return signature

    def similarity(self, sig_a: Sequence[int], sig_b: Sequence[int]) -> float:
        matches = sum(1 for value_a, value_b in zip(sig_a, sig_b) if value_a == value_b)
        return matches / self.num_hashes


class LSHIndex:
    def __init__(self, bands: int = 8, rows: int = 8) -> None:
        self.bands = bands
        self.rows = rows
        self.buckets: List[Dict[str, List[str]]] = [{} for _ in range(bands)]

    def _band_key(self, slice_values: Sequence[int]) -> str:
        return hashlib.blake2b("|".join(map(str, slice_values)).encode("utf-8"), digest_size=8).hexdigest()

    def insert(self, item_id: str, signature: Sequence[int]) -> None:
        for band in range(self.bands):
            start = band * self.rows
            end = start + self.rows
            key = self._band_key(signature[start:end])
            self.buckets[band].setdefault(key, []).append(item_id)

    def query_candidates(self, signature: Sequence[int]) -> List[str]:
        candidates = set()
        for band in range(self.bands):
            start = band * self.rows
            end = start + self.rows
            key = self._band_key(signature[start:end])
            candidates.update(self.buckets[band].get(key, []))
        return list(candidates)


def build_catalog(size: int = 5000, seed: int = 42) -> List[str]:
    rng = random.Random(seed)
    adjectives = [
        "silent",
        "hidden",
        "electric",
        "golden",
        "red",
        "last",
        "broken",
        "wild",
        "cosmic",
        "urban",
        "midnight",
        "deep",
    ]
    nouns = [
        "signal",
        "legend",
        "voyage",
        "empire",
        "code",
        "memory",
        "mirror",
        "archive",
        "shadow",
        "pulse",
        "network",
        "horizon",
    ]
    suffixes = ["origins", "chronicles", "protocol", "files", "effect", "session", "story"]

    titles = []
    seen = set()
    while len(titles) < size:
        title = f"{rng.choice(adjectives)} {rng.choice(nouns)} {rng.choice(suffixes)} {rng.randint(1, 999)}"
        if title not in seen:
            seen.add(title)
            titles.append(title)
    titles.sort()
    return titles


def generate_prefix_queries(titles: Sequence[str], count: int = 600, seed: int = 42) -> List[str]:
    rng = random.Random(seed)
    prefixes = []
    for _ in range(count):
        title = rng.choice(titles)
        end = rng.randint(3, min(10, len(title)))
        prefixes.append(title[:end])
    return prefixes


def generate_watch_histories(
    num_users: int = 800,
    clusters: int = 12,
    titles_per_cluster: int = 40,
    watches_per_user: int = 18,
    noise: int = 1,
    seed: int = 42,
) -> Dict[str, set[str]]:
    rng = random.Random(seed)
    base_titles = {
        cluster: [f"cluster_{cluster}_title_{idx}" for idx in range(titles_per_cluster)] for cluster in range(clusters)
    }
    catalog = [title for titles in base_titles.values() for title in titles]
    histories: Dict[str, set[str]] = {}

    for user_idx in range(num_users):
        cluster = user_idx % clusters
        primary = set(rng.sample(base_titles[cluster], watches_per_user))
        secondary_cluster = (cluster + rng.randint(1, clusters - 1)) % clusters
        secondary = set(rng.sample(base_titles[secondary_cluster], noise))
        global_noise = set(rng.sample(catalog, noise))
        histories[f"user_{user_idx:04d}"] = primary | secondary | global_noise
    return histories


def jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    return len(left & right) / len(left | right)


def generate_access_stream(
    n: int,
    catalog_size: int = 25_000,
    skew: float = 1.2,
    seed: int = 42,
) -> List[str]:
    catalog = [f"title_{idx:05d}" for idx in range(catalog_size)]
    return zipf_sample(n, catalog, skew=skew, seed=seed)


def generate_duplicate_events(
    n: int,
    users: int = 3000,
    content_size: int = 8000,
    hot_ips: int = 40,
    suspicious_ratio: float = 0.05,
    seed: int = 42,
) -> Tuple[List[str], List[str], List[str], Dict[str, int]]:
    rng = random.Random(seed)
    content_ids = [f"title_{idx:05d}" for idx in range(content_size)]
    ip_pool = [f"10.0.{idx // 255}.{idx % 255}" for idx in range(600)]
    suspicious_ips = ip_pool[:hot_ips]

    duplicate_tokens: List[str] = []
    ip_stream: List[str] = []
    seen_tokens: List[str] = []
    exact_ip_counts: Dict[str, int] = collections.Counter()

    for idx in range(n):
        user_id = f"user_{rng.randint(0, users - 1):05d}"
        content_id = content_ids[stable_hash(f"{idx}|{user_id}", seed) % len(content_ids)]
        if seen_tokens and rng.random() < suspicious_ratio:
            token = rng.choice(seen_tokens)
        else:
            token = f"{user_id}:{content_id}:{rng.randint(0, 500)}"
            seen_tokens.append(token)

        ip = rng.choice(suspicious_ips if rng.random() < suspicious_ratio else ip_pool)
        duplicate_tokens.append(token)
        ip_stream.append(ip)
        exact_ip_counts[ip] += 1

    negatives = [f"negative_{idx}_{seed}" for idx in range(max(1000, n // 10))]
    return duplicate_tokens, negatives, ip_stream, exact_ip_counts


def timed_repeat(callback, repeats: int = 3) -> Tuple[float, Any]:
    timings = []
    last_value = None
    for _ in range(repeats):
        start = time.perf_counter()
        last_value = callback()
        timings.append((time.perf_counter() - start) * 1000)
    return statistics.mean(timings), last_value


def priority_order_sample(sample_size: int = 15) -> Any:
    events = generate_stream_events(sample_size, seed=101)
    queue = HeapPriorityQueue()
    for event in events:
        queue.insert(event)

    rows = []
    while True:
        event = queue.pop()
        if event is None:
            break
        rows.append(
            {
                "seq": event.seq,
                "event_type": event.event_type,
                "user_type": event.user_type,
                "priority": event.priority,
            }
        )
    if pd is None:
        return rows
    return pd.DataFrame(rows)


def benchmark_priority_processing(
    sizes: Sequence[int] = (2_000, 5_000, 10_000, 20_000),
    repeats: int = 3,
) -> Any:
    rows = []
    structures = [
        ("Heap", HeapPriorityQueue),
        ("SkipList", SkipListPriorityQueue),
    ]
    for size in sizes:
        events = generate_stream_events(size, seed=size)
        for label, constructor in structures:
            def runner() -> None:
                queue = constructor()
                for event in events:
                    queue.insert(event)
                while queue.pop() is not None:
                    pass

            elapsed_ms, _ = timed_repeat(runner, repeats=repeats)
            rows.append({"size": size, "structure": label, "elapsed_ms": elapsed_ms})
    if pd is None:
        return rows
    return pd.DataFrame(rows)


def benchmark_cache_system(
    sizes: Sequence[int] = (5_000, 10_000, 20_000, 40_000),
    capacity: int = 4_000,
    repeats: int = 3,
) -> Any:
    rows = []
    structures = [
        ("HashMap + SkipList", HashMapSkipListLRUCache),
        ("HashMap + B+Tree", HashMapBPlusTreeLRUCache),
    ]
    for size in sizes:
        stream = generate_access_stream(size, seed=size)
        for label, constructor in structures:
            stats_capture: Dict[str, float] = {}

            def runner() -> None:
                cache = constructor(capacity=capacity)
                for item in stream:
                    cache.access(item)
                stats_capture.update(cache.stats())

            elapsed_ms, _ = timed_repeat(runner, repeats=repeats)
            rows.append(
                {
                    "size": size,
                    "structure": label,
                    "elapsed_ms": elapsed_ms,
                    "hit_rate": stats_capture["hit_rate"],
                }
            )
    if pd is None:
        return rows
    return pd.DataFrame(rows)


def benchmark_prefix_search(
    sizes: Sequence[int] = (2_000, 5_000, 10_000),
    query_count: int = 500,
    repeats: int = 3,
) -> Any:
    rows = []
    for size in sizes:
        titles = build_catalog(size=size, seed=size)
        prefixes = generate_prefix_queries(titles, count=query_count, seed=size)
        trie = Trie()
        for title in titles:
            trie.insert(title)

        def trie_runner() -> None:
            for prefix in prefixes:
                trie.autocomplete(prefix, limit=5)

        def linear_runner() -> None:
            for prefix in prefixes:
                matches = [title for title in titles if title.startswith(prefix)]
                matches[:5]

        trie_ms, _ = timed_repeat(trie_runner, repeats=repeats)
        linear_ms, _ = timed_repeat(linear_runner, repeats=repeats)
        rows.extend(
            [
                {"size": size, "structure": "Trie", "elapsed_ms": trie_ms},
                {"size": size, "structure": "Linear Scan", "elapsed_ms": linear_ms},
            ]
        )
    if pd is None:
        return rows
    return pd.DataFrame(rows)


def top_k_exact(query_user: str, histories: Dict[str, set[str]], k: int = 5) -> List[str]:
    scores = []
    query_set = histories[query_user]
    for user_id, watched in histories.items():
        if user_id == query_user:
            continue
        scores.append((jaccard_similarity(query_set, watched), user_id))
    scores.sort(reverse=True)
    return [user_id for _, user_id in scores[:k]]


def benchmark_lsh_search(
    user_sizes: Sequence[int] = (240, 480, 960),
    query_count: int = 50,
    k: int = 5,
    repeats: int = 1,
) -> Any:
    rows = []
    precision_rows = []
    for num_users in user_sizes:
        histories = generate_watch_histories(num_users=num_users, seed=num_users)
        minhash = MinHash(num_hashes=64, seed=num_users)
        lsh = LSHIndex(bands=32, rows=2)
        signatures = {}
        for user_id, watched in histories.items():
            signature = minhash.signature(watched)
            signatures[user_id] = signature
            lsh.insert(user_id, signature)

        rng = random.Random(num_users)
        queries = rng.sample(list(histories.keys()), k=min(query_count, len(histories)))

        def exact_runner() -> List[List[str]]:
            return [top_k_exact(user_id, histories, k=k) for user_id in queries]

        def lsh_runner() -> List[List[str]]:
            results = []
            for user_id in queries:
                candidates = set(lsh.query_candidates(signatures[user_id]))
                candidates.discard(user_id)
                if not candidates:
                    results.append([])
                    continue
                scores = []
                for candidate in candidates:
                    similarity = jaccard_similarity(histories[user_id], histories[candidate])
                    scores.append((similarity, candidate))
                scores.sort(reverse=True)
                results.append([candidate for _, candidate in scores[:k]])
            return results

        exact_ms, exact_results = timed_repeat(exact_runner, repeats=repeats)
        lsh_ms, lsh_results = timed_repeat(lsh_runner, repeats=repeats)
        rows.extend(
            [
                {"size": num_users, "structure": "Exact Jaccard", "elapsed_ms": exact_ms},
                {"size": num_users, "structure": "MinHash + LSH", "elapsed_ms": lsh_ms},
            ]
        )
        precision = []
        for exact_result, lsh_result in zip(exact_results, lsh_results):
            exact_set = set(exact_result[:k])
            lsh_set = set(lsh_result[:k])
            precision.append(len(exact_set & lsh_set) / k if k else 0.0)
        precision_rows.append({"size": num_users, "precision_at_k": statistics.mean(precision) if precision else 0.0})

    if pd is None:
        return rows, precision_rows
    return pd.DataFrame(rows), pd.DataFrame(precision_rows)


def benchmark_anomaly_detection(
    sizes: Sequence[int] = (10_000, 20_000, 40_000),
    repeats: int = 3,
) -> Tuple[Any, Any]:
    rows = []
    quality_rows = []
    for size in sizes:
        duplicate_tokens, negatives, ip_stream, exact_ip_counts = generate_duplicate_events(size, seed=size)

        exact_seen_container: Dict[str, int] = {}
        bloom = BloomFilter(size_bits=max(200_000, size * 10), num_hashes=7)
        cms = CountMinSketch(width=max(2_000, size // 5), depth=7)

        def exact_duplicate_runner() -> None:
            seen = set()
            for token in duplicate_tokens:
                if token not in seen:
                    seen.add(token)

        def bloom_runner() -> None:
            local_bloom = BloomFilter(size_bits=max(200_000, size * 10), num_hashes=7)
            for token in duplicate_tokens:
                if token not in local_bloom:
                    local_bloom.add(token)

        def exact_counter_runner() -> None:
            counter = collections.Counter()
            for ip in ip_stream:
                counter[ip] += 1
            exact_seen_container["exact_counter_bytes"] = deep_size(counter)

        def cms_runner() -> None:
            local_cms = CountMinSketch(width=max(2_000, size // 5), depth=7)
            for ip in ip_stream:
                local_cms.add(ip)

        exact_dup_ms, _ = timed_repeat(exact_duplicate_runner, repeats=repeats)
        bloom_ms, _ = timed_repeat(bloom_runner, repeats=repeats)
        exact_counter_ms, _ = timed_repeat(exact_counter_runner, repeats=repeats)
        cms_ms, _ = timed_repeat(cms_runner, repeats=repeats)

        seen = set()
        for token in duplicate_tokens:
            if token not in bloom:
                bloom.add(token)
            seen.add(token)
        for ip in ip_stream:
            cms.add(ip)

        false_positive_rate = sum(1 for token in negatives if token in bloom) / len(negatives)
        sampled_ips = list(exact_ip_counts.keys())[: min(200, len(exact_ip_counts))]
        avg_error = statistics.mean(abs(cms.estimate(ip) - exact_ip_counts[ip]) for ip in sampled_ips)

        rows.extend(
            [
                {
                    "size": size,
                    "structure": "Bloom Filter",
                    "elapsed_ms": bloom_ms,
                    "memory_kb": bloom.memory_bytes / 1024,
                },
                {
                    "size": size,
                    "structure": "Exact Set",
                    "elapsed_ms": exact_dup_ms,
                    "memory_kb": deep_size(seen) / 1024,
                },
                {
                    "size": size,
                    "structure": "Count-Min Sketch",
                    "elapsed_ms": cms_ms,
                    "memory_kb": cms.memory_bytes / 1024,
                },
                {
                    "size": size,
                    "structure": "Exact Counter",
                    "elapsed_ms": exact_counter_ms,
                    "memory_kb": exact_seen_container.get("exact_counter_bytes", 0) / 1024,
                },
            ]
        )
        quality_rows.append(
            {
                "size": size,
                "bloom_false_positive_rate": false_positive_rate,
                "cms_avg_error": avg_error,
            }
        )

    if pd is None:
        return rows, quality_rows
    return pd.DataFrame(rows), pd.DataFrame(quality_rows)


def benchmark_topk_tracking(
    sizes: Sequence[int] = (20_000, 50_000, 100_000),
    unique_items: int = 3000,
    k: int = 20,
    repeats: int = 3,
) -> Any:
    rows = []
    for size in sizes:
        stream = zipf_sample(size, [f"title_{idx:04d}" for idx in range(unique_items)], skew=1.25, seed=size)

        exact_counter: collections.Counter[str] = collections.Counter()
        mg = MisraGries(k=k * 4)

        def exact_runner() -> None:
            counter = collections.Counter()
            for item in stream:
                counter[item] += 1

        def mg_runner() -> None:
            tracker = MisraGries(k=k * 4)
            for item in stream:
                tracker.add(item)

        exact_ms, _ = timed_repeat(exact_runner, repeats=repeats)
        mg_ms, _ = timed_repeat(mg_runner, repeats=repeats)

        for item in stream:
            exact_counter[item] += 1
            mg.add(item)

        exact_topk = [item for item, _ in exact_counter.most_common(k)]
        mg_topk = [item for item, _ in mg.top_candidates(k)]
        precision = len(set(exact_topk) & set(mg_topk)) / k

        rows.extend(
            [
                {
                    "size": size,
                    "structure": "Exact Counter",
                    "elapsed_ms": exact_ms,
                    "memory_kb": deep_size(exact_counter) / 1024,
                    "precision_at_k": 1.0,
                },
                {
                    "size": size,
                    "structure": "Misra-Gries",
                    "elapsed_ms": mg_ms,
                    "memory_kb": deep_size(mg.counters) / 1024,
                    "precision_at_k": precision,
                },
            ]
        )
    if pd is None:
        return rows
    return pd.DataFrame(rows)


class InteractiveAutocompleteDemo:
    def __init__(self, titles: Sequence[str]) -> None:
        self.titles = sorted(title.lower() for title in titles)
        self.trie = Trie()
        for title in self.titles:
            self.trie.insert(title)

    def suggest(self, prefix: str, limit: int = 8) -> Dict[str, Any]:
        prefix = prefix.strip().lower()
        if not prefix:
            return {
                "prefix": prefix,
                "suggestions": [],
                "trie_ms": 0.0,
                "linear_ms": 0.0,
            }

        start = time.perf_counter()
        trie_suggestions = self.trie.autocomplete(prefix, limit=limit)
        trie_ms = (time.perf_counter() - start) * 1000

        start = time.perf_counter()
        linear_suggestions = [title for title in self.titles if title.startswith(prefix)][:limit]
        linear_ms = (time.perf_counter() - start) * 1000

        return {
            "prefix": prefix,
            "suggestions": trie_suggestions,
            "linear_suggestions": linear_suggestions,
            "trie_ms": trie_ms,
            "linear_ms": linear_ms,
        }


class InteractiveSimilaritySearchDemo:
    def __init__(self, num_users: int = 360, seed: int = 42) -> None:
        self.histories = generate_watch_histories(num_users=num_users, seed=seed)
        self.minhash = MinHash(num_hashes=64, seed=seed)
        self.lsh = LSHIndex(bands=32, rows=2)
        self.signatures: Dict[str, List[int]] = {}
        for user_id, watched in self.histories.items():
            signature = self.minhash.signature(watched)
            self.signatures[user_id] = signature
            self.lsh.insert(user_id, signature)

    def query(self, user_id: str, k: int = 5) -> Dict[str, Any]:
        start = time.perf_counter()
        exact = top_k_exact(user_id, self.histories, k=k)
        exact_ms = (time.perf_counter() - start) * 1000

        start = time.perf_counter()
        candidates = set(self.lsh.query_candidates(self.signatures[user_id]))
        candidates.discard(user_id)
        if candidates:
            ranked = []
            for candidate in candidates:
                similarity = jaccard_similarity(self.histories[user_id], self.histories[candidate])
                ranked.append((similarity, candidate))
            ranked.sort(reverse=True)
            approx = [candidate for _, candidate in ranked[:k]]
        else:
            approx = []
        lsh_ms = (time.perf_counter() - start) * 1000

        overlap = len(set(exact) & set(approx))
        precision = overlap / k if k else 0.0
        return {
            "exact": exact,
            "approx": approx,
            "candidate_count": len(candidates),
            "exact_ms": exact_ms,
            "lsh_ms": lsh_ms,
            "precision_at_k": precision,
        }


class InteractiveCacheDemo:
    def __init__(self, titles: Sequence[str], capacity: int = 12, seed: int = 42) -> None:
        self.titles = list(titles)
        self.capacity = capacity
        self.skip_cache = HashMapSkipListLRUCache(capacity=capacity)
        self.btree_cache = HashMapBPlusTreeLRUCache(capacity=capacity)
        self.history: collections.deque[Dict[str, Any]] = collections.deque(maxlen=12)
        self.random_stream = generate_access_stream(20_000, catalog_size=len(self.titles), seed=seed)
        self.pointer = 0

    def _next_hot_title(self) -> str:
        token = self.random_stream[self.pointer % len(self.random_stream)]
        self.pointer += 1
        index = int(token.split("_")[1])
        return self.titles[index % len(self.titles)]

    def access(self, title: str) -> Dict[str, Any]:
        title = title.strip().lower()
        skip_hit = title in self.skip_cache.entries
        btree_hit = title in self.btree_cache.entries

        self.skip_cache.access(title)
        self.btree_cache.access(title)

        record = {
            "title": title,
            "skip_result": "hit" if skip_hit else "miss",
            "btree_result": "hit" if btree_hit else "miss",
            "cache_size": len(self.btree_cache.entries),
        }
        self.history.appendleft(record)
        return record

    def access_random_hot(self, count: int = 1) -> List[Dict[str, Any]]:
        results = []
        for _ in range(count):
            results.append(self.access(self._next_hot_title()))
        return results

    def resident_items(self, limit: int = 12) -> List[Dict[str, Any]]:
        ordered = sorted(self.btree_cache.entries.items(), key=lambda item: item[1], reverse=True)
        return [{"title": title, "last_access_ts": timestamp} for title, timestamp in ordered[:limit]]

    def stats_frame(self) -> Any:
        stats = self.btree_cache.stats()
        rows = [
            {"metric": "capacidad", "value": self.capacity},
            {"metric": "size", "value": stats["size"]},
            {"metric": "hits", "value": stats["hits"]},
            {"metric": "misses", "value": stats["misses"]},
            {"metric": "hit_rate", "value": round(stats["hit_rate"], 4)},
        ]
        if pd is None:
            return rows
        return pd.DataFrame(rows)


class InteractivePriorityDemo:
    def __init__(self, titles: Sequence[str], seed: int = 42) -> None:
        self.titles = list(titles)
        self.random = random.Random(seed)
        self.heap = HeapPriorityQueue()
        self.skip = SkipListPriorityQueue()
        self.sequence = 0
        self.last_processed: Optional[Dict[str, Any]] = None

    def enqueue(self, user_type: str, event_type: str, content_id: str, user_id: Optional[str] = None) -> StreamEvent:
        if user_id is None:
            user_id = f"user_{self.sequence:05d}"
        event = StreamEvent(
            seq=self.sequence,
            user_id=user_id,
            user_type=user_type,
            event_type=event_type,
            content_id=content_id.strip().lower(),
            priority=priority_level(user_type, event_type),
        )
        self.sequence += 1
        self.heap.insert(event)
        self.skip.insert(event)
        return event

    def enqueue_random(self, count: int = 5) -> List[StreamEvent]:
        user_types = ["premium", "standard"]
        event_types = ["play", "search", "resume", "download", "payment", "subscription", "browse"]
        user_weights = [0.3, 0.7]
        event_weights = [0.34, 0.2, 0.14, 0.08, 0.06, 0.03, 0.15]

        events = []
        for _ in range(count):
            user_type = self.random.choices(user_types, weights=user_weights, k=1)[0]
            event_type = self.random.choices(event_types, weights=event_weights, k=1)[0]
            content_id = self.random.choice(self.titles)
            events.append(self.enqueue(user_type, event_type, content_id))
        return events

    def process_next(self) -> Optional[Dict[str, Any]]:
        heap_event = self.heap.pop()
        skip_event = self.skip.pop()
        if heap_event is None or skip_event is None:
            self.last_processed = None
            return None

        consistent = heap_event.seq == skip_event.seq
        payload = {
            "seq": heap_event.seq,
            "user_type": heap_event.user_type,
            "event_type": heap_event.event_type,
            "content_id": heap_event.content_id,
            "priority": heap_event.priority,
            "consistent": consistent,
        }
        self.last_processed = payload
        return payload

    def queue_preview(self, limit: int = 10) -> Any:
        ordered = [event for _, event in sorted(self.heap.heap, key=lambda item: item[0])[:limit]]
        rows = [
            {
                "seq": event.seq,
                "priority": event.priority,
                "event_type": event.event_type,
                "user_type": event.user_type,
                "content_id": event.content_id,
            }
            for event in ordered
        ]
        if pd is None:
            return rows
        return pd.DataFrame(rows)


class InteractiveAnomalyDemo:
    def __init__(self, anomaly_threshold: int = 8) -> None:
        self.bloom = BloomFilter(size_bits=200_000, num_hashes=7)
        self.cms = CountMinSketch(width=5_000, depth=7)
        self.seen_exact: set[str] = set()
        self.ip_counts: collections.Counter[str] = collections.Counter()
        self.anomaly_threshold = anomaly_threshold
        self.history: collections.deque[Dict[str, Any]] = collections.deque(maxlen=12)

    def ingest(self, token: str, ip: str) -> Dict[str, Any]:
        token = token.strip().lower()
        ip = ip.strip()
        duplicate_exact = token in self.seen_exact
        duplicate_bloom = token in self.bloom

        self.seen_exact.add(token)
        self.bloom.add(token)
        self.ip_counts[ip] += 1
        self.cms.add(ip)

        exact_count = self.ip_counts[ip]
        estimate = self.cms.estimate(ip)
        anomalous = estimate >= self.anomaly_threshold
        payload = {
            "token": token,
            "ip": ip,
            "duplicate_exact": duplicate_exact,
            "duplicate_bloom": duplicate_bloom,
            "exact_ip_count": exact_count,
            "cms_estimate": estimate,
            "anomalous": anomalous,
        }
        self.history.appendleft(payload)
        return payload

    def ingest_suspicious_batch(self, count: int = 5) -> List[Dict[str, Any]]:
        results = []
        repeated_token = "shared_user:shared_title:bot"
        suspicious_ip = "10.0.0.1"
        for idx in range(count):
            token = repeated_token if idx % 2 == 0 else f"token_{len(self.history)}_{idx}"
            results.append(self.ingest(token, suspicious_ip))
        return results


class InteractiveTopKDemo:
    def __init__(self, titles: Sequence[str], k: int = 10, seed: int = 42) -> None:
        self.titles = list(titles)
        self.k = k
        self.counter: collections.Counter[str] = collections.Counter()
        self.mg = MisraGries(k=k * 4)
        self.history: collections.deque[str] = collections.deque(maxlen=20)
        self.random_stream = generate_access_stream(20_000, catalog_size=len(self.titles), seed=seed + 99)
        self.pointer = 0

    def _next_hot_title(self) -> str:
        token = self.random_stream[self.pointer % len(self.random_stream)]
        self.pointer += 1
        index = int(token.split("_")[1])
        return self.titles[index % len(self.titles)]

    def ingest(self, title: str) -> None:
        title = title.strip().lower()
        self.counter[title] += 1
        self.mg.add(title)
        self.history.appendleft(title)

    def ingest_random_batch(self, count: int = 10) -> None:
        for _ in range(count):
            self.ingest(self._next_hot_title())

    def exact_topk(self, limit: Optional[int] = None) -> List[Tuple[str, int]]:
        if limit is None:
            limit = self.k
        return self.counter.most_common(limit)

    def approx_topk(self, limit: Optional[int] = None) -> List[Tuple[str, int]]:
        if limit is None:
            limit = self.k
        return self.mg.top_candidates(limit)

    def precision_at_k(self, limit: Optional[int] = None) -> float:
        if limit is None:
            limit = self.k
        exact = [title for title, _ in self.exact_topk(limit)]
        approx = [title for title, _ in self.approx_topk(limit)]
        if not exact:
            return 0.0
        return len(set(exact) & set(approx)) / min(limit, len(exact))


def launch_interactive_colab_demo(
    catalog_size: int = 3000,
    cache_capacity: int = 12,
    topk_k: int = 10,
    show: bool = True,
) -> Any:
    try:
        import ipywidgets as widgets
        from IPython.display import Markdown, clear_output, display
    except Exception as exc:  # pragma: no cover - depends on notebook runtime.
        raise ImportError(
            "La demo interactiva requiere ipywidgets. En Colab ejecuta `pip install ipywidgets` si fuera necesario."
        ) from exc

    titles = build_catalog(size=catalog_size, seed=123)
    autocomplete_demo = InteractiveAutocompleteDemo(titles)
    similarity_demo = InteractiveSimilaritySearchDemo(num_users=360, seed=321)
    cache_demo = InteractiveCacheDemo(titles, capacity=cache_capacity, seed=222)
    priority_demo = InteractivePriorityDemo(titles, seed=333)
    anomaly_demo = InteractiveAnomalyDemo(anomaly_threshold=8)
    topk_demo = InteractiveTopKDemo(titles, k=topk_k, seed=444)

    def as_frame(rows: Any, columns: Optional[Sequence[str]] = None) -> Any:
        if pd is None:
            return rows
        if isinstance(rows, pd.DataFrame):
            return rows
        return pd.DataFrame(rows, columns=columns)

    def render_frame(frame: Any) -> None:
        if pd is not None and isinstance(frame, pd.DataFrame):
            display(frame)
        else:
            print(frame)

    autocomplete_title = widgets.HTML("<h3>Autocomplete con Trie</h3><p>Escribe un prefijo y compara Trie vs busqueda lineal.</p>")
    autocomplete_input = widgets.Text(
        value="silent",
        description="Prefijo:",
        placeholder="Ej: silent",
        layout=widgets.Layout(width="420px"),
    )
    autocomplete_limit = widgets.IntSlider(value=8, min=3, max=12, step=1, description="Top:")
    autocomplete_output = widgets.Output()

    def refresh_autocomplete(*_args: Any) -> None:
        result = autocomplete_demo.suggest(autocomplete_input.value, autocomplete_limit.value)
        with autocomplete_output:
            clear_output(wait=True)
            render_frame(
                as_frame(
                    [{"suggestion": suggestion} for suggestion in result["suggestions"]],
                    columns=["suggestion"],
                )
            )
            render_frame(
                as_frame(
                    [
                        {"strategy": "Trie", "latency_ms": round(result["trie_ms"], 4)},
                        {"strategy": "Linear Scan", "latency_ms": round(result["linear_ms"], 4)},
                    ]
                )
            )

    autocomplete_input.observe(refresh_autocomplete, names="value")
    autocomplete_limit.observe(refresh_autocomplete, names="value")
    refresh_autocomplete()

    similarity_title = widgets.HTML("<h3>Busqueda de Similares con MinHash + LSH</h3><p>Consulta usuarios similares y compara exacto vs aproximado.</p>")
    similarity_user = widgets.Dropdown(
        options=sorted(similarity_demo.histories.keys())[:200],
        value=sorted(similarity_demo.histories.keys())[0],
        description="Usuario:",
        layout=widgets.Layout(width="320px"),
    )
    similarity_k = widgets.IntSlider(value=5, min=3, max=10, step=1, description="Top-K:")
    similarity_button = widgets.Button(description="Buscar similares", button_style="info")
    similarity_output = widgets.Output()

    def refresh_similarity(_button: Optional[Any] = None) -> None:
        result = similarity_demo.query(similarity_user.value, k=similarity_k.value)
        with similarity_output:
            clear_output(wait=True)
            render_frame(
                as_frame(
                    [
                        {"mode": "Exact Jaccard", "latency_ms": round(result["exact_ms"], 4)},
                        {
                            "mode": "MinHash + LSH",
                            "latency_ms": round(result["lsh_ms"], 4),
                            "candidates": result["candidate_count"],
                            "precision_at_k": round(result["precision_at_k"], 4),
                        },
                    ]
                )
            )
            rows = []
            for index in range(max(len(result["exact"]), len(result["approx"]))):
                rows.append(
                    {
                        "rank": index + 1,
                        "exact": result["exact"][index] if index < len(result["exact"]) else "",
                        "approx": result["approx"][index] if index < len(result["approx"]) else "",
                    }
                )
            render_frame(as_frame(rows))

    similarity_button.on_click(refresh_similarity)
    refresh_similarity()

    cache_title = widgets.HTML("<h3>Cache Inteligente</h3><p>Registra accesos y observa hit/miss y el estado LRU.</p>")
    cache_input = widgets.Text(
        value=titles[0],
        description="Titulo:",
        layout=widgets.Layout(width="500px"),
    )
    cache_access_button = widgets.Button(description="Acceder", button_style="success")
    cache_batch_button = widgets.Button(description="Simular 10 accesos populares", button_style="info")
    cache_output = widgets.Output()

    def refresh_cache(_button: Optional[Any] = None, batch: bool = False) -> None:
        if batch:
            cache_demo.access_random_hot(10)
        else:
            cache_demo.access(cache_input.value)
        with cache_output:
            clear_output(wait=True)
            render_frame(cache_demo.stats_frame())
            display(Markdown("**Elementos residentes en cache (mas recientes primero)**"))
            render_frame(as_frame(cache_demo.resident_items()))
            display(Markdown("**Ultimos accesos**"))
            render_frame(as_frame(list(cache_demo.history)))

    cache_access_button.on_click(lambda button: refresh_cache(button, batch=False))
    cache_batch_button.on_click(lambda button: refresh_cache(button, batch=True))
    refresh_cache(batch=False)

    priority_title = widgets.HTML("<h3>Procesamiento de Streams con Prioridades</h3><p>Encola eventos y procesa el siguiente segun prioridad y FIFO.</p>")
    priority_user_type = widgets.Dropdown(options=["premium", "standard"], value="premium", description="Usuario:")
    priority_event_type = widgets.Dropdown(
        options=["play", "search", "resume", "download", "payment", "subscription", "browse"],
        value="payment",
        description="Evento:",
    )
    priority_content = widgets.Text(
        value=titles[1],
        description="Titulo:",
        layout=widgets.Layout(width="500px"),
    )
    priority_add_button = widgets.Button(description="Encolar", button_style="success")
    priority_batch_button = widgets.Button(description="Generar 5 aleatorios", button_style="info")
    priority_process_button = widgets.Button(description="Procesar siguiente", button_style="warning")
    priority_output = widgets.Output()

    def refresh_priority(_button: Optional[Any] = None, action: str = "view") -> None:
        if action == "add":
            priority_demo.enqueue(priority_user_type.value, priority_event_type.value, priority_content.value)
        elif action == "batch":
            priority_demo.enqueue_random(5)
        elif action == "process":
            priority_demo.process_next()

        with priority_output:
            clear_output(wait=True)
            if priority_demo.last_processed is not None:
                display(Markdown("**Ultimo evento procesado**"))
                render_frame(as_frame([priority_demo.last_processed]))
            display(Markdown("**Vista previa de la cola**"))
            render_frame(priority_demo.queue_preview())

    priority_add_button.on_click(lambda button: refresh_priority(button, action="add"))
    priority_batch_button.on_click(lambda button: refresh_priority(button, action="batch"))
    priority_process_button.on_click(lambda button: refresh_priority(button, action="process"))
    refresh_priority(action="batch")

    anomaly_title = widgets.HTML("<h3>Deteccion de Duplicados y Anomalias</h3><p>Ingresa un token de evento y una IP para ver Bloom Filter y Count-Min Sketch en accion.</p>")
    anomaly_token = widgets.Text(
        value="user_00001:title_00001:session_a",
        description="Token:",
        layout=widgets.Layout(width="500px"),
    )
    anomaly_ip = widgets.Text(value="10.0.0.1", description="IP:", layout=widgets.Layout(width="320px"))
    anomaly_add_button = widgets.Button(description="Registrar evento", button_style="success")
    anomaly_batch_button = widgets.Button(description="Simular patron sospechoso", button_style="danger")
    anomaly_output = widgets.Output()

    def refresh_anomaly(_button: Optional[Any] = None, batch: bool = False) -> None:
        if batch:
            anomaly_demo.ingest_suspicious_batch(5)
        else:
            anomaly_demo.ingest(anomaly_token.value, anomaly_ip.value)

        with anomaly_output:
            clear_output(wait=True)
            display(Markdown("**Ultimos eventos observados**"))
            render_frame(as_frame(list(anomaly_demo.history)))

    anomaly_add_button.on_click(lambda button: refresh_anomaly(button, batch=False))
    anomaly_batch_button.on_click(lambda button: refresh_anomaly(button, batch=True))
    refresh_anomaly(batch=False)

    topk_title = widgets.HTML("<h3>Top-K en Tiempo Real</h3><p>Alimenta el stream y compara el ranking exacto contra Misra-Gries.</p>")
    topk_input = widgets.Text(value=titles[2], description="Titulo:", layout=widgets.Layout(width="500px"))
    topk_add_button = widgets.Button(description="Agregar vista", button_style="success")
    topk_batch_button = widgets.Button(description="Simular 20 vistas populares", button_style="info")
    topk_output = widgets.Output()

    def refresh_topk(_button: Optional[Any] = None, batch: bool = False) -> None:
        if batch:
            topk_demo.ingest_random_batch(20)
        else:
            topk_demo.ingest(topk_input.value)

        with topk_output:
            clear_output(wait=True)
            render_frame(
                as_frame(
                    [
                        {"metric": "precision_at_k", "value": round(topk_demo.precision_at_k(), 4)},
                        {"metric": "items_observed", "value": int(sum(topk_demo.counter.values()))},
                    ]
                )
            )
            display(Markdown("**Top-K exacto**"))
            render_frame(as_frame([{"title": title, "count": count} for title, count in topk_demo.exact_topk()]))
            display(Markdown("**Top-K aproximado con Misra-Gries**"))
            render_frame(as_frame([{"title": title, "estimate": count} for title, count in topk_demo.approx_topk()]))

    topk_add_button.on_click(lambda button: refresh_topk(button, batch=False))
    topk_batch_button.on_click(lambda button: refresh_topk(button, batch=True))
    refresh_topk(batch=True)

    search_tab = widgets.VBox(
        [
            autocomplete_title,
            widgets.HBox([autocomplete_input, autocomplete_limit]),
            autocomplete_output,
            widgets.HTML("<hr>"),
            similarity_title,
            widgets.HBox([similarity_user, similarity_k, similarity_button]),
            similarity_output,
        ]
    )
    cache_tab = widgets.VBox(
        [
            cache_title,
            widgets.HBox([cache_input, cache_access_button, cache_batch_button]),
            cache_output,
        ]
    )
    priority_tab = widgets.VBox(
        [
            priority_title,
            widgets.HBox([priority_user_type, priority_event_type]),
            widgets.HBox([priority_content, priority_add_button, priority_batch_button, priority_process_button]),
            priority_output,
        ]
    )
    anomaly_tab = widgets.VBox(
        [
            anomaly_title,
            widgets.HBox([anomaly_token]),
            widgets.HBox([anomaly_ip, anomaly_add_button, anomaly_batch_button]),
            anomaly_output,
        ]
    )
    topk_tab = widgets.VBox(
        [
            topk_title,
            widgets.HBox([topk_input, topk_add_button, topk_batch_button]),
            topk_output,
        ]
    )

    tabs = widgets.Tab(children=[search_tab, cache_tab, priority_tab, anomaly_tab, topk_tab])
    tab_names = [
        "Busqueda",
        "Cache",
        "Prioridades",
        "Anomalias",
        "Top-K",
    ]
    for index, name in enumerate(tab_names):
        tabs.set_title(index, name)

    app = widgets.VBox(
        [
            widgets.HTML(
                "<h2>Netflix Advanced Structures Interactive Demo</h2>"
                "<p>Demo funcional para Colab: interactua con cada estructura y luego ejecuta benchmarks si quieres evidencia empirica.</p>"
            ),
            tabs,
        ]
    )
    if show:
        display(app)
    return app


def _plot_lines(frame: Any, x: str, y: str, hue: str, title: str, ylabel: str) -> None:
    plt.figure(figsize=(8, 4.5))
    if pd is None:
        grouped = collections.defaultdict(list)
        for row in frame:
            grouped[row[hue]].append(row)
        for label, rows in grouped.items():
            rows = sorted(rows, key=lambda item: item[x])
            plt.plot([row[x] for row in rows], [row[y] for row in rows], marker="o", label=label)
    else:
        for label, group in frame.groupby(hue):
            ordered = group.sort_values(x)
            plt.plot(ordered[x], ordered[y], marker="o", linewidth=2, label=label)
    plt.title(title)
    plt.xlabel("Tamaño del problema")
    plt.ylabel(ylabel)
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    backend = plt.get_backend().lower()
    if "agg" in backend and "inline" not in backend:
        plt.close()
    else:
        plt.show()


def _plot_quality(frame: Any, x: str, columns: Sequence[str], title: str) -> None:
    plt.figure(figsize=(8, 4.5))
    if pd is None:
        ordered = sorted(frame, key=lambda row: row[x])
        for column in columns:
            plt.plot([row[x] for row in ordered], [row[column] for row in ordered], marker="o", label=column)
    else:
        ordered = frame.sort_values(x)
        for column in columns:
            plt.plot(ordered[x], ordered[column], marker="o", linewidth=2, label=column)
    plt.title(title)
    plt.xlabel("Tamaño del problema")
    plt.ylabel("Métrica")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    backend = plt.get_backend().lower()
    if "agg" in backend and "inline" not in backend:
        plt.close()
    else:
        plt.show()


def run_priority_demo() -> Tuple[Any, Any]:
    order_sample = priority_order_sample()
    benchmark = benchmark_priority_processing()
    print("\n[1] Procesamiento de Streams con Prioridades")
    _display_table(order_sample)
    _display_table(benchmark)
    _plot_lines(
        benchmark,
        x="size",
        y="elapsed_ms",
        hue="structure",
        title="Priority Queue: Heap vs SkipList",
        ylabel="Latencia total (ms)",
    )
    return order_sample, benchmark


def run_cache_demo() -> Any:
    benchmark = benchmark_cache_system()
    print("\n[2] Sistema de Caché Inteligente")
    _display_table(benchmark)
    _plot_lines(
        benchmark,
        x="size",
        y="elapsed_ms",
        hue="structure",
        title="Caché Inteligente: HashMap + Índice Ordenado",
        ylabel="Latencia de accesos (ms)",
    )
    _plot_lines(
        benchmark,
        x="size",
        y="hit_rate",
        hue="structure",
        title="Hit Rate del Caché",
        ylabel="Hit rate",
    )
    return benchmark


def run_search_demo() -> Tuple[Any, Any, Any]:
    prefix_benchmark = benchmark_prefix_search()
    lsh_benchmark, lsh_quality = benchmark_lsh_search()
    print("\n[3] Búsqueda Ultra-Rápida")
    _display_table(prefix_benchmark)
    _display_table(lsh_benchmark)
    _display_table(lsh_quality)
    _plot_lines(
        prefix_benchmark,
        x="size",
        y="elapsed_ms",
        hue="structure",
        title="Búsqueda por Prefijo: Trie vs Linear Scan",
        ylabel="Latencia de consultas (ms)",
    )
    _plot_lines(
        lsh_benchmark,
        x="size",
        y="elapsed_ms",
        hue="structure",
        title="Búsqueda de Similares: Exacto vs MinHash + LSH",
        ylabel="Latencia de consultas (ms)",
    )
    _plot_quality(
        lsh_quality,
        x="size",
        columns=["precision_at_k"],
        title="Precisión de la Búsqueda Aproximada",
    )
    return prefix_benchmark, lsh_benchmark, lsh_quality


def run_anomaly_demo() -> Tuple[Any, Any]:
    benchmark, quality = benchmark_anomaly_detection()
    print("\n[4] Detección de Duplicados y Anomalías")
    _display_table(benchmark)
    _display_table(quality)
    _plot_lines(
        benchmark,
        x="size",
        y="elapsed_ms",
        hue="structure",
        title="Throughput: detección exacta vs estructuras aproximadas",
        ylabel="Latencia de ingestión (ms)",
    )
    _plot_lines(
        benchmark,
        x="size",
        y="memory_kb",
        hue="structure",
        title="Memoria: exacto vs Bloom/CMS",
        ylabel="Memoria (KB)",
    )
    _plot_quality(
        quality,
        x="size",
        columns=["bloom_false_positive_rate", "cms_avg_error"],
        title="Calidad de aproximación en detección/anomalías",
    )
    return benchmark, quality


def run_topk_demo() -> Any:
    benchmark = benchmark_topk_tracking()
    print("\n[5] Top-K Tracker en Tiempo Real")
    _display_table(benchmark)
    _plot_lines(
        benchmark,
        x="size",
        y="elapsed_ms",
        hue="structure",
        title="Top-K en streaming: Exact Counter vs Misra-Gries",
        ylabel="Latencia de actualización (ms)",
    )
    _plot_lines(
        benchmark,
        x="size",
        y="memory_kb",
        hue="structure",
        title="Memoria del tracker Top-K",
        ylabel="Memoria (KB)",
    )
    _plot_lines(
        benchmark,
        x="size",
        y="precision_at_k",
        hue="structure",
        title="Precisión del Top-K estimado",
        ylabel="Precision@K",
    )
    return benchmark


def run_colab_demo() -> Dict[str, Any]:
    print("Arquitectura integrada para Colab: Heap/SkipList, HashMap+B+Tree, Trie, LSH, Bloom, CMS y Misra-Gries")
    priority = run_priority_demo()
    cache = run_cache_demo()
    search = run_search_demo()
    anomaly = run_anomaly_demo()
    topk = run_topk_demo()
    return {
        "priority": priority,
        "cache": cache,
        "search": search,
        "anomaly": anomaly,
        "topk": topk,
    }


def run_smoke_test() -> Dict[str, Any]:
    return {
        "priority_rows": benchmark_priority_processing(sizes=(300, 700), repeats=1),
        "cache_rows": benchmark_cache_system(sizes=(500, 1000), capacity=200, repeats=1),
        "prefix_rows": benchmark_prefix_search(sizes=(500, 1200), query_count=60, repeats=1),
        "lsh_rows": benchmark_lsh_search(user_sizes=(120, 240), query_count=10, repeats=1),
        "anomaly_rows": benchmark_anomaly_detection(sizes=(1000, 2000), repeats=1),
        "topk_rows": benchmark_topk_tracking(sizes=(2000, 5000), k=10, repeats=1),
    }


if __name__ == "__main__":
    run_colab_demo()
