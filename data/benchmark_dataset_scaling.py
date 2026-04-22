"""
Benchmark de escalabilidad para el generador de datos sintéticos.

Mide:
    - Tiempo de generación de usuarios/videos/eventos
    - Memoria pico (tracemalloc)
    - Tamaño en disco del dataset serializado (JSON)

Uso:
    python data/benchmark_dataset_scaling.py
"""

from __future__ import annotations

import json
import time
import tracemalloc
from pathlib import Path
from typing import Dict, List

from synthetic_data_generator import generate_users, generate_videos, generate_events


def run_benchmark(n_events: int, out_dir: Path) -> Dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    tracemalloc.start()
    t0 = time.perf_counter()

    users = generate_users(1000)
    videos = generate_videos(500)
    t1 = time.perf_counter()

    events = generate_events(users, videos, n_events)
    t2 = time.perf_counter()

    dataset = {
        "usuarios": users,
        "videos": videos,
        "eventos": events,
    }

    out_file = out_dir / f"dataset_{n_events}.json"
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(dataset, f, ensure_ascii=False)

    t3 = time.perf_counter()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return {
        "n_events": n_events,
        "users": len(users),
        "videos": len(videos),
        "events": len(events),
        "t_users_videos_s": round(t1 - t0, 4),
        "t_events_s": round(t2 - t1, 4),
        "t_total_s": round(t3 - t0, 4),
        "peak_mem_mb": round(peak / (1024 * 1024), 4),
        "disk_mb": round(out_file.stat().st_size / (1024 * 1024), 4),
        "file": str(out_file),
    }


def main() -> None:
    out_dir = Path(__file__).resolve().parent / "benchmarks"
    scales: List[int] = [10_000, 100_000]

    results = []
    for n in scales:
        r = run_benchmark(n, out_dir)
        results.append(r)
        print(
            f"events={r['events']:,} | t_total={r['t_total_s']:.2f}s | "
            f"t_events={r['t_events_s']:.2f}s | peak_mem={r['peak_mem_mb']:.2f} MB | "
            f"disk={r['disk_mb']:.2f} MB"
        )

    summary_file = out_dir / "benchmark_summary.json"
    summary_file.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"summary={summary_file}")


if __name__ == "__main__":
    main()
