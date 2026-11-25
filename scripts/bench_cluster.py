"""Benchmark a real multi-process cluster on localhost.

1. Throughput / latency of writes for 1, 3 and 5 nodes at several client
   concurrencies. The 1-node "cluster" is the no-replication baseline: same
   code, same disk writes, but no network round trip to followers.
2. Leader failover: kill -9 the leader under a steady write load and measure
   the gap between the last successful write before the kill and the first one
   after it.

Writes reports/cluster_bench.json and reports/cluster_bench.png.
Usage: python scripts/bench_cluster.py [--duration 5] [--trials 5] [--no-fsync]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import platform
import shutil
import signal
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from raftkv.cli import local_cluster_spec, spawn_cluster  # noqa: E402
from raftkv.client import RaftClient  # noqa: E402
from raftkv.server import parse_cluster  # noqa: E402

REPORTS = os.path.join(os.path.dirname(__file__), "..", "reports")


def pct(xs, q):
    xs = sorted(xs)
    k = (len(xs) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


async def wait_ready(cluster, timeout=10.0):
    c = RaftClient(cluster, attempt_timeout=0.3)
    end = time.monotonic() + timeout
    try:
        while time.monotonic() < end:
            try:
                await c.execute("put", "__ready__", deadline=1.0, value=1)
                return
            except Exception:  # noqa: BLE001 - keep polling until ready
                await asyncio.sleep(0.1)
        raise RuntimeError("cluster did not become ready")
    finally:
        await c.close()


async def load(cluster, concurrency, duration):
    lat, errors = [], 0
    stop = time.monotonic() + duration

    async def worker(i):
        nonlocal errors
        c = RaftClient(cluster, client_id=f"bench{i}", attempt_timeout=1.0)
        await c.find_leader()
        n = 0
        try:
            while time.monotonic() < stop:
                t = time.perf_counter()
                try:
                    await c.put(f"key{i % 64}", n)
                    lat.append((time.perf_counter() - t) * 1000)
                except Exception:  # noqa: BLE001
                    errors += 1
                n += 1
        finally:
            await c.close()

    t0 = time.monotonic()
    await asyncio.gather(*(worker(i) for i in range(concurrency)))
    elapsed = time.monotonic() - t0
    return {
        "ops": len(lat),
        "errors": errors,
        "ops_per_sec": round(len(lat) / elapsed, 1),
        "latency_ms_p50": round(pct(lat, 0.5), 3),
        "latency_ms_p99": round(pct(lat, 0.99), 3),
        "latency_ms_mean": round(statistics.mean(lat), 3),
    }


async def failover_trial(cluster, procs):
    c = RaftClient(cluster, client_id="failover", attempt_timeout=0.25)
    leader = await c.find_leader()
    idx = list(cluster).index(leader)
    successes = []
    stop = asyncio.Event()

    async def writer():
        n = 0
        while not stop.is_set():
            try:
                await c.execute("put", "fo", deadline=10.0, value=n)
                successes.append(time.monotonic())
            except Exception:  # noqa: BLE001
                pass
            n += 1
            await asyncio.sleep(0.005)

    task = asyncio.ensure_future(writer())
    await asyncio.sleep(0.5)
    kill_at = time.monotonic()
    procs[idx].send_signal(signal.SIGKILL)
    procs[idx].wait()
    while not any(t > kill_at + 0.0 for t in successes):
        await asyncio.sleep(0.01)
        if time.monotonic() - kill_at > 15:
            break
    await asyncio.sleep(0.2)
    stop.set()
    await task
    await c.close()
    before = max(t for t in successes if t <= kill_at)
    after = min(t for t in successes if t > kill_at)
    return {"killed": leader, "gap_ms": round((after - before) * 1000, 1),
            "kill_to_first_write_ms": round((after - kill_at) * 1000, 1)}, idx


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=5.0)
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--no-fsync", action="store_true")
    ap.add_argument("--base-port", type=int, default=7400)
    args = ap.parse_args()
    fsync = not args.no_fsync
    os.makedirs(REPORTS, exist_ok=True)
    report = {
        "env": {"python": platform.python_version(), "machine": platform.machine(),
                "system": platform.system(), "cpus": os.cpu_count()},
        "fsync": fsync,
        "tick_ms": 10,
        "election_timeout_ms": [150, 300],
        "heartbeat_ms": 50,
        "duration_s": args.duration,
        "throughput": {},
        "failover": {},
    }

    port = args.base_port
    for n in (1, 3, 5):
        for conc in (1, 16, 64):
            tmp = tempfile.mkdtemp(prefix="raftkv-bench-")
            procs = spawn_cluster(n, port, tmp, fsync=fsync)
            cluster = parse_cluster(local_cluster_spec(n, port))
            try:
                asyncio.run(wait_ready(cluster))
                r = asyncio.run(load(cluster, conc, args.duration))
            finally:
                for p in procs:
                    p.kill()
                for p in procs:
                    p.wait()
                shutil.rmtree(tmp, ignore_errors=True)
            report["throughput"].setdefault(f"{n}_nodes", {})[f"c{conc}"] = r
            print(f"n={n} c={conc}: {r}")
            port += 10

    trials = []
    for t in range(args.trials):
        tmp = tempfile.mkdtemp(prefix="raftkv-fo-")
        procs = spawn_cluster(3, port, tmp, fsync=fsync)
        cluster = parse_cluster(local_cluster_spec(3, port))
        try:
            asyncio.run(wait_ready(cluster))
            r, _ = asyncio.run(failover_trial(cluster, procs))
            trials.append(r)
            print(f"failover trial {t}: {r}")
        finally:
            for p in procs:
                p.kill()
            for p in procs:
                p.wait()
            shutil.rmtree(tmp, ignore_errors=True)
        port += 10
    gaps = [t["gap_ms"] for t in trials]
    report["failover"] = {
        "cluster_nodes": 3,
        "trials": trials,
        "gap_ms_median": statistics.median(gaps),
        "gap_ms_min": min(gaps),
        "gap_ms_max": max(gaps),
    }
    with open(os.path.join(REPORTS, "cluster_bench.json"), "w") as f:
        json.dump(report, f, indent=2)
    chart(report)


def chart(report):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ink, grid = "#1f2a30", "#d9d4c7"
    cols = {"1_nodes": "#7a7a6e", "3_nodes": "#1b6b73", "5_nodes": "#8c3b3b"}
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
    concs = ["c1", "c16", "c64"]
    w = 0.27
    for k, (n, col) in enumerate(cols.items()):
        d = report["throughput"][n]
        xs = [i + (k - 1) * w for i in range(len(concs))]
        axes[0].bar(xs, [d[c]["ops_per_sec"] for c in concs], w, color=col, label=n.replace("_", " "))
        axes[1].bar(xs, [d[c]["latency_ms_p99"] for c in concs], w, color=col, label=n.replace("_", " "))
    for ax, t in zip(axes, ("write throughput (ops/s)", "p99 write latency (ms)")):
        ax.set_xticks(range(len(concs)))
        ax.set_xticklabels([c[1:] + " clients" for c in concs])
        ax.set_title(t, color=ink)
        ax.grid(axis="y", color=grid)
        ax.set_axisbelow(True)
    axes[0].legend(frameon=False)
    fig.suptitle(f"Localhost cluster, fsync={'on' if report['fsync'] else 'off'}", color=ink)
    fig.tight_layout()
    fig.savefig(os.path.join(REPORTS, "cluster_bench.png"), dpi=140)
    plt.close(fig)


if __name__ == "__main__":
    main()
