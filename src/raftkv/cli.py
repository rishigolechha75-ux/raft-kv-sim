"""Command-line interface: ``raftkv <command>``.

Examples::

    raftkv cluster --nodes 3                    # run a local 3-node cluster
    raftkv put greeting hello                   # talk to it
    raftkv get greeting
    raftkv status
    raftkv sim --seeds 200 --profile chaos      # simulation campaign
    raftkv sim --seed 17 --bug skip_log_check --trace   # replay one run
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from typing import List

from .node import KNOWN_BUGS, RaftConfig
from .server import parse_cluster, run_server
from .sim import PROFILES, run_simulation

DEFAULT_CLUSTER = "n0=127.0.0.1:7000,n1=127.0.0.1:7001,n2=127.0.0.1:7002"


def local_cluster_spec(n: int, base_port: int, host: str = "127.0.0.1") -> str:
    return ",".join(f"n{i}={host}:{base_port + i}" for i in range(n))


def spawn_cluster(n: int, base_port: int, data_dir: str, fsync: bool = True, tick_ms: float = 10.0) -> List[subprocess.Popen]:
    """Start ``n`` node processes on localhost and return their handles."""
    spec = local_cluster_spec(n, base_port)
    procs = []
    for i in range(n):
        cmd = [
            sys.executable, "-m", "raftkv.cli", "serve",
            "--id", f"n{i}", "--cluster", spec,
            "--data-dir", os.path.join(data_dir, f"n{i}"),
            "--tick-ms", str(tick_ms),
        ]
        if not fsync:
            cmd.append("--no-fsync")
        procs.append(subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    return procs


def _client_cmd(args: argparse.Namespace) -> int:
    from .client import RaftClient

    async def go() -> int:
        c = RaftClient(parse_cluster(args.cluster))
        try:
            if args.cmd == "status":
                out = {}
                for nid in c.ids:
                    try:
                        out[nid] = await c.status(nid)
                    except Exception as e:  # noqa: BLE001 - report any failure per node
                        out[nid] = {"error": type(e).__name__}
                print(json.dumps(out, indent=2))
                return 0
            if args.cmd == "put":
                res = await c.put(args.key, args.value)
            elif args.cmd == "get":
                res = await c.get(args.key)
            elif args.cmd == "delete":
                res = await c.delete(args.key)
            else:
                expected = None if args.expected == "-" else args.expected
                res = await c.cas(args.key, expected, args.value)
            print(json.dumps(res))
            return 0
        finally:
            await c.close()

    return asyncio.run(go())


def _sim_cmd(args: argparse.Namespace) -> int:
    bugs = frozenset(args.bug or [])
    raft = RaftConfig(pre_vote=not args.no_pre_vote, max_batch=args.max_batch)
    seeds = [args.seed] if args.seed is not None else list(range(args.seeds))
    failures = 0
    t0 = time.time()
    for s in seeds:
        r = run_simulation(
            s, profile=args.profile, n_nodes=args.nodes, bugs=bugs, raft=raft,
            fault_ticks=args.ticks, trace=args.trace,
        )
        if args.trace:
            print("\n".join(r.trace))
        if not r.ok:
            failures += 1
            first = r.violations[0] if r.violations else {"kind": "linearizability"}
            print(f"seed {s}: FAIL {first}")
        elif args.seed is not None:
            print(f"seed {s}: ok")
    print(
        f"{len(seeds)} runs, {failures} failing, profile={args.profile}, "
        f"nodes={args.nodes}, bugs={sorted(bugs) or 'none'}, {time.time() - t0:.1f}s"
    )
    return 1 if failures else 0


def main(argv: List[str] = None) -> int:
    p = argparse.ArgumentParser(prog="raftkv", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run one node")
    s.add_argument("--id", required=True)
    s.add_argument("--cluster", default=DEFAULT_CLUSTER)
    s.add_argument("--data-dir", default=None)
    s.add_argument("--no-fsync", action="store_true")
    s.add_argument("--tick-ms", type=float, default=10.0)

    c = sub.add_parser("cluster", help="run a local multi-process cluster")
    c.add_argument("--nodes", type=int, default=3)
    c.add_argument("--base-port", type=int, default=7000)
    c.add_argument("--data-dir", default="data")
    c.add_argument("--no-fsync", action="store_true")

    for name in ("put", "get", "delete", "cas", "status"):
        q = sub.add_parser(name, help=f"{name} via the cluster")
        q.add_argument("--cluster", default=DEFAULT_CLUSTER)
        if name != "status":
            q.add_argument("key")
        if name == "cas":
            q.add_argument("expected", help="expected value, or - for absent")
        if name in ("put", "cas"):
            q.add_argument("value")

    m = sub.add_parser("sim", help="deterministic simulation runs")
    m.add_argument("--seeds", type=int, default=100)
    m.add_argument("--seed", type=int, default=None, help="run a single seed")
    m.add_argument("--profile", choices=sorted(PROFILES), default="chaos")
    m.add_argument("--nodes", type=int, default=5)
    m.add_argument("--ticks", type=int, default=2000)
    m.add_argument("--bug", action="append", choices=sorted(KNOWN_BUGS))
    m.add_argument("--no-pre-vote", action="store_true")
    m.add_argument("--max-batch", type=int, default=64)
    m.add_argument("--trace", action="store_true")

    args = p.parse_args(argv)
    if args.cmd == "serve":
        cluster = parse_cluster(args.cluster)
        if args.id not in cluster:
            p.error(f"--id {args.id} not in --cluster")
        run_server(args.id, cluster, args.data_dir or os.path.join("data", args.id), not args.no_fsync, args.tick_ms)
        return 0
    if args.cmd == "cluster":
        procs = spawn_cluster(args.nodes, args.base_port, args.data_dir, fsync=not args.no_fsync)
        print(f"started {args.nodes} nodes: {local_cluster_spec(args.nodes, args.base_port)} (Ctrl-C to stop)")
        try:
            while all(pr.poll() is None for pr in procs):
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            for pr in procs:
                pr.terminate()
            for pr in procs:
                pr.wait(timeout=5)
        return 0
    if args.cmd == "sim":
        return _sim_cmd(args)
    return _client_cmd(args)


if __name__ == "__main__":
    sys.exit(main())
