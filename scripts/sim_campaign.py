"""Simulation campaign: safety, availability, ablations and mutation testing.

Produces:
  reports/sim_campaign.json  - correct implementation across fault profiles
  reports/ablation.json      - PreVote and batching ablations
  reports/mutation.json      - how often the checkers catch each injected bug
  reports/*.png              - charts

Usage: python scripts/sim_campaign.py [--seeds 300] [--mutation-seeds 500] [--workers 8]
"""
from __future__ import annotations

import argparse
import json
import math
import os
import platform
import statistics
import sys
import time
from multiprocessing import Pool

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from raftkv.node import KNOWN_BUGS, RaftConfig  # noqa: E402
from raftkv.sim import PROFILES, SimConfig, run_simulation  # noqa: E402

REPORTS = os.path.join(os.path.dirname(__file__), "..", "reports")


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def wilson(k, n, z=1.96):
    """95% Wilson score interval for a binomial proportion."""
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (round((c - m) / d, 4), round((c + m) / d, 4))


def _run(job):
    seed, kw, tag = job
    r = run_simulation(seed, **kw)
    first = None
    if r.violations:
        first = r.violations[0]["kind"]
    elif r.linearizable is False:
        first = "linearizability"
    return tag, {
        "seed": seed,
        "ok": r.ok,
        "safety_ok": r.safety_ok,
        "linearizable": r.linearizable,
        "first": first,
        "invoked": r.ops_invoked,
        "completed": r.ops_completed,
        "lat": r.latencies,
        "leaders": r.leaders_elected,
        "sent": r.messages_sent,
    }


def aggregate(rows, total_ticks):
    lat = [x for r in rows for x in r["lat"]]
    completed = sum(r["completed"] for r in rows)
    return {
        "runs": len(rows),
        "failing_runs": sum(1 for r in rows if not r["ok"]),
        "safety_violations": sum(1 for r in rows if not r["safety_ok"]),
        "linearizability_failures": sum(1 for r in rows if r["linearizable"] is False),
        "linearizability_unknown": sum(1 for r in rows if r["linearizable"] is None),
        "ops_invoked": sum(r["invoked"] for r in rows),
        "ops_completed": completed,
        "ops_per_1k_ticks": round(1000 * completed / (len(rows) * total_ticks), 2),
        "latency_ticks_p50": pct(lat, 0.5),
        "latency_ticks_p99": pct(lat, 0.99),
        "mean_leaders_elected": round(statistics.mean(r["leaders"] for r in rows), 2),
        "messages_per_completed_op": round(sum(r["sent"] for r in rows) / max(completed, 1), 2),
    }


def run_jobs(jobs, workers):
    out = {}
    with Pool(workers) as pool:
        for tag, row in pool.imap_unordered(_run, jobs, chunksize=4):
            out.setdefault(tag, []).append(row)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=300)
    ap.add_argument("--mutation-seeds", type=int, default=500)
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    args = ap.parse_args()
    os.makedirs(REPORTS, exist_ok=True)
    base = SimConfig()
    total_ticks = base.fault_ticks + base.heal_ticks
    env = {"python": platform.python_version(), "machine": platform.machine(), "system": platform.system()}

    # ---------------------------------------------------------- A: correctness
    t0 = time.time()
    jobs = [
        (s, {"profile": p, "n_nodes": n}, f"{p}|{n}")
        for p in PROFILES for n in (3, 5) for s in range(args.seeds)
    ]
    res = run_jobs(jobs, args.workers)
    campaign = {
        "description": "Correct implementation, default RaftConfig (PreVote on, max_batch 64).",
        "seeds_per_cell": args.seeds,
        "ticks_per_run": total_ticks,
        "fault_ticks": base.fault_ticks,
        "clients": base.n_clients,
        "keys": base.n_keys,
        "results": {},
        "env": env,
    }
    for tag, rows in sorted(res.items()):
        p, n = tag.split("|")
        campaign["results"].setdefault(p, {})[f"{n}_nodes"] = aggregate(rows, total_ticks)
    campaign["total_runs"] = len(jobs)
    campaign["total_failing_runs"] = sum(1 for rows in res.values() for r in rows if not r["ok"])
    campaign["seconds"] = round(time.time() - t0, 1)
    with open(os.path.join(REPORTS, "sim_campaign.json"), "w") as f:
        json.dump(campaign, f, indent=2)
    print(f"campaign: {len(jobs)} runs, {campaign['total_failing_runs']} failing, {campaign['seconds']}s")

    # ------------------------------------------------------------- B: ablation
    t0 = time.time()
    variants = {
        "default": RaftConfig(),
        "no_prevote": RaftConfig(pre_vote=False),
        "batch_1": RaftConfig(max_batch=1),
    }
    jobs = [
        (s, {"profile": p, "n_nodes": 5, "raft": cfg}, f"{name}|{p}")
        for name, cfg in variants.items() for p in ("partitions", "chaos") for s in range(args.seeds)
    ]
    res = run_jobs(jobs, args.workers)
    ablation = {"description": "5 nodes; same seeds for every variant.", "seeds": args.seeds, "results": {}}
    for tag, rows in sorted(res.items()):
        name, p = tag.split("|")
        ablation["results"].setdefault(p, {})[name] = aggregate(rows, total_ticks)
    ablation["seconds"] = round(time.time() - t0, 1)
    with open(os.path.join(REPORTS, "ablation.json"), "w") as f:
        json.dump(ablation, f, indent=2)
    print(f"ablation done in {ablation['seconds']}s")

    # ------------------------------------------------------------- C: mutation
    t0 = time.time()
    configs = {
        "default": {"profile": "chaos", "n_nodes": 5, "raft": RaftConfig()},
        "adversarial": {"profile": "elections", "n_nodes": 3, "raft": RaftConfig(pre_vote=False, max_batch=1)},
    }
    bugs = [None] + sorted(KNOWN_BUGS)
    jobs = [
        (s, {**cfg, "bugs": frozenset([b]) if b else frozenset()}, f"{b or 'none'}|{cname}")
        for b in bugs for cname, cfg in configs.items() for s in range(args.mutation_seeds)
    ]
    res = run_jobs(jobs, args.workers)
    mutation = {
        "description": (
            "Fraction of seeds in which a deliberately injected bug is detected by the safety monitor "
            "or the linearizability checker. 'none' is the correct implementation (false-positive check)."
        ),
        "seeds": args.mutation_seeds,
        "configs": {
            "default": "chaos profile, 5 nodes, PreVote on, max_batch 64",
            "adversarial": "elections profile, 3 nodes, PreVote off, max_batch 1",
        },
        "results": {},
    }
    for tag, rows in sorted(res.items()):
        b, cname = tag.split("|")
        rows.sort(key=lambda r: r["seed"])
        detected = [r for r in rows if not r["ok"]]
        by = {}
        for r in detected:
            by[r["first"]] = by.get(r["first"], 0) + 1
        mutation["results"].setdefault(b, {})[cname] = {
            "detected": len(detected),
            "runs": len(rows),
            "detection_rate": round(len(detected) / len(rows), 4),
            "ci95": wilson(len(detected), len(rows)),
            "first_detecting_seed": detected[0]["seed"] if detected else None,
            "first_signal": by,
            "linearizability_unknown": sum(1 for r in rows if r["linearizable"] is None),
        }
    mutation["seconds"] = round(time.time() - t0, 1)
    with open(os.path.join(REPORTS, "mutation.json"), "w") as f:
        json.dump(mutation, f, indent=2)
    print(f"mutation done in {mutation['seconds']}s")

    charts(campaign, ablation, mutation)


def charts(campaign, ablation, mutation):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ink, grid = "#1f2a30", "#d9d4c7"
    c1, c2, c3 = "#1b6b73", "#d9822b", "#8c3b3b"
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": ink, "axes.labelcolor": ink,
                         "xtick.color": ink, "ytick.color": ink})

    # Mutation detection rates.
    bugs = [b for b in mutation["results"] if b != "none"]
    fig, ax = plt.subplots(figsize=(8, 4))
    w = 0.38
    for k, (cname, col) in enumerate((("default", c1), ("adversarial", c2))):
        rates = [100 * mutation["results"][b][cname]["detection_rate"] for b in bugs]
        xs = [i + (k - 0.5) * w for i in range(len(bugs))]
        ax.bar(xs, rates, w, color=col, label=cname)
        for x, r in zip(xs, rates):
            ax.text(x, r + 1, f"{r:.1f}", ha="center", fontsize=8, color=ink)
    ax.set_xticks(range(len(bugs)))
    ax.set_xticklabels(bugs, rotation=20, ha="right")
    ax.set_ylabel("seeds detecting the bug (%)")
    ax.set_title(f"Mutation testing: detection rate per injected bug ({mutation['seeds']} seeds each)")
    ax.grid(axis="y", color=grid)
    ax.set_axisbelow(True)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(REPORTS, "mutation_detection.png"), dpi=140)
    plt.close(fig)

    # Availability and latency per profile.
    profs = list(campaign["results"])
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
    for k, (n, col) in enumerate((("3_nodes", c1), ("5_nodes", c3))):
        xs = [i + (k - 0.5) * w for i in range(len(profs))]
        axes[0].bar(xs, [campaign["results"][p][n]["ops_per_1k_ticks"] for p in profs], w, color=col, label=n)
        axes[1].bar(xs, [campaign["results"][p][n]["latency_ticks_p99"] for p in profs], w, color=col, label=n)
    for ax, t in zip(axes, ("completed ops per 1000 ticks", "p99 op latency (ticks)")):
        ax.set_xticks(range(len(profs)))
        ax.set_xticklabels(profs, rotation=20)
        ax.set_title(t)
        ax.grid(axis="y", color=grid)
        ax.set_axisbelow(True)
    axes[0].legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(REPORTS, "profiles.png"), dpi=140)
    plt.close(fig)


if __name__ == "__main__":
    main()
