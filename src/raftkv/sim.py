"""Deterministic simulation of a Raft cluster under injected faults.

The whole cluster - nodes, network, clients and the fault injector ("nemesis")
- runs in one thread on a logical clock. Every random decision comes from a
generator seeded by the run's seed, so ``run_simulation(seed=42, ...)`` always
produces the identical sequence of events. This is the approach popularised by
FoundationDB and TigerBeetle: explore thousands of fault schedules quickly,
and replay any failing one exactly.

Faults injected (depending on the profile):

* message loss, duplication and random delay (which also reorders messages);
* network partitions, including isolating the current leader;
* crash + restart (volatile state lost, ``MemoryStorage`` kept);
* process pauses (a node stops ticking and handling messages for a while,
  then resumes with stale beliefs - the classic "GC pause" scenario).

After the fault phase the nemesis heals everything and the simulation keeps
running so outstanding client operations can finish. Then the history is
checked for linearizability and the safety monitor's findings are collected.
"""
from __future__ import annotations

import heapq
import random
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from .checker.invariants import SafetyMonitor
from .checker.linearizability import INF, Op, check_history
from .kv import KVStateMachine
from .node import LEADER, RaftConfig, RaftNode
from .storage import MemoryStorage


@dataclass(frozen=True)
class FaultProfile:
    """Knobs for the network and the nemesis. Rates are per tick."""

    name: str
    delay_min: int = 1
    delay_max: int = 3
    drop_prob: float = 0.0
    dup_prob: float = 0.0
    partition_rate: float = 0.0
    partition_len: Tuple[int, int] = (20, 120)
    crash_rate: float = 0.0
    crash_len: Tuple[int, int] = (10, 150)
    pause_rate: float = 0.0
    pause_len: Tuple[int, int] = (10, 60)


PROFILES: Dict[str, FaultProfile] = {
    "clean": FaultProfile("clean"),
    "lossy": FaultProfile("lossy", delay_max=12, drop_prob=0.10, dup_prob=0.05),
    "partitions": FaultProfile("partitions", delay_max=4, partition_rate=0.01),
    "crashes": FaultProfile("crashes", delay_max=4, crash_rate=0.01),
    "pauses": FaultProfile("pauses", delay_max=4, pause_rate=0.01),
    # Short, frequent crashes and partitions: maximises competing elections.
    "elections": FaultProfile(
        "elections",
        delay_max=6,
        drop_prob=0.02,
        partition_rate=0.02,
        partition_len=(5, 40),
        crash_rate=0.03,
        crash_len=(1, 12),
    ),
    "chaos": FaultProfile(
        "chaos",
        delay_max=10,
        drop_prob=0.05,
        dup_prob=0.03,
        partition_rate=0.006,
        crash_rate=0.006,
        pause_rate=0.006,
    ),
}


@dataclass
class SimConfig:
    n_nodes: int = 5
    n_clients: int = 4
    n_keys: int = 3
    fault_ticks: int = 2000
    heal_ticks: int = 600
    client_timeout: int = 40
    profile: str = "chaos"
    bugs: FrozenSet[str] = frozenset()
    raft: RaftConfig = field(default_factory=RaftConfig)
    check_linearizability: bool = True
    lin_budget: int = 300_000
    trace: bool = False


@dataclass
class SimResult:
    seed: int
    profile: str
    bugs: List[str]
    safety_ok: bool
    linearizable: Optional[bool]
    violations: List[Dict[str, Any]]
    ops_invoked: int
    ops_completed: int
    latencies: List[int]
    leaders_elected: int
    messages_sent: int
    messages_dropped: int
    crashes: int
    partitions: int
    pauses: int
    lin_states: int
    trace: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.safety_ok and self.linearizable is not False

    def summary(self) -> Dict[str, Any]:
        d = asdict(self)
        d.pop("trace")
        d.pop("latencies")
        return d


class _Client:
    """A closed-loop client: one outstanding operation, retried until answered."""

    def __init__(self, cid: str, servers: List[str], keys: List[str], rng: random.Random, timeout: int):
        self.id = cid
        self.servers = servers
        self.keys = keys
        self.rng = rng
        self.timeout = timeout
        self.seq = 0
        self.target = rng.choice(servers)
        self.current: Optional[Op] = None
        self.cmd: Optional[Dict[str, Any]] = None
        self.sent_at = 0
        self.idle_until = 0
        self.seen_values: List[Any] = [None]

    def new_command(self, now: int, op_id: int) -> Tuple[Op, Dict[str, Any]]:
        self.seq += 1
        key = self.rng.choice(self.keys)
        r = self.rng.random()
        value = f"{self.id}:{self.seq}"  # unique values make anomalies visible
        if r < 0.40:
            op, args = "get", {}
        elif r < 0.75:
            op, args = "put", {"value": value}
        elif r < 0.95:
            op, args = "cas", {"expected": self.rng.choice(self.seen_values[-4:]), "value": value}
        else:
            op, args = "delete", {}
        cmd = {"op": op, "key": key, "client": self.id, "seq": self.seq, **args}
        rec = Op(id=op_id, client=self.id, key=key, op=op, args=args, invoke=now)
        self.current, self.cmd = rec, cmd
        return rec, cmd


class Simulation:
    """One deterministic run. Use :func:`run_simulation` for the common case."""

    def __init__(self, seed: int, cfg: SimConfig) -> None:
        self.seed = seed
        self.cfg = cfg
        self.prof = PROFILES[cfg.profile]
        self.rng = random.Random(seed)
        self.net_rng = random.Random(f"{seed}-net")
        self.nem_rng = random.Random(f"{seed}-nemesis")
        self.ids = [f"n{i}" for i in range(cfg.n_nodes)]
        self.storages = {nid: MemoryStorage() for nid in self.ids}
        self.restarts = {nid: 0 for nid in self.ids}
        self.nodes: Dict[str, Optional[RaftNode]] = {nid: self._make_node(nid) for nid in self.ids}
        self.pending: Dict[str, Dict[int, Tuple[str, int, int]]] = {nid: {} for nid in self.ids}
        self.dirty: set = set()
        self.now = 0
        self.queue: List[Tuple[int, int, str, str, Any]] = []
        self._qseq = 0
        self.group: Dict[str, int] = {nid: 0 for nid in self.ids}
        self.partition_until = -1
        self.crashed_until: Dict[str, int] = {}
        self.paused_until: Dict[str, int] = {}
        keys = [f"k{i}" for i in range(cfg.n_keys)]
        self.clients = {
            f"c{i}": _Client(f"c{i}", self.ids, keys, random.Random(f"{seed}-c{i}"), cfg.client_timeout)
            for i in range(cfg.n_clients)
        }
        self.history: List[Op] = []
        self.monitor = SafetyMonitor()
        self.counts = {"sent": 0, "dropped": 0, "crashes": 0, "partitions": 0, "pauses": 0}
        self.trace: List[str] = []

    # ----------------------------------------------------------------- setup
    def _make_node(self, nid: str) -> RaftNode:
        dedup = "no_dedup" not in self.cfg.bugs
        node_bugs = frozenset(b for b in self.cfg.bugs if b != "no_dedup")
        return RaftNode(
            nid,
            self.ids,
            self.storages[nid],
            KVStateMachine(dedup=dedup),
            rng=random.Random(f"{self.seed}-{nid}-{self.restarts[nid]}"),
            config=self.cfg.raft,
            bugs=node_bugs,
        )

    def _log(self, text: str) -> None:
        if self.cfg.trace:
            self.trace.append(f"t={self.now:5d} {text}")

    # --------------------------------------------------------------- network
    def _connected(self, a: str, b: str) -> bool:
        if a.startswith("c") or b.startswith("c"):
            return True  # clients sit outside the partitioned network
        return self.group[a] == self.group[b]

    def _send(self, src: str, dst: str, payload: Any) -> None:
        self.counts["sent"] += 1
        p = self.prof
        if self.net_rng.random() < p.drop_prob:
            self.counts["dropped"] += 1
            return
        copies = 2 if self.net_rng.random() < p.dup_prob else 1
        for _ in range(copies):
            t = self.now + self.net_rng.randint(p.delay_min, p.delay_max)
            self._qseq += 1
            heapq.heappush(self.queue, (t, self._qseq, src, dst, payload))

    def _deliver_due(self) -> None:
        while self.queue and self.queue[0][0] <= self.now:
            _, _, src, dst, payload = heapq.heappop(self.queue)
            if not self._connected(src, dst):
                self.counts["dropped"] += 1
                continue
            if dst in self.clients:
                self._client_reply(self.clients[dst], payload)
                continue
            node = self.nodes[dst]
            if node is None:
                self.counts["dropped"] += 1
                continue
            if self.paused_until.get(dst, -1) > self.now:
                # Paused process: the message waits in the socket buffer.
                self._qseq += 1
                heapq.heappush(self.queue, (self.paused_until[dst], self._qseq, src, dst, payload))
                continue
            if isinstance(payload, dict):
                self._client_request(dst, node, src, payload)
            else:
                node.step(payload)

    # ---------------------------------------------------------------- clients
    def _client_request(self, nid: str, node: RaftNode, client: str, cmd: Dict[str, Any]) -> None:
        if node.role != LEADER:
            self._send(nid, client, {"ok": False, "seq": cmd["seq"], "leader": node.leader_id})
            return
        index, term = node.propose(dict(cmd))
        self.pending[nid][index] = (client, cmd["seq"], term)
        self.dirty.add(nid)

    def _client_reply(self, c: _Client, reply: Dict[str, Any]) -> None:
        if c.current is None or reply["seq"] != c.seq:
            return  # stale reply for an older request
        if reply["ok"]:
            c.current.ret = self.now
            c.current.output = reply["result"]
            if c.current.op == "get":
                c.seen_values.append(reply["result"])
            elif c.current.op in ("put", "cas"):
                c.seen_values.append(c.current.args["value"])
            self._log(f"{c.id} done {c.cmd} -> {reply['result']!r}")
            c.current, c.cmd = None, None
            c.idle_until = self.now + c.rng.randint(0, 3)
        else:
            hint = reply.get("leader")
            c.target = hint if hint else c.rng.choice(self.ids)
            c.sent_at = self.now
            self._send(c.id, c.target, dict(c.cmd))

    def _client_tick(self, issue_new: bool) -> None:
        for c in self.clients.values():
            if c.current is None:
                if issue_new and self.now >= c.idle_until:
                    rec, cmd = c.new_command(self.now, len(self.history))
                    self.history.append(rec)
                    c.sent_at = self.now
                    self._send(c.id, c.target, dict(cmd))
            elif self.now - c.sent_at >= c.timeout:
                # Timed out: retry the same (client, seq) elsewhere.
                c.target = c.rng.choice(self.ids)
                c.sent_at = self.now
                self._send(c.id, c.target, dict(c.cmd))

    # ---------------------------------------------------------------- nemesis
    def _nemesis(self, faults_on: bool) -> None:
        p, r = self.prof, self.nem_rng
        # Restarts and un-pauses happen even in the heal phase.
        for nid, until in list(self.crashed_until.items()):
            if self.now >= until or not faults_on:
                del self.crashed_until[nid]
                self.restarts[nid] += 1
                self.nodes[nid] = self._make_node(nid)
                self._log(f"restart {nid}")
        if not faults_on:
            self.paused_until.clear()
        if self.partition_until >= 0 and (self.now >= self.partition_until or not faults_on):
            self.group = {nid: 0 for nid in self.ids}
            self.partition_until = -1
            self._log("heal partition")
        if not faults_on:
            return

        minority = (len(self.ids) - 1) // 2
        if self.partition_until < 0 and r.random() < p.partition_rate:
            leader = self._current_leader()
            if leader is not None and r.random() < 0.5:
                isolated = [leader]
            else:
                isolated = r.sample(self.ids, r.randint(1, max(1, minority)))
            self.group = {nid: (1 if nid in isolated else 0) for nid in self.ids}
            self.partition_until = self.now + r.randint(*p.partition_len)
            self.counts["partitions"] += 1
            self._log(f"partition {sorted(isolated)} away")
        if len(self.crashed_until) < minority and r.random() < p.crash_rate:
            victim = r.choice([n for n in self.ids if n not in self.crashed_until])
            self.nodes[victim] = None
            self.pending[victim] = {}
            self.dirty.discard(victim)
            self.crashed_until[victim] = self.now + r.randint(*p.crash_len)
            self.counts["crashes"] += 1
            self._log(f"crash {victim}")
        if r.random() < p.pause_rate:
            victim = r.choice(self.ids)
            if self.nodes[victim] is not None and self.paused_until.get(victim, -1) <= self.now:
                self.paused_until[victim] = self.now + r.randint(*p.pause_len)
                self.counts["pauses"] += 1
                self._log(f"pause {victim}")

    def _current_leader(self) -> Optional[str]:
        best = None
        for nid, n in self.nodes.items():
            if n is not None and n.role == LEADER and (best is None or n.current_term > best[1]):
                best = (nid, n.current_term)
        return best[0] if best else None

    # -------------------------------------------------------------- main loop
    def _flush_nodes(self) -> None:
        for nid in self.ids:
            node = self.nodes[nid]
            if node is None:
                continue
            if nid in self.dirty:
                node.broadcast_append()
            for index, term, cmd, result in node.take_applied():
                self.monitor.on_applied(nid, index, term, cmd, self.now)
                p = self.pending[nid].pop(index, None)
                if p is not None and p[2] == term and cmd.get("client") == p[0] and cmd.get("seq") == p[1]:
                    self._send(nid, p[0], {"ok": True, "seq": p[1], "result": result})
            for msg in node.take_messages():
                self._send(nid, msg.dst, msg)
        self.dirty.clear()

    def run(self) -> SimResult:
        total = self.cfg.fault_ticks + self.cfg.heal_ticks
        prev_roles: Dict[str, str] = {}
        for self.now in range(total):
            faults_on = self.now < self.cfg.fault_ticks
            self._nemesis(faults_on)
            self._deliver_due()
            for nid, node in self.nodes.items():
                if node is not None and self.paused_until.get(nid, -1) <= self.now:
                    node.tick()
            # Stop issuing new operations shortly before the end so they can finish.
            self._client_tick(issue_new=self.now < total - self.cfg.heal_ticks // 2)
            self._flush_nodes()
            self.monitor.observe(self.nodes, self.now)
            if self.cfg.trace:
                for nid, node in self.nodes.items():
                    role = node.role if node else "down"
                    if prev_roles.get(nid) != role:
                        self._log(f"{nid} -> {role} (term {node.current_term if node else '-'})")
                        prev_roles[nid] = role
            if self.now % 200 == 0:
                self.monitor.check_log_matching(self.nodes, self.now)
        self.monitor.check_log_matching(self.nodes, self.now)

        lin: Optional[bool] = True
        lin_states = 0
        if self.cfg.check_linearizability:
            res = check_history(self.history, budget_per_key=self.cfg.lin_budget)
            lin, lin_states = res.ok, res.states_explored
            if res.ok is False:
                self._log(f"linearizability violation on key {res.failing_key}")
        done = [o for o in self.history if o.ret != INF]
        return SimResult(
            seed=self.seed,
            profile=self.cfg.profile,
            bugs=sorted(self.cfg.bugs),
            safety_ok=self.monitor.ok,
            linearizable=lin,
            violations=[asdict(v) for v in self.monitor.violations],
            ops_invoked=len(self.history),
            ops_completed=len(done),
            latencies=[int(o.ret - o.invoke) for o in done],
            leaders_elected=len(self.monitor.leaders),
            messages_sent=self.counts["sent"],
            messages_dropped=self.counts["dropped"],
            crashes=self.counts["crashes"],
            partitions=self.counts["partitions"],
            pauses=self.counts["pauses"],
            lin_states=lin_states,
            trace=self.trace,
        )


def run_simulation(seed: int, cfg: Optional[SimConfig] = None, **overrides: Any) -> SimResult:
    """Run one seeded simulation. ``overrides`` replace fields of ``cfg``."""
    cfg = cfg or SimConfig()
    if overrides:
        cfg = SimConfig(**{**cfg.__dict__, **overrides})
    return Simulation(seed, cfg).run()
