"""Online checks of Raft's safety properties (Figure 3 of the Raft paper).

The simulator calls :class:`SafetyMonitor` after every event. Because the
simulator can see every node's internal state at once - something no real
deployment can do - these properties can be checked directly:

* **Election safety** - at most one leader per term.
* **State machine safety** - no two nodes apply different commands at the
  same log index.
* **Leader completeness** - a newly elected leader's log contains every entry
  that any node has already applied (i.e. every committed entry).
* **Log matching** - if two logs contain an entry with the same index and
  term, the logs are identical up to that index (checked on committed
  prefixes, which keeps the check cheap).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class Violation:
    kind: str
    time: int
    detail: str


def _canon(cmd: Dict[str, Any]) -> str:
    return json.dumps(cmd, sort_keys=True)


class SafetyMonitor:
    """Accumulates global knowledge and records the first violations seen."""

    def __init__(self) -> None:
        self.leaders: Dict[int, str] = {}
        self.applied: Dict[int, Tuple[int, str]] = {}  # index -> (term, command)
        self.violations: List[Violation] = []
        self._leader_seen: Dict[str, Tuple[int, str]] = {}

    def _flag(self, kind: str, time: int, detail: str) -> None:
        if len(self.violations) < 20:
            self.violations.append(Violation(kind, time, detail))

    def on_applied(self, node_id: str, index: int, term: int, command: Dict[str, Any], time: int) -> None:
        c = _canon(command)
        prev = self.applied.get(index)
        if prev is None:
            self.applied[index] = (term, c)
        elif prev != (term, c):
            self._flag(
                "state_machine_safety",
                time,
                f"node {node_id} applied {c} (term {term}) at index {index}, "
                f"another node applied {prev[1]} (term {prev[0]})",
            )

    def observe(self, nodes: Dict[str, Any], time: int) -> None:
        """Inspect every live node; call after each simulator event."""
        for nid, node in nodes.items():
            if node is None or node.role != "leader":
                continue
            term = node.current_term
            holder = self.leaders.get(term)
            if holder is None:
                self.leaders[term] = nid
                self._check_leader_completeness(nid, node, time)
            elif holder != nid:
                self._flag("election_safety", time, f"term {term} has leaders {holder} and {nid}")

    def _check_leader_completeness(self, nid: str, node: Any, time: int) -> None:
        for index, (term, c) in self.applied.items():
            if index > node.last_index or node.term_at(index) != term:
                self._flag(
                    "leader_completeness",
                    time,
                    f"leader {nid} (term {node.current_term}) is missing committed index {index}",
                )
                return

    def check_log_matching(self, nodes: Dict[str, Any], time: int) -> None:
        """Pairwise check of committed prefixes; run occasionally (O(n^2 * log))."""
        live = [(nid, n) for nid, n in nodes.items() if n is not None]
        for nid, n in live:
            if n.commit_index > n.last_index:
                self._flag("log_matching", time, f"{nid} commit_index {n.commit_index} beyond log end {n.last_index}")
                return
        for i in range(len(live)):
            for j in range(i + 1, len(live)):
                a_id, a = live[i]
                b_id, b = live[j]
                upto = min(a.commit_index, b.commit_index)
                for idx in range(upto, 0, -1):
                    if a.log[idx - 1] != b.log[idx - 1]:
                        self._flag("log_matching", time, f"{a_id} and {b_id} differ at committed index {idx}")
                        return

    @property
    def ok(self) -> bool:
        return not self.violations

    def first(self) -> Optional[Violation]:
        return self.violations[0] if self.violations else None
