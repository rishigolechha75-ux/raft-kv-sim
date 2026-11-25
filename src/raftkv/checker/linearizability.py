"""Linearizability checker for key-value histories.

A history is a list of client operations, each with an invocation time, a
return time and an observed output. The history is *linearizable* if there is
a total order of the operations that (a) respects real time - if A returned
before B was invoked, A comes first - and (b) is a legal sequential execution
of a single-copy key-value store.

The search is the Wing & Gong algorithm with the memoisation introduced by
Lowe ("Testing for linearizability", 2017) and used by tools such as
Porcupine and Knossos:

* the history is first split by key (linearizability is compositional, so each
  key's sub-history can be checked independently);
* a depth-first search repeatedly picks an operation that may legally come
  next (no other pending operation finished before it started), applies it to
  the model, and backtracks on a mismatch;
* ``(set of linearised ops, model state)`` pairs already explored are cached,
  which collapses the otherwise exponential search on realistic histories.

Operations whose reply never arrived ("indeterminate") have an infinite return
time and an unknown output: they may take effect at any point after their
invocation, or not at all.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

INF = math.inf


@dataclass
class Op:
    """One client operation in a history."""

    id: int
    client: str
    key: str
    op: str  # put | get | cas | delete
    args: Dict[str, Any] = field(default_factory=dict)
    output: Any = None
    invoke: float = 0.0
    ret: float = INF  # INF means the outcome is unknown

    @property
    def complete(self) -> bool:
        return self.ret != INF


def step_model(state: Any, op: Op) -> Tuple[bool, Any]:
    """Apply ``op`` to a single-register model. Returns ``(legal, new_state)``."""
    kind = op.op
    if kind == "put":
        return (not op.complete or op.output == "ok"), op.args["value"]
    if kind == "get":
        return (not op.complete or op.output == state), state
    if kind == "delete":
        existed = state is not None
        return (not op.complete or op.output == existed), None
    if kind == "cas":
        success = state == op.args.get("expected")
        if op.complete and op.output != success:
            return False, state
        return True, (op.args["value"] if success else state)
    raise ValueError(f"unknown op {kind!r}")


@dataclass
class CheckResult:
    ok: Optional[bool]  # None means the search budget ran out
    keys_checked: int
    states_explored: int
    failing_key: Optional[str] = None
    failing_ops: List[Op] = field(default_factory=list)


def _check_key(ops: List[Op], budget: int) -> Tuple[Optional[bool], int]:
    """Check one key's sub-history. Returns ``(ok, states_explored)``."""
    ops = sorted(ops, key=lambda o: (o.invoke, o.ret))
    n = len(ops)
    complete_mask = 0
    for i, o in enumerate(ops):
        if o.complete:
            complete_mask |= 1 << i
    seen = set()
    explored = 0
    # Each stack frame: (linearised mask, model state).
    stack: List[Tuple[int, Any]] = [(0, None)]
    while stack:
        mask, state = stack.pop()
        if mask & complete_mask == complete_mask:
            return True, explored
        if (mask, state) in seen:
            continue
        seen.add((mask, state))
        explored += 1
        if explored > budget:
            return None, explored
        # Earliest return among pending ops bounds who may go next.
        min_ret = INF
        for i in range(n):
            if not mask >> i & 1 and ops[i].ret < min_ret:
                min_ret = ops[i].ret
        candidates = []
        for i in range(n):
            if mask >> i & 1:
                continue
            o = ops[i]
            if o.invoke > min_ret:
                break  # sorted by invoke: nobody later can go next either
            legal, new_state = step_model(state, o)
            if legal:
                candidates.append((o.ret, i, new_state))
        # Push so that the op with the earliest return is explored first.
        candidates.sort(reverse=True)
        for _, i, new_state in candidates:
            stack.append((mask | 1 << i, new_state))
    return False, explored


def check_history(history: List[Op], budget_per_key: int = 2_000_000) -> CheckResult:
    """Check a whole multi-key history; stops at the first failing key."""
    by_key: Dict[str, List[Op]] = defaultdict(list)
    for o in history:
        if o.op == "get" and not o.complete:
            continue  # an unanswered read has no observable effect
        by_key[o.key].append(o)
    total = 0
    result_ok: Optional[bool] = True
    for key in sorted(by_key, key=str):
        ok, explored = _check_key(by_key[key], budget_per_key)
        total += explored
        if ok is False:
            return CheckResult(False, len(by_key), total, key, sorted(by_key[key], key=lambda o: o.invoke))
        if ok is None:
            result_ok = None
    return CheckResult(result_ok, len(by_key), total)
