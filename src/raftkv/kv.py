"""The replicated application: a key-value store with client deduplication.

Commands are applied in log order on every replica, so all replicas end up in
the same state. Each client request carries ``(client, seq)``; if a client
retries a request that was already applied (for example because the reply was
lost), the cached result is returned instead of applying it a second time.
This is what turns Raft's at-least-once delivery into exactly-once semantics.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

OPS = ("put", "get", "cas", "delete")


class KVStateMachine:
    """Deterministic key-value store applied to committed log entries."""

    def __init__(self, dedup: bool = True) -> None:
        self.data: Dict[str, Any] = {}
        self.sessions: Dict[str, Tuple[int, Any]] = {}
        self.dedup = dedup
        self.applied_ops = 0

    def apply(self, cmd: Dict[str, Any]) -> Any:
        op = cmd.get("op")
        if op == "noop":
            return None
        client, seq = cmd.get("client"), cmd.get("seq")
        if self.dedup and client is not None:
            last = self.sessions.get(client)
            if last is not None and seq <= last[0]:
                # Duplicate of the latest request: return the cached result.
                # (Older duplicates cannot be answered and get None.)
                return last[1] if seq == last[0] else None
        result = self._execute(op, cmd)
        self.applied_ops += 1
        if client is not None:
            self.sessions[client] = (seq, result)
        return result

    def _execute(self, op: Optional[str], cmd: Dict[str, Any]) -> Any:
        key = cmd.get("key")
        if op == "put":
            self.data[key] = cmd["value"]
            return "ok"
        if op == "get":
            return self.data.get(key)
        if op == "delete":
            return self.data.pop(key, None) is not None
        if op == "cas":
            if self.data.get(key) == cmd.get("expected"):
                self.data[key] = cmd["value"]
                return True
            return False
        raise ValueError(f"unknown op {op!r}")
