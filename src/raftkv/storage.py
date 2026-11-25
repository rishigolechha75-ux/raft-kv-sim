"""Durable state for a Raft node: ``current_term``, ``voted_for`` and the log.

Raft is only safe if these three things survive a crash. Two backends are
provided:

* :class:`MemoryStorage` - used by the simulator. A simulated "crash" throws
  away the :class:`~raftkv.node.RaftNode` object but keeps the storage object,
  which models a disk that survives a process restart.
* :class:`FileStorage` - used by real TCP nodes. Hard state is written with an
  atomic rename; log entries are appended as JSON lines. ``fsync`` is
  configurable so the benchmark can show its cost.
"""
from __future__ import annotations

import json
import os
from typing import List, Optional, Tuple

from .messages import Entry


class Storage:
    """Interface shared by all storage backends."""

    def load(self) -> Tuple[int, Optional[str], List[Entry]]:
        raise NotImplementedError

    def save_hard_state(self, term: int, voted_for: Optional[str]) -> None:
        raise NotImplementedError

    def append(self, first_index: int, entries: List[Entry]) -> None:
        """Persist ``entries`` so that ``entries[0]`` lands at ``first_index``.

        Anything already stored at or after ``first_index`` is discarded first
        (this is how conflicting suffixes are truncated).
        """
        raise NotImplementedError


class MemoryStorage(Storage):
    """Storage that lives in memory but outlives the node object that uses it."""

    def __init__(self) -> None:
        self.term = 0
        self.voted_for: Optional[str] = None
        self.entries: List[Entry] = []
        self.writes = 0

    def load(self) -> Tuple[int, Optional[str], List[Entry]]:
        return self.term, self.voted_for, list(self.entries)

    def save_hard_state(self, term: int, voted_for: Optional[str]) -> None:
        self.term, self.voted_for = term, voted_for
        self.writes += 1

    def append(self, first_index: int, entries: List[Entry]) -> None:
        del self.entries[first_index - 1:]
        self.entries.extend(entries)
        self.writes += 1


class FileStorage(Storage):
    """JSON-lines log plus a small hard-state file inside ``directory``."""

    def __init__(self, directory: str, fsync: bool = True) -> None:
        os.makedirs(directory, exist_ok=True)
        self.dir = directory
        self.fsync = fsync
        self.state_path = os.path.join(directory, "hardstate.json")
        self.log_path = os.path.join(directory, "log.jsonl")
        self._count = 0
        _, _, entries = self.load()
        self._count = len(entries)
        self._log = open(self.log_path, "a", encoding="utf-8")

    def load(self) -> Tuple[int, Optional[str], List[Entry]]:
        term, voted_for = 0, None
        if os.path.exists(self.state_path):
            with open(self.state_path, encoding="utf-8") as f:
                hs = json.load(f)
            term, voted_for = hs["term"], hs["voted_for"]
        entries: List[Entry] = []
        if os.path.exists(self.log_path):
            with open(self.log_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                    except json.JSONDecodeError:
                        break  # torn final write after a crash: ignore the tail
                    entries.append(Entry(d["t"], d["c"]))
        return term, voted_for, entries

    def save_hard_state(self, term: int, voted_for: Optional[str]) -> None:
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"term": term, "voted_for": voted_for}, f)
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        os.replace(tmp, self.state_path)

    def append(self, first_index: int, entries: List[Entry]) -> None:
        if first_index <= self._count:
            # Truncation is rare (only after a leader change), so rewrite.
            _, _, existing = self.load()
            keep = existing[: first_index - 1]
            self._log.close()
            tmp = self.log_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                for e in keep:
                    f.write(json.dumps({"t": e.term, "c": e.command}) + "\n")
            os.replace(tmp, self.log_path)
            self._log = open(self.log_path, "a", encoding="utf-8")
            self._count = len(keep)
        for e in entries:
            self._log.write(json.dumps({"t": e.term, "c": e.command}) + "\n")
        self._log.flush()
        if self.fsync and entries:
            os.fsync(self._log.fileno())
        self._count += len(entries)

    def close(self) -> None:
        self._log.close()
