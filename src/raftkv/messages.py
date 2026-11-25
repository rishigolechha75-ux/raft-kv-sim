"""Wire-level message types exchanged between Raft peers.

Every message carries the sender's ``term`` so the receiver can apply the
"higher term wins" rule before doing anything else. Messages are plain
dataclasses so the same objects can travel through the in-process simulator
and, after ``to_dict``/``from_dict``, over a real TCP connection as JSON.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class Entry:
    """A single replicated log entry: the term it was created in plus a command."""

    term: int
    command: Dict[str, Any]


@dataclass
class RequestVote:
    term: int
    src: str
    dst: str
    last_log_index: int
    last_log_term: int
    pre_vote: bool = False


@dataclass
class RequestVoteReply:
    term: int
    src: str
    dst: str
    granted: bool
    pre_vote: bool = False


@dataclass
class AppendEntries:
    term: int
    src: str
    dst: str
    prev_log_index: int
    prev_log_term: int
    entries: List[Entry] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesReply:
    term: int
    src: str
    dst: str
    success: bool
    # On success: highest index known to match the leader's log.
    match_index: int = 0
    # On failure: hint for where the leader should retry (fast log backtracking).
    conflict_index: int = 0


Message = Any  # Union of the four peer message types above.

_TYPES = {
    "RequestVote": RequestVote,
    "RequestVoteReply": RequestVoteReply,
    "AppendEntries": AppendEntries,
    "AppendEntriesReply": AppendEntriesReply,
}


def to_dict(msg: Message) -> Dict[str, Any]:
    """Serialise a peer message to a JSON-friendly dict."""
    d = asdict(msg)
    d["type"] = type(msg).__name__
    return d


def from_dict(d: Dict[str, Any]) -> Message:
    """Inverse of :func:`to_dict`."""
    d = dict(d)
    cls = _TYPES[d.pop("type")]
    if cls is AppendEntries:
        d["entries"] = [Entry(e["term"], e["command"]) for e in d.get("entries", [])]
    return cls(**d)


def message_kind(msg: Message) -> Optional[str]:
    """Short name of a message type, used for metrics."""
    return type(msg).__name__
