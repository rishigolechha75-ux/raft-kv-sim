"""The Raft consensus algorithm as a deterministic, I/O-free state machine.

``RaftNode`` never touches the network, the clock or the disk directly:

* time advances only when the driver calls :meth:`RaftNode.tick`;
* incoming messages are passed to :meth:`RaftNode.step`;
* outgoing messages accumulate in ``node.outbox`` and are drained by the driver;
* committed entries are applied to the attached state machine and reported via
  :meth:`RaftNode.take_applied`;
* randomness (election timeouts) comes from an injected ``random.Random``.

Because of this, the exact same code runs inside the deterministic simulator
(``raftkv.sim``) and inside the asyncio TCP server (``raftkv.server``). A seed
fully determines a simulated run, so any failure can be replayed exactly.

The implementation follows the extended Raft paper (Ongaro & Ousterhout, 2014)
plus three well-known refinements: a no-op entry on election (needed to commit
entries from earlier terms), PreVote (section 9.6 of Ongaro's thesis) and fast
log backtracking using ``conflict_index``.

``bugs`` lets tests switch on deliberately broken behaviour so we can measure
whether the simulator's checkers actually catch real Raft bugs.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

from .messages import (
    AppendEntries,
    AppendEntriesReply,
    Entry,
    Message,
    RequestVote,
    RequestVoteReply,
)
from .storage import Storage

FOLLOWER, PRE_CANDIDATE, CANDIDATE, LEADER = "follower", "pre_candidate", "candidate", "leader"

KNOWN_BUGS = frozenset(
    {
        "commit_old_term",  # leader commits earlier-term entries by counting replicas (Figure 8)
        "no_vote_persist",  # voted_for is not written to disk before replying
        "skip_log_check",  # votes are granted without the "candidate log is up to date" check
        "unbounded_follower_commit",  # follower sets commit_index = leader_commit blindly
        "truncate_always",  # follower truncates its log on every AppendEntries
        "no_dedup",  # (state machine) client retries are applied twice
    }
)


@dataclass
class RaftConfig:
    """Timing and batching parameters, all measured in ticks."""

    election_timeout_min: int = 10
    election_timeout_max: int = 20
    heartbeat_interval: int = 3
    max_batch: int = 64
    pre_vote: bool = True


class RaftNode:
    """One member of a Raft cluster."""

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        storage: Storage,
        state_machine: Any,
        rng: Optional[random.Random] = None,
        config: Optional[RaftConfig] = None,
        bugs: FrozenSet[str] = frozenset(),
    ) -> None:
        unknown = set(bugs) - KNOWN_BUGS
        if unknown:
            raise ValueError(f"unknown bugs: {sorted(unknown)}")
        self.id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.storage = storage
        self.sm = state_machine
        self.rng = rng or random.Random()
        self.cfg = config or RaftConfig()
        self.bugs = frozenset(bugs)

        # Persistent state (restored from storage on restart).
        self.current_term, self.voted_for, self.log = storage.load()

        # Volatile state.
        self.role = FOLLOWER
        self.leader_id: Optional[str] = None
        self.commit_index = 0
        self.last_applied = 0
        self.votes: Set[str] = set()
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        self.election_elapsed = 0
        self.heartbeat_elapsed = 0
        self.election_timeout = self._random_timeout()

        self.outbox: List[Message] = []
        self._applied: List[Tuple[int, int, Dict[str, Any], Any]] = []
        self.stats = {"elections": 0, "terms_as_leader": 0}

    # ------------------------------------------------------------------ helpers
    @property
    def quorum(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    @property
    def last_index(self) -> int:
        return len(self.log)

    def term_at(self, index: int) -> int:
        """Term of the entry at ``index`` (1-based); 0 for index 0."""
        if index == 0:
            return 0
        return self.log[index - 1].term

    @property
    def last_term(self) -> int:
        return self.term_at(self.last_index)

    def _random_timeout(self) -> int:
        return self.rng.randint(self.cfg.election_timeout_min, self.cfg.election_timeout_max)

    def _persist_hard_state(self) -> None:
        voted = self.voted_for
        if "no_vote_persist" in self.bugs:
            # Simulate forgetting the vote: only persist the term.
            voted = None
        self.storage.save_hard_state(self.current_term, voted)

    def _send(self, msg: Message) -> None:
        self.outbox.append(msg)

    def take_messages(self) -> List[Message]:
        out, self.outbox = self.outbox, []
        return out

    def take_applied(self) -> List[Tuple[int, int, Dict[str, Any], Any]]:
        """Return ``(index, term, command, result)`` for newly applied entries."""
        out, self._applied = self._applied, []
        return out

    def _log_up_to_date(self, last_index: int, last_term: int) -> bool:
        """Election restriction (paper section 5.4.1)."""
        if "skip_log_check" in self.bugs:
            return True
        return (last_term, last_index) >= (self.last_term, self.last_index)

    # ------------------------------------------------------------ role changes
    def _become_follower(self, term: int, leader: Optional[str]) -> None:
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._persist_hard_state()
        self.role = FOLLOWER
        self.leader_id = leader
        self.votes = set()

    def _become_pre_candidate(self) -> None:
        self.role = PRE_CANDIDATE
        self.leader_id = None
        self.votes = {self.id}
        self.election_elapsed = 0
        self.election_timeout = self._random_timeout()
        self.heartbeat_elapsed = 0
        if len(self.votes) >= self.quorum:
            self._become_candidate()
            return
        self._request_votes(pre_vote=True)

    def _request_votes(self, pre_vote: bool) -> None:
        term = self.current_term + 1 if pre_vote else self.current_term
        for p in self.peers:
            if p in self.votes:
                continue
            self._send(
                RequestVote(
                    term=term,
                    src=self.id,
                    dst=p,
                    last_log_index=self.last_index,
                    last_log_term=self.last_term,
                    pre_vote=pre_vote,
                )
            )

    def _become_candidate(self) -> None:
        self.role = CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self._persist_hard_state()
        self.leader_id = None
        self.votes = {self.id}
        self.election_elapsed = 0
        self.election_timeout = self._random_timeout()
        self.stats["elections"] += 1
        self.heartbeat_elapsed = 0
        if len(self.votes) >= self.quorum:
            self._become_leader()
            return
        self._request_votes(pre_vote=False)

    def _become_leader(self) -> None:
        self.role = LEADER
        self.leader_id = self.id
        self.stats["terms_as_leader"] += 1
        self.next_index = {p: self.last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self.heartbeat_elapsed = 0
        # A no-op in the new term lets the leader commit entries left over from
        # earlier terms (Raft only commits current-term entries by counting).
        self._append_local([Entry(self.current_term, {"op": "noop"})])
        self._maybe_advance_commit()
        self.broadcast_append()

    # ------------------------------------------------------------------- input
    def tick(self) -> None:
        """Advance logical time by one tick."""
        if self.role == LEADER:
            self.heartbeat_elapsed += 1
            if self.heartbeat_elapsed >= self.cfg.heartbeat_interval:
                self.heartbeat_elapsed = 0
                self.broadcast_append()
        else:
            if self.role in (PRE_CANDIDATE, CANDIDATE):
                # RPCs may be lost: re-ask peers that have not voted yet.
                self.heartbeat_elapsed += 1
                if self.heartbeat_elapsed >= self.cfg.heartbeat_interval:
                    self.heartbeat_elapsed = 0
                    self._request_votes(pre_vote=self.role == PRE_CANDIDATE)
            self.election_elapsed += 1
            if self.election_elapsed >= self.election_timeout:
                if self.cfg.pre_vote:
                    self._become_pre_candidate()
                else:
                    self._become_candidate()

    def propose(self, command: Dict[str, Any]) -> Optional[Tuple[int, int]]:
        """Append a client command if leader. Returns ``(index, term)`` or None.

        Replication is not triggered here; call :meth:`broadcast_append` after
        a batch of proposals so several commands share one AppendEntries.
        """
        if self.role != LEADER:
            return None
        self._append_local([Entry(self.current_term, command)])
        self._maybe_advance_commit()  # single-node clusters commit immediately
        return self.last_index, self.current_term

    def step(self, msg: Message) -> None:
        """Process one incoming peer message."""
        is_pre_vote_req = isinstance(msg, RequestVote) and msg.pre_vote
        is_granted_pre_vote_reply = isinstance(msg, RequestVoteReply) and msg.pre_vote and msg.granted
        if msg.term > self.current_term and not is_pre_vote_req and not is_granted_pre_vote_reply:
            leader = msg.src if isinstance(msg, AppendEntries) else None
            self._become_follower(msg.term, leader)

        if isinstance(msg, RequestVote):
            self._on_request_vote(msg)
        elif isinstance(msg, RequestVoteReply):
            self._on_vote_reply(msg)
        elif isinstance(msg, AppendEntries):
            self._on_append(msg)
        elif isinstance(msg, AppendEntriesReply):
            self._on_append_reply(msg)
        else:  # pragma: no cover - defensive
            raise TypeError(f"unexpected message {msg!r}")

    # ---------------------------------------------------------------- handlers
    def _on_request_vote(self, m: RequestVote) -> None:
        if m.pre_vote:
            # Grant a pre-vote only if we would vote for this candidate in the
            # next term and we have not heard from a live leader recently.
            heard_from_leader = (
                self.role == LEADER
                or (self.leader_id is not None and self.election_elapsed < self.cfg.election_timeout_min)
            )
            granted = (
                m.term > self.current_term
                and not heard_from_leader
                and self._log_up_to_date(m.last_log_index, m.last_log_term)
            )
            self._send(
                RequestVoteReply(
                    term=m.term if granted else self.current_term,
                    src=self.id,
                    dst=m.src,
                    granted=granted,
                    pre_vote=True,
                )
            )
            return

        granted = False
        if m.term == self.current_term and self.voted_for in (None, m.src):
            if self._log_up_to_date(m.last_log_index, m.last_log_term):
                granted = True
                self.voted_for = m.src
                self._persist_hard_state()
                self.election_elapsed = 0  # granting a vote resets our timer
        self._send(RequestVoteReply(term=self.current_term, src=self.id, dst=m.src, granted=granted))

    def _on_vote_reply(self, m: RequestVoteReply) -> None:
        if m.pre_vote:
            if self.role == PRE_CANDIDATE and m.granted and m.term == self.current_term + 1:
                self.votes.add(m.src)
                if len(self.votes) >= self.quorum:
                    self._become_candidate()
            return
        if self.role == CANDIDATE and m.term == self.current_term and m.granted:
            self.votes.add(m.src)
            if len(self.votes) >= self.quorum:
                self._become_leader()

    def _on_append(self, m: AppendEntries) -> None:
        if m.term < self.current_term:
            self._send(AppendEntriesReply(term=self.current_term, src=self.id, dst=m.src, success=False))
            return
        # A valid leader exists for this term.
        if self.role != FOLLOWER:
            self._become_follower(m.term, m.src)
        self.leader_id = m.src
        self.election_elapsed = 0

        if m.prev_log_index > self.last_index:
            self._send(
                AppendEntriesReply(
                    term=self.current_term,
                    src=self.id,
                    dst=m.src,
                    success=False,
                    conflict_index=self.last_index + 1,
                )
            )
            return
        if self.term_at(m.prev_log_index) != m.prev_log_term:
            # Skip back over the whole conflicting term in one round trip.
            bad_term = self.term_at(m.prev_log_index)
            idx = m.prev_log_index
            while idx > self.commit_index + 1 and self.term_at(idx - 1) == bad_term:
                idx -= 1
            self._send(
                AppendEntriesReply(
                    term=self.current_term, src=self.id, dst=m.src, success=False, conflict_index=idx
                )
            )
            return

        # Find the first entry that is new or conflicts; only truncate there.
        # Truncating unconditionally would let a delayed, shorter AppendEntries
        # erase entries we already acknowledged.
        new_from = None
        for i, e in enumerate(m.entries):
            idx = m.prev_log_index + 1 + i
            if idx > self.last_index or self.term_at(idx) != e.term:
                new_from = i
                break
        if "truncate_always" in self.bugs:
            new_from = 0
            if not m.entries and self.last_index > m.prev_log_index:
                self._truncate_and_append(m.prev_log_index + 1, [])
        if new_from is not None and m.entries:
            self._truncate_and_append(m.prev_log_index + 1 + new_from, list(m.entries[new_from:]))

        last_new = m.prev_log_index + len(m.entries)
        if m.leader_commit > self.commit_index:
            if "unbounded_follower_commit" in self.bugs:
                self.commit_index = min(m.leader_commit, self.last_index)
            else:
                self.commit_index = min(m.leader_commit, last_new)
            self._apply_committed()
        self._send(
            AppendEntriesReply(
                term=self.current_term, src=self.id, dst=m.src, success=True, match_index=last_new
            )
        )

    def _on_append_reply(self, m: AppendEntriesReply) -> None:
        if self.role != LEADER or m.term != self.current_term:
            return
        p = m.src
        if m.success:
            if m.match_index > self.match_index[p]:
                self.match_index[p] = m.match_index
            self.next_index[p] = max(self.next_index[p], self.match_index[p] + 1)
            self._maybe_advance_commit()
            if self.next_index[p] <= self.last_index:
                self._send_append(p)
        else:
            # Back off (never below what we know the follower already has).
            hint = m.conflict_index if m.conflict_index > 0 else self.next_index[p] - 1
            self.next_index[p] = max(self.match_index[p] + 1, min(self.next_index[p] - 1, hint), 1)
            self._send_append(p)

    # ------------------------------------------------------------- replication
    def broadcast_append(self) -> None:
        """Send AppendEntries (entries or heartbeat) to every peer."""
        if self.role != LEADER:
            return
        for p in self.peers:
            self._send_append(p)

    def _send_append(self, p: str) -> None:
        nxt = self.next_index[p]
        prev = nxt - 1
        entries = self.log[prev: prev + self.cfg.max_batch]
        self._send(
            AppendEntries(
                term=self.current_term,
                src=self.id,
                dst=p,
                prev_log_index=prev,
                prev_log_term=self.term_at(prev),
                entries=list(entries),
                leader_commit=self.commit_index,
            )
        )
        # Optimistic pipelining: assume delivery; a rejection resets next_index.
        self.next_index[p] = prev + len(entries) + 1

    def _append_local(self, entries: List[Entry]) -> None:
        first = self.last_index + 1
        self.log.extend(entries)
        self.storage.append(first, entries)

    def _truncate_and_append(self, first_index: int, entries: List[Entry]) -> None:
        del self.log[first_index - 1:]
        self.log.extend(entries)
        self.storage.append(first_index, entries)
        if self.commit_index > self.last_index and "truncate_always" in self.bugs:
            self.commit_index = self.last_index

    def _maybe_advance_commit(self) -> None:
        for n in range(self.last_index, self.commit_index, -1):
            if self.term_at(n) != self.current_term and "commit_old_term" not in self.bugs:
                # Only entries from the current term are committed by counting.
                break
            replicas = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if replicas >= self.quorum:
                self.commit_index = n
                self._apply_committed()
                break

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            e = self.log[self.last_applied - 1]
            result = self.sm.apply(e.command)
            self._applied.append((self.last_applied, e.term, e.command, result))

    # ------------------------------------------------------------- inspection
    def status(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "role": self.role,
            "term": self.current_term,
            "leader": self.leader_id,
            "last_index": self.last_index,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
        }
