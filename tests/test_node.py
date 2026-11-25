"""Unit tests for the Raft state machine, driven by hand-delivered messages."""
import random

import pytest

from raftkv.kv import KVStateMachine
from raftkv.messages import AppendEntries, AppendEntriesReply, Entry, RequestVote, RequestVoteReply
from raftkv.node import CANDIDATE, FOLLOWER, LEADER, RaftConfig, RaftNode
from raftkv.storage import MemoryStorage

IDS = ["a", "b", "c"]


def make(nid, ids=IDS, pre_vote=False, bugs=frozenset(), storage=None, seed=0):
    return RaftNode(
        nid, ids, storage or MemoryStorage(), KVStateMachine(), rng=random.Random(seed),
        config=RaftConfig(pre_vote=pre_vote), bugs=bugs,
    )


def pump(nodes, drop=lambda m: False, rounds=50):
    """Deliver messages between in-memory nodes until the network is quiet."""
    for _ in range(rounds):
        msgs = [m for n in nodes.values() for m in n.take_messages()]
        if not msgs:
            return
        for m in msgs:
            if not drop(m):
                nodes[m.dst].step(m)


def elect(nodes, nid):
    n = nodes[nid]
    while n.role != LEADER:
        n.tick()
        pump(nodes)
    return n


def test_single_node_elects_itself_and_commits():
    n = make("a", ids=["a"])
    for _ in range(25):
        n.tick()
    assert n.role == LEADER
    idx, _ = n.propose({"op": "put", "key": "k", "value": 1, "client": "x", "seq": 1})
    assert n.commit_index == idx
    applied = n.take_applied()
    assert applied[-1][3] == "ok"


def test_three_node_election_and_replication():
    nodes = {i: make(i, seed=k) for k, i in enumerate(IDS)}
    leader = elect(nodes, "a")
    assert sum(1 for n in nodes.values() if n.role == LEADER) == 1
    leader.propose({"op": "put", "key": "k", "value": "v", "client": "x", "seq": 1})
    leader.broadcast_append()
    pump(nodes)
    leader.broadcast_append()  # heartbeat carries the new commit index
    pump(nodes)
    for n in nodes.values():
        assert n.sm.data == {"k": "v"}
        assert n.commit_index == leader.last_index


def test_reject_vote_for_stale_term():
    n = make("a")
    n.current_term = 5
    n.step(RequestVote(term=3, src="b", dst="a", last_log_index=0, last_log_term=0))
    reply = n.take_messages()[0]
    assert isinstance(reply, RequestVoteReply) and not reply.granted and reply.term == 5


def test_one_vote_per_term():
    n = make("a")
    n.step(RequestVote(term=1, src="b", dst="a", last_log_index=0, last_log_term=0))
    n.step(RequestVote(term=1, src="c", dst="a", last_log_index=0, last_log_term=0))
    r1, r2 = n.take_messages()
    assert r1.granted and not r2.granted
    assert n.storage.voted_for == "b"  # persisted before replying


def test_vote_requires_up_to_date_log():
    n = make("a")
    n.log = [Entry(1, {"op": "noop"}), Entry(2, {"op": "noop"})]
    n.current_term = 2
    # Candidate has a longer log but an older last term: must be refused.
    n.step(RequestVote(term=3, src="b", dst="a", last_log_index=5, last_log_term=1))
    assert not n.take_messages()[0].granted
    n.step(RequestVote(term=4, src="c", dst="a", last_log_index=2, last_log_term=2))
    assert n.take_messages()[0].granted


def test_skip_log_check_bug_grants_bad_vote():
    n = make("a", bugs=frozenset({"skip_log_check"}))
    n.log = [Entry(2, {"op": "noop"})]
    n.current_term = 2
    n.step(RequestVote(term=3, src="b", dst="a", last_log_index=0, last_log_term=0))
    assert n.take_messages()[0].granted


def test_append_rejects_missing_prev_and_hints_conflict():
    n = make("b")
    n.step(AppendEntries(term=1, src="a", dst="b", prev_log_index=3, prev_log_term=1, entries=[]))
    r = n.take_messages()[0]
    assert not r.success and r.conflict_index == 1


def test_append_truncates_conflicting_suffix_only():
    n = make("b")
    n.current_term = 3
    n.log = [Entry(1, {"op": "noop"}), Entry(2, {"x": 1}), Entry(2, {"x": 2})]
    n.step(AppendEntries(term=3, src="a", dst="b", prev_log_index=1, prev_log_term=1,
                         entries=[Entry(3, {"y": 1})], leader_commit=0))
    assert [e.term for e in n.log] == [1, 3]
    assert n.take_messages()[0].success


def test_delayed_short_append_does_not_erase_entries():
    n = make("b")
    full = AppendEntries(term=1, src="a", dst="b", prev_log_index=0, prev_log_term=0,
                         entries=[Entry(1, {"i": 1}), Entry(1, {"i": 2})])
    stale = AppendEntries(term=1, src="a", dst="b", prev_log_index=0, prev_log_term=0,
                          entries=[Entry(1, {"i": 1})])
    n.step(full)
    n.step(stale)  # arrives late, reordered by the network
    assert n.last_index == 2


def test_truncate_always_bug_erases_entries():
    n = make("b", bugs=frozenset({"truncate_always"}))
    n.step(AppendEntries(term=1, src="a", dst="b", prev_log_index=0, prev_log_term=0,
                         entries=[Entry(1, {"i": 1}), Entry(1, {"i": 2})]))
    n.step(AppendEntries(term=1, src="a", dst="b", prev_log_index=0, prev_log_term=0,
                         entries=[Entry(1, {"i": 1})]))
    assert n.last_index == 1


def test_leader_does_not_commit_old_term_entries_by_counting():
    """Figure 8 of the Raft paper: count replicas only for current-term entries."""
    for bugs, expect_commit in ((frozenset(), 0), (frozenset({"commit_old_term"}), 1)):
        n = make("a", bugs=bugs)
        n.log = [Entry(1, {"op": "put", "key": "k", "value": 1})]
        n.storage.entries = list(n.log)
        n.current_term = 2
        n.role = CANDIDATE
        n.votes = {"a", "b"}
        n._become_leader()  # appends a term-2 no-op at index 2
        n.take_messages()
        # Follower b acknowledges only index 1 (the old-term entry).
        n.step(AppendEntriesReply(term=2, src="b", dst="a", success=True, match_index=1))
        assert n.commit_index == expect_commit
        n.step(AppendEntriesReply(term=2, src="b", dst="a", success=True, match_index=2))
        assert n.commit_index == 2  # the no-op commits both


def test_higher_term_makes_leader_step_down():
    nodes = {i: make(i, seed=k) for k, i in enumerate(IDS)}
    leader = elect(nodes, "a")
    leader.step(AppendEntriesReply(term=leader.current_term + 1, src="b", dst="a", success=False))
    assert leader.role == FOLLOWER


def test_pre_vote_does_not_increase_terms():
    nodes = {i: make(i, pre_vote=True, seed=k) for k, i in enumerate(IDS)}
    leader = elect(nodes, "a")
    term = leader.current_term
    # Partition c away so it keeps timing out; drop everything it sends.
    for _ in range(100):
        nodes["c"].tick()
        pump(nodes, drop=lambda m: m.src == "c" or m.dst == "c")
    assert nodes["c"].current_term == term  # never bumped its term
    # When it rejoins it is refused pre-votes while the leader is alive.
    for _ in range(10):
        leader.tick()
        nodes["b"].tick()
        nodes["c"].tick()
        pump(nodes)
    assert leader.role == LEADER and leader.current_term == term


def test_restart_recovers_persistent_state():
    st = MemoryStorage()
    n = make("a", ids=["a"], storage=st)
    for _ in range(25):
        n.tick()
    n.propose({"op": "put", "key": "k", "value": 9, "client": "x", "seq": 1})
    term, last = n.current_term, n.last_index
    n2 = make("a", ids=["a"], storage=st, seed=1)
    assert n2.current_term == term and n2.last_index == last and n2.role == FOLLOWER
    assert n2.commit_index == 0  # volatile, re-learned after restart


def test_unknown_bug_rejected():
    with pytest.raises(ValueError):
        make("a", bugs=frozenset({"nope"}))


def test_propose_on_follower_returns_none():
    assert make("a").propose({"op": "noop"}) is None
