"""Tests for the state machine, storage backends, messages and the checkers."""
import json

import pytest

from raftkv.checker.invariants import SafetyMonitor
from raftkv.checker.linearizability import INF, Op, check_history
from raftkv.kv import KVStateMachine
from raftkv.messages import AppendEntries, Entry, RequestVote, from_dict, to_dict
from raftkv.storage import FileStorage, MemoryStorage


# ------------------------------------------------------------------ KV store
def cmd(op, key="k", seq=1, client="c", **kw):
    return {"op": op, "key": key, "client": client, "seq": seq, **kw}


def test_kv_operations():
    kv = KVStateMachine()
    assert kv.apply(cmd("put", value=1, seq=1)) == "ok"
    assert kv.apply(cmd("get", seq=2)) == 1
    assert kv.apply(cmd("cas", expected=2, value=3, seq=3)) is False
    assert kv.apply(cmd("cas", expected=1, value=3, seq=4)) is True
    assert kv.apply(cmd("delete", seq=5)) is True
    assert kv.apply(cmd("delete", seq=6)) is False
    assert kv.apply(cmd("get", seq=7)) is None
    assert kv.apply({"op": "noop"}) is None


def test_kv_dedup_returns_cached_result():
    kv = KVStateMachine()
    kv.apply(cmd("put", value=0, seq=1, client="other"))
    first = kv.apply(cmd("cas", expected=0, value=1, seq=1))
    retry = kv.apply(cmd("cas", expected=0, value=1, seq=1))
    assert first is True and retry is True and kv.applied_ops == 2


def test_kv_without_dedup_applies_twice():
    kv = KVStateMachine(dedup=False)
    kv.apply(cmd("put", value=0, seq=1, client="other"))
    assert kv.apply(cmd("cas", expected=0, value=1, seq=1)) is True
    assert kv.apply(cmd("cas", expected=0, value=1, seq=1)) is False


def test_kv_unknown_op():
    with pytest.raises(ValueError):
        KVStateMachine().apply(cmd("explode"))


# ------------------------------------------------------------------ storage
def test_memory_storage_truncates():
    st = MemoryStorage()
    st.append(1, [Entry(1, {"a": 1}), Entry(1, {"a": 2}), Entry(1, {"a": 3})])
    st.append(2, [Entry(2, {"b": 1})])
    assert [e.term for e in st.load()[2]] == [1, 2]


def test_file_storage_roundtrip_and_truncation(tmp_path):
    st = FileStorage(str(tmp_path), fsync=False)
    st.save_hard_state(3, "n1")
    st.append(1, [Entry(1, {"a": 1}), Entry(2, {"a": 2})])
    st.append(3, [Entry(3, {"a": 3})])
    st.append(2, [Entry(4, {"z": 9})])
    st.close()
    term, voted, entries = FileStorage(str(tmp_path)).load()
    assert (term, voted) == (3, "n1")
    assert [(e.term, e.command) for e in entries] == [(1, {"a": 1}), (4, {"z": 9})]


def test_file_storage_ignores_torn_tail(tmp_path):
    st = FileStorage(str(tmp_path), fsync=False)
    st.append(1, [Entry(1, {"a": 1})])
    st.close()
    with open(tmp_path / "log.jsonl", "a") as f:
        f.write('{"t": 1, "c": {"a"')  # crash mid-write
    assert len(FileStorage(str(tmp_path)).load()[2]) == 1


# ----------------------------------------------------------------- messages
def test_message_roundtrip_through_json():
    m = AppendEntries(term=2, src="a", dst="b", prev_log_index=1, prev_log_term=1,
                      entries=[Entry(2, {"op": "put", "key": "k", "value": 1})], leader_commit=1)
    back = from_dict(json.loads(json.dumps(to_dict(m))))
    assert back == m
    v = RequestVote(term=1, src="a", dst="b", last_log_index=0, last_log_term=0, pre_vote=True)
    assert from_dict(to_dict(v)) == v


# --------------------------------------------------------- linearizability
def op(i, client, kind, inv, ret, out=None, key="x", **args):
    return Op(id=i, client=client, key=key, op=kind, args=args, output=out, invoke=inv, ret=ret)


def test_sequential_history_is_linearizable():
    h = [op(0, "a", "put", 0, 1, "ok", value=1), op(1, "b", "get", 2, 3, 1)]
    assert check_history(h).ok is True


def test_stale_read_is_detected():
    h = [op(0, "a", "put", 0, 1, "ok", value=1), op(1, "a", "put", 2, 3, "ok", value=2),
         op(2, "b", "get", 4, 5, 1)]
    r = check_history(h)
    assert r.ok is False and r.failing_key == "x"


def test_concurrent_ops_may_linearize_either_way():
    # get overlaps the put, so seeing either the old or new value is fine.
    for seen in (None, 1):
        h = [op(0, "a", "put", 0, 10, "ok", value=1), op(1, "b", "get", 2, 5, seen)]
        assert check_history(h).ok is True


def test_cas_outputs_are_checked():
    good = [op(0, "a", "put", 0, 1, "ok", value=1), op(1, "b", "cas", 2, 3, True, expected=1, value=2),
            op(2, "c", "get", 4, 5, 2)]
    bad = [op(0, "a", "put", 0, 1, "ok", value=1), op(1, "b", "cas", 2, 3, True, expected=1, value=2),
           op(2, "c", "cas", 4, 5, True, expected=1, value=3)]
    assert check_history(good).ok is True
    assert check_history(bad).ok is False


def test_indeterminate_write_may_or_may_not_happen():
    h = [op(0, "a", "put", 0, INF, None, value=1), op(1, "b", "get", 5, 6, 1)]
    assert check_history(h).ok is True
    h2 = [op(0, "a", "put", 0, INF, None, value=1), op(1, "b", "get", 5, 6, None)]
    assert check_history(h2).ok is True


def test_keys_are_checked_independently():
    h = [op(0, "a", "put", 0, 1, "ok", key="x", value=1), op(1, "b", "get", 2, 3, None, key="y")]
    assert check_history(h).ok is True


def test_budget_exhaustion_returns_unknown():
    h = [op(i, f"c{i}", "put", 0, 100, "ok", value=i) for i in range(12)] + [op(99, "z", "get", 101, 102, -1)]
    assert check_history(h, budget_per_key=50).ok is None


def test_empty_history():
    assert check_history([]).ok is True


# ---------------------------------------------------------------- invariants
class FakeNode:
    def __init__(self, role, term, log=(), commit=0):
        self.role, self.current_term, self.log, self.commit_index = role, term, list(log), commit

    @property
    def last_index(self):
        return len(self.log)

    def term_at(self, i):
        return self.log[i - 1].term if i else 0


def test_monitor_flags_two_leaders_in_one_term():
    m = SafetyMonitor()
    m.observe({"a": FakeNode("leader", 3)}, 1)
    m.observe({"b": FakeNode("leader", 3)}, 2)
    assert not m.ok and m.first().kind == "election_safety"


def test_monitor_flags_divergent_apply():
    m = SafetyMonitor()
    m.on_applied("a", 1, 1, {"op": "put", "value": 1}, 0)
    m.on_applied("b", 1, 1, {"op": "put", "value": 1}, 0)
    assert m.ok
    m.on_applied("c", 1, 2, {"op": "put", "value": 2}, 0)
    assert m.first().kind == "state_machine_safety"


def test_monitor_flags_incomplete_leader_and_log_mismatch():
    m = SafetyMonitor()
    m.on_applied("a", 1, 1, {"x": 1}, 0)
    m.observe({"b": FakeNode("leader", 2, log=[])}, 1)
    assert m.first().kind == "leader_completeness"
    m2 = SafetyMonitor()
    a = FakeNode("follower", 1, [Entry(1, {"x": 1})], commit=1)
    b = FakeNode("follower", 1, [Entry(1, {"x": 2})], commit=1)
    m2.check_log_matching({"a": a, "b": b}, 0)
    assert m2.first().kind == "log_matching"
