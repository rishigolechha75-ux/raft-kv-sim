"""Integration tests for the deterministic simulator."""
import pytest

from raftkv.cli import main
from raftkv.node import RaftConfig
from raftkv.sim import PROFILES, run_simulation


def test_same_seed_same_run():
    a = run_simulation(7, profile="chaos", trace=True)
    b = run_simulation(7, profile="chaos", trace=True)
    assert a.summary() == b.summary()
    assert a.trace == b.trace and a.latencies == b.latencies


def test_different_seeds_differ():
    assert run_simulation(1).summary() != run_simulation(2).summary()


@pytest.mark.parametrize("profile", sorted(PROFILES))
def test_correct_raft_is_safe_and_linearizable(profile):
    for seed in range(8):
        r = run_simulation(seed, profile=profile)
        assert r.safety_ok, r.violations
        assert r.linearizable is True
        assert r.ops_completed > 0


def test_clean_network_completes_every_operation():
    r = run_simulation(3, profile="clean")
    assert r.ops_completed == r.ops_invoked
    assert r.leaders_elected == 1


def test_three_node_cluster_and_single_node():
    assert run_simulation(0, n_nodes=3, profile="chaos").ok
    r = run_simulation(0, n_nodes=1, profile="clean")
    assert r.ok and r.ops_completed == r.ops_invoked


@pytest.mark.parametrize("bug", ["skip_log_check", "truncate_always", "no_dedup"])
def test_injected_bugs_are_caught(bug):
    found = any(not run_simulation(s, profile="chaos", bugs=frozenset({bug})).ok for s in range(40))
    assert found, f"{bug} was never detected"


def test_faults_are_actually_injected():
    r = run_simulation(5, profile="chaos")
    assert r.crashes + r.partitions + r.pauses > 0
    assert r.messages_dropped > 0


def test_prevote_off_still_safe():
    for seed in range(5):
        assert run_simulation(seed, profile="elections", raft=RaftConfig(pre_vote=False)).ok


def test_cli_sim_command(capsys):
    assert main(["sim", "--seeds", "3", "--profile", "lossy", "--nodes", "3"]) == 0
    assert "0 failing" in capsys.readouterr().out
    assert main(["sim", "--seed", "1", "--bug", "truncate_always", "--profile", "partitions"]) in (0, 1)
