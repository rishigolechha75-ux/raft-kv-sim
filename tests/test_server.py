"""Integration tests: real asyncio TCP servers in one event loop."""
import asyncio
import socket

import pytest

from raftkv.client import ClusterUnavailable, RaftClient
from raftkv.server import RaftServer, parse_cluster


def free_ports(n):
    socks, ports = [], []
    for _ in range(n):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        socks.append(s)
        ports.append(s.getsockname()[1])
    for s in socks:
        s.close()
    return ports


def make_cluster(tmp_path, n=3):
    ports = free_ports(n)
    cluster = {f"n{i}": ("127.0.0.1", ports[i]) for i in range(n)}
    return cluster, lambda nid: RaftServer(nid, cluster, str(tmp_path / nid), fsync=False, tick_ms=5)


async def wait_leader(client, timeout=5.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        leader = await client.find_leader()
        if leader:
            return leader
        await asyncio.sleep(0.05)
    raise AssertionError("no leader elected")


def test_parse_cluster():
    assert parse_cluster("a=h:1, b=h:2") == {"a": ("h", 1), "b": ("h", 2)}
    with pytest.raises(ValueError):
        parse_cluster("")


def test_replicated_reads_writes_and_failover(tmp_path):
    async def go():
        cluster, factory = make_cluster(tmp_path)
        servers = {nid: factory(nid) for nid in cluster}
        for s in servers.values():
            await s.start()
        client = RaftClient(cluster, attempt_timeout=0.5)
        try:
            await wait_leader(client)
            assert await client.put("k", "v1") == "ok"
            assert await client.get("k") == "v1"
            assert await client.cas("k", "v1", "v2") is True
            assert await client.cas("k", "v1", "v3") is False
            # Every replica applies the same commands.
            await asyncio.sleep(0.1)
            assert all(s.node.sm.data.get("k") == "v2" for s in servers.values())

            # Kill the leader; the survivors elect a new one and keep the data.
            old = await client.find_leader()
            await servers.pop(old).stop()
            assert await client.get("k") == "v2"
            assert await client.put("k2", "after-failover") == "ok"
            new = await client.find_leader()
            assert new is not None and new != old

            # Restart the old leader from disk: it catches up as a follower.
            servers[old] = factory(old)
            await servers[old].start()
            assert servers[old].node.last_index >= 1  # log reloaded from disk
            for _ in range(100):
                if servers[old].node.sm.data.get("k2") == "after-failover":
                    break
                await asyncio.sleep(0.05)
            assert servers[old].node.sm.data.get("k2") == "after-failover"
        finally:
            await client.close()
            for s in servers.values():
                await s.stop()

    asyncio.run(go())


def test_no_quorum_means_unavailable(tmp_path):
    async def go():
        cluster, factory = make_cluster(tmp_path)
        s0 = factory("n0")  # only one of three nodes is up
        await s0.start()
        client = RaftClient(cluster, attempt_timeout=0.2)
        try:
            with pytest.raises(ClusterUnavailable):
                await client.execute("put", "k", deadline=1.0, value=1)
        finally:
            await client.close()
            await s0.stop()

    asyncio.run(go())
