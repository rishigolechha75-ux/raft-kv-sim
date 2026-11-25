"""Run a :class:`~raftkv.node.RaftNode` as a real process over TCP (asyncio).

Wire format: newline-delimited JSON. Every line is one of

* ``{"kind": "peer", "msg": {...}}`` - a Raft message from another node;
* ``{"kind": "client", "rid": 1, "cmd": {...}}`` - a client command;
* ``{"kind": "status", "rid": 1}`` - ask for the node's current status.

The server owns the clock (a tick every ``tick_ms`` milliseconds), the disk
(:class:`~raftkv.storage.FileStorage`) and the sockets. All Raft logic stays
in ``RaftNode``. After any input, a single "flush" is scheduled with
``loop.call_soon``; it sends one AppendEntries per peer covering every command
proposed since the last flush, which batches concurrent client requests.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Any, Dict, Optional, Tuple

from .kv import KVStateMachine
from .messages import from_dict, to_dict
from .node import LEADER, RaftConfig, RaftNode
from .storage import FileStorage

log = logging.getLogger("raftkv.server")

Address = Tuple[str, int]


def parse_cluster(spec: str) -> Dict[str, Address]:
    """Parse ``"n0=127.0.0.1:7000,n1=127.0.0.1:7001"`` into a dict."""
    out: Dict[str, Address] = {}
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        nid, addr = part.split("=")
        host, port = addr.rsplit(":", 1)
        out[nid] = (host, int(port))
    if not out:
        raise ValueError("empty cluster spec")
    return out


class RaftServer:
    """asyncio host for one Raft node."""

    def __init__(
        self,
        node_id: str,
        cluster: Dict[str, Address],
        data_dir: str,
        fsync: bool = True,
        tick_ms: float = 10.0,
        config: Optional[RaftConfig] = None,
        request_timeout: float = 3.0,
    ) -> None:
        self.id = node_id
        self.cluster = cluster
        self.tick_ms = tick_ms
        self.request_timeout = request_timeout
        self.storage = FileStorage(data_dir, fsync=fsync)
        self.node = RaftNode(
            node_id,
            list(cluster),
            self.storage,
            KVStateMachine(),
            rng=random.Random(),
            config=config
            or RaftConfig(election_timeout_min=15, election_timeout_max=30, heartbeat_interval=5, max_batch=256),
        )
        self.pending: Dict[int, Tuple[int, asyncio.Future]] = {}
        self.peer_writers: Dict[str, Optional[asyncio.StreamWriter]] = {p: None for p in self.node.peers}
        self._connecting: set = set()
        self._flush_scheduled = False
        self._dirty = False
        self._server: Optional[asyncio.base_events.Server] = None
        self._tasks: list = []
        self._conns: set = set()

    # -------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        host, port = self.cluster[self.id]
        self._server = await asyncio.start_server(self._handle_conn, host, port)
        self._tasks.append(asyncio.ensure_future(self._tick_loop()))
        log.info("node %s listening on %s:%d", self.id, host, port)

    async def serve_forever(self) -> None:
        await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self._server is not None:
            self._server.close()
            # Since Python 3.12, wait_closed() also waits for accepted
            # connections, so close them explicitly first.
            for w in list(self._conns):
                w.close()
            await self._server.wait_closed()
        for w in self.peer_writers.values():
            if w is not None:
                w.close()
        self._fail_pending("shutdown")
        self.storage.close()

    async def _tick_loop(self) -> None:
        while True:
            await asyncio.sleep(self.tick_ms / 1000.0)
            self.node.tick()
            self._flush()

    # ------------------------------------------------------------- networking
    async def _handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._conns.add(writer)
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    req = json.loads(line)
                except json.JSONDecodeError:
                    continue
                kind = req.get("kind")
                if kind == "peer":
                    self.node.step(from_dict(req["msg"]))
                    self._schedule_flush()
                elif kind == "client":
                    asyncio.ensure_future(self._client_request(req, writer))
                elif kind == "status":
                    self._reply(writer, {"rid": req.get("rid"), "ok": True, "status": self.node.status()})
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self._conns.discard(writer)
            writer.close()

    @staticmethod
    def _reply(writer: asyncio.StreamWriter, obj: Dict[str, Any]) -> None:
        if not writer.is_closing():
            writer.write((json.dumps(obj) + "\n").encode())

    async def _connect_peer(self, peer: str) -> None:
        host, port = self.cluster[peer]
        try:
            _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=1.0)
            self.peer_writers[peer] = writer
        except (OSError, asyncio.TimeoutError):
            await asyncio.sleep(0.2)  # back off before the next attempt
        finally:
            self._connecting.discard(peer)

    def _send_peer(self, peer: str, msg: Any) -> None:
        w = self.peer_writers.get(peer)
        if w is None or w.is_closing():
            self.peer_writers[peer] = None
            if peer not in self._connecting:
                self._connecting.add(peer)
                asyncio.ensure_future(self._connect_peer(peer))
            return  # dropped: Raft retries on the next heartbeat
        try:
            w.write((json.dumps({"kind": "peer", "msg": to_dict(msg)}) + "\n").encode())
        except (ConnectionError, RuntimeError):
            self.peer_writers[peer] = None

    # ----------------------------------------------------------------- client
    async def _client_request(self, req: Dict[str, Any], writer: asyncio.StreamWriter) -> None:
        rid = req.get("rid")
        if self.node.role != LEADER:
            leader = self.node.leader_id
            addr = None
            if leader in self.cluster:
                addr = "%s:%d" % self.cluster[leader]
            self._reply(writer, {"rid": rid, "ok": False, "error": "not_leader", "leader": leader, "addr": addr})
            return
        res = self.node.propose(req["cmd"])
        assert res is not None
        index, term = res
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self.pending[index] = (term, fut)
        self._dirty = True
        self._schedule_flush()
        try:
            result = await asyncio.wait_for(fut, timeout=self.request_timeout)
            self._reply(writer, {"rid": rid, "ok": True, "result": result})
        except asyncio.TimeoutError:
            self.pending.pop(index, None)
            self._reply(writer, {"rid": rid, "ok": False, "error": "timeout"})
        except _LeadershipLost:
            self._reply(writer, {"rid": rid, "ok": False, "error": "leadership_lost"})

    def _schedule_flush(self) -> None:
        if not self._flush_scheduled:
            self._flush_scheduled = True
            asyncio.get_running_loop().call_soon(self._flush)

    def _fail_pending(self, why: str) -> None:
        for _, fut in self.pending.values():
            if not fut.done():
                fut.set_exception(_LeadershipLost(why))
        self.pending.clear()

    def _flush(self) -> None:
        self._flush_scheduled = False
        if self._dirty:
            self._dirty = False
            self.node.broadcast_append()
        for index, term, _cmd, result in self.node.take_applied():
            p = self.pending.pop(index, None)
            if p is not None and not p[1].done():
                if p[0] == term:
                    p[1].set_result(result)
                else:
                    p[1].set_exception(_LeadershipLost("entry overwritten"))
        if self.node.role != LEADER and self.pending:
            self._fail_pending("not leader")
        for msg in self.node.take_messages():
            self._send_peer(msg.dst, msg)


class _LeadershipLost(Exception):
    pass


def run_server(node_id: str, cluster: Dict[str, Address], data_dir: str, fsync: bool, tick_ms: float) -> None:
    """Blocking entry point used by the CLI."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    server = RaftServer(node_id, cluster, data_dir, fsync=fsync, tick_ms=tick_ms)
    try:
        asyncio.run(server.serve_forever())
    except KeyboardInterrupt:
        pass
