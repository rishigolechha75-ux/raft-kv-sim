"""Async client for a running raftkv cluster.

The client hides leader discovery and retries: it sends each command to the
node it believes is the leader, follows ``not_leader`` redirects, and on
timeouts or connection errors retries the *same* ``(client, seq)`` on another
node. The server-side deduplication table guarantees that a retried command
takes effect at most once.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import uuid
from typing import Any, Dict, Optional, Tuple

from .server import Address


class ClusterUnavailable(Exception):
    """Raised when no node accepted the command before the deadline."""


class RaftClient:
    def __init__(self, cluster: Dict[str, Address], client_id: Optional[str] = None, attempt_timeout: float = 1.0):
        self.cluster = cluster
        self.ids = list(cluster)
        self.client_id = client_id or uuid.uuid4().hex[:12]
        self.attempt_timeout = attempt_timeout
        self.seq = 0
        self._leader: Optional[str] = self.ids[0]
        self._conns: Dict[str, Tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._rid = itertools.count(1)
        self._rr = itertools.cycle(self.ids)

    async def _conn(self, nid: str) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        c = self._conns.get(nid)
        if c is None or c[1].is_closing():
            host, port = self.cluster[nid]
            c = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=self.attempt_timeout)
            self._conns[nid] = c
        return c

    async def _call(self, nid: str, payload: Dict[str, Any], timeout: float) -> Dict[str, Any]:
        reader, writer = await self._conn(nid)
        rid = next(self._rid)
        payload = {**payload, "rid": rid}
        writer.write((json.dumps(payload) + "\n").encode())
        await writer.drain()
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if not line:
                raise ConnectionError("closed")
            reply = json.loads(line)
            if reply.get("rid") == rid:
                return reply

    def _drop(self, nid: str) -> None:
        c = self._conns.pop(nid, None)
        if c is not None:
            c[1].close()

    async def execute(self, op: str, key: str, deadline: float = 10.0, **args: Any) -> Any:
        """Run one command with retries until it succeeds or ``deadline`` passes."""
        self.seq += 1
        cmd = {"op": op, "key": key, "client": self.client_id, "seq": self.seq, **args}
        loop = asyncio.get_running_loop()
        end = loop.time() + deadline
        target = self._leader or next(self._rr)
        while loop.time() < end:
            try:
                reply = await self._call(target, {"kind": "client", "cmd": cmd}, timeout=self.attempt_timeout * 4)
            except (OSError, asyncio.TimeoutError, ConnectionError, json.JSONDecodeError):
                self._drop(target)
                self._leader = None
                target = next(self._rr)
                await asyncio.sleep(0.02)
                continue
            if reply.get("ok"):
                self._leader = target
                return reply.get("result")
            hint = reply.get("leader")
            if reply.get("error") == "not_leader" and hint in self.cluster and hint != target:
                target = hint
            else:
                target = next(self._rr)
                await asyncio.sleep(0.02)
        raise ClusterUnavailable(f"{op} {key!r} did not complete within {deadline}s")

    async def put(self, key: str, value: Any) -> Any:
        return await self.execute("put", key, value=value)

    async def get(self, key: str) -> Any:
        return await self.execute("get", key)

    async def delete(self, key: str) -> Any:
        return await self.execute("delete", key)

    async def cas(self, key: str, expected: Any, value: Any) -> Any:
        return await self.execute("cas", key, expected=expected, value=value)

    async def status(self, nid: str) -> Dict[str, Any]:
        reply = await self._call(nid, {"kind": "status"}, timeout=self.attempt_timeout)
        return reply["status"]

    async def find_leader(self) -> Optional[str]:
        for nid in self.ids:
            try:
                st = await self.status(nid)
            except (OSError, asyncio.TimeoutError, ConnectionError):
                self._drop(nid)
                continue
            if st["role"] == "leader":
                return nid
        return None

    async def close(self) -> None:
        for nid in list(self._conns):
            self._drop(nid)
