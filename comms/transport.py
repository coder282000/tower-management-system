"""TCP transport (newline-delimited JSON) with a seeded link emulator.

The Link adds delay, jitter and loss to every emulated send, so network
conditions are reproducible on any OS. Control-plane calls pass emulate=False.
"""
import asyncio
import socket
import random

from .message import Message

LINE_LIMIT = 16 * 1024 * 1024        # one message = one line; bulk name registration (Milestone 10) needs more than asyncio's 64 KB default


class Link:
    def __init__(self, delay_ms=0.0, jitter_ms=0.0, loss=0.0, seed=1):
        self.delay = delay_ms / 1000.0
        self.jitter = jitter_ms / 1000.0
        self.loss = loss
        self.rng = random.Random(seed)
        self.sent = 0
        self.dropped = 0

    def transit(self):
        """None if the message is lost, otherwise the delay in seconds."""
        self.sent += 1
        if self.rng.random() < self.loss:
            self.dropped += 1
            return None
        return max(0.0, self.delay + self.rng.uniform(-self.jitter, self.jitter))


class Transport:
    def __init__(self, host, port, handler, link):
        self.host, self.port, self.handler, self.link = host, port, handler, link
        self.server = None

    async def start(self):
        _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _sock.bind((self.host, self.port))
        _sock.listen(socket.SOMAXCONN)
        _sock.setblocking(False)
        self.server = await asyncio.start_server(self._conn, sock=_sock, limit=LINE_LIMIT)

    async def _conn(self, reader, writer):
        try:
            line = await reader.readline()
            if line:
                reply = await self.handler(Message.decode(line))
                if reply is not None:
                    writer.write(reply.encode())
                    await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    async def send(self, host, port, msg, wait_reply=False, emulate=True, timeout=2.0):
        """Returns True (sent), the reply Message (wait_reply), or None (dropped/failed)."""
        if emulate:
            delay = self.link.transit()
            if delay is None:
                return None
            await asyncio.sleep(delay)
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port, limit=LINE_LIMIT), timeout)
            writer.write(msg.encode())
            await writer.drain()
            reply = None
            if wait_reply:
                line = await asyncio.wait_for(reader.readline(), timeout)
                reply = Message.decode(line) if line else None
            writer.close()
            return reply if wait_reply else True
        except (OSError, asyncio.TimeoutError):
            return None
