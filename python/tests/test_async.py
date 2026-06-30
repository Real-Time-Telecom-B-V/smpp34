"""End-to-end async tests: a Python SMPP client against a Python SMPP server.

Both ends are the Rust/tokio core, so this exercises the full bridge — bind,
submit_sm, deliver_sm, accept/reject, and graceful unbind — across the GIL.

Tests are plain sync functions driving an event loop with ``asyncio.run`` so the
suite needs no ``pytest-asyncio`` dependency.
"""

from __future__ import annotations

import asyncio
import socket

import smpp34


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Harness:
    """A running Python SMSC plus the bookkeeping a test needs."""

    def __init__(self, system_id: str = "TESTSMSC") -> None:
        self.port = _free_port()
        self.server = smpp34.Server(host="127.0.0.1", port=self.port, system_id=system_id)
        self.esmes: dict[str, smpp34.Esme] = {}
        self.received: list[bytes] = []
        self._task: asyncio.Task | None = None
        # behaviour knobs
        self.reject_with: int | None = None

    async def start(self) -> None:
        await self.server.start()
        self._task = asyncio.create_task(self._serve())
        await asyncio.sleep(0.15)

    async def _serve(self) -> None:
        while True:
            ev = await self.server.next()
            if isinstance(ev, smpp34.Esme):
                self.esmes[ev.session_id] = ev
            elif isinstance(ev, smpp34.SubmitSmEvent):
                if self.reject_with is not None:
                    ev.reject(self.reject_with)
                else:
                    self.received.append(ev.short_message)
                    ev.accept(message_id="msg-%d" % len(self.received))
            elif isinstance(ev, smpp34.Unbound):
                self.esmes.pop(ev.session_id, None)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        await self.server.stop()

    def client(self, **kw) -> smpp34.Client:
        kw.setdefault("system_id", "esme")
        kw.setdefault("password", "pw")
        kw.setdefault("bind_type", "TRX")
        return smpp34.Client(host="127.0.0.1", port=self.port, **kw)


def test_bind_submit_deliver_unbind() -> None:
    async def scenario() -> None:
        h = Harness()
        await h.start()
        try:
            smsc = await h.client().connect()
            assert smsc.session_id  # a UUID was assigned
            assert smsc.system_id == "esme"  # the ESME's own bound system_id

            resp = await smsc.submit_sm(
                destination_addr="31600000000",
                source_addr="12345",
                short_message=b"hello",
            )
            assert resp.is_success
            assert resp.message_id == "msg-1"
            assert h.received == [b"hello"]

            # server -> client deliver_sm, pulled via smsc.next()
            esme = next(iter(h.esmes.values()))
            dlr = await esme.deliver_sm(
                destination_addr="12345",
                source_addr="31600000000",
                short_message=b"delivery receipt",
            )
            assert dlr.is_success
            ev = await asyncio.wait_for(smsc.next(), timeout=5)
            assert isinstance(ev, smpp34.DeliverSmEvent)
            assert ev.short_message == b"delivery receipt"
            assert ev.source_addr == "31600000000"

            # Graceful unbind: the SMSC closes the socket, so the client's next()
            # resolves to the Disconnected sentinel (rather than hanging).
            await smsc.unbind()
            ev = await asyncio.wait_for(smsc.next(), timeout=5)
            assert isinstance(ev, smpp34.Disconnected)
        finally:
            await h.stop()

    asyncio.run(scenario())


def test_multiple_messages_in_order() -> None:
    async def scenario() -> None:
        h = Harness()
        await h.start()
        try:
            smsc = await h.client().connect()
            ids = []
            for i in range(20):
                r = await smsc.submit_sm(
                    destination_addr="31600000000", short_message=b"m%d" % i
                )
                ids.append(r.message_id)
            assert ids == ["msg-%d" % i for i in range(1, 21)]
            assert h.received == [b"m%d" % i for i in range(20)]
            await smsc.unbind()
        finally:
            await h.stop()

    asyncio.run(scenario())


def test_server_reject_returns_unsuccessful_resp() -> None:
    # A server-side reject is delivered as a submit_sm_resp with a non-zero
    # command_status, NOT an exception (exceptions are for transport failures).
    async def scenario() -> None:
        h = Harness()
        h.reject_with = smpp34.ESME_RINVDSTADR
        await h.start()
        try:
            smsc = await h.client().connect()
            resp = await smsc.submit_sm(destination_addr="bad", short_message=b"x")
            assert not resp.is_success
            assert resp.command_status == smpp34.ESME_RINVDSTADR
            assert resp.message_id is None
            await smsc.unbind()
        finally:
            await h.stop()

    asyncio.run(scenario())


def test_concurrent_inflight_submits() -> None:
    async def scenario() -> None:
        h = Harness()
        await h.start()
        try:
            smsc = await h.client().connect()
            # fire many submits concurrently (windowed) — the Rust core handles
            # sequence correlation; all must resolve successfully.
            results = await asyncio.gather(
                *[
                    smsc.submit_sm(destination_addr="31600000000", short_message=b"c%d" % i)
                    for i in range(50)
                ]
            )
            assert all(r.is_success for r in results)
            assert len(h.received) == 50
            await smsc.unbind()
        finally:
            await h.stop()

    asyncio.run(scenario())
