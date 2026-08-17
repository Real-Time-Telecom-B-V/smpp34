"""Session establishment: honest errors, and a server that is up when it says so.

`Client.connect()` to a port nothing listens on used to panic a tokio worker
inside the Rust session task and then report "bind timed out" once the whole
connect timeout had elapsed — a message that named the bind rather than the
refused connect, and took ten seconds to say nothing useful.

`Server.start()` used to return before its listening socket was bound, so
connecting straight afterwards was a race. It lost under free-threaded CPython
in CI, which is how this was found.
"""

from __future__ import annotations

import asyncio
import socket
import time

import pytest

import smpp34


def closed_port() -> int:
    """A port nothing is listening on, so a connect to it is refused."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_a_refused_connect_reports_the_refusal_not_a_bind_timeout() -> None:
    async def go() -> None:
        port = closed_port()
        client = smpp34.Client("127.0.0.1", port, "u", "p", connect_timeout_ms=10_000)

        started = time.monotonic()
        with pytest.raises(smpp34.SmppError) as caught:
            await client.connect()
        elapsed = time.monotonic() - started

        message = str(caught.value).lower()
        assert "refused" in message, f"should carry the OS refusal, got: {caught.value}"
        assert "connect" in message, f"should blame the connect, got: {caught.value}"
        assert str(port) in str(caught.value), f"should name the port, got: {caught.value}"
        assert "bind timed out" not in message
        # The refusal is known immediately; it must not cost the connect timeout.
        assert elapsed < 2.0, f"took {elapsed:.1f}s, should be immediate"

    asyncio.run(go())


def test_server_start_is_accepting_by_the_time_it_returns() -> None:
    """No sleep between start() and connect(): this is the race that failed CI."""

    async def go() -> None:
        port = closed_port()
        server = smpp34.Server("127.0.0.1", port, system_id="READY-SMSC")
        await server.start()

        # Deliberately immediate — the whole point is that no settling time is
        # needed. This is what the 3.14t CI failure looked like.
        client = smpp34.Client("127.0.0.1", port, "u", "p", bind_type="TRX")
        smsc = await client.connect()
        assert smsc is not None

        esme = await asyncio.wait_for(server.next(), timeout=5)
        assert isinstance(esme, smpp34.Esme)

        await smsc.unbind()
        await server.stop()

    asyncio.run(go())


def test_a_bind_conflict_raises_instead_of_pretending_to_be_up() -> None:
    async def go() -> None:
        held = socket.socket()
        held.bind(("127.0.0.1", 0))
        held.listen(1)
        port = held.getsockname()[1]
        try:
            server = smpp34.Server("127.0.0.1", port)
            with pytest.raises(smpp34.SmppError) as caught:
                await server.start()
            assert str(port) in str(caught.value)
            assert "bind" in str(caught.value).lower()
        finally:
            held.close()

    asyncio.run(go())
