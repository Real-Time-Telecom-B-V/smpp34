#!/usr/bin/env python3
"""In-process Python throughput check — the Python mirror of
``examples/perf_loopback.rs``.

A Python SMSC and one or more Python ESMEs in a single asyncio loop fire
``submit_sm`` as fast as the bridge allows, then report submit_sm/s. The entire
SMPP machinery runs in the Rust/tokio core; this measures the *Python-visible*
throughput, i.e. the cost of crossing the GIL once per message.

Run::

    COUNT=200000 SESSIONS=4 WINDOW=64 python python/perf/perf_loopback.py
"""

from __future__ import annotations

import asyncio
import os
import socket
import time

import smpp34


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


async def run(count: int, sessions: int, window: int) -> None:
    port = _free_port()
    server = smpp34.Server(host="127.0.0.1", port=port, system_id="PERF")
    await server.start()

    received = [0]

    async def serve() -> None:
        while True:
            ev = await server.next()
            if isinstance(ev, smpp34.SubmitSmEvent):
                received[0] += 1
                ev.accept(message_id="1")

    serve_task = asyncio.create_task(serve())
    await asyncio.sleep(0.2)

    smscs = []
    for i in range(sessions):
        client = smpp34.Client(
            host="127.0.0.1",
            port=port,
            system_id=f"perf{i}",
            password="perf",
            bind_type="TRX",
            window_size=window,
        )
        smscs.append((client, await client.connect()))

    counter = {"n": count}

    async def worker(smsc: smpp34.Smsc) -> None:
        while counter["n"] > 0:
            counter["n"] -= 1
            await smsc.submit_sm(
                destination_addr="31600000000",
                source_addr="12345",
                short_message=b"perf test message",
            )

    start = time.perf_counter()
    tasks = [
        asyncio.create_task(worker(smsc))
        for _, smsc in smscs
        for _ in range(window)
    ]
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start

    print(
        f"perf-loopback: {count} submit_sm in {elapsed:.2f}s = "
        f"{count / elapsed:,.0f} submit_sm/s ({sessions}x{window}), "
        f"server received {received[0]}"
    )

    for _, smsc in smscs:
        await smsc.unbind()
    serve_task.cancel()
    await server.stop()


def main() -> None:
    count = _env_int("COUNT", 200_000)
    sessions = _env_int("SESSIONS", 4)
    window = _env_int("WINDOW", 64)
    asyncio.run(run(count, sessions, window))


if __name__ == "__main__":
    main()
