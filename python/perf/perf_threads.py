#!/usr/bin/env python3
"""Thread-scaling throughput check — the demonstration of free-threaded ("no-GIL")
scaling.

Spawns ``THREADS`` OS threads, each running an *independent* asyncio event loop
driving its own in-process SMPP loopback, and reports the aggregate submit_sm/s.

* On a normal (GIL) CPython, aggregate throughput stays roughly flat as THREADS
  grows — the GIL serialises the per-message Python work.
* On a **free-threaded** build, it scales with THREADS, because the Rust/tokio
  core is already parallel and there is no GIL to serialise the Python side.

Run::

    THREADS=8 COUNT=40000 SESSIONS=2 WINDOW=64 \
        SMPP34_WORKER_THREADS=8 python python/perf/perf_threads.py
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
import threading
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


async def _loopback(count: int, sessions: int, window: int) -> None:
    port = _free_port()
    server = smpp34.Server(host="127.0.0.1", port=port, system_id="PERF")
    await server.start()

    async def serve() -> None:
        while True:
            ev = await server.next()
            if isinstance(ev, smpp34.SubmitSmEvent):
                ev.accept(message_id="1")

    serve_task = asyncio.create_task(serve())
    await asyncio.sleep(0.2)

    smscs = []
    for i in range(sessions):
        client = smpp34.Client(
            host="127.0.0.1", port=port, system_id=f"p{i}", password="p",
            bind_type="TRX", window_size=window,
        )
        smscs.append((client, await client.connect()))

    counter = {"n": count}

    async def worker(smsc: smpp34.Smsc) -> None:
        while counter["n"] > 0:
            counter["n"] -= 1
            await smsc.submit_sm(destination_addr="31600000000", short_message=b"x")

    tasks = [
        asyncio.create_task(worker(smsc))
        for _, smsc in smscs
        for _ in range(window)
    ]
    await asyncio.gather(*tasks)
    for _, smsc in smscs:
        await smsc.unbind()
    serve_task.cancel()
    await server.stop()


def _thread_body(count: int, sessions: int, window: int) -> None:
    asyncio.run(_loopback(count, sessions, window))


def main() -> None:
    n_threads = _env_int("THREADS", 4)
    per = _env_int("COUNT", 40_000)
    sessions = _env_int("SESSIONS", 2)
    window = _env_int("WINDOW", 64)

    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    threads = [
        threading.Thread(target=_thread_body, args=(per, sessions, window))
        for _ in range(n_threads)
    ]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - start

    total = per * n_threads
    print(
        f"gil={'on' if gil else 'OFF'} threads={n_threads} "
        f"per-thread={per}: {total} submit_sm in {wall:.2f}s = "
        f"{total / wall:,.0f} submit_sm/s aggregate"
    )


if __name__ == "__main__":
    main()
