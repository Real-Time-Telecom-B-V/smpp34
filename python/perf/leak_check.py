#!/usr/bin/env python3
"""Python memory-leak check — the Python mirror of ``examples/leak_check.rs``.

Two phases, each measuring resident-set growth against a budget:

* **bind/unbind churn** — guards the per-session teardown path (the Rust
  enquire-link ticker leak that ``leak_check.rs`` was built to catch would show
  up here too, as every leaked bind crosses into Python).
* **sustained submit_sm** — guards the per-message bridge path (a leaked PyObject
  or oneshot per message would accumulate).

RSS is read from ``/proc/self/statm`` (current, not peak). Budgets are generous
because Python's allocator and glibc arenas cache freely; the point is to catch
*gross* leaks (per-bind tasks, per-message objects), not byte-level drift.

Exit code is non-zero if any phase exceeds budget. Tunable via env:
``CHURN_CYCLES``, ``SUBMIT_COUNT``, ``CHURN_BUDGET_MB``, ``SUBMIT_BUDGET_MB``.
"""

from __future__ import annotations

import asyncio
import gc
import os
import socket
import sys

import smpp34

PAGE = os.sysconf("SC_PAGE_SIZE")


def rss_bytes() -> int:
    with open("/proc/self/statm", encoding="ascii") as f:
        resident_pages = int(f.read().split()[1])
    return resident_pages * PAGE


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


async def _serve(server: smpp34.Server) -> None:
    while True:
        ev = await server.next()
        if isinstance(ev, smpp34.SubmitSmEvent):
            ev.accept(message_id="1")


async def _bind_unbind(port: int) -> None:
    client = smpp34.Client(
        host="127.0.0.1", port=port, system_id="u", password="p",
        bind_type="TRX", window_size=8,
    )
    smsc = await client.connect()
    await smsc.unbind()


def _report(phase: str, base: int, after: int, budget_mb: float) -> bool:
    growth = after - base
    ok = growth <= budget_mb * 1024 * 1024
    print(
        f"  {phase}: RSS {after / 1e6:.1f} MB "
        f"(Δ {growth / 1e6:+.2f} MB, budget {budget_mb:.0f} MB) "
        f"{'PASS' if ok else 'FAIL'}"
    )
    return ok


async def main() -> int:
    churn_warmup = _env_int("CHURN_WARMUP", 25)
    churn_cycles = _env_int("CHURN_CYCLES", 300)
    submit_count = _env_int("SUBMIT_COUNT", 100_000)
    churn_budget = float(os.environ.get("CHURN_BUDGET_MB", 12))
    submit_budget = float(os.environ.get("SUBMIT_BUDGET_MB", 16))

    port = _free_port()
    server = smpp34.Server(host="127.0.0.1", port=port, system_id="LEAK")
    await server.start()
    serve_task = asyncio.create_task(_serve(server))
    await asyncio.sleep(0.2)

    ok = True

    # ── Phase 1: bind/unbind churn ──────────────────────────────────────────
    for _ in range(churn_warmup):
        await _bind_unbind(port)
    gc.collect()
    base = rss_bytes()
    for _ in range(churn_cycles):
        await _bind_unbind(port)
    gc.collect()
    ok &= _report(f"bind/unbind x{churn_cycles}", base, rss_bytes(), churn_budget)

    # ── Phase 2: sustained submit_sm on one session ─────────────────────────
    client = smpp34.Client(
        host="127.0.0.1", port=port, system_id="load", password="p",
        bind_type="TRX", window_size=32,
    )
    smsc = await client.connect()

    async def fire(n: int) -> None:
        for _ in range(n):
            await smsc.submit_sm(
                destination_addr="31600000000", short_message=b"leak test"
            )

    # warm the allocator, then measure
    await fire(submit_count // 10)
    gc.collect()
    base = rss_bytes()
    # window=32 concurrent firers
    per = submit_count // 32
    await asyncio.gather(*[asyncio.create_task(fire(per)) for _ in range(32)])
    gc.collect()
    ok &= _report(f"submit_sm x{per * 32}", base, rss_bytes(), submit_budget)

    await smsc.unbind()
    serve_task.cancel()
    await server.stop()

    print("LEAK CHECK:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
