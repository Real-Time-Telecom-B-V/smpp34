#!/usr/bin/env python3
"""Minimal SMPP client (ESME) showing **both directions**: it binds, submits a
message (MO), then receives inbound deliver_sm (MT — an MO from the SMSC or a
delivery receipt) via ``smsc.next()``. The counterpart of ``examples/client.rs``.

Start a server first (``python/examples/server.py``), then run this.
"""

import asyncio

import smpp34


async def main() -> None:
    client = smpp34.Client(
        host="127.0.0.1",
        port=2775,
        system_id="esme1",
        password="secret",
        bind_type="TRX",
    )
    smsc = await client.connect()
    print("bound to", smsc.system_id, "(session", smsc.session_id + ")")

    resp = await smsc.submit_sm(
        destination_addr="31600000000",
        source_addr="12345",
        short_message=b"hello from python",
    )
    print("submit_sm_resp:", resp.message_id, "success=", resp.is_success)

    # Receive inbound MT: an MO delivered by the SMSC, or the delivery receipt for
    # the message we just submitted.
    ev = await smsc.next()
    if isinstance(ev, smpp34.DeliverSmEvent):
        print(f"MT deliver_sm {ev.source_addr} -> {ev.destination_addr}: {ev.short_message!r}")
    elif isinstance(ev, smpp34.Disconnected):
        print("session closed")

    await smsc.unbind()


if __name__ == "__main__":
    asyncio.run(main())
