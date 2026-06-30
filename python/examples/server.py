#!/usr/bin/env python3
"""Minimal SMPP server (SMSC) showing **both directions**: it acks every inbound
submit_sm (MO) and pushes a deliver_sm delivery receipt (MT) back to the ESME
that sent it. The Python counterpart of ``examples/server.rs``.

Run it, then run ``python/examples/client.py`` in another shell.
"""

import asyncio

import smpp34


async def main() -> None:
    server = smpp34.Server(host="127.0.0.1", port=2775, system_id="ExampleSMSC")
    await server.start()
    print("SMSC listening on 127.0.0.1:2775 — Ctrl-C to stop")

    sessions: dict[str, smpp34.Esme] = {}
    while True:
        ev = await server.next()
        if isinstance(ev, smpp34.Esme):
            # A client bound: stash the handle so we can push MT to it.
            sessions[ev.session_id] = ev
            print("bound:", ev.system_id)
        elif isinstance(ev, smpp34.SubmitSmEvent):
            print(f"MO submit_sm {ev.source_addr} -> {ev.destination_addr}: {ev.short_message!r}")
            ev.accept(message_id="msg-1")  # or ev.reject(smpp34.ESME_RMSGQFUL)

            # Push a delivery receipt (MT deliver_sm) back to that ESME.
            esme = sessions.get(ev.session_id)
            if esme is not None:
                await esme.deliver_sm(
                    source_addr=ev.destination_addr,
                    destination_addr=ev.source_addr,
                    esm_class=0x04,  # delivery receipt
                    short_message=b"id:msg-1 stat:DELIVRD",
                )
        elif isinstance(ev, smpp34.Unbound):
            sessions.pop(ev.session_id, None)
            print("unbound:", ev.session_id)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
