# Where smpp34 sits

A qualitative map of the SMPP landscape. **Only the smpp34 numbers were measured**
(see the README "Performance" section); the other projects are characterised from
their language/runtime and public documentation, not benchmarked head-to-head on
the same box — treat cross-stack throughput as positioning, not a leaderboard.

| Project | Language | Memory safety | Concurrency model | Footprint | Shape |
|---|---|---|---|---|---|
| **smpp34** | Rust | compile-time (no GC, no GC pauses) | async / `tokio` | **~6–7 MB RSS** | library (codec + client + server) |
| Kannel | C | manual (use-after-free / overflow class of bugs) | processes + threads | moderate | full SMS gateway (routing, store-fwd) |
| jSMPP | Java | GC | thread-per-session / NIO | JVM, 100s of MB | library |
| python-smpplib | Python | GC, GIL-bound | synchronous | small | library (single-threaded) |
| go-smpp | Go | GC | goroutines | small–moderate | library |

## What smpp34 brings

- **Memory-safe at compile time, no garbage collector** — no use-after-free
  (Kannel's class of risk) and no GC pause jitter (the JVM/Go tail-latency story).
- **Async from the ground up** on `tokio`, with sequence-number windowing — one
  connection pipelines thousands of in-flight PDUs.
- **Measured**: ~7M decode / ~13.6M encode PDUs/sec per core; **600k+ submit_sm/s**
  end-to-end; flat memory under load and across bind/unbind churn (counting-allocator
  leak check, not just RSS). Numbers are reproducible — `cargo bench`,
  `perf/docker-compose.yml`, `scripts/mem_leak_test.sh`.
- **Tiny footprint** — single-digit MB RSS while pushing those rates.
- **A library, not a monolith** — compose it into exactly the network function you
  need (SMSC, ESME, IP-SM-GW, SMPP↔SS7 gateway), MIT-licensed.

## What it deliberately is not

- Not a turnkey gateway. Kannel ships routing, persistence, and admin out of the
  box; smpp34 hands you the protocol and stays out of your architecture.
- Not SMPP 5.0 (see [COMPLIANCE.md](COMPLIANCE.md)).
