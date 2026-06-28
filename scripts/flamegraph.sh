#!/usr/bin/env bash
#
# smpp34 CPU flamegraph.
#
# Profiles the in-process loopback (`perf_loopback`) end-to-end and writes
# `flamegraph.svg`. The loopback runs both the SMSC and ESME hot paths, so the
# graph shows where submit_sm time goes (decode/encode, per-PDU task spawn, the
# `to_vec` copies, tokio scheduling) — the map for the next optimisation pass.
#
# Requires: `perf` + `cargo install flamegraph`, and perf sampling access:
#   sudo sysctl kernel.perf_event_paranoid=1     # (or run this script with sudo)
#
# Usage: COUNT=2000000 SESSIONS=4 WINDOW=64 ./scripts/flamegraph.sh

set -euo pipefail
cd "$(dirname "$0")/.."

paranoid="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)"
if [ "$paranoid" -gt 1 ]; then
    echo "WARN: kernel.perf_event_paranoid=$paranoid — perf sampling needs <= 1."
    echo "      run:  sudo sysctl kernel.perf_event_paranoid=1   (then re-run)"
fi

export COUNT="${COUNT:-500000}"
export SESSIONS="${SESSIONS:-4}"
export WINDOW="${WINDOW:-64}"

# Force frame pointers so perf can unwind the optimized stacks (otherwise every
# frame is [unknown]). This rebuilds deps once. -F 499 + a shorter run keep perf
# from dropping chunks at these PDU rates.
export RUSTFLAGS="-C force-frame-pointers=yes ${RUSTFLAGS:-}"

echo "[*] flamegraph: $COUNT submit_sm, $SESSIONS sessions x window $WINDOW"
cargo flamegraph --freq 499 --profile profiling --example perf_loopback --output flamegraph.svg
echo "[+] wrote flamegraph.svg"
