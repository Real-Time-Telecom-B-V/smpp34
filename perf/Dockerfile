# Builds the perf SMSC + ESME example binaries for the docker-compose harness.
FROM rust:1-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --example perf_smsc --example perf_esme

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates bash \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/examples/perf_smsc /usr/local/bin/perf_smsc
COPY --from=build /src/target/release/examples/perf_esme /usr/local/bin/perf_esme
