FROM rust:1.94-slim-bookworm AS builder

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY specs ./specs
COPY igc-net-rs ./igc-net-rs

WORKDIR /workspace/igc-net-rs
RUN cargo build --release -p igc-net-grpc

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/igc-net-rs/target/release/igc-net-grpc /usr/local/bin/igc-net-grpc

VOLUME ["/data"]
EXPOSE 50051

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z 127.0.0.1 50051 || exit 1

ENTRYPOINT ["/usr/local/bin/igc-net-grpc"]
CMD ["--data-dir", "/data", "--grpc-addr", "0.0.0.0:50051"]
