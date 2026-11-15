# syntax=docker/dockerfile:1
# The agnt5-runtime image. Build from the repository root:
#   docker build -f server/Dockerfile .

FROM rust:1-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends cmake clang \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
# Copy the binary out in this RUN: cache mounts are absent from later layers.
RUN --mount=type=cache,id=agnt5-runtime-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=agnt5-runtime-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=agnt5-runtime-target,target=/build/target,sharing=locked \
    cargo build --locked --release -p agnt5-runtime \
    && cp /build/target/release/agnt5-runtime /usr/local/bin/agnt5-runtime \
    && cd /usr/local/bin && sha256sum agnt5-runtime > agnt5-runtime.sha256

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /var/lib/agnt5 --create-home agnt5
COPY --from=builder /usr/local/bin/agnt5-runtime /usr/local/bin/agnt5-runtime
# Test suites copy the binary out of the image; the checksum lets them
# confirm they got the exact file this image was built with.
COPY --from=builder /usr/local/bin/agnt5-runtime.sha256 /usr/local/share/agnt5/agnt5-runtime.sha256

USER agnt5
WORKDIR /var/lib/agnt5
# The binary listens on 127.0.0.1 by default. To reach it from outside the
# container, set AGNT5_HTTP_ADDR and AGNT5_GRPC_ADDR to 0.0.0.0 addresses.
EXPOSE 34185 34186
ENTRYPOINT ["agnt5-runtime"]
