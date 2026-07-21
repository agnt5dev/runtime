FROM lukemathwalker/cargo-chef:latest-rust-1-bookworm AS chef
WORKDIR /src

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked -p agnt5-runtime

FROM debian:bookworm-slim AS runtime
RUN useradd --system --uid 10001 --create-home agnt5
COPY --from=builder /src/target/release/agnt5-runtime /usr/local/bin/agnt5-runtime
USER agnt5
ENTRYPOINT ["agnt5-runtime"]
