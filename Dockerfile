FROM rust:1.72.1-slim-bookworm as builder

WORKDIR /usr/src/telcoin-network

RUN apt-get update \
    && apt-get install -y build-essential cmake libclang-15-dev

COPY Cargo* ./
COPY bin/ ./bin/
COPY crates/ ./crates/

# must move the resulting binary out of the cache for subsequent build steps
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=./target \
    cargo build --release \
    && mv ./target/release/telcoin-network /tmp/


FROM debian:bookworm-slim

COPY --from=builder /tmp/telcoin-network /usr/local/bin/telcoin-network

CMD ["telcoin-network", "node", "--dev", "--http", "--http.api", "eth,net,web3", "--http.addr", "0.0.0.0"]
