FROM rust:1.89-slim-bookworm AS builder

WORKDIR /usr/src/app

# Copie du projet
COPY . .

RUN cargo build --release -p orchestrator

FROM debian:bookworm-slim

# Installation des certificats CA (essentiel pour certaines communications Docker/API)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/app/target/release/orchestrator /app/orchestrator

EXPOSE 3631/udp

CMD ["./orchestrator"]