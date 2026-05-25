FROM rust:1.89-slim-bookworm AS builder

WORKDIR /usr/src/app

# Copie du projet
COPY . .

RUN cargo build --release -p orchestrator

FROM debian:bookworm-slim

WORKDIR /app

# On récupère l'exécutable
COPY --from=builder /usr/src/app/target/release/broker /app/broker

EXPOSE 3632/udp

CMD ["./broker"]