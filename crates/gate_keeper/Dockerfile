FROM rust:1.89-slim-bookworm AS builder

WORKDIR /usr/src/app

# Copie du projet
COPY . .

RUN cargo build --release -p gate_keeper

FROM debian:bookworm-slim

WORKDIR /app

# On récupère l'exécutable
COPY --from=builder /usr/src/app/target/release/gate_keeper /app/gate_keeper

EXPOSE 3630/tcp

CMD ["./gate_keeper"]