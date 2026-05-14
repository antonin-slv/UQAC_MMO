FROM rust:1.89-slim-bookworm AS builder

WORKDIR /usr/src/app

COPY . .

RUN cargo build --release -p game_server

FROM debian:bookworm-slim

WORKDIR /app

# On récupère l'exécutable depuis l'étape 1
COPY --from=builder /usr/src/app/target/release/game_server /app/game_server

# Expose le port 5000 du conainter. à ccustomiser plus tard ?
EXPOSE 5000/udp

CMD ["./game_server"]