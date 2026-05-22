FROM rust:1.89-slim-bookworm AS builder

WORKDIR /usr/src/app

# Copie du projet
COPY . .

RUN cargo build --release -p spatial_server

FROM debian:bookworm-slim

WORKDIR /app

# On récupère l'exécutable
COPY --from=builder /usr/src/app/target/release/spatial_server /app/spatial_server

EXPOSE 3632/udp

CMD ["./spatial_server"]