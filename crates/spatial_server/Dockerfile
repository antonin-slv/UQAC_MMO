FROM rust:1.89-slim-bookworm AS builder

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libx11-dev \
    libasound2-dev \
    libudev-dev \
    libwayland-dev \
    libxkbcommon-dev \
    && rm -rf /var/lib/apt/lists/*

# Copie du projet
COPY . .

RUN cargo build --release -p spatial_server

FROM debian:bookworm-slim

WORKDIR /app

# On récupère l'exécutable
COPY --from=builder /usr/src/app/target/release/spatial_server /app/spatial_server

EXPOSE 3634/udp

CMD ["./spatial_server"]