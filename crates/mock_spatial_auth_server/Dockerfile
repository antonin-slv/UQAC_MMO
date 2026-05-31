FROM rust:1.89-slim-bookworm AS builder

WORKDIR /usr/src/app

# Copie du projet
COPY . .

RUN cargo build --release -p mock_spatial_auth_server

RUN ls -la /usr/src/app/target/release/


FROM debian:bookworm-slim

# Installation des certificats CA (essentiel pour certaines communications Docker/API)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app


COPY --from=builder /usr/src/app/target/release/mock_spatial_auth_server /app/mock_spatial_auth_server

EXPOSE 3634/udp

CMD ["./mock_spatial_auth_server"]