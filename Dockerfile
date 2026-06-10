# ======================================================================
# ÉTAPE COMMUNE 1 : Préparation & Dépendances
# ======================================================================
FROM lukemathwalker/cargo-chef:latest-rust-1.89 AS chef
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libx11-dev libasound2-dev libudev-dev libwayland-dev libxkbcommon-dev \
    && rm -rf /var/lib/apt/lists/*

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /usr/src/app/recipe.json recipe.json
# LE FAMEUX CACHE UNIQUE POUR TOUT LE MONDE
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .

# ======================================================================
# ÉTAPE 2 : Compilations Spécifiques
# (Docker n'exécutera que celle demandée par le docker-compose)
# ======================================================================
FROM builder AS build_game_server
RUN cargo build --release -p game_server

FROM builder AS build_broker
RUN cargo build --release -p broker

FROM builder AS build_orchestrator
RUN cargo build --release -p orchestrator

FROM builder AS build_gate_keeper
RUN cargo build --release -p gate_keeper

FROM builder AS build_spatial
RUN cargo build --release -p spatial_server



# ======================================================================
# ÉTAPE 3 : Bases d'exécution (Runtime)
# ======================================================================
# Base allégée pour les services Backend purs
FROM debian:bookworm-slim AS runtime_backend
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Base lourde pour les services liés à Bevy
FROM debian:bookworm-slim AS runtime_engine
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libasound2 libudev1 libwayland-client0 libxkbcommon0 libx11-6 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# ======================================================================
# ÉTAPE 4 : Les Cibles Finales (Targets)
# ======================================================================

# --- TARGET: Game Server ---
FROM runtime_backend AS final_game_server
COPY --from=build_game_server /usr/src/app/target/release/game_server /app/game_server
EXPOSE 3635/udp
CMD ["./game_server"]

# --- TARGET: Broker ---
FROM runtime_backend AS final_broker
COPY --from=build_broker /usr/src/app/target/release/broker /app/broker
EXPOSE 3632/udp 3633/udp
CMD ["./broker"]

# --- TARGET: Orchestrator ---
FROM runtime_backend AS final_orchestrator
COPY --from=build_orchestrator /usr/src/app/target/release/orchestrator /app/orchestrator
EXPOSE 3631/udp
CMD ["./orchestrator"]

# --- TARGET: Gate Keeper ---
FROM runtime_backend AS final_gate_keeper
COPY --from=build_gate_keeper /usr/src/app/target/release/gate_keeper /app/gate_keeper
EXPOSE 3630/tcp
CMD ["./gate_keeper"]

# --- TARGET: Spatial Server ---
FROM runtime_engine AS final_spatial_server
COPY --from=build_spatial /usr/src/app/target/release/spatial_server /app/spatial_server
EXPOSE 3634/udp
CMD ["./spatial_server"]

