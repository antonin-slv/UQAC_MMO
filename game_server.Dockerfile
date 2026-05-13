FROM rust:1.89-slim-bookworm AS builder

# On crée un dossier de travail
WORKDIR /usr/src/app

# On installe les dépendances système requises par ton projet (ex: pour la crypto ou QUIC)
RUN apt-get update && apt-get install -y pkg-config libssl-dev

# On copie TOUT le workspace
COPY . .

# On compile UNIQUEMENT le serveur de jeu en mode release
RUN cargo build --release -p game_server

# --- ÉTAPE 2 : RUNTIME ---
# On utilise une image Debian ultra-légère pour faire tourner le jeu
FROM debian:bookworm-slim

WORKDIR /app

# On installe les certificats de base pour le réseau
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# On récupère l'exécutable depuis l'étape 1
COPY --from=builder /usr/src/app/target/release/game_server /app/game_server

# On expose le port UDP de ton jeu (ex: 5000)
EXPOSE 5000/udp

# On lance le serveur
CMD ["./game_server"]