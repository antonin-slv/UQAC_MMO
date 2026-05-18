# UQACMMORPGOFADAMANDANTONIN

## Description

This project explores MMORPG games, especially the network side of it (as it is in the context of a course on network
programming ).

# Game Server Deployment

### Start GateKeeper + Orchestrator + Servers 
````bash
cp .env.example .env
docker compose up -d
````
### Start client (with the port return by the GateKeeper)
````bash
cargo run --package game_client --bin game_client -- 127.0.0.1 [PORT]
````
