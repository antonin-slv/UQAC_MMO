# UQACMMORPGOFADAMANDANTONIN

## Description

This project explores MMORPG games, especially the network side of it (as it is in the context of a course on network programming ).

# Game Server Deployement

````bash
docker build -t mon_game_server:local -f Dockerfile .

#pour tester en local : 
docker run -d --name serveur_test -p 5000:5000/udp -e HEARTBEAT_INTERVAL=3  -e ORCHESTRATOR_URL=127.0.0.1:8080  -e SERV_FREQUENCY=60  -e SERVER_UUID=550e8400-e29b-41d4-a716-446655440000 -e SERVER_EXT_IP=127.0.0.1 -e SERVER_EXT_PORT=5000 mon_game_server:local
````
