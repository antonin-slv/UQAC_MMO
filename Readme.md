# UQACMMORPGOFADAMANDANTONIN

## Description

This project explores MMORPG games, especially the network side of it (as it is in the context of a course on network programming ).

# Game Server Deployement

````bash
docker build -t mon_game_server:local -f game_server.Dockerfile .

#pour tester en local : 
docker run -d --name serveur_test -p 5000:5000/udp -e DS_PORT=5000 -e SERVER_LISTEN_URL=0.0.0.0 -e SERV_FREQUENCY=60 -e ORCHESTRATOR_URL=127.0.0.1:8080 mon_game_server:local
````