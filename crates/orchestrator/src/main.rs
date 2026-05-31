extern crate redis;

mod docker_manager;

use crate::docker_manager::DockerManager;
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use dotenv::dotenv;
use shared_replication::broker_topics::{BrokerMessageHeaders, Namespace, SecurityDomain, TopicBuilder};
use shared_replication::redis_manager::{GameServer, RedisManager};
use shared_replication::servers::ServerType;
use shared_replication::Heartbeat;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use uuid::Uuid;
use shared_replication::broker_client::{ClientNetworkEvent, MmoNetworkClient};
use tokio::sync::oneshot; // <-- Import du canal de synchronisation

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    println!("Starting Orchestrator");
    println!("Starting docker");

    let docker = Arc::new(DockerManager::new().await?);
    println!("Starting redis");
    let redis = Arc::new(RedisManager::new().await?);

    /*
    let orchestrator_port: u16 = env::var("ORCH_PORT")
        .expect("Env ORCH_PORT is not set")
        .parse()
        .expect("Env ORCH_PORT is not a number");
    */
    let hot_servers_min: u16 = env::var("HOT_SERVERS_MIN")
        .expect("Env HOT_SERVERS_MIN is not set")
        .parse()
        .expect("Env HOT_SERVERS_MIN is not an integer");

    println!("Start tasks");

    let redis_heartbeat = Arc::clone(&redis);
    let temp_docker = Arc::clone(&docker);

    // Création du canal de synchronisation
    let (broker_ready_tx, broker_ready_rx) = oneshot::channel();

    // --- TASK 1: Écoute Réseau (Heartbeats & Broker) ---
    let heartbeat_handle = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(100));

        let mut client = MmoNetworkClient::new();
        client.connect(temp_docker.broker_ip.as_str(), temp_docker.broker_private_port).expect("TODO: panic message");

        println!(
            "Orchestrator connecting to Broker at {}:{}",
            temp_docker.broker_ip, temp_docker.broker_private_port
        );

        // On encapsule le sender dans un Option pour pouvoir le consommer (take) une seule fois
        let mut ready_tx_opt = Some(broker_ready_tx);

        loop {
            ticker.tick().await;

            // On passe le canal à listen_broker pour qu'il le déclenche au bon moment
            if let Err(e) = listen_broker(&mut client, &redis_heartbeat, &mut ready_tx_opt).await {
                println!("Orchestrator network error: {}", e);
            }
        }
    });

    // --- TASK 2: Scaling Automatique (Docker) ---
    let redis_scaler = Arc::clone(&redis);
    let docker_scaler = Arc::clone(&docker);

    let scaler_handle = tokio::spawn(async move {
        println!("[Orchestrator] En attente de la connexion au Broker avant d'autoriser le scaling...");

        // La tâche se bloque ici tant que le signal n'est pas reçu !
        let _ = broker_ready_rx.await;

        println!("[Orchestrator] Broker est Ready ! Démarrage de l'auto-scaler.");

        // Le scaler peut continuer à tourner doucement (1 seconde)
        let mut ticker = interval(Duration::from_secs(1));

        loop {
            ticker.tick().await;

            let available_server = redis.get_available_servers().await;

            if let Ok(mut available_server) = available_server {
                while available_server.len() < hot_servers_min as usize {
                    println!("Spawn new server");
                    match spawn_server(&docker_scaler, &redis_scaler).await {
                        Ok(spawned_server) => available_server.push(spawned_server),
                        Err(e) => {
                            eprintln!("{e}")
                        }
                    }
                }

                available_server.retain(|s| s.players_online == 0);

                while available_server.len() > hot_servers_min as usize {
                    if let Some(server) = available_server.first() {
                        docker
                            .terminate_container(&server.id)
                            .await
                            .expect("Couldn't terminate container");
                        redis
                            .remove_server(&server.id)
                            .await
                            .expect("Couldn't remove server");
                        available_server.remove(0);
                    }
                }
            }
        }
    });

    tokio::try_join!(heartbeat_handle, scaler_handle)?;

    Ok(())
}

async fn spawn_server(docker: &DockerManager, redis: &RedisManager) -> Result<GameServer> {
    let new_server_id = Uuid::new_v4();
    println!("Spawning new server with id {}", new_server_id);
    let port = docker.spawn_container(new_server_id.to_string()).await?;

    let server = redis.create_server(new_server_id.to_string(), port).await?;

    Ok(server)
}

// Nouvelle fonction de routage qui accepte le canal de synchronisation
async fn listen_broker(
    client: &mut MmoNetworkClient,
    redis: &RedisManager,
    ready_tx: &mut Option<oneshot::Sender<()>>
) -> Result<()> {
    while let Some(event) = client.poll() {
        match event {
            ClientNetworkEvent::Ready => {
                // 1. L'Orchestrateur s'abonne au canal "Director" pour recevoir les Heartbeats
                let heartbeat_topic = TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::Heartbeat).build();
                client.subscribe(heartbeat_topic, 0);
                println!("[Orchestrator] Abonné au topic Heartbeats.");

                // 2. Envoi du Handshake / Hello
                let hello_packet_header = BrokerMessageHeaders::FriendHello as u8;
                let friend_type = ServerType::Orchestrator as u8;
                let mut data = BytesMut::with_capacity(2);
                data.put_u8(hello_packet_header);
                data.put_u8(friend_type);

                // L'Orchestrateur annonce son existence sur le canal d'authentification
                let auth_topic = TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ServerConnection).build();
                client.publish_reliable(auth_topic, data.freeze());
                println!("[Orchestrator] Handshake 'Hello' envoyé.");

                client.subscribe(auth_topic, 0); //he listens the hello of the servers, I guess.

                // ---> MAGIE : On débloque la tâche de scaling ! <---
                if let Some(tx) = ready_tx.take() {
                    let _ = tx.send(()); // Le canal est consommé, le signal est envoyé
                }
            }
            ClientNetworkEvent::Connected => {
                println!("[Orchestrator] Connected (waiting to be ready)");
            }
            ClientNetworkEvent::Disconnected => {
                println!("[Orchestrator] Déconnecté du Broker !");
            }
            ClientNetworkEvent::DataReceived { stream: _, payload } => {
                let discard_message = BrokerMessageHeaders::DiscardedMessageBecauseYouKnow as u8;
                let header_byte = payload.first().unwrap_or(&discard_message);
                let header = BrokerMessageHeaders::from(*header_byte);
                match header {
                    BrokerMessageHeaders::Heartbeat => {
                        let heartbeat_len = payload
                            .get(1..3)
                            .unwrap_or_default()
                            .try_into()
                            .unwrap_or([0, 0]);
                        let heartbeat_len = u16::from_le_bytes(heartbeat_len) as usize;

                        let heartbeat_payload = payload.get(3..(3 + heartbeat_len)).unwrap_or_default();

                        match serde_json::from_slice::<Heartbeat>(heartbeat_payload) {
                            Ok(heartbeat) => on_heartbeat_received(redis, heartbeat).await?,
                            Err(e) => {
                                eprintln!("Invalid heartbeat JSON : {}", e)
                            }
                        }
                    }
                    _ => {
                        // Exclu des logs pour éviter le spam, ou mis en debug
                    }
                }
            }
        }
    }

    Ok(())
}

async fn on_heartbeat_received(redis: &RedisManager, heartbeat: Heartbeat) -> Result<()> {
    let hid = heartbeat.id.clone();
    let server = redis.get_server(heartbeat.id).await?;

    if let Some(mut server) = server {
        server.players_online = heartbeat.player_count as u32;
        redis.update_server(&server).await?;
    } else {
        eprintln!("Euh michel on reçoit un heartbeat mais il est pas à nous celui là");
        eprintln!("\t bad heartbeat id : {}", hid)
    }

    Ok(())
}