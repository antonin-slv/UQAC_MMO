extern crate redis;

mod docker_manager;

use crate::docker_manager::DockerManager;
use anyhow::Result;
use broker_client::{ClientNetworkEvent, MmoNetworkClient};
use broker_protocol::topics::{Namespace, SecurityDomain, TopicBuilder};
use dotenv::dotenv;
use game_message::GameMessageHeaders;
use game_message::msg_dgs::{Heartbeat};
use game_message::msg_servers::{ServerHelloMSG, ServerType, SpawnServerMSG};
use not_games::redis_manager::{GameServer, RedisManager};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::interval;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    println!("Starting Orchestrator");
    println!("Starting docker");

    let docker = Arc::new(DockerManager::new().await?);
    println!("Starting redis");
    let redis = Arc::new(RedisManager::new().await?);

    let hot_servers_min: u16 = env::var("HOT_SERVERS_MIN")
        .expect("Env HOT_SERVERS_MIN is not set")
        .parse()
        .expect("Env HOT_SERVERS_MIN is not an integer");

    println!("Start tasks");

    let redis_heartbeat = Arc::clone(&redis);
    let docker_heartbeat = Arc::clone(&docker);

    // Création du canal de synchronisation
    let (broker_ready_tx, broker_ready_rx) = oneshot::channel();

    // --- TASK 1: Écoute Réseau (Heartbeats & Broker) ---
    let heartbeat_handle = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(100));

        let mut client = MmoNetworkClient::new();
        client
            .connect(
                docker_heartbeat.broker_ip.as_str(),
                docker_heartbeat.broker_private_port,
            )
            .expect("TODO: panic message");

        println!(
            "Orchestrator connecting to Broker at {}:{}",
            docker_heartbeat.broker_ip, docker_heartbeat.broker_private_port
        );

        // On encapsule le sender dans un Option pour pouvoir le consommer (take) une seule fois
        let mut ready_tx_opt = Some(broker_ready_tx);

        loop {
            ticker.tick().await;

            // On passe le canal à listen_broker pour qu'il le déclenche au bon moment
            if let Err(e) = listen_broker(
                &mut client,
                &docker_heartbeat,
                &redis_heartbeat,
                &mut ready_tx_opt,
            )
            .await
            {
                println!("Orchestrator network error: {}", e);
            }
        }
    });

    // --- TASK 2: Scaling Automatique (Docker) ---
    let redis_scaler = Arc::clone(&redis);
    let docker_scaler = Arc::clone(&docker);

    let scaler_handle = tokio::spawn(async move {
        println!(
            "[Orchestrator] En attente de la connexion au Broker avant d'autoriser le scaling..."
        );

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

                available_server.retain(|s| s.nb_chunk == 0);

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

async fn listen_broker(
    broker_api: &mut MmoNetworkClient,
    docker: &DockerManager,
    redis: &RedisManager,
    ready_tx: &mut Option<oneshot::Sender<()>>,
) -> Result<()> {
    while let Some(event) = broker_api.poll() {
        match event {
            ClientNetworkEvent::Ready => {
                let my_id = broker_api.node_id.unwrap_or_else(|| {
                    panic!("Orchestrator has no node ID after Ready event, using 0 as fallback");
                });

                // 1. L'Orchestrateur s'abonne au canal "Director" pour recevoir les Heartbeats
                let heartbeat_topic =
                    TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::Heartbeat).build();
                broker_api.subscribe(heartbeat_topic, 0);
                println!("[Orchestrator] Abonné au topic Heartbeats.");

                let msg = ServerHelloMSG {
                    server_type: ServerType::Orchestrator,
                    id: my_id,
                };

                // L'Orchestrateur annonce son existence sur le canal d'authentification
                let auth_topic =
                    TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ServerConnection)
                        .build();
                broker_api.publish_reliable(auth_topic.clone(), &msg);
                println!("[Orchestrator] Handshake 'Hello' envoyé.");

                broker_api.subscribe(auth_topic, 0); //he listens the hello of the servers, I guess.

                broker_api.subscribe(
                    TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::Director).build(),
                    0,
                );

                // ---> MAGIE : On débloque la tâche de scaling ! <---
                if let Some(tx) = ready_tx.take() {
                    let _ = tx.send(()); // Le canal est consommé, le signal est envoyé
                }
            }
            ClientNetworkEvent::Connected => {
                println!("[Orchestrator] Connected (waiting to be ready)");
            }
            ClientNetworkEvent::Disconnected(node_id) => {
                if node_id == 0 || node_id == broker_api.node_id.unwrap_or(node_id) {
                    println!(
                        "❌ [Spatial/Auth] Perte de connexion au Broker ! Tentative de reconnexion dans 1s..."
                    );
                }
            }
            ClientNetworkEvent::DataReceived {
                client_id: _,
                stream: _,
                mut payload,
            } => {
                match payload.header {
                    GameMessageHeaders::Heartbeat => {
                        let heartbeat = payload.extract::<Heartbeat>();
                        match heartbeat {
                            Ok(heartbeat) => {
                                on_heartbeat_received(redis, heartbeat).await?;
                            }
                            Err(e) => {
                                println!("[Orchestrator] Heartbeat error: {}", e);
                            }
                        }
                    }
                    GameMessageHeaders::SpawnServer => {
                        let spawn_server_msg = payload.extract::<SpawnServerMSG>();
                        match spawn_server_msg {
                            Ok(spawn_server_msg) => {
                                for _ in 0..spawn_server_msg.server_count {
                                    match spawn_server(&docker, &redis).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            eprintln!("{e}")
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                println!("[Orchestrator] Spawn server error: {}", e);
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
    let server = redis.get_server(heartbeat.id).await;

    match server {
        Ok(server) => {
            if let Some(mut server) = server {
                server.nb_chunk = heartbeat.chunk_managed.len();
                server.in_use = heartbeat.is_in_use;
                redis.update_server(&server).await?;
            } else {
                eprintln!("Euh michel on reçoit un heartbeat mais il est pas à nous celui là");
                eprintln!("\t bad heartbeat id : {}", hid)
            }
        }
        Err(e) => {
            eprintln!(
                "[Orchestrator] recieving heartbeat : Error with redis : {}",
                e
            );
        }
    }
    Ok(())
}
