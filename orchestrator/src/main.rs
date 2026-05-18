extern crate redis;

mod docker_manager;

use crate::docker_manager::DockerManager;
use anyhow::Result;
use bytes::Bytes;
use dotenv::dotenv;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use shared_replication::redis_manager::{GameServer, RedisManager};
use shared_replication::{Heartbeat, STREAM_HANDSHAKE, STREAM_HEARTBEAT, ServerInfo};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    println!("Starting Orchestrator");
    println!("Starting docker");

    let docker = Arc::new(DockerManager::new().await?);
    println!("Starting redis");
    let redis = Arc::new(RedisManager::new().await?);

    let orchestrator_port: u16 = env::var("ORCH_PORT")
        .expect("Env ORCH_PORT is not set")
        .parse()
        .expect("Env ORCH_PORT is not a number");

    let hot_servers_min: u16 = env::var("HOT_SERVERS_MIN")
        .expect("Env HOT_SERVERS_MIN is not set")
        .parse()
        .expect("Env HOT_SERVERS_MIN is not an integer");

    println!("Start tasks");

    let redis_heartbeat = Arc::clone(&redis);
    let docker_heartbeat = Arc::clone(&docker);

    let heartbeat_handle = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(3));

        let mut peer = GamePeer::new(QuicBackend::new());

        peer.listen(&"0.0.0.0", orchestrator_port)
            .expect("Cannot create socket");

        println!("Orchestrator listening to 0.0.0.0:{}", orchestrator_port);

        loop {
            ticker.tick().await;

            if let Err(e) = listen_heartbeat(&mut peer, &redis_heartbeat, &docker_heartbeat).await {
                println!("orchestrator error: {}", e);
            }
        }
    });

    let redis_scaler = Arc::clone(&redis);
    let docker_scaler = Arc::clone(&docker);

    let scaler_handle = tokio::spawn(async move {
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
            }
        }
    });

    tokio::try_join!(heartbeat_handle, scaler_handle)?;

    Ok(())
}

async fn spawn_server(docker: &DockerManager, redis: &RedisManager) -> Result<GameServer> {
    let mut server = redis.create_server().await?;

    let port = docker.spawn_container(&server.id).await?;

    server.port = port;

    redis.update_server(&server).await?;

    Ok(server)
}

async fn listen_heartbeat(
    peer: &mut GamePeer,
    redis: &RedisManager,
    docker: &DockerManager,
) -> Result<()> {
    while let Ok(Some(event)) = peer.poll() {
        match event {
            GameNetworkEvent::Connected(connection) => {
                let server_info = ServerInfo {
                    ip: "".to_string(),
                    port: 2,
                    zone: "".to_string(),
                };
                if let Ok(bytes) = bincode::serialize(&server_info) {
                    let data = Bytes::from(bytes);
                    let gs = GameStream::new(STREAM_HANDSHAKE, GameStreamReliability::Unreliable);
                    let _ = peer.send(&connection, &gs, data);
                }
            }
            GameNetworkEvent::Message {
                connection,
                stream,
                data,
            } => match stream.real_stream_id() {
                STREAM_HEARTBEAT => match serde_json::from_slice::<Heartbeat>(&data) {
                    Ok(heartbeat) => on_heartbeat_received(&redis, &docker, heartbeat).await?,
                    Err(e) => {
                        eprintln!("Invalid heartbeat {} : {}", connection.connection_uuid, e)
                    }
                },
                _ => {}
            },
            _ => {}
        }
    }

    Ok(())
}

async fn on_heartbeat_received(
    redis: &RedisManager,
    docker: &DockerManager,
    heartbeat: Heartbeat,
) -> Result<()> {
    let server = redis.get_server(heartbeat.id).await?;
    if let Some(mut server) = server {
        if heartbeat.player_count == 0 {
            let hot_servers_min: u16 = env::var("HOT_SERVERS_MIN")
                .expect("Env HOT_SERVERS_MIN is not set")
                .parse()
                .expect("Env HOT_SERVERS_MIN is not an integer");

            let available_server = redis.get_available_servers().await?;

            if available_server.len() > hot_servers_min as usize {
                docker.terminate_container(&server.id).await?;
                redis.remove_server(&server.id).await?;
            }
        }
        server.players_online = heartbeat.player_count as u32;
        server.area = heartbeat.zone;
        redis.update_server(&server).await?;
    } else {
        eprintln!("Euh michel on reçoit un heartbeat mais il est pas à nous celui là")
    }

    Ok(())
}
