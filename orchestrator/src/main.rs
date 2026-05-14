extern crate redis;

mod docker_manager;
mod redis_manager;

use crate::docker_manager::DockerManager;
use crate::redis_manager::{GameServer, RedisManager};
use anyhow::Result;
use dotenv::dotenv;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameNetworkEvent, GamePeer};
use shared_replication::{Heartbeat, STREAM_HEARTBEAT};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let docker = DockerManager::new().await?;
    let redis = RedisManager::new().await?;

    let mut peer = GamePeer::new(QuicBackend::new());

    let orchestrator_address =
        env::var("ORCH_ADDRESS").expect("Env ORCHESTRATOR_ADDRESS is not set");
    let orchestrator_port: u16 = env::var("ORCH_PORT")
        .expect("Env ORCH_PORT is not set")
        .parse()
        .expect("Env ORCH_PORT is not a number");
    peer.listen(&orchestrator_address, orchestrator_port)
        .expect("Cannot create socket");

    println!(
        "Orchestrator listening on {}:{}",
        orchestrator_address, orchestrator_port
    );

    loop {
        while let Ok(Some(event)) = peer.poll() {
            match event {
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

        let hot_servers_min: u16 = env::var("HOT_SERVERS_MIN")
            .expect("Env HOT_SERVERS_MIN is not set")
            .parse()
            .expect("Env HOT_SERVERS_MIN is not an integer");

        let mut available_server = redis.get_available_servers().await?;

        while available_server.len() < hot_servers_min as usize {
            available_server.push(spawn_server(&docker, &redis).await?);
        }
    }
}

async fn spawn_server(docker: &DockerManager, redis: &RedisManager) -> Result<GameServer> {
    let mut server = redis.create_server().await?;

    let ip_address = docker.spawn_container(&server.id).await?;
    server.address = ip_address;

    Ok(server)
}

async fn on_heartbeat_received(
    redis: &RedisManager,
    docker: &DockerManager,
    heartbeat: Heartbeat,
) -> Result<()> {
    let server = redis.get_server(heartbeat.id).await?;
    if let Some(mut server) = server {
        if heartbeat.player_count == 0 {
            docker.terminate_container(&server.id).await?;
            redis.remove_server(&server.id).await?;
        } else {
            server.players_online = heartbeat.player_count as u32;
            redis.update_server(&server).await?;
        }
    } else {
        eprintln!("Euh michel on reçoit un heartbeat mais il est pas à nous celui là")
    }

    Ok(())
}
