extern crate redis;
#[macro_use]
extern crate rocket;

mod docker_manager;
mod redis_manager;
mod rocket_manager;

use crate::docker_manager::DockerManager;
use crate::redis_manager::{GameServer, RedisManager};
use crate::rocket_manager::RocketManager;
use anyhow::Result;
use dotenv::dotenv;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameNetworkEvent, GamePeer};
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Heartbeat {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub player_count: usize,
    pub max_players: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let docker = Arc::new(DockerManager::new().await?);
    let redis = Arc::new(RedisManager::new().await?);

    let redis_for_rocket = Arc::clone(&redis);
    let docker_for_rocket = Arc::clone(&docker);

    tokio::spawn(async move {
        RocketManager::new(redis_for_rocket, docker_for_rocket).await;
    });

    let mut peer = GamePeer::new(QuicBackend::new());

    loop {
        while let Ok(Some(event)) = peer.poll() {
            match event {
                GameNetworkEvent::Message {
                    connection,
                    stream,
                    data,
                } => match stream.real_stream_id() {
                    1 => match serde_json::from_slice::<Heartbeat>(&data) {
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

        println!("Available servers: {:?}", available_server.len());

        while available_server.len() < hot_servers_min as usize {
            available_server.push(spawn_server(&docker, &redis).await?);
        }
    }
}

async fn update_dashboard(redis: &RedisManager) -> Result<()> {
    let servers = redis.get_all_servers().await?;
    println!("Liste des serveurs actifs :");
    for s in servers {
        println!(
            " - {} ({}) : {}/{} joueurs",
            s.id, s.address, s.players_online, s.players_max
        );
    }

    Ok(())
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
