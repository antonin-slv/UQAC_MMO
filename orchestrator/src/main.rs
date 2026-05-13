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
use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let docker = Arc::new(DockerManager::new().await?);
    let redis = Arc::new(RedisManager::new(
        &env::var("REDIS_ADDRESS").expect("Env REDIS_ADDRESS is not set"),
    )?);

    let docker_for_rocket = Arc::clone(&docker);
    let redis_for_rocket = Arc::clone(&redis);

    tokio::spawn(async move {
        RocketManager::new(redis_for_rocket, docker_for_rocket).await;
    });

    loop {}
}

async fn update_dashboard(redis: &RedisManager) -> Result<()> {
    let servers = redis.get_all_servers().await?;
    println!("Liste des serveurs actifs :");
    for s in servers {
        println!(
            " - {} ({}) : {}/{} joueurs",
            s.name, s.address, s.players_online, s.players_max
        );
    }

    Ok(())
}

async fn on_client_connected(docker: &DockerManager, redis: &RedisManager) -> Result<GameServer> {
    let available_server = redis.get_available_server().await?;

    let mut server_to_connect: GameServer;

    if let Some(available_server) = available_server {
        server_to_connect = available_server;
    } else {
        server_to_connect = redis.create_server().await?;

        let ip_address = docker.spawn_container(&server_to_connect.name).await?;
        server_to_connect.address = ip_address;
    }

    server_to_connect.players_online += 1;
    redis.update_server(&server_to_connect).await?;

    Ok(server_to_connect)
}

async fn on_client_disconnected(docker: &DockerManager, redis: &RedisManager) -> Result<()> {
    let servers = redis.get_all_servers().await?;

    for mut s in servers {
        s.players_online -= 1;

        if s.players_online <= 0 {
            docker.terminate_container(&s.name).await?;
            redis.remove_server(&s.name).await?;
        } else {
            redis.update_server(&s).await?;
        }

        return Ok(());
    }

    Ok(())
}
