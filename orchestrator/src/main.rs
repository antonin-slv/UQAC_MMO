extern crate redis;
mod docker_manager;
mod redis_manager;

use crate::docker_manager::DockerManager;
use crate::redis_manager::{GameServer, RedisManager};
use anyhow::Result;
use dotenv::dotenv;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let docker = DockerManager::new().await?;
    let redis =
        RedisManager::new(&env::var("REDIS_ADDRESS").expect("Env REDIS_ADDRESS is not set"))?;

    // Test connections
    let num_clients_to_test = 16;

    for _ in 0..num_clients_to_test {
        on_client_connected(&docker, &redis).await?;
    }

    update_dashboard(&redis).await?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    for _ in 0..num_clients_to_test {
        on_client_disconnected(&docker, &redis).await?;
    }

    update_dashboard(&redis).await?;

    Ok(())
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
