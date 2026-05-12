extern crate redis;
mod docker_manager;
mod server_manager;

use crate::docker_manager::DockerManager;
use crate::server_manager::{GameServer, ServerManager};
use anyhow::Result;
use dotenv::dotenv;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let docker_manager = DockerManager::new().await?;
    let server_manager =
        ServerManager::new(&env::var("REDIS_ADDRESS").expect("Env REDIS_ADDRESS is not set"))?;

    // Test connections
    let num_clients_to_test = 16;

    for _ in 0..num_clients_to_test {
        on_client_connected(&docker_manager, &server_manager).await?;
    }

    update_dashboard(&server_manager).await?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    for _ in 0..num_clients_to_test {
        on_client_disconnected(&docker_manager, &server_manager).await?;
    }

    update_dashboard(&server_manager).await?;

    Ok(())
}

async fn update_dashboard(server_manager: &ServerManager) -> Result<()> {
    let servers = server_manager.get_all_servers().await?;
    println!("Liste des serveurs actifs :");
    for s in servers {
        println!(
            " - {} ({}) : {}/{} joueurs",
            s.name, s.address, s.players_online, s.players_max
        );
    }

    Ok(())
}

async fn on_client_connected(docker: &DockerManager, server: &ServerManager) -> Result<GameServer> {
    let available_server = server.get_available_server().await?;

    let mut server_to_connect: GameServer;

    if let Some(available_server) = available_server {
        server_to_connect = available_server;
    } else {
        server_to_connect = server.create_server().await?;

        let ip_address = docker
            .spawn_container(
                &server_to_connect.name,
                &env::var("GAME_SERVER_IMAGE").expect("Env GAME_SERVER_IMAGE is not set"),
            )
            .await?;
        server_to_connect.address = ip_address;
    }

    server_to_connect.players_online += 1;
    server.update_server(&server_to_connect).await?;

    Ok(server_to_connect)
}

async fn on_client_disconnected(docker: &DockerManager, server: &ServerManager) -> Result<()> {
    let servers = server.get_all_servers().await?;

    for mut s in servers {
        s.players_online -= 1;

        if s.players_online <= 0 {
            docker.terminate_container(&s.name).await?;
            server.remove_server(&s.name).await?;
        } else {
            server.update_server(&s).await?;
        }

        return Ok(());
    }

    Ok(())
}
