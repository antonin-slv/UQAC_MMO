#[macro_use]
extern crate rocket;
mod database_manager;
mod rocket_manager;

use crate::database_manager::DatabaseManager;
use crate::rocket_manager::RocketManager;
use dotenv::dotenv;
use std::sync::Arc;
use not_games::redis_manager::RedisManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    println!("Starting gatekeeper server...");
    println!("Connecting to database server...");
    let database_manager = Arc::new(DatabaseManager::new().await?);
    let redis_manager = Arc::new(RedisManager::new().await?);

    tokio::spawn(async move {
        println!("Start rocket");
        RocketManager::new(database_manager, redis_manager).await;
    });

    loop {}
}
