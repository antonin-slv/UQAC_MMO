#[macro_use]
extern crate rocket;
mod database_manager;
mod rocket_manager;

use crate::database_manager::DatabaseManager;
use crate::rocket_manager::RocketManager;
use dotenv::dotenv;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let database_manager = Arc::new(DatabaseManager::new().await?);

    tokio::spawn(async move {
        RocketManager::new(database_manager).await;
    });

    loop {}
}
