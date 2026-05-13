use crate::docker_manager::DockerManager;
use crate::redis_manager::RedisManager;
use crate::{on_client_connected, on_client_disconnected, update_dashboard};
use rocket::{Ignite, Rocket, State};
use std::sync::Arc;

pub struct RocketManager {
    rocket: Rocket<Ignite>,
}

impl RocketManager {
    pub async fn new(redis_manager: Arc<RedisManager>, docker_manager: Arc<DockerManager>) -> Self {
        let rocket = rocket::build()
            .manage(redis_manager) // On injecte les managers dans l'état Rocket
            .manage(docker_manager)
            .mount("/api", routes![connect, disconnect])
            .launch()
            .await
            .expect("Rocket failed to launch");
        RocketManager { rocket }
    }
}

#[get("/connect")]
async fn connect(
    redis: &State<Arc<RedisManager>>,
    docker_manager: &State<Arc<DockerManager>>,
) -> String {
    let game_server = on_client_connected(docker_manager, redis)
        .await
        .map_err(|e| e.to_string());

    update_dashboard(redis).await.expect("TODO: panic message");

    match game_server {
        Ok(game_server) => game_server.address,
        Err(e) => e,
    }
}

#[get("/disconnect")]
async fn disconnect(
    redis: &State<Arc<RedisManager>>,
    docker_manager: &State<Arc<DockerManager>>,
) -> String {
    let result = on_client_disconnected(docker_manager, redis)
        .await
        .map_err(|e| e.to_string());

    update_dashboard(redis).await.expect("TODO: panic message");

    match result {
        Ok(_) => "Disconnected".to_string(),
        Err(e) => e,
    }
}
