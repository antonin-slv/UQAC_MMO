use crate::docker_manager::DockerManager;
use crate::redis_manager::RedisManager;
use crate::update_dashboard;
use rocket::http::Status;
use rocket::{Ignite, Rocket, State};
use std::env;
use std::sync::Arc;

pub struct RocketManager {
    rocket: Rocket<Ignite>,
}

impl RocketManager {
    pub async fn new(redis_manager: Arc<RedisManager>, docker_manager: Arc<DockerManager>) -> Self {
        let orchestrator_port: u16 = env::var("ORCH_PORT")
            .expect("Env ORCH_PORT is not set")
            .parse()
            .expect("Env ORCH_PORT is not a number ");

        let rocket = rocket::build()
            .manage(redis_manager)
            .manage(docker_manager)
            .mount("/orchestrator", routes![connect, clear_servers])
            .configure(rocket::Config::figment().merge(("port", orchestrator_port)))
            .launch()
            .await
            .expect("Rocket failed to launch");
        RocketManager { rocket }
    }
}

#[get("/connect")]
async fn connect(redis: &State<Arc<RedisManager>>) -> Result<String, Status> {
    let game_server = redis
        .get_available_server()
        .await
        .map_err(|e| e.to_string());

    update_dashboard(redis).await.expect("TODO: panic message");

    match game_server {
        Ok(game_server) => {
            if let Some(mut game_server) = game_server {
                game_server.players_online += 1;

                let _ = redis.update_server(&game_server).await;

                Ok(game_server.address)
            } else {
                Err(Status::ServiceUnavailable)
            }
        }
        Err(e) => {
            eprintln!("{}", e);
            Err(Status::ServiceUnavailable)
        }
    }
}

#[get("/clear-servers")]
async fn clear_servers(
    redis: &State<Arc<RedisManager>>,
    docker: &State<Arc<DockerManager>>,
) -> Result<String, Status> {
    let servers = redis.get_all_servers().await;

    if let Ok(servers) = servers {
        for server in servers {
            let _ = docker.terminate_container(&server.id).await;
            let _ = redis.remove_server(&server.id).await;
        }
    }

    Ok("C'est delete".to_string())
}
