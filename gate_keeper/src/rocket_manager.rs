use crate::database_manager::DatabaseManager;
use rocket::http::Status;
use rocket::response::content;
use rocket::serde::{Deserialize, json::Json};
use rocket::{Ignite, Rocket, State};
use shared_replication::redis_manager::RedisManager;
use shared_replication::{ServerInfo, Login, LoginResponse, Register};
use std::env;
use std::sync::Arc;

pub struct RocketManager {
    rocket: Rocket<Ignite>,
}

impl RocketManager {
    pub async fn new(database: Arc<DatabaseManager>, redis: Arc<RedisManager>) -> Self {
        let gatekeeper_port: u16 = env::var("GATEKEEPER_PORT")
            .expect("Env GATEKEEPER_PORT is not set")
            .parse()
            .expect("Env GATEKEEPER_PORT is not a number ");

        let rocket = rocket::build()
            .manage(database)
            .manage(redis)
            .mount("/gate-keeper", routes![login, register, health])
            .configure(rocket::Config::figment().merge(("port", gatekeeper_port)))
            .launch()
            .await
            .expect("Rocket failed to launch");
        RocketManager { rocket }
    }
}

#[post("/login", data = "<login>")]
async fn login(
    database: &State<Arc<DatabaseManager>>,
    redis: &State<Arc<RedisManager>>,
    login: Json<Login>,
) -> Result<Json<LoginResponse>, Status> {
    let login = database
        .login(login.username.as_str(), login.password.as_str())
        .await;

    if !login {
        return Err(Status::Unauthorized);
    }

    get_available_server(redis).await
}

#[post("/register", data = "<register>")]
async fn register(
    database: &State<Arc<DatabaseManager>>,
    redis: &State<Arc<RedisManager>>,
    register: Json<Register>,
) -> Result<Json<LoginResponse>, Status> {
    let _ = database
        .register(register.username.as_str(), register.password.as_str())
        .await;

    get_available_server(redis).await
}

#[get("/health")]
async fn health() -> content::RawJson<&'static str> {
    content::RawJson("{ 'status': 'ok' }")
}

async fn get_available_server(
    redis: &State<Arc<RedisManager>>,
) -> Result<Json<LoginResponse>, Status> {
    //todo : rework this

    let host_address = env::var("HOST_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1".to_string());

    let broker_port = env::var("BROKER_PUBLIC_PORT")
        .expect("Env BROKER_PRIVATE_PORT is not set")
        .parse()
        .expect("Env BROKER_PRIVATE_PORT is not a u16");


    let available_servers = redis.get_available_servers().await;
    if let Ok(available_servers) = available_servers {
        if let Some(server) = available_servers.first() {
            let response = LoginResponse {
                player_id: "7db9b582-7771-4654-8e81-799f9c73e34b".to_string(),
                server: ServerInfo {
                    ip: host_address,
                    port: broker_port,
                    zone: server.area.clone(),
                },
            };

            return Ok(Json(response));
        }
    }

    Err(Status::ServiceUnavailable)
}
