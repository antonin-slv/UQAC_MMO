use crate::database_manager::DatabaseManager;
use not_games::redis_manager::RedisManager;
use not_games::{Login, LoginResponse, Register, ServerInfo};
use rocket::http::Status;
use rocket::response::content;
use rocket::serde::json::Json;
use rocket::{Ignite, Rocket, State};
use std::env;
use std::sync::Arc;

pub struct RocketManager {
    _rocket: Rocket<Ignite>,
}

impl RocketManager {
    pub async fn new(database: Arc<DatabaseManager>) -> Self {
        let gatekeeper_port: u16 = env::var("GATEKEEPER_PORT")
            .expect("Env GATEKEEPER_PORT is not set")
            .parse()
            .expect("Env GATEKEEPER_PORT is not a number ");

        let rocket = rocket::build()
            .manage(database)
            .mount("/gate-keeper", routes![login, register, health])
            .configure(rocket::Config::figment().merge(("port", gatekeeper_port)))
            .launch()
            .await
            .expect("Rocket failed to launch");
        RocketManager { _rocket: rocket }
    }
}

#[post("/login", data = "<login>")]
async fn login(
    database: &State<Arc<DatabaseManager>>,
    login: Json<Login>,
) -> Result<Json<LoginResponse>, Status> {
    let login = database
        .login(login.username.as_str(), login.password.as_str())
        .await;

    if !login {
        return Err(Status::Unauthorized);
    }

    get_available_server().await
}

#[post("/register", data = "<register>")]
async fn register(
    database: &State<Arc<DatabaseManager>>,
    register: Json<Register>,
) -> Result<Json<LoginResponse>, Status> {
    let result = database
        .register(register.username.as_str(), register.password.as_str())
        .await;

    if let Ok(_) = result {
        get_available_server().await
    } else {
        Err(Status::Conflict)
    }
}

#[get("/health")]
async fn health() -> content::RawJson<&'static str> {
    content::RawJson("{ 'status': 'ok' }")
}

async fn get_available_server() -> Result<Json<LoginResponse>, Status> {
    //todo : rework this

    let host_address = env::var("HOST_ADDRESS").unwrap_or_else(|_| "127.0.0.1".to_string());

    let broker_port = env::var("BROKER_PUBLIC_PORT")
        .expect("Env BROKER_PRIVATE_PORT is not set")
        .parse()
        .expect("Env BROKER_PRIVATE_PORT is not a u16");

    let response = LoginResponse {
        player_id: "7db9b582-7771-4654-8e81-799f9c73e34b".to_string(),
        server: ServerInfo {
            ip: host_address,
            port: broker_port,
        },
    };

    Ok(Json(response))
}
