use crate::database_manager::DatabaseManager;
use rocket::serde::{Deserialize, json::Json};
use rocket::{Ignite, Rocket, State};
use std::env;
use std::sync::Arc;

pub struct RocketManager {
    rocket: Rocket<Ignite>,
}

impl RocketManager {
    pub async fn new(database: Arc<DatabaseManager>) -> Self {
        let gatekeeper_port: u16 = env::var("GATEKEEPER_PORT")
            .expect("Env GATEKEEPER_PORT is not set")
            .parse()
            .expect("Env GATEKEEPER_PORT is not a number ");

        let rocket = rocket::build()
            .manage(database)
            .mount("/gate-keeper", routes![login, register])
            .configure(rocket::Config::figment().merge(("port", gatekeeper_port)))
            .launch()
            .await
            .expect("Rocket failed to launch");
        RocketManager { rocket }
    }
}

#[derive(Deserialize)]
#[serde(crate = "rocket::serde")]
struct Login {
    username: String,
    password: String,
}

#[post("/login", data = "<login>")]
async fn login(database: &State<Arc<DatabaseManager>>, login: Json<Login>) -> String {
    let login = database
        .login(login.username.as_str(), login.password.as_str())
        .await;

    if let Ok(login) = login
        && login
    {
        let gatekeeper_address =
            &env::var("ORCHESTRATOR_ADDRESS").expect("Env ORCHESTRATOR_ADDRESS is not set");
        let gatekeeper_port: u16 = env::var("ORCHESTRATOR_PORT")
            .expect("Env ORCHESTRATOR_PORT is not set")
            .parse()
            .expect("Env ORCHESTRATOR_PORT is not a valid number");

        let orchestrator_address = format!(
            "http://{}:{}/orchestrator",
            gatekeeper_address, gatekeeper_port
        );
        let response = reqwest::get(format!("{}/connect", orchestrator_address)).await;

        if let Ok(response) = response
            && response.status().is_success()
        {
            let body = response.text().await;
            if let Ok(body) = body {
                return body;
            }
        }
    }

    "Orchestrator failed to login.".to_string()
}

#[derive(Deserialize)]
#[serde(crate = "rocket::serde")]
struct Register {
    username: String,
    password: String,
}

#[post("/register", data = "<register>")]
async fn register(database: &State<Arc<DatabaseManager>>, register: Json<Register>) -> String {
    let _ = database
        .register(register.username.as_str(), register.password.as_str())
        .await;

    "Registered".to_string()
}
