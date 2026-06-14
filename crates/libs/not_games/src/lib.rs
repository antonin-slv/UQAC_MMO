use serde::{Deserialize, Serialize};

pub mod redis_manager;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct ServerInfo {
    pub ip: String,
    pub port: u16,
}

// Client + Gatekeeper
#[derive(Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
pub struct Login {
    pub username: String,
    pub password: String,
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
pub struct Register {
    pub username: String,
    pub password: String,
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
pub struct LoginResponse {
    pub player_id: String,
    pub server: ServerInfo,
}
