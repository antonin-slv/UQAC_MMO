// shared/src/lib.rs
use serde::{Deserialize, Serialize};

pub mod redis_manager;
pub mod msg_client_server;
pub mod math;
pub mod broker_topics;
pub mod broker_client;
pub mod broker_message;
pub mod msg_game_payload;
pub mod msg_dgs;
pub mod msg_servers;
pub const STREAM_HANDSHAKE: u16 = 0;
pub const STREAM_SNAPSHOTS: u16 = 100;
pub const STREAM_INPUTS: u16 = 101;
pub const STREAM_HEARTBEAT: u16 = 102;



//
// SERVER - ORCHESTRATOR COMMUNICATION
//

//Data of the server
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct ServerInfo {
    pub ip: String,
    pub port: u16,
    pub zone: String,
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
