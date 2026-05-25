// shared/src/lib.rs
use serde::{Deserialize, Serialize};

pub mod redis_manager;
pub mod client_server;
pub mod broker;
pub mod math;

pub const STREAM_HANDSHAKE: u16 = 0;
pub const STREAM_SNAPSHOTS: u16 = 100;
pub const STREAM_INPUTS: u16 = 101;
pub const STREAM_HEARTBEAT: u16 = 102;



//
// SERVER - ORCHESTRATOR COMMUNICATION
//

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Heartbeat {
    pub id: String,
    pub zone: String,
    pub player_count: usize,
    pub max_players: usize,
}

//Data of the server
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "rocket::serde")]
pub struct ServerInfo {
    pub ip: String,
    pub port: u16,
    pub zone: String,
}

//
// ...
//
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NetMessages {
    JOIN(String),         //sent by a client to join a server,
    WELCOME(String),      //Server welcomes client with the uuid of the client
    HEARTBEAT(Heartbeat), //server send this to the orchestrator
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
