// shared/src/lib.rs
use serde::{Deserialize, Serialize};

// -- les différents streams de données

pub const STREAM_HANDSHAKE: u16 = 0;
pub const STREAM_SNAPSHOTS: u16 = 1;
pub const STREAM_INPUTS: u16    = 2;


//
// CLIENT SERVER COMMUNICATION
//
#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub struct EntitySnapshot {
    pub network_id: u32,
    pub position: [f32; 2],
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PersonalSnapshot {
    pub entities: Vec<EntitySnapshot>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct PlayerInput {
    pub up: bool,
    pub down: bool,
    pub left: bool,
    pub right: bool,
}

//
// SERVER - ORCHESTRATOR COMMUNICATION
//

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Heartbeat {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub player_count: usize,
    pub max_players: usize,
}


//Data of the server... This is passed by the Orchestrator to game servers by environnement variables.
#[derive(Debug, Serialize, Deserialize, Clone)]
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
    JOIN(String), //sent by a client to join a server,
    WELCOME(String), //Server welcomes client with the uuid of the client
    HEARTBEAT(Heartbeat), //server send this to the orchestrator
}