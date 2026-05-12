// shared/src/lib.rs
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub struct EntitySnapshot {
    pub network_id: u64,
    pub position: [f32; 2],
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PersonalSnapshot {
    pub entities: Vec<EntitySnapshot>,
}

pub enum NetworkEvent {
    PlayerConnected(u64),
    PlayerDisconnected(u64),
    PlayerInput(u64, PlayerInput),
}

pub enum ServerMessage {
    SendTo(u64, Vec<u8>),
    Broadcast(Vec<u8>),
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct PlayerInput {
    pub up: bool,
    pub down: bool,
    pub left: bool,
    pub right: bool,
    // pub space: bool, // Pour sauter, etc.
}