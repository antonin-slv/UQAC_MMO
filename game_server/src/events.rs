use bevy::asset::uuid;
use bevy::prelude::*;

#[derive(Message, Debug)]
pub struct PlayerConnected {
    pub client_id: uuid::Uuid,
}

#[derive(Message, Debug)]
pub struct PlayerDisconnected {
    pub client_id: uuid::Uuid,
}

#[derive(Message, Debug)]
pub struct PlayerInputEvent {
    pub client_id: uuid::Uuid,
    pub input_data: shared_replication::PlayerInput,
}