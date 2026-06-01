use bevy::prelude::*;
use shared_replication::broker_message::ClientId;
use shared_replication::client_server;

#[derive(Message, Debug)]
pub struct PlayerConnected {
    pub client_id: ClientId,
    pub player_name: String
}


#[derive(Message, Debug)]
pub struct PlayerDisconnected {
    pub client_id: ClientId,
}

#[derive(Message, Debug)]
pub struct PlayerInputEvent {
    pub client_id: ClientId,
    pub input_data: client_server::PlayerInput,
}

#[derive(Debug)]
pub struct GameChunk {
    pub x : i32,
    pub y : i32,
}

#[derive(Resource, Debug)]
pub struct AssignedChunks {
    pub chunk : Option<GameChunk>,
}
#[derive(Message, Debug)]
pub struct ChunkAssignedEvent {
    pub chunk : GameChunk
}