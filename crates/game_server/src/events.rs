use bevy::prelude::*;
use shared_replication::broker_message::NodeId;
use shared_replication::msg_client_server;

#[derive(Message, Debug)]
pub struct PlayerConnected {
    pub client_id: NodeId,
    pub player_name: String
}


#[derive(Message, Debug)]
pub struct PlayerDisconnected {
    pub client_id: NodeId,
}

#[derive(Message, Debug)]
pub struct PlayerInputEvent {
    pub client_id: NodeId,
    pub input_data: msg_client_server::PlayerInput,
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