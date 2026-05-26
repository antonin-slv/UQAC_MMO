use bevy::prelude::*;
use shared_replication::broker::ClientId;
use shared_replication::client_server;

#[derive(Message, Debug)]
pub struct PlayerConnected {
    pub client_id: ClientId,
    pub player_name: String,
    pub stream_used : game_sockets::GameStream,
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