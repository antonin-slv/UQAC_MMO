use uuid::Uuid;
use bevy::prelude::*;
use shared_replication::client_server;

#[derive(Message, Debug)]
pub struct NetConnexion {
    pub client_id: Uuid,
}
#[derive(Message, Debug)]
pub struct NetDisconnection {
    pub client_id: Uuid,
}

#[derive(Message, Debug)]
pub struct PlayerConnected {
    pub client_id: Uuid,
    pub player_name: String,
    pub stream_used : game_sockets::GameStream,
}


#[derive(Message, Debug)]
pub struct PlayerDisconnected {
    pub client_id: Uuid,
}

#[derive(Message, Debug)]
pub struct PlayerInputEvent {
    pub client_id: Uuid,
    pub input_data: client_server::PlayerInput,
}