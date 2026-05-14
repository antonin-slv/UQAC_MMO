use uuid::Uuid;
use bevy::prelude::*;

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
}


#[derive(Message, Debug)]
pub struct PlayerDisconnected {
    pub client_id: Uuid,
}

#[derive(Message, Debug)]
pub struct PlayerInputEvent {
    pub client_id: Uuid,
    pub input_data: shared_replication::PlayerInput,
}