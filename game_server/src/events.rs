use bevy::prelude::*;

#[derive(Message, Debug)]
pub struct PlayerConnected {
    pub client_id: u64,
}

#[derive(Message, Debug)]
pub struct PlayerDisconnected {
    pub client_id: u64,
}

#[derive(Message, Debug)]
pub struct PlayerInputEvent {
    pub client_id: u64,
    pub input_data: shared_replication::PlayerInput, // Ta structure de touches (ex : ZQSD, clic souris)
}