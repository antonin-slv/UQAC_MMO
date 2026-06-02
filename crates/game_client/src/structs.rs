use bevy::prelude::{States, Resource};
use shared_replication::broker_message::NodeId;

// Les états de notre client
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Hash, States)]
pub enum ClientState {
    #[default]
    LoginMenu,
    Connecting,
    InGame,
}

#[derive(Resource, Default)]
pub(crate) struct LocalPlayer {
    pub net_id: NodeId,
    pub pseudo: Option<String>,

    pub x_chunk: i32,
    pub y_chunk: i32,
}