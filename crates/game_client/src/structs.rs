use bevy::prelude::{States, Resource};
use broker_protocol::broker_message::NodeId;
use core_types::GameChunk;

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

    pub chunk : GameChunk,
}