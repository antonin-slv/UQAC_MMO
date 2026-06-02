use bevy::prelude::{States, Resource};
use shared_replication::broker_message::NodeId;
use shared_replication::msg_dgs::GameChunk;

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