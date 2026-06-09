use bevy::prelude::{States, Resource, Component};
use broker_protocol::broker_message::NodeId;
use core_types::chunks::GameChunk;
use game_message::msg_entities::NetworkEntityId;

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
    pub entity_net_id: Option<NetworkEntityId>,
    pub chunk : GameChunk,
}

#[derive(Component)]
pub struct LocalControlledComponent;

#[derive(Resource)]
pub struct Chunking{
    pub chunk_size: f32,
}