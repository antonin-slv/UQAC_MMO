use bevy::prelude::{States, Resource};
use shared_replication::broker::ClientId;

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
    pub net_id: ClientId,
    pub pseudo: Option<String>
}