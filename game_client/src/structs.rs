use bevy::prelude::{States, Resource};

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
    // Vaut 'None' tant qu'on n'a pas reçu le WELCOME
    pub net_id: Option<uuid::Uuid>,
    pub pseudo: Option<String>
}