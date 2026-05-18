use bevy::prelude::States;

// Les états de notre client
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Hash, States)]
pub enum ClientState {
    #[default]
    LoginMenu,
    Connecting,
    InGame,
}