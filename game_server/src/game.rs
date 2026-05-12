// server/src/player.rs
use bevy::prelude::*;
use crate::events;


#[derive(Component, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct NetworkId(pub u64);

#[derive(Component, Default)]
pub struct Player {
}

#[derive(Bundle)]
pub struct PlayerBundle {
    pub player: Player,
    pub net_id: NetworkId,
    pub transform: Transform,
}

impl PlayerBundle {
    // Petite fonction utilitaire pour créer le bundle facilement
    pub fn new(id: u64, position: Vec3) -> Self {
        Self {
            player: Player::default(),
            net_id: NetworkId(id),
            transform: Transform::from_translation(position),
        }
    }
}

pub struct GameLogicPlugin;

impl Plugin for GameLogicPlugin {
    fn build(&self, app: &mut App) {
        // On enregistre les systèmes liés aux joueurs

        app
            .add_systems(Update, (handle_new_players, handle_disconnected, apply_player_inputs))
            .add_systems(Update, simulate_game);
    }
}

fn handle_new_players(
    mut ev_connected: MessageReader<events::PlayerConnected>,
    mut commands: Commands,
) {
    // ev_connected.read() permet de lire l'événement sans le détruire !
    for ev in ev_connected.read() {
        println!("Création de l'avatar pour {}", ev.client_id);
        commands.spawn(PlayerBundle::new(ev.client_id, Vec3::ZERO));
    }
}

fn handle_disconnected(
    mut ev_disconnect : MessageReader<events::PlayerDisconnected>,
    player_query: Query<(Entity, &NetworkId), With<Player>>,
    mut commands: Commands,
) {
    for ev in ev_disconnect.read() {
        for (entity, network_id) in player_query.iter() {
            if network_id.0 == ev.client_id {
                commands.entity(entity).despawn();
                break;
            }
        }
    }
}

fn apply_player_inputs(
    mut ev_input: MessageReader<events::PlayerInputEvent>,
    mut query: Query<(&NetworkId, &mut Transform), With<Player>>,
) {
    for ev in ev_input.read() {
        // On trouve le joueur correspondant et on applique son input
        for (net_id, mut transform) in query.iter_mut() {
            if net_id.0 == ev.client_id {
                // Logique de déplacement (ex: if ev.input_data.up { transform.translation.z += 1.0; })
                transform.translation.x += f32::from(ev.input_data.right) - f32::from(ev.input_data.left)  ;
                transform.translation.y -= f32::from(ev.input_data.up) - f32::from(ev.input_data.down)  ;
                break;
            }
        }
    }
}

fn simulate_game(
    mut query : Query<&mut Transform, With<Player>>,
) {
    for mut transform in query.iter_mut() {
        transform.translation += Vec3::new(0.0, -0.5, 0.0);
    }
}