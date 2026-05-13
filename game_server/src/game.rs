// server/src/player.rs
use bevy::prelude::*;
pub(crate) use crate::network::{NetworkId, ControlledBy, NetworkIdGenerator};
use crate::events;

#[derive(Component, Default)]
pub struct Player {
}

#[derive(Bundle)]
pub struct PlayerBundle {
    pub player: Player,
    pub net_id: NetworkId,
    pub controlled_by: ControlledBy,
    pub transform: Transform,
    pub global_transform: GlobalTransform,
}

impl PlayerBundle {
    pub fn new(net_id: u32, connection: uuid::Uuid, position: Vec3) -> Self {
        Self {
            player: Player::default(),
            net_id: NetworkId(net_id),
            controlled_by: ControlledBy { owner_uuid: connection }, // On assigne la propriété
            transform: Transform::from_translation(position),
            global_transform: GlobalTransform::default(),
        }
    }
}

pub struct GameLogicPlugin;

impl Plugin for GameLogicPlugin {
    fn build(&self, app: &mut App) {
        // On enregistre les systèmes liés aux joueurs

        app
            .add_systems(Update,((handle_new_players, handle_disconnected, apply_player_inputs), simulate_game).chain());

    }
}

fn handle_new_players(
    mut reader: MessageReader<events::PlayerConnected>,
    mut id_gen: ResMut<NetworkIdGenerator>,
    mut commands: Commands,
) {
    for msg in reader.read() {
        let net_id = id_gen.next();
        let connection = msg.client_id;
        commands.spawn(PlayerBundle::new(net_id.0, connection, Vec3::ZERO));
    }
}

fn handle_disconnected(
    mut ev_disconnect : MessageReader<events::PlayerDisconnected>,
    player_query: Query<(Entity, &ControlledBy), With<Player>>,
    mut commands: Commands,
) {
    for ev in ev_disconnect.read() {
        for (entity, network_id) in player_query.iter() {
            if network_id.owner_uuid == ev.client_id {
                commands.entity(entity).despawn();
                //todo : Tell the connected clients it happened
                break;
            }
        }
    }
}

fn apply_player_inputs(
    mut ev_input: MessageReader<events::PlayerInputEvent>,
    mut query: Query<(&ControlledBy, &mut Transform), With<Player>>,
) {
    for ev in ev_input.read() {
        // On trouve le joueur correspondant et on applique son input
        for (net_id, mut transform) in query.iter_mut() {
            if net_id.owner_uuid == ev.client_id {
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
        //todo : faire le jeu
        transform.translation += Vec3::new(0.0, -0.5, 0.0);
    }
}