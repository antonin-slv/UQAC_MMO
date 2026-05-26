use std::collections::HashMap;
// server/src/player.rs
use bevy::prelude::*;
use shared_replication::{NetMessages};
pub(crate) use crate::network::{NetworkId, ControlledBy, NetworkIdGenerator};
use crate::events;
use crate::network::{BrockerManager};

#[derive(Resource, Default)]
pub struct ClientDirectory {
    pub sessions: HashMap<u32, Entity>,
}

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
    pub fn new(net_id: u32, connection: u32, position: Vec3) -> Self {
        Self {
            player: Player::default(),
            net_id: NetworkId(net_id),
            controlled_by: ControlledBy { client_id: connection }, // On assigne la propriété
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
            .insert_resource(ClientDirectory::default())
            .add_systems(Update,((handle_new_players, handle_disconnected, apply_player_inputs), simulate_game).chain());

    }
}

fn handle_new_players(
    mut net: ResMut<BrockerManager>,
    mut reader: MessageReader<events::PlayerConnected>,
    mut id_gen: ResMut<NetworkIdGenerator>,
    mut commands: Commands,
    mut client_directory: ResMut<ClientDirectory>,
) {
    let Some(conn) = &net.connection else { return };

    for msg in reader.read() {
        let net_id = id_gen.next();
        let client_uuid = msg.client_id;

        let player_entity = commands.spawn(PlayerBundle::new(net_id.0, client_uuid, Vec3::ZERO)).id();
        client_directory.sessions.insert(client_uuid, player_entity);

        let welcome = NetMessages::WELCOME (client_uuid.to_string());

        if let Ok(bytes) = bincode::serialize(&welcome) {
            let data = bytes.into();
            let _ = net.peer.send(conn, &msg.stream_used, data);

            println!("[Logic] WELCOME envoyé à {}", client_uuid);
        }
    }

}

fn handle_disconnected(
    mut ev_disconnect : MessageReader<events::PlayerDisconnected>,
    mut commands: Commands,
    mut client_directory: ResMut<ClientDirectory>,
) {
    for ev in ev_disconnect.read() {
        if let Some(player_entity) = client_directory.sessions.remove(&ev.client_id) {
            commands.entity(player_entity).despawn();
            println!("[Logic] Player {} disconnected and despawned", ev.client_id);
        }
    }
}

fn apply_player_inputs(
    mut ev_input: MessageReader<events::PlayerInputEvent>,
    mut query: Query<&mut Transform, With<Player>>,
    client_directory: ResMut<ClientDirectory>,
) {
    for ev in ev_input.read() {
        if let Some(player_entity) = client_directory.sessions.get(&ev.client_id) {
            if let Ok((mut transform)) = query.get_mut(*player_entity) {

                let xdiff = f32::from(ev.input_data.is_right()) - f32::from(ev.input_data.is_left());
                let ydiff = f32::from(ev.input_data.is_up()) - f32::from(ev.input_data.is_down());
                transform.translation.x +=  xdiff * 5.0;
                transform.translation.y -= ydiff * 5.0;
            }
        }
    }
}

fn simulate_game(
    mut query : Query<&mut Transform, With<Player>>,
) {
    for mut transform in query.iter_mut() {
        //todo : faire le jeu
        transform.translation += Vec3::new(0.0, -0.2, 0.0);
    }
}