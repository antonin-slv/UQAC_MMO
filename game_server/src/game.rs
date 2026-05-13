use std::collections::HashMap;
// server/src/player.rs
use bevy::prelude::*;
use game_sockets::{GameStream, GameStreamReliability};
use shared_replication::{NetMessages, STREAM_HANDSHAKE};
pub(crate) use crate::network::{NetworkId, ControlledBy, NetworkIdGenerator};
use crate::events;
use crate::network::NetworkManager;

#[derive(Resource, Default)]
pub struct ClientDirectory {
    pub sessions: HashMap<uuid::Uuid, Entity>,
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
            .insert_resource(ClientDirectory::default())
            .add_systems(Update,((handle_new_players, handle_disconnected, apply_player_inputs), simulate_game).chain());

    }
}

fn handle_new_players(
    mut net: ResMut<NetworkManager>,
    mut reader: MessageReader<events::PlayerConnected>,
    mut id_gen: ResMut<NetworkIdGenerator>,
    mut commands: Commands,
    mut client_directory: ResMut<ClientDirectory>,
) {
    for msg in reader.read() {
        let net_id = id_gen.next();
        let client_uuid = msg.client_id;

        let player_entity = commands.spawn(PlayerBundle::new(net_id.0, client_uuid, Vec3::ZERO)).id();
        client_directory.sessions.insert(client_uuid, player_entity);

        let welcome = NetMessages::WELCOME (client_uuid.to_string());

        if let Ok(bytes) = bincode::serialize(&welcome) {
            let data = bytes.into();
            let stream = GameStream::new(STREAM_HANDSHAKE, GameStreamReliability::Reliable);

            // 3. Envoi direct au client
            let target = game_sockets::GameConnection {connection_uuid : client_uuid };
            let _ = net.peer.send(&target, &stream, data);

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
                transform.translation.x += f32::from(ev.input_data.right) - f32::from(ev.input_data.left)  ;
                transform.translation.y -= f32::from(ev.input_data.up) - f32::from(ev.input_data.down)  ;
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