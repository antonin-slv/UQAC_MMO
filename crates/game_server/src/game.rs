use crate::dgs_network::{BrockerManager, NetworkIdComponent};
pub(crate) use crate::dgs_network::{ControlledBy, NetworkIdGenerator};
use crate::events;
use crate::events::{AssignedChunks, Authority};
use bevy::prelude::*;
use broker_protocol::broker_message::NodeId;
use game_message::msg_entities::NetworkEntityId;
use std::collections::HashMap;

#[derive(Resource, Default)]
pub struct ClientDirectory {
    pub sessions: HashMap<NodeId, Vec<(Entity, NetworkEntityId)>>,
}

#[derive(Component, Default)]
pub struct Player {}

#[derive(Bundle)]
pub struct PlayerBundle {
    pub player: Player,
    pub net_id: NetworkIdComponent,
    pub controlled_by: ControlledBy,
    pub transform: Transform,
    pub global_transform: GlobalTransform,
    pub authority: Authority,
}

impl PlayerBundle {
    pub fn new(
        net_id: NetworkEntityId,
        owner_id: NodeId,
        position: Vec3,
        authority: Authority,
    ) -> Self {
        Self {
            player: Player::default(),
            net_id: NetworkIdComponent(net_id),
            controlled_by: ControlledBy {
                client_id: owner_id,
            }, // On assigne la propriété
            transform: Transform::from_translation(position),
            global_transform: GlobalTransform::default(),
            authority,
        }
    }
}

pub struct GameLogicPlugin;

#[derive(SystemSet, Debug, Hash, PartialEq, Eq, Clone)]
pub struct GameLogicSet;

impl Plugin for GameLogicPlugin {
    fn build(&self, app: &mut App) {
        // On enregistre les systèmes liés aux joueurs

        app.insert_resource(ClientDirectory::default()).add_systems(
            Update,
            (
                (handle_new_players, handle_disconnected, apply_player_inputs),
                simulate_game,
            )
                .chain()
                .in_set(GameLogicSet),
        );
    }
}

fn handle_new_players(
    _broker: ResMut<BrockerManager>,
    mut reader: MessageReader<events::PlayerConnected>,
    mut id_gen: ResMut<NetworkIdGenerator>,
    mut commands: Commands,
    mut client_directory: ResMut<ClientDirectory>,
    chunk_manager: Res<AssignedChunks>,
) {
    if id_gen.server_id.is_none() {
        return;
    }
    for msg in reader.read() {
        let net_id = id_gen.next().unwrap();
        let spawn_chunk = msg.msg.chunk;

        let mut player_pos = Vec3::new(spawn_chunk.x as f32 + 0.5, spawn_chunk.y as f32 + 0.5, 0.0);
        player_pos *= chunk_manager.chunk_size;

        let player_entity = commands
            .spawn(PlayerBundle::new(
                net_id,
                msg.msg.client_id,
                player_pos,
                Authority::Authoritative,
            ))
            .id();
        client_directory
            .sessions
            .entry(msg.msg.client_id)
            .or_insert_with(Vec::new)
            .push((player_entity, net_id));

        println!(
            "[Logic] Player {} connected with net_id {} and spawned at chunk ({}, {})",
            msg.msg.client_id, net_id, spawn_chunk.x, spawn_chunk.y
        );
    }
}

fn handle_disconnected(
    mut ev_disconnect: MessageReader<events::PlayerDisconnected>,
    mut commands: Commands,
    mut client_directory: ResMut<ClientDirectory>,
) {
    for ev in ev_disconnect.read() {
        if let Some(player_entity) = client_directory.sessions.remove(&ev.client_id) {
            for player_entity in player_entity {
                commands.entity(player_entity.0).despawn();
                //todo : prévenir les gens qui écoutent.
            }
            println!("[Logic] Player {} disconnected and despawned", ev.client_id);
        }
    }
}

fn apply_player_inputs(
    mut ev_input: MessageReader<events::PlayerInputEvent>,
    mut query: Query<(&mut Transform, &mut Authority), With<Player>>,
    client_directory: ResMut<ClientDirectory>,
) {
    for ev in ev_input.read() {
        if let Some(possible_player_entities) = client_directory.sessions.get(&ev.client_id) {
            if let Some(player_entity) = possible_player_entities
                .iter()
                .find(|(_, net_id)| *net_id == ev.entity_id)
            {
                if let Ok((mut transform, autority)) = query.get_mut(player_entity.0) {
                    if *autority == Authority::Ghost {
                        continue; // On n'applique les inputs que si le serveur a l'autorité
                    }
                    let xdiff =
                        f32::from(ev.input_data.is_right()) - f32::from(ev.input_data.is_left());
                    let ydiff =
                        f32::from(ev.input_data.is_up()) - f32::from(ev.input_data.is_down());
                    transform.translation.x += xdiff * 5.0;
                    transform.translation.y -= ydiff * 5.0;
                }
            }
        }
    }
}

fn simulate_game(mut query: Query<(&mut Transform, &mut Authority), With<Player>>) {
    for (mut transform, autority) in query.iter_mut() {
        //todo : faire le jeu
        if *autority == Authority::Authoritative {
            transform.translation += Vec3::new(0.0, 0.05, 0.0);
        }
    }
}
