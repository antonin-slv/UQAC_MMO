use crate::dgs_entity_functions::{spawn_entity, Authority};
use crate::dgs_network::BrockerManager;
pub(crate) use crate::dgs_network::{ControlledBy, NetworkIdGenerator};
use crate::events;
use crate::events::AssignedChunks;
use bevy::prelude::*;
use broker_protocol::broker_message::NodeId;
use game_message::msg_entities::{EntityData, EntityType, NetComponent, NetworkEntityId};
use std::collections::HashMap;

#[derive(Resource, Default)]
pub struct ClientDirectory {
    pub sessions: HashMap<NodeId, Vec<NetworkEntityId>>,
}

#[derive(Resource, Default)]
pub struct EntityDirectory {
    pub entities: HashMap<NetworkEntityId, Entity>,
}

#[derive(Component, Default)]
pub struct Player {}

pub struct GameLogicPlugin;

#[derive(SystemSet, Debug, Hash, PartialEq, Eq, Clone)]
pub struct GameLogicSet;

impl Plugin for GameLogicPlugin {
    fn build(&self, app: &mut App) {
        // On enregistre les systèmes liés aux joueurs

        app.insert_resource(ClientDirectory::default())
            .insert_resource(EntityDirectory::default())
            .add_systems(
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
    mut entity_directory: ResMut<EntityDirectory>,
    chunk_manager: Res<AssignedChunks>,
) {
    if id_gen.server_id.is_none() {
        return;
    }
    for msg in reader.read() {
        let net_id = id_gen.next().unwrap();

        let mut player_pos =
            core_types::Vec2::new(msg.msg.chunk.x as f32 + 0.5, msg.msg.chunk.y as f32 + 0.5);
        player_pos *= chunk_manager.chunk_size;
        let player_pos = NetComponent::Position(player_pos);
        let player_comp = NetComponent::Type(EntityType::Player);

        let entity_data = EntityData {
            net_id,
            owner_id: msg.msg.client_id,
            updates: vec![player_pos, player_comp],
        };
        let _ = spawn_entity(
            &mut commands,
            Authority::Authoritative,
            &mut client_directory,
            &mut entity_directory,
            &entity_data,
        );

        println!(
            "[Logic] Player {} connected with entity {} and spawned at chunk ({}, {})",
            msg.msg.client_id, net_id, msg.msg.chunk.x, msg.msg.chunk.y
        );

        if !chunk_manager.assigned_chunks.contains(&msg.msg.chunk) {
            eprintln!("\t Warning : This isn't one of our chunks !");
        }
    }
}

fn handle_disconnected(
    mut ev_disconnect: MessageReader<events::PlayerDisconnected>,
    mut commands: Commands,
    mut client_directory: ResMut<ClientDirectory>,
    mut entity_directory: ResMut<EntityDirectory>,
) {
    for ev in ev_disconnect.read() {
        if let Some(player_entity_ids) = client_directory.sessions.remove(&ev.client_id) {
            for entity_id in player_entity_ids {
                if let Some(entity) = entity_directory.entities.remove(&entity_id) {
                    commands.entity(entity).despawn();
                    //todo : prévenir les gens qui écoutent.

                    println!("[Logic] Player {} disconnected and despawned", ev.client_id);
                }
            }
        }
    }
}

fn apply_player_inputs(
    mut ev_input: MessageReader<events::PlayerInputEvent>,
    mut query: Query<(&mut Transform, &mut Authority), With<Player>>,
    client_directory: ResMut<ClientDirectory>,
    entity_directory: ResMut<EntityDirectory>,
) {
    //todo : passer par les buffers
    for ev in ev_input.read() {
        if let Some(possible_player_entities) = client_directory.sessions.get(&ev.client_id) {
            if let Some(_) = possible_player_entities
                .iter()
                .find(|net_id| *net_id == &ev.entity_id)
            {
                if let Some(entity) = entity_directory.entities.get(&ev.entity_id) {
                    if let Ok((mut transform, autority)) = query.get_mut(*entity) {
                        if *autority == Authority::Ghost {
                            continue; // On n'applique les inputs que si le serveur a l'autorité
                        }
                        let last_input = ev.input_data.get_last_input();
                        if let Some(last_input) = last_input {
                            let xdiff =
                                f32::from(last_input.1.right) - f32::from(last_input.1.left);
                            let ydiff = f32::from(last_input.1.up) - f32::from(last_input.1.down);
                            transform.translation.x += xdiff * 5.0;
                            transform.translation.y -= ydiff * 5.0;
                        }
                    }
                }
            }
        }
    }
}

fn simulate_game(mut query: Query<(&mut Transform, &mut Authority), With<Player>>) {
    for (mut transform, autority) in query.iter_mut() {
        //todo : faire le jeu
        if *autority == Authority::Authoritative {
            transform.translation += Vec3::new(0.0, 0.5, 0.0);
        }
    }
}
