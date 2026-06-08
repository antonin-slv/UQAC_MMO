// server/src/snapshot.rs
use crate::dgs_network::{BrockerManager, NetworkIdComponent, ServerStats};
use crate::dgs_to_dgs_message::{FullEntityMetaData, FullEntityState};
use crate::events::{AssignedChunks, Authority, FastMap};
use crate::game::{ClientDirectory, ControlledBy};
use bevy::prelude::*;
use broker_protocol::topics::{Namespace, SecurityDomain, TopicBuilder};
use core_types::chunks::{GameChunk, GameChunkAera};
use core_types::get_chunk;
use game_message::core_types::SerializedGameChunkAera;
use game_message::msg_client_server::*;
use game_message::msg_dgs;

pub struct SnapshotPlugin;

#[derive(SystemSet, Debug, Hash, PartialEq, Eq, Clone)]
pub struct SnapshotSet;

impl Plugin for SnapshotPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(
            PostUpdate,
            (
                update_and_send_autority_pre_snapshot,
                broadcast_snapshots,
                update_existence,
            )
                .chain()
                .in_set(SnapshotSet),
        );
    }
}

// gère le passage de margins. Actuellement : pas de marge. Si dans ghost chunk, alors handoff.
fn update_and_send_autority_pre_snapshot(
    broker: Res<BrockerManager>,
    chunk_assigned: ResMut<AssignedChunks>,
    mut query_entities: Query<(&Transform, &ControlledBy, &mut Authority)>,
) {
    let chunk_size = chunk_assigned.chunk_size;
    let my_chunks = &chunk_assigned.assigned_chunks;

    let mut handoffmap = FastMap::default();
    for (transform, owner, mut auth) in query_entities.iter_mut() {
        if *auth != Authority::Authoritative {
            continue;
        }

        let translation = transform.translation.xy();
        let entity_chunk = get_chunk(translation[0], translation[1], chunk_size);
        if !my_chunks.contains(&entity_chunk) {
            *auth = Authority::LastAuthFrame;
        }
        let entity_meta_data = FullEntityMetaData {
            entity_type: 0,
            health: 0,
            extra_data: vec![],
        };

        let entity_data = FullEntityState {
            network_entity_id: 0,
            owner_node_id: owner.client_id,
            position: translation.to_array(),
            velocity: [0.0, 0.0],
            meta_data: entity_meta_data,
        };

        handoffmap
            .entry(entity_chunk)
            .or_insert_with(Vec::new)
            .push(bitcode::encode(&entity_data));
    }

    for (chunk, vec_of_serialized_entities) in handoffmap {
        let message = msg_dgs::EntityStateTransferHandoff {
            chunk_handoff: false,
            entity_handoff: true,
            origin_aera: SerializedGameChunkAera::from(GameChunkAera::from(chunk)),
            data: vec_of_serialized_entities,
        };

        broker.client.publish_reliable(
            TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ChunkEntityHandOff)
                .append_chunk(&chunk)
                .build(),
            &message,
        );
    }
}

fn broadcast_snapshots(
    net: ResMut<BrockerManager>,
    _server_data: ResMut<ServerStats>,
    chunk_assigned: ResMut<AssignedChunks>,
    query_all_entities: Query<(&NetworkIdComponent, &ControlledBy, &Transform, &Authority)>,
) {
    if !net.client.is_connected() {
        return;
    }

    let chunk_size = chunk_assigned.chunk_size;

    let entity_count = query_all_entities.iter().count();
    if entity_count == 0 {
        return;
    }

    // Pré-calcul de l'état du monde ---
    let mut precomputed_targets: FastMap<GameChunk, Vec<EntitySnapshot>> = FastMap::default();

    for (net_id, owner, transform, auth) in query_all_entities.iter() {
        if *auth == Authority::Ghost {
            continue;
        }

        let trans = transform.translation.truncate().to_array();
        let entity_snapshot = EntitySnapshot {
            network_id: net_id.0,
            owner_id: owner.client_id,
            position: trans,
        };
        let entity_chunk = get_chunk(trans[0], trans[1], chunk_size);
        precomputed_targets
            .entry(entity_chunk)
            .or_insert_with(Vec::new)
            .push(entity_snapshot);
    }

    for (chunk, snapshot) in precomputed_targets.iter() {
        let personal_snapshot = PersonalSnapshot {
            entities: snapshot.clone(),
        };
        let topic = TopicBuilder::new(SecurityDomain::PublicReadPrivateWrite, Namespace::Chunk)
            .append_chunk(&chunk)
            .build();
        let msg = SnapshotMsg {
            snapshot: personal_snapshot,
        };
        net.client.publish_unreliable(topic, &msg);
    }
}

// 3 cas possibles :
//      -- dans mes chunks : authoritative
//      -- dans mes ghost chunks : ghost
//      -- ailleurs : despawn
fn update_existence(
    mut commands: Commands,
    chunk_assigned: ResMut<AssignedChunks>,
    mut query_entities: Query<(Entity, &Transform, &ControlledBy, &mut Authority)>,
    mut client_directory: ResMut<ClientDirectory>,
) {
    let chunk_size = chunk_assigned.chunk_size;
    let my_chunks = &chunk_assigned.assigned_chunks;
    let my_ghost_chunks = &chunk_assigned.ghost_chunks;

    for (entity, transform, owner, mut auth) in query_entities.iter_mut() {
        if *auth == Authority::LastAuthFrame {
            *auth = Authority::Ghost;
        }
        let trans = transform.translation.truncate();
        let entity_chunk = get_chunk(trans[0], trans[1], chunk_size);

        if my_chunks.contains(&entity_chunk) || my_ghost_chunks.contains(&entity_chunk) {
            continue;
        } else {
            client_directory
                .sessions
                .get_mut(&owner.client_id)
                .map(|entities| {
                    entities.retain(|&e| e.0 != entity);
                });
            commands.entity(entity).despawn();
        }
    }
}
