// server/src/snapshot.rs
use crate::dgs_entity_functions::{Authority, EntityTypeComponent, InputComponent};
use crate::dgs_network::{BrockerManager, NetworkIdComponent, ServerStats};
use crate::events::{AssignedChunks, FastMap};
use crate::game::{ClientDirectory, ControlledBy, EntityDirectory};
use bevy::prelude::*;
use broker_protocol::topics::{Namespace, SecurityDomain, TopicBuilder};
use core_types::chunks::GameChunk;
use core_types::get_chunk;
use game_message::msg_client_server::*;
use game_message::msg_dgs;
use game_message::msg_entities::{EntityData, NetComponent};

pub struct SnapshotPlugin;

#[derive(SystemSet, Debug, Hash, PartialEq, Eq, Clone)]
pub struct SnapshotSet;

impl Plugin for SnapshotPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(
            PostUpdate,
            (
                update_and_send_authority_pre_snapshot,
                broadcast_snapshots,
                update_existence,
            )
                .chain()
                .in_set(SnapshotSet),
        );
    }
}

// gère le passage de margins. Actuellement : pas de marge. Si dans ghost chunk, alors handoff.
fn update_and_send_authority_pre_snapshot(
    broker: Res<BrockerManager>,
    chunk_assigned: ResMut<AssignedChunks>,
    mut query_entities: Query<(
        &Transform,
        &ControlledBy,
        &NetworkIdComponent,
        &EntityTypeComponent,
        &InputComponent,
        &mut Authority,
    )>,
) {
    let chunk_size = chunk_assigned.chunk_size;
    let my_chunks = &chunk_assigned.assigned_chunks;

    let mut handoffmap = FastMap::default();
    for (transform, owner, ett_net_id, entity, inputs, mut auth) in query_entities.iter_mut() {
        if *auth != Authority::Authoritative {
            continue;
        }

        let translation = transform.translation.xy();
        let entity_chunk = get_chunk(translation[0], translation[1], chunk_size);
        if !my_chunks.contains(&entity_chunk) {
            *auth = Authority::LastAuthFrame;
        } else {
            continue;
        }

        let mut updates: Vec<NetComponent> = Vec::new();
        updates.push((*owner).into());
        updates.push((*ett_net_id).into());
        updates.push(NetComponent::Position(core_types::Vec2::new(
            translation.x,
            translation.y,
        )));
        updates.push(NetComponent::Type(entity.0));
        updates.push(NetComponent::Inputs(inputs.input_buffer.clone()));
        updates.push(NetComponent::Velocity(core_types::Vec2::new(0.0, 0.0)));

        let entity_data = EntityData {
            net_id: ett_net_id.0,
            owner_id: owner.client_id,
            updates,
        };
        println!(
            "Relieving Author of Ett {} for {:?}",
            ett_net_id.0, entity_chunk
        );

        handoffmap
            .entry(entity_chunk)
            .or_insert_with(Vec::new)
            .push(entity_data);
    }

    for (chunk, data) in handoffmap {
        let message = msg_dgs::EntityHandOff { data };

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
    query_all_entities: Query<(
        &NetworkIdComponent,
        &ControlledBy,
        &Transform,
        &Authority,
        &EntityTypeComponent,
    )>,
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
    let mut precomputed_targets: FastMap<GameChunk, PersonalSnapshot> = FastMap::default();

    for (net_id, owner, transform, auth, entity_type) in query_all_entities.iter() {
        if *auth == Authority::Ghost {
            continue;
        }

        let trans = transform.translation.truncate().to_array();
        let replicated_components = vec![
            NetComponent::Velocity(core_types::Vec2::new(0.0, 0.0)),
            NetComponent::Position(core_types::Vec2::new(
                transform.translation.x,
                transform.translation.y,
            )),
            NetComponent::Type(entity_type.0),
        ];

        let entity_snapshot = EntityData {
            net_id: net_id.0,
            owner_id: owner.client_id,
            updates: replicated_components,
        };
        let entity_chunk = get_chunk(trans[0], trans[1], chunk_size);

        precomputed_targets
            .entry(entity_chunk)
            .or_insert(PersonalSnapshot { entities: vec![] })
            .entities
            .push(entity_snapshot);
    }

    for (chunk, snapshot) in precomputed_targets.iter() {
        let topic = TopicBuilder::new(SecurityDomain::PublicReadPrivateWrite, Namespace::Chunk)
            .append_chunk(&chunk)
            .build();

        net.client.publish_unreliable(topic, snapshot);
    }
}

// 3 cas possibles :
//      -- dans mes chunks : authoritative
//      -- dans mes ghost chunks : ghost
//      -- ailleurs : despawn
fn update_existence(
    mut commands: Commands,
    chunk_assigned: ResMut<AssignedChunks>,
    mut query_entities: Query<(
        Entity,
        &NetworkIdComponent,
        &Transform,
        &ControlledBy,
        &mut Authority,
    )>,
    mut client_directory: ResMut<ClientDirectory>,
    mut entity_directory: ResMut<EntityDirectory>,
) {
    let chunk_size = chunk_assigned.chunk_size;
    let my_chunks = &chunk_assigned.assigned_chunks;
    let my_ghost_chunks = &chunk_assigned.ghost_chunks;

    for (entity, net_id, transform, owner, mut auth) in query_entities.iter_mut() {
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
                    entities.retain(|&e| e != net_id.0);
                });
            entity_directory.entities.remove(&net_id.0);
            commands.entity(entity).despawn();
            println!("Delete ref to entity {:?}", entity);
        }
    }
}
