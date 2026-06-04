// server/src/snapshot.rs
use crate::dgs_network::{BrockerManager, NetworkId, ServerStats};
use crate::events::AssignedChunks;
use bevy::prelude::*;
use broker_protocol::broker_topics::{Namespace, SecurityDomain, TopicBuilder};
use game_message::msg_client_server::*;

pub struct SnapshotPlugin;

impl Plugin for SnapshotPlugin {
    fn build(&self, app: &mut App) {
        // todo : mieux controller les moments d'envois de snapshots (actuellement 60 fps -> passer à 20 en tournant entre les clients ?)
        app.add_systems(PostUpdate, broadcast_snapshots);
    }
}

fn broadcast_snapshots(
    net: ResMut<BrockerManager>,
    _server_data: ResMut<ServerStats>,
    chunk_assigned: ResMut<AssignedChunks>,
    query_all_entities: Query<(&NetworkId, &Transform)>,
) {
    if !net.client.is_connected() {
        return;
    }

    if chunk_assigned.chunk.is_none() {
        return;
    }

    let chunk_assigned = chunk_assigned.chunk.as_ref().unwrap();

    let entity_count = query_all_entities.iter().count();
    if entity_count == 0 {
        return;
    }

    // Pré-calcul de l'état du monde ---
    let mut precomputed_targets = Vec::with_capacity(entity_count);

    for (net_id, transform) in query_all_entities.iter() {
        let snapshot = EntitySnapshot {
            network_id: net_id.0,
            position: transform.translation.truncate().to_array(),
        };
        precomputed_targets.push((snapshot, transform.translation));
    }
    let mut personal_snapshot = PersonalSnapshot {
        entities: Vec::with_capacity(entity_count),
    };
    for (target_snapshot, _) in &precomputed_targets {
        personal_snapshot.entities.push(*target_snapshot);
    }
    let topic = TopicBuilder::new(SecurityDomain::PublicReadPrivateWrite, Namespace::Chunk)
        .append_chunk(&chunk_assigned) //todo : faire une vraie grille d'AOI
        .build();
    let msg = SnapshotMsg {
        snapshot: personal_snapshot,
    };
    net.client.publish_unreliable(topic, &msg);
}
