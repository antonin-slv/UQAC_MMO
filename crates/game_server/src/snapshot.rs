// server/src/snapshot.rs
use crate::dgs_network::{BrockerManager, NetworkId, ServerStats};
use bevy::prelude::*;
use bytes::Bytes;
use shared_replication::broker_topics::{BrokerMessageHeaders, Namespace, SecurityDomain, TopicBuilder};
use shared_replication::client_server::*;
use crate::events::{AssignedChunks};

const _AOI_RADIUS: f32 = 100.0;

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

    let snapshot_header = BrokerMessageHeaders::Snapshot as u8;

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

    match bincode::serialize(&personal_snapshot) {
        Ok(snapshot_as_bytes) => {
            let topic = TopicBuilder::new(SecurityDomain::PublicReadPrivateWrite, Namespace::Chunk)
                .append_grid(chunk_assigned.x, chunk_assigned.y) //todo : faire une vraie grille d'AOI
                .build();

            let mut payload = Vec::with_capacity(1 + snapshot_as_bytes.len());
            payload.push(snapshot_header);
            payload.extend(snapshot_as_bytes);
            net.client
                .publish_unreliable(topic, Bytes::from(payload));
        }
        Err(e) => eprintln!("Erreur de sérialisation bincode: {}", e),
    }
}
