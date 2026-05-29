// server/src/snapshot.rs
use bevy::prelude::*;
use bytes::{BufMut, BytesMut};
use shared_replication::client_server::*;
use game_sockets::{GameStream, GameStreamReliability};
use shared_replication::broker::BrokerMessageHeaders;
//use crate::game::{ControlledBy, Player};
use crate::network::{NetworkId, BrockerManager, ServerStats};
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
    server_data: ResMut<ServerStats>,
    query_all_entities: Query<(&NetworkId, &Transform)>,
) {
    let connexion;

    if let Some(co) = net.connection {
        connexion = co;
    } else {
        eprintln!("Broadcast before connexion with the broker");
        return;
    }

    let entity_count = query_all_entities.iter().count();
    if entity_count == 0 { return; }

    // Pré-calcul de l'état du monde ---
    let mut precomputed_targets = Vec::with_capacity(entity_count);

    for (net_id, transform) in query_all_entities.iter() {
        let snapshot = EntitySnapshot {
            network_id: net_id.0,
            position: transform.translation.truncate().to_array(),
        };
        precomputed_targets.push((snapshot, transform.translation));
    }

    let stream = GameStream::new(shared_replication::STREAM_SNAPSHOTS, GameStreamReliability::Unreliable);

    let mut personal_snapshot = PersonalSnapshot {
        entities: Vec::with_capacity(entity_count)
    };
    for (target_snapshot, _) in &precomputed_targets {
        personal_snapshot.entities.push(*target_snapshot);
    }


    match bincode::serialize(&personal_snapshot) {
        Ok(snapshot_as_bytes) => {
            let publish_head = BrokerMessageHeaders::Publish;
            //extract topic from server_data
            let topic = server_data.topic;
            let data_len = snapshot_as_bytes.len();
            let data_len_u16 = data_len as u16;
            let data_len_bytes = data_len_u16.to_le_bytes();
            let mut data = BytesMut::with_capacity(1 + topic.len() + data_len_bytes.len() + data_len);
            data.put_u8(publish_head as u8);
            data.put_slice(&topic);
            data.put_slice(&data_len_bytes);
                data.put_slice(&snapshot_as_bytes);

            // Envoi du snapshot !

            if let Err(e) = net.peer.send(&connexion, &stream, data.freeze()) {
                eprintln!("Erreur d'envoi du snapshot au broker message: {}", e);
            }
        }
        Err(e) => eprintln!("Erreur de sérialisation bincode: {}", e),
    }
}