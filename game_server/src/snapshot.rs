// server/src/snapshot.rs
use bevy::prelude::*;
use bytes::Bytes;
use shared_replication::client_server::*;
use game_sockets::{GameConnection, GameStream, GameStreamReliability};
use crate::game::{ControlledBy, Player};
use crate::network::{NetworkManager, NetworkId};
const AOI_RADIUS: f32 = 100.0;

pub struct SnapshotPlugin;

impl Plugin for SnapshotPlugin {
    fn build(&self, app: &mut App) {
        // todo : mieux controller les moments d'envois de snapshots (actuellement 60 fps -> passer à 20 en tournant entre les clients ?)
        app.add_systems(PostUpdate, broadcast_aoi_snapshots);
    }
}

fn broadcast_aoi_snapshots(
    net: ResMut<NetworkManager>,
    //todo : Stocker les joueurs dans une hashmap pour éviter le double parcours
    query_receivers: Query<(&ControlledBy, &Transform), With<Player>>,
    query_all_entities: Query<(&NetworkId, &Transform)>,
) {
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

    // --- BOUCLE D'ENVOI (Sur les joueurs uniquement) ---
    for (player_net_id, player_transform) in query_receivers.iter() {

        let mut personal_snapshot = PersonalSnapshot {
            entities: Vec::with_capacity(entity_count)
        };

        // On filtre ce qui est autour du joueur
        for (target_snapshot, target_translation) in &precomputed_targets {
            let distance = player_transform.translation.distance(*target_translation);

            if distance <= AOI_RADIUS {
                personal_snapshot.entities.push(*target_snapshot);
            }
        }

        // On sérialise et on envoie avec net_lib
        match bincode::serialize(&personal_snapshot) {
            Ok(bytes) => {
                let data = Bytes::from(bytes);

                // On transforme l'Uuid en GameConnection
                let conn = GameConnection::from(player_net_id.owner_uuid);

                // Envoi du snapshot !
                if let Err(e) = net.peer.send(&conn, &stream, data) {
                    eprintln!("Erreur d'envoi du snapshot à {}: {:?}", player_net_id.owner_uuid, e);
                }
            }
            Err(e) => eprintln!("Erreur de sérialisation bincode: {}", e),
        }
    }
}