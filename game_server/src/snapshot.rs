// server/src/snapshot.rs
use bevy::prelude::*;
use shared_replication::{EntitySnapshot, PersonalSnapshot, ServerMessage};
use crate::network::NetworkSender;
use crate::game::NetworkId;

const AOI_RADIUS: f32 = 100.0;

pub struct SnapshotPlugin;

impl Plugin for SnapshotPlugin {
    fn build(&self, app: &mut App) {
        // On peut même utiliser un "Timer" pour n'envoyer des snapshots que 20 fois par seconde (Tickrate)
        app.add_systems(Update, broadcast_aoi_snapshots);
    }
}

fn broadcast_aoi_snapshots(
    sender: Res<NetworkSender>,
    query_players: Query<(&NetworkId, &Transform)>,
) {
    // --- OPTIMISATION 1 : Pré-calcul des Snapshots ---
    // On compte le nombre de joueurs pour allouer la mémoire exacte d'un coup (évite les réallocations)
    let player_count = query_players.iter().count();
    let mut precomputed_targets = Vec::with_capacity(player_count);

    // On parcourt tout le monde UNE SEULE FOIS pour générer les DTOs
    for (net_id, transform) in query_players.iter() {
        let snapshot = EntitySnapshot {
            network_id: net_id.0,
            position: transform.translation.truncate().to_array(), // retire le z
        };
        // On stocke le snapshot ET la position 3D (pour le calcul de distance qui va suivre)
        precomputed_targets.push((snapshot, transform.translation));
    }

    // --- BOUCLE D'ENVOI ---
    for (recv_net_id, receiver_transform) in query_players.iter() {

        let mut personal_snapshot = PersonalSnapshot {
            // Optimisation : On alloue la capacité max possible pour éviter que le Vec ne grandisse dynamiquement
            entities: Vec::with_capacity(player_count)
        };

        // 2. On itère sur notre liste PRÉ-CALCULÉE
        for (target_snapshot, target_translation) in &precomputed_targets {

            // Calcul de distance mathématique
            let distance = receiver_transform.translation.distance(*target_translation);

            // 3. Le filtre AOI
            if distance <= AOI_RADIUS {
                // Copie directe vers le snapshot
                personal_snapshot.entities.push(*target_snapshot);
            }
        }

        // 4. On sérialise le snapshot final contenant tous les joueurs dans l'AOI
        match bincode::serialize(&personal_snapshot) {
            Ok(bytes) => {
                let _ = sender.0.send(ServerMessage::SendTo(recv_net_id.0, bytes));
            }
            Err(e) => eprintln!("Erreur de sérialisation bincode: {}", e),
        }
    }
}