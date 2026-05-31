use crate::PlayerBundle;
use crate::structs::{ClientState, LocalPlayer};
use bevy::app::{App, Plugin, PreUpdate, Update};
use bevy::asset::Assets;
use bevy::color::Color;
use bevy::mesh::{Mesh, Mesh2d};
use bevy::prelude::*;
use shared_replication::broker_client::{ClientNetworkEvent, MmoNetworkClient};
use shared_replication::broker_message::ClientId;
use shared_replication::broker_topics::{
    AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION, BrokerMessageHeaders, SecurityDomain, TopicBuilder,
};
use shared_replication::client_server::*;
#[derive(Component)]
struct NetworkEntity(u32);

#[derive(Message)]
struct SnapshotMessage(PersonalSnapshot);

// La ressource qui gère la librairie réseau, utilisant maintenant notre API de haut niveau
#[derive(Resource)]
pub struct NetworkManager {
    pub client: MmoNetworkClient,
}

pub struct ClientNetworkPlugin;

impl Plugin for ClientNetworkPlugin {
    fn build(&self, app: &mut App) {
        // 1. Initialisation de la librairie réseau unifiée
        let client = MmoNetworkClient::new();

        app.insert_resource(NetworkManager { client })
            .insert_resource(LocalPlayer::default())
            // Plus besoin de ServerConnection !
            .add_message::<SnapshotMessage>()
            .add_systems(
                PreUpdate,
                network_bridge_system
                    .run_if(in_state(ClientState::Connecting).or(in_state(ClientState::InGame))),
            )
            .add_systems(
                Update,
                process_snapshots.run_if(in_state(ClientState::InGame)),
            );
    }
}

// --- LE PONT RÉSEAU ---
// Lit la lib et génère des messages Bevy
fn network_bridge_system(
    mut net: ResMut<NetworkManager>,
    mut local_player: ResMut<LocalPlayer>,
    mut msg_snapshot: MessageWriter<SnapshotMessage>,
    mut next_state: ResMut<NextState<ClientState>>,
) {
    while let Some(event) = net.client.poll() {
        match event {
            // ÉTAPE 1 : Connecté au Broker. On envoie immédiatement le Handshake
            ClientNetworkEvent::Ready => {
                println!("[Client] Client prêt. Envoi du Handshake...");
                let topic_auth_recieve = TopicBuilder::new(
                    SecurityDomain::PublicReadPrivateWrite,
                    AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION,
                )
                .build();
                net.client.subscribe(topic_auth_recieve, 0);

                let pseudo = local_player
                    .pseudo
                    .clone()
                    .unwrap_or("NO_PSEUDO".to_string());
                // On publie de manière fiable
                net.client.actual_game_client_not_server_say_hello(pseudo);
            }

            // ÉTAPE 2 : Réception des données du jeu
            ClientNetworkEvent::DataReceived { stream: _, payload } => {
                if payload.is_empty() {
                    continue;
                }

                // Le payload est PUR (le broker a déjà retiré ses propres tags réseaux).
                // Le 1er octet est donc forcément un tag métier du jeu (ClientWelcome, Snapshot, etc.)
                let header_byte = payload[0];
                let header = BrokerMessageHeaders::from(header_byte);

                match header {
                    BrokerMessageHeaders::ClientWelcome => {
                        if payload.len() >= 5 {
                            let id = ClientId::from_le_bytes(payload[1..5].try_into().unwrap());
                            let x_chunk = i32::from_le_bytes(payload[5..9].try_into().unwrap());
                            let y_chunk = i32::from_le_bytes(payload[9..13].try_into().unwrap());
                            println!("[Client] Bienvenue ! Client ID : {}", id);
                            local_player.net_id = id;
                            local_player.pseudo = None;
                            local_player.x_chunk = x_chunk;
                            local_player.y_chunk = y_chunk;
                            next_state.set(ClientState::InGame);
                        }
                    }

                    // ⚠️ ATTENTION SUR CE POINT (Voir explication plus bas)
                    // Puisqu'on ne vérifie plus le tag "Broadcast" du broker, il faut que ton serveur
                    // ajoute un tag "Snapshot" au début des données qu'il envoie.
                    BrokerMessageHeaders::Snapshot => {
                        // On désérialise les données bincode juste après l'octet du header [1..]
                        if let Ok(snapshot) =
                            bincode::deserialize::<PersonalSnapshot>(&payload[1..])
                        {
                            msg_snapshot.write(SnapshotMessage(snapshot));
                        } else {
                            eprintln!("[Client] Erreur de désérialisation du snapshot");
                        }
                    }
                    _ => {}
                }
            }

            // ÉTAPE 3 : Déconnexion
            ClientNetworkEvent::Disconnected => {
                println!("[Client] Déconnecté du serveur.");
                local_player.net_id = 0;
                next_state.set(ClientState::LoginMenu);
            }
            ClientNetworkEvent::Connected => {
                println!("[Client] ClientBrokerLib Connected. Waiting for readyness");
            }
        }
    }
}

fn process_snapshots(
    mut reader: MessageReader<SnapshotMessage>,
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
    mut query_net_entities: Query<(Entity, &NetworkEntity, &mut Transform)>,
) {
    // On récupère tout les snapshot reçu lors de cette frame
    for msg in reader.read() {
        let snapshot = msg.0.clone();
        for net_entity in snapshot.entities {
            let existing_entity = query_net_entities
                .iter_mut()
                .find(|(_, existing_id, _)| existing_id.0 == net_entity.network_id);

            if let Some((_, _, mut transform)) = existing_entity {
                transform.translation.x = net_entity.position[0];
                transform.translation.z = net_entity.position[1];
                continue;
            }

            println!(
                "Nouvelle entité réseau découverte : {}",
                net_entity.network_id
            );

            commands.spawn((
                PlayerBundle {
                    mesh: Mesh2d(meshes.add(Circle::new(10.0))),
                    material: MeshMaterial2d(materials.add(Color::srgb(0.2, 0.7, 0.9))),
                    transform: Transform::from_xyz(
                        net_entity.position[0],
                        net_entity.position[1],
                        0.0,
                    ),
                },
                NetworkEntity(net_entity.network_id),
            ));
        }
    }
}
