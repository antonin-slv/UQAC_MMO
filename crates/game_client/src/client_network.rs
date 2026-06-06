use crate::PlayerBundle;
use crate::structs::{ClientState, LocalPlayer};
use bevy::app::{App, Plugin, PreUpdate, Update};
use bevy::asset::Assets;
use bevy::color::Color;
use bevy::mesh::{Mesh, Mesh2d};
use bevy::prelude::*;
use broker_client::{ClientNetworkEvent, MmoNetworkClient};
use broker_protocol::topics::{
    AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION, SecurityDomain, TopicBuilder,
};
use game_message::GameMessageHeaders;
use game_message::msg_client_server::*;
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
            ClientNetworkEvent::Ready => {
                local_player.net_id = net.client.node_id.unwrap_or(0);
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

                let topic = TopicBuilder::new(
                    SecurityDomain::PrivateReadPublicWrite,
                    AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION,
                )
                .build();
                let payload = ClientHelloMsg { pseudo };

                net.client.publish_reliable(topic, &payload);
            }

            // ÉTAPE 2 : Réception des données du jeu
            ClientNetworkEvent::DataReceived {
                client_id: _,
                stream: _,
                mut payload,
            } => match payload.header {
                GameMessageHeaders::ClientWelcome => {
                    let msg = payload.extract::<ClientWelcomeMsg>();
                    match msg {
                        Ok(msg) => {
                            local_player.chunk = msg.chunk;
                            local_player.net_id = msg.client_id;
                            println!("[Client] Got welcome message (ID: {})", msg.client_id);
                            next_state.set(ClientState::InGame);
                        }
                        Err(e) => {
                            println!("[Client] Got welcome error: {}", e);
                        }
                    }
                }
                GameMessageHeaders::Snapshot => match payload.extract::<SnapshotMsg>() {
                    Ok(snapshot) => {
                        msg_snapshot.write(SnapshotMessage(snapshot.snapshot));
                    }
                    Err(e) => {
                        eprintln!("[Client] Erreur de parsing du Snapshot : {}", e);
                    }
                },
                _ => {}
            },

            // ÉTAPE 3 : Déconnexion
            ClientNetworkEvent::Disconnected(node_id) => {
                if node_id != 0 && local_player.net_id != node_id {
                    continue;
                }
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
    let mut entity_to_spawn: Vec<EntitySnapshot> = Vec::new();
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

            let index_of_spawnentity = entity_to_spawn
                .iter()
                .position(|e| e.network_id == net_entity.network_id);
            if let Some(index) = index_of_spawnentity {
                entity_to_spawn.remove(index);
            }

            entity_to_spawn.push(net_entity);
        }
    }

    for entity in entity_to_spawn {
        println!("Nouvelle entité réseau découverte : {}", entity.network_id);
        commands.spawn((
            PlayerBundle {
                mesh: Mesh2d(meshes.add(Circle::new(10.0))),
                material: MeshMaterial2d(materials.add(Color::srgb(0.2, 0.7, 0.9))),
                transform: Transform::from_xyz(entity.position[0], entity.position[1], 0.0),
            },
            NetworkEntity(entity.network_id),
        ));
    }
}
