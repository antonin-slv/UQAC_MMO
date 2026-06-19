use crate::client_helper::{insert_net_component, update_net_component};
use crate::structs::{Chunking, ClientState, LocalControlledComponent, LocalPlayer};
use crate::{CameraBundle, RootBundle};
use bevy::app::{App, Plugin, PreUpdate, Update};
use bevy::asset::Assets;
use bevy::color::Color;
use bevy::mesh::{Mesh, Mesh2d};
use bevy::prelude::*;
use broker_client::{ClientNetworkEvent, MmoNetworkClient};
use broker_protocol::topics::{
    Namespace, SecurityDomain, TopicBuilder, AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION,
};
use core_types::chunks::GameChunkAera;
use core_types::get_chunk;
use game_message::msg_client_server::*;
use game_message::msg_entities::{EntityData, NetComponent, NetworkEntityId};
use game_message::GameMessageHeaders;
use std::mem;

#[derive(Component)]
struct NetIdComp(NetworkEntityId);

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
                (process_snapshots, what_is_my_chunks).run_if(in_state(ClientState::InGame)),
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
    mut chunking: ResMut<Chunking>,
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
                GameMessageHeaders::ClientWelcome => match payload.extract::<ClientWelcomeMsg>() {
                    Ok(msg) => {
                        local_player.chunk = msg.chunk;
                        local_player.net_id = msg.client_id;
                        chunking.chunk_size = msg.chunk_size;
                        println!("[Client] Got welcome message (ID: {})", msg.client_id);

                        let mut borders = GameChunkAera::from(msg.chunk).get_borders(1);
                        borders.push(msg.chunk.clone());
                        let topic_builder = TopicBuilder::new(
                            SecurityDomain::PublicReadPrivateWrite,
                            Namespace::Chunk,
                        );

                        for border_chunk in borders {
                            let topic = topic_builder.clone().append_chunk(&border_chunk).build();
                            net.client.subscribe(topic, 0);
                        }
                        next_state.set(ClientState::InGame);
                    }
                    Err(e) => {
                        println!("[Client] Got welcome error: {}", e);
                    }
                },
                GameMessageHeaders::Snapshot => match payload.extract::<PersonalSnapshot>() {
                    Ok(snapshot) => {
                        msg_snapshot.write(SnapshotMessage(snapshot));
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
    broker: Res<NetworkManager>,
    mut local_player: ResMut<LocalPlayer>,
    mut reader: MessageReader<SnapshotMessage>,
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
    mut query_net_entities: Query<(Entity, &NetIdComp, &mut Transform)>,
) {
    let mut entity_to_spawn: Vec<EntityData> = Vec::new();
    // On récupère tout les snapshot reçu lors de cette frame
    for msg in reader.read() {
        let snapshot = msg.0.clone();
        for mut net_entity in snapshot.entities {
            let existing_entity = query_net_entities
                .iter_mut()
                .find(|(_, existing_id, _)| existing_id.0 == net_entity.net_id);

            if let Some((ett, _, mut transform)) = existing_entity {
                for comp in net_entity.updates {
                    match comp {
                        NetComponent::Position(position) => {
                            transform.translation.x = position.x;
                            transform.translation.y = position.y;
                        }
                        _ => {
                            update_net_component(ett, &comp, &mut commands);
                        }
                    }
                }
                continue;
            }

            let index_of_spaw_nentity = entity_to_spawn
                .iter()
                .position(|e| e.net_id == net_entity.net_id);
            if let Some(index) = index_of_spaw_nentity {
                let rslt = entity_to_spawn.remove(index);
                for comp in rslt.updates {
                    if net_entity
                        .updates
                        .iter()
                        .find(|&f| mem::discriminant(f) == mem::discriminant(&comp))
                        .is_none()
                    {
                        net_entity.updates.push(comp); //on ne garde que les composants qui n'ont pas été plus tard.
                    }
                }
            }

            entity_to_spawn.push(net_entity);
        }
    }

    for entity in entity_to_spawn {
        println!(
            "Nouvelle entité réseau découverte : {} de {}",
            entity.net_id, entity.owner_id
        );
        println!("(je suis {})", broker.client.node_id.unwrap_or(0));

        let i_am_owner = entity.owner_id == broker.client.node_id.unwrap_or(0);

        if i_am_owner {
            local_player.entity_net_id = Some(entity.net_id);
        }

        let spawn_color = if i_am_owner {
            Color::srgb(0.9, 0.2, 0.7)
        } else {
            Color::srgb(0.2, 0.7, 0.9)
        };

        let bundle_to_spawn = (
            RootBundle {
                mesh: Mesh2d(meshes.add(Circle::new(10.0))),
                material: MeshMaterial2d(materials.add(spawn_color)),
            },
            NetIdComp(entity.net_id),
        );

        let mut entity_handle = commands.spawn(bundle_to_spawn);
        for component in entity.updates {
            insert_net_component(&mut entity_handle, &component)
        }

        if i_am_owner {
            entity_handle.insert(LocalControlledComponent);
        }
    }
}

fn what_is_my_chunks(
    broker: Res<NetworkManager>,
    my_entity: Query<&Transform, With<LocalControlledComponent>>,
    chunking: Res<Chunking>,
    mut local_player: ResMut<LocalPlayer>,
) {
    my_entity.iter().for_each(|transform| {
        let c_chunk = get_chunk(
            transform.translation.x,
            transform.translation.y,
            chunking.chunk_size,
        );

        if c_chunk != local_player.chunk {
            local_player.chunk = c_chunk;
            let mut borders = GameChunkAera::from(c_chunk).get_borders(1);
            borders.push(c_chunk.clone());
            let topic_builder =
                TopicBuilder::new(SecurityDomain::PublicReadPrivateWrite, Namespace::Chunk);

            for border_chunk in borders {
                let topic = topic_builder.clone().append_chunk(&border_chunk).build();
                broker.client.subscribe(topic, 0);
            }

            println!(
                "Je suis maintenant dans le chunk ({}, {})",
                c_chunk.x, c_chunk.y
            );
        }
    })
}
