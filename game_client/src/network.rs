use crate::{PlayerBundle};
use crate::structs::{ClientState, LocalPlayer};
use bevy::app::{App, Plugin, PreUpdate, Update};
use bevy::asset::Assets;
use bevy::color::Color;
use bevy::mesh::{Mesh, Mesh2d};
use bevy::prelude::*;
use bytes::{BufMut, BytesMut};
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream};
use shared_replication::{STREAM_HANDSHAKE};
use shared_replication::client_server::*;
use shared_replication::broker::{BrokerMessageHeaders, ClientId, SafeExtract};

#[derive(Component)]
struct NetworkEntity(u32);

// Pour stocker la connexion au serveur (pour savoir à qui envoyer nos inputs)
#[derive(Resource, Default)]
pub(crate) struct ServerConnection{
    pub(crate) game_connection: Option<GameConnection>,
    pub(crate) handshake_stream: Option<GameStream>,
}

// Notre Message Bevy pour les snapshots
#[derive(Message)]
struct SnapshotMessage(PersonalSnapshot);

// La ressource qui gère la librairie réseau
#[derive(Resource)]
pub struct NetworkManager {
    pub(crate) peer: GamePeer,
}

pub struct ClientNetworkPlugin;

impl Plugin for ClientNetworkPlugin {
    fn build(&self, app: &mut App) {

        // On enregistre les systèmes liés aux joueurs
        // 1. Initialisation de la librairie réseau
        let peer = GamePeer::new(QuicBackend::new());

        app.insert_resource(NetworkManager { peer })
            .insert_resource(ServerConnection::default())
            .insert_resource(LocalPlayer::default())
            // "event" réception de snapshot
            .add_message::<SnapshotMessage>()
            .add_systems(PreUpdate, network_bridge_system.run_if(in_state(ClientState::Connecting).or(in_state(ClientState::InGame))))
            .add_systems(Update, process_snapshots.run_if(in_state(ClientState::InGame)));
    }
}

// --- LE PONT RÉSEAU ---
// Lit la lib et génère des messages Bevy
fn network_bridge_system(
    mut net: ResMut<NetworkManager>,
    mut server_conn: ResMut<ServerConnection>,
    mut local_player: ResMut<LocalPlayer>, // On injecte notre nouvelle ressource
    mut msg_snapshot: MessageWriter<SnapshotMessage>,
    mut next_state: ResMut<NextState<ClientState>>,
) {
    while let Ok(Some(event)) = net.peer.poll() {
        match event {
            // ÉTAPE 1 : Le socket QUIC/UDP est ouvert
            GameNetworkEvent::Connected(conn) => {
                println!("[Client] Socket connecté. Attente de la création du stream");
                server_conn.game_connection = Some(conn.clone());

            }

            // ÉTAPE 2 & 3 : Réception des messages du serveur
            GameNetworkEvent::Message { stream, data, .. } => {

                let discard_message = BrokerMessageHeaders::DiscardedMessageBecauseYouKnow as u8;
                let header_byte = data.first().unwrap_or(&discard_message);
                let header = BrokerMessageHeaders::from(*header_byte);

                match header {
                    BrokerMessageHeaders::Broadcast => {
                        let data_len : u16 = u16::from_le_bytes(data[1..3].try_into().unwrap_or([0, 0]));
                        let content = &data[3..(3 + data_len as usize)];
                        if let Ok(snapshot) = bincode::deserialize::<PersonalSnapshot>(content) {
                            msg_snapshot.write(SnapshotMessage(snapshot));
                        } else {
                            eprintln!("[Client] Erreur de désérialisation du snapshot");
                        }
                    }
                    BrokerMessageHeaders::ClientWelcome => {
                        let client_id = ClientId::extract_from_slice(&data[1..5]);
                        if let Some(id) = client_id {
                            println!("[Client] Bienvenue ! Client ID : {}", id);
                            local_player.net_id = id;
                            next_state.set(ClientState::InGame);
                        } else {
                            eprintln!("[Client] Erreur lors de l'extraction du Client ID");
                            next_state.set(ClientState::LoginMenu);
                        }
                    }
                    _ => {}
                }
            }

            GameNetworkEvent::Disconnected(_) => {
                println!("[Client] Déconnecté du serveur.");
                server_conn.game_connection = None;
                server_conn.handshake_stream = None;
                local_player.net_id = 0;
                next_state.set(ClientState::LoginMenu)

            }

            GameNetworkEvent::StreamCreated(conn, stream) => {

                println!("[Client] Stream créé : {}", stream.real_stream_id());
                if stream.real_stream_id() == STREAM_HANDSHAKE {

                    let header = BrokerMessageHeaders::ClientHello as u8;
                    let pseudo = local_player.pseudo.clone().unwrap_or("NO_PSEUDO".to_string());
                    let pseudo = pseudo.as_bytes();
                    let pseudo_len = pseudo.len();
                    let pseudo_u16 = pseudo.len() as u16;

                    let mut hello_packet = BytesMut::with_capacity(1 + 2 + pseudo_len);
                    hello_packet.put_u8(header);
                    hello_packet.put_u16_le(pseudo_u16);
                    hello_packet.put_slice(pseudo);

                    let _ = net.peer.send(&conn, &stream, hello_packet.freeze());

                    println!("[Client] Stream de handshake prêt, requête de connexion envoyée !");
                    server_conn.handshake_stream = Some(stream);
                }
            }
            _ => {}
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
    // On récupère le dernier snapshot reçu lors de cette frame
    let mut latest_snapshot = None;
    for msg in reader.read() {
        latest_snapshot = Some(&msg.0);
    }

    let Some(snapshot) = latest_snapshot else {
        return;
    };

    for net_entity in &snapshot.entities {
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
                transform: Transform::from_xyz(net_entity.position[0], net_entity.position[1], 0.0),
            },
            NetworkEntity(net_entity.network_id),
        ));
    }
}
