use crate::{PlayerBundle};
use crate::structs::{ClientState, LocalPlayer};
use bevy::app::{App, Plugin, PreUpdate, Update};
use bevy::asset::Assets;
use bevy::color::Color;
use bevy::mesh::{Mesh, Mesh2d};
use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use shared_replication::NetMessages::WELCOME;
use shared_replication::{NetMessages, PersonalSnapshot, STREAM_HANDSHAKE, STREAM_SNAPSHOTS};
use std::env;

#[derive(Component)]
struct NetworkEntity(u32);

// Pour stocker la connexion au serveur (pour savoir à qui envoyer nos inputs)
#[derive(Resource, Default)]
pub(crate) struct ServerConnection(pub(crate) Option<GameConnection>);

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

        let args: Vec<String> = env::args().collect();

        println!("{}", args[1]);
        let _ip = args[1].clone();
        println!("{:?}", args[2]);
        let port = args[2].clone();
        let _port = port.parse::<u16>().expect("Le port doit être un nombre");
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
                println!("[Client] Socket connecté. Envoi de la requête de jointure...");
                server_conn.0 = Some(conn.clone());

                // On prépare notre demande de connexion
                let join_req = NetMessages::JOIN(local_player.pseudo.clone().unwrap_or("NO_PSEUDO".to_string()));

                if let Ok(bytes) = bincode::serialize(&join_req) {
                    let data = Bytes::from(bytes);
                    // todo : make this reliable
                    let stream =
                        GameStream::new(STREAM_HANDSHAKE, GameStreamReliability::Unreliable);
                    let _ = net.peer.send(&conn, &stream, data);
                }
            }

            // ÉTAPE 2 & 3 : Réception des messages du serveur
            GameNetworkEvent::Message { stream, data, .. } => {
                match stream.real_stream_id() {
                    // --- GESTION DU HANDSHAKE ---
                    STREAM_HANDSHAKE => match bincode::deserialize::<NetMessages>(&data) {
                        Ok(WELCOME(welcome)) => {
                            println!("[Client] Reçu WELCOME : {}", welcome);
                            if let Ok(client_uuid) = uuid::Uuid::parse_str(&welcome) {
                                println!("[Client] UUID du joueur : {}", client_uuid);
                                local_player.net_id = Some(client_uuid);
                                next_state.set(ClientState::InGame);
                            }
                        }
                        _ => {
                            println!("Recieved Handshake but could deserialize");
                        }
                    },

                    // --- GESTION DES SNAPSHOTS ---
                    STREAM_SNAPSHOTS => {
                        // On ignore les données du monde tant qu'on n'est pas officiellement en jeu
                        //if local_player.net_id.is_some() {
                            if let Ok(snapshot) = bincode::deserialize::<PersonalSnapshot>(&data) {
                                msg_snapshot.write(SnapshotMessage(snapshot));
                            }
                        //}
                    }
                    _ => {
                        println!("[Client] Unknowned stream : {}", stream.real_stream_id());
                    }
                }
            }

            GameNetworkEvent::Disconnected(_) => {
                println!("[Client] Déconnecté du serveur.");
                server_conn.0 = None;
                local_player.net_id = None; // On repasse en mode "Non connecté"
                next_state.set(ClientState::LoginMenu)

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
