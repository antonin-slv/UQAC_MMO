use bevy::app::{App, Plugin, PreUpdate, Update};
use bevy::asset::Assets;
use bevy::color::Color;
use bevy::mesh::{Mesh, Mesh2d};
use bevy::prelude::{Circle, ColorMaterial, Commands, Component, Entity, MeshMaterial2d, Message, MessageReader, MessageWriter, Query, ResMut, Resource, Transform};
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer};
use game_sockets::protocols::QuicBackend;
use shared_replication::{PersonalSnapshot, STREAM_SNAPSHOTS};
use crate::{PlayerBundle};


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
        // On enregistre les systèmes liés aux joueurs
        // 1. Initialisation de la librairie réseau
        let peer = GamePeer::new(QuicBackend::new());

        println!("[Client] Connexion au serveur...");
        peer.connect("127.0.0.1", 5000).expect("Impossible de se connecter");

        app
            .insert_resource(NetworkManager { peer })
            .insert_resource(ServerConnection::default())
            // "event" réception de snapshot
            .add_message::<SnapshotMessage>()
            .add_systems(PreUpdate, network_bridge_system)
            .add_systems(Update,process_snapshots);
    }
}


// --- LE PONT RÉSEAU ---
// Lit la lib et génère des messages Bevy
fn network_bridge_system(
    mut net: ResMut<NetworkManager>,
    mut server_conn: ResMut<ServerConnection>,
    mut msg_snapshot: MessageWriter<SnapshotMessage>,
) {
    while let Ok(Some(event)) = net.peer.poll() {
        match event {
            GameNetworkEvent::Connected(conn) => {
                println!("[Client] Connecté au serveur !");
                // On sauvegarde la connexion pour pouvoir lui envoyer nos touches
                server_conn.0 = Some(conn);
            }
            GameNetworkEvent::Disconnected(_) => {
                println!("[Client] Déconnecté du serveur.");
                server_conn.0 = None;
            }
            //on peut aussi récupérer l'uuid et le stream (id+type)
            GameNetworkEvent::Message { stream, data, .. } => {
                match stream.real_stream_id() {
                    // --- CANAL 0 : Snapshots de jeu ---
                    STREAM_SNAPSHOTS => {
                        match bincode::deserialize::<PersonalSnapshot>(&data) {
                            Ok(snapshot) => {
                                // On écrit un message Bevy pour que les systèmes graphiques puissent le lire
                                msg_snapshot.write(SnapshotMessage(snapshot));
                            }
                            Err(e) => {
                                eprintln!("Erreur lors de la désérialisation du snapshot: {:?}", e);
                            }
                        }
                    }

                    _ => {
                        eprintln!("Message reçu sur un stream inconnu: {}", stream.real_stream_id());
                    }
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

    let Some(snapshot) = latest_snapshot else { return };

    for net_entity in &snapshot.entities {
        let existing_entity = query_net_entities
            .iter_mut()
            .find(|(_, existing_id, _)| existing_id.0 == net_entity.network_id);

        if let Some((_, _, mut transform)) = existing_entity {
            transform.translation.x = net_entity.position[0];
            transform.translation.z = net_entity.position[1];
            continue;
        }

        println!("Nouvelle entité réseau découverte : {}", net_entity.network_id);

        commands.spawn((
            PlayerBundle {
                mesh: Mesh2d(meshes.add(Circle::new(10.0))),
                material: MeshMaterial2d(materials.add(Color::srgb(0.2, 0.7, 0.9))),
                transform: Transform::from_xyz(net_entity.position[0], net_entity.position[1], 0.0 )
            },
            NetworkEntity(net_entity.network_id),
        ));
    }
}