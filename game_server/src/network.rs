// server/src/network.rs
use bevy::prelude::*;

use crate::events;
use game_sockets::{GamePeer, GameNetworkEvent};
use game_sockets::protocols::QuicBackend;
use events::{PlayerConnected, PlayerDisconnected, PlayerInputEvent};
use shared_replication::{PlayerInput, STREAM_INPUTS};

const SERV_URL: &str = "0.0.0.0";
const SERVER_PORT: u16 = 5000;

#[derive(Resource)]
pub struct NetworkManager {
    pub peer: GamePeer,
}

#[derive(Component, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct NetworkId(pub u32);

#[derive(Component)]
pub struct ControlledBy {
    pub owner_uuid: uuid::Uuid,
}

#[derive(Resource, Default)]
pub struct NetworkIdGenerator {
    next_id: u32,
}

impl NetworkIdGenerator {
    pub fn next(&mut self) -> NetworkId {
        let id = self.next_id;
        self.next_id += 1;
        NetworkId(id)
    }
}

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App) {
        let peer = GamePeer::new(QuicBackend::new());

        peer.listen(SERV_URL, SERVER_PORT).expect("Impossible de bind le port UDP");

        app.insert_resource(NetworkManager { peer })
            .insert_resource(NetworkIdGenerator::default());

        app.add_message::<PlayerConnected>()
            .add_message::<PlayerDisconnected>()
            .add_message::<PlayerInputEvent>();

        app.add_systems(PreUpdate, network_bridge_system);
    }
}

fn network_bridge_system(
    mut net: ResMut<NetworkManager>,
    mut msg_connected: MessageWriter<PlayerConnected>,
    mut msg_disconnected: MessageWriter<PlayerDisconnected>,
    mut msg_input: MessageWriter<PlayerInputEvent>,
) {
    // net.peer.poll() retourne Result<Option<GameNetworkEvent>, Error>
    while let Ok(Some(event)) = net.peer.poll() {
        match event {
            GameNetworkEvent::Connected(conn) => {
                println!("Nouveau joueur connecté : {}", conn.connection_uuid);
                msg_connected.write(PlayerConnected { client_id: conn.connection_uuid });
            }
            GameNetworkEvent::Disconnected(conn) => {
                println!("joueur disconnect : {}", conn.connection_uuid);
                msg_disconnected.write(PlayerDisconnected { client_id: conn.connection_uuid });
            }
            GameNetworkEvent::Message { connection, stream, data } => {
                match stream.real_stream_id() {

                    // --- CANAL Uniquement des Inputs ---
                    STREAM_INPUTS => {
                        match bincode::deserialize::<PlayerInput>(&data) {
                            Ok(input_data) => {
                                msg_input.write(PlayerInputEvent {
                                    client_id: connection.connection_uuid,
                                    input_data
                                });
                            }
                            Err(e) => eprintln!("Input corrompu de {} : {}", connection.connection_uuid, e),
                        }
                    }

                    // --- CANAL INCONNU ---
                    _ => {
                        eprintln!("Message reçu sur un flux non géré : {}", stream.real_stream_id());
                    }
                }
            }
            GameNetworkEvent::Error { connection, inner } => {
                eprintln!("Erreur réseau pour {}: {:?}", connection.connection_uuid, inner);
            }
            _ => {} // todo : Gérer StreamCreated / StreamClosed ?
        }
    }
}