use std::env;
// server/src/network.rs
use bevy::prelude::*;
use bevy::reflect::Map;
use bytes::Bytes;
use crate::{events};
use game_sockets::{GamePeer, GameNetworkEvent, GameStream, GameStreamReliability};
use game_sockets::protocols::{QuicBackend};
use events::{NetConnexion, NetDisconnection, PlayerConnected, PlayerDisconnected, PlayerInputEvent};
use shared_replication::{Heartbeat, PlayerInput, STREAM_INPUTS, STREAM_HANDSHAKE, NetMessages};
use crate::game::ClientDirectory;

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

#[derive(Resource)]
pub struct ServerStats {
    total_players: usize,
    max_players: usize,
    external_url: String,
    external_port: u16,
    zone: String,
    uuid: uuid::Uuid,
}

#[derive(Resource)]
pub struct OrchestratorManager {
    pub peer: GamePeer,
    pub connection: Option<game_sockets::GameConnection>, // Sera rempli quand le socket UDP sera prêt
    pub timer: Timer,
}

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App) {
        let listen_url : String = env::var("SERVER_LISTEN_URL").unwrap_or_else(|_| {
            eprintln!("Error: SERVER_LISTEN_URL environment variable not set. Defaulting to 0.0.0.0");
            "0.0.0.0".to_string() });


        let server_port  = env::var("DS_PORT");

        let server_port = match server_port {
            Ok(port_str) => port_str.parse::<u16>().unwrap_or_else(|_| {
                eprintln!("[Server] DS_PORT must be a valid u16. Defaulting to 5000.");
                5000
            }),
            Err(_) => {
                eprintln!("[Server] DS_PORT must be set. Defaulting to 5000.");
                5000
            },
        };

        let heartbeat_interval = env::var("HEARTBEAT_INTERVAL");

        let heartbeat_interval = match heartbeat_interval {
            Ok(interval_str) => interval_str.parse::<u8>().unwrap_or_else(|_| {
                eprintln!("[Server] HEARTBEAT_INTERVAL must be a valid int8. Defaulting to 5 seconds.");
                5
            }),
            Err(_) => {
                eprintln!("[Server] HEARTBEAT_INTERVAL must be set. Defaulting to 5 seconds.");
                5
            },
        };

        let orchestrator_url = env::var("ORCHESTRATOR_URL");
        let orchestrator_url = match orchestrator_url {
            Ok(url) => url,
            Err(E) => panic!("[Server] ORCHESTRATOR_URL environment variable not set."),
        };

        let (orch_ip, orch_port_str) = orchestrator_url.split_once(':')
            .expect("[Server] ORCHESTRATOR_URL doit être au format ip:port");
        let orch_port: u16 = orch_port_str.parse().expect("Port de l'orchestrateur invalide");

        let orch_peer = GamePeer::new(QuicBackend::new());
        orch_peer.connect(orch_ip, orch_port).expect("Impossible de configurer le socket vers l'Orchestrateur");

        let client_peer = GamePeer::new(QuicBackend::new());
        client_peer.listen(&*listen_url, server_port).expect("Impossible de bind le port QUIC");

        app.insert_resource(NetworkManager { peer: client_peer })
            .insert_resource(NetworkIdGenerator::default())
            .insert_resource(OrchestratorManager {
                peer: orch_peer,
                connection: None,
                timer: Timer::from_seconds(heartbeat_interval as f32, TimerMode::Repeating),
            })
            .insert_resource(
                //todo : check the values that should go there
                ServerStats {
                    zone: "default".to_string(),
                    total_players: 0,
                    max_players: 100,
                    external_url: "dunno yet".to_string(),
                    external_port: server_port,
                    uuid : uuid::Uuid::new_v4(),
                });

        app.add_message::<PlayerConnected>()
            .add_message::<PlayerDisconnected>()
            .add_message::<PlayerInputEvent>();

        app.add_systems(PreUpdate, (orchestrator_bridge_system, network_bridge_system));
        app.add_systems(Update, send_heartbeat_system);
    }
}
fn orchestrator_bridge_system(
    mut orch: ResMut<OrchestratorManager>
) {
    while let Ok(Some(event)) = orch.peer.poll() {
        if let GameNetworkEvent::Connected(conn) = event {
            println!("[Server] Connexion UDP établie avec l'Orchestrateur ! (ID interne: {})", conn.connection_uuid);
            orch.connection = Some(conn);
        }
    }
}

fn send_heartbeat_system(
    time: Res<Time>,
    mut orch: ResMut<OrchestratorManager>,
    server_info: Res<ServerStats>,
    client_directory: ResMut<ClientDirectory>,
) {
    orch.timer.tick(time.delta());

    if orch.timer.just_finished() {
        let Some(conn) = &orch.connection else { return };

        // On construit le payload
        let heartbeat = Heartbeat {
            id: server_info.uuid.to_string(),
            ip: server_info.external_url.clone(),
            port: server_info.external_port,
            zone: server_info.zone.clone(),
            player_count: client_directory.sessions.len(),
            max_players: server_info.max_players,
        };

        // On sérialise en JSON (souvent plus simple pour un Orchestrateur web/Axum)
        // ou en Bincode selon ta préférence. Ici, JSON est standard pour les Gatekeepers.
        if let Ok(json_bytes) = serde_json::to_vec(&heartbeat) {
            let data = Bytes::from(json_bytes);

            // Le flux n'a pas d'importance en UDP pur, mais on utilise 0 par défaut
            let useless_stream = GameStream::new(0, GameStreamReliability::Unreliable);

            if let Err(e) = orch.peer.send(conn, &useless_stream, data) {
                eprintln!("[Server] Erreur d'envoi du heartbeat: {:?}", e);
            } else {
                println!("[Server] Heartbeat envoyé (Joueurs: {}/{})", heartbeat.player_count, heartbeat.max_players);
            }
        }
    }
}

fn network_bridge_system(
    mut net: ResMut<NetworkManager>,
    mut msg_net_connexion: MessageWriter<NetConnexion>,
    mut msg_net_deconnexion: MessageWriter<NetDisconnection>,
    mut msg_connected: MessageWriter<PlayerConnected>,
    mut msg_disconnected: MessageWriter<PlayerDisconnected>,
    mut msg_input: MessageWriter<PlayerInputEvent>,
) {
    while let Ok(Some(event)) = net.peer.poll() {
        match event {
            // 1. Délégation de la gestion des connexions brutes
            GameNetworkEvent::Connected(_) | GameNetworkEvent::Disconnected(_) => {
                route_connection_events(event, &mut msg_net_connexion, &mut msg_net_deconnexion);
            }

            // 2. Délégation de la gestion des données
            GameNetworkEvent::Message { .. } => {
                route_message_events(event, &mut msg_input, &mut msg_connected, &mut msg_disconnected);
            }

            // 3. Gestion des erreurs
            GameNetworkEvent::Error { connection, inner } => {
                eprintln!("Erreur réseau pour {}: {:?}", connection.connection_uuid, inner);
            }
            // 4. création de streams ???
            _ => {}
        }
    }
}

/// Gère uniquement les ouvertures et fermetures de socket UDP/QUIC
fn route_connection_events(
    event: GameNetworkEvent,
    msg_connected: &mut MessageWriter<NetConnexion>,
    msg_disconnected: &mut MessageWriter<NetDisconnection>,
) {
    match event {
        GameNetworkEvent::Connected(conn) => {
            // Note : À ce stade, le socket est ouvert, mais le joueur n'est pas encore "loggué"
            println!("[Réseau] Socket ouvert : {}", conn.connection_uuid);
            msg_connected.write(NetConnexion { client_id: conn.connection_uuid });
        }
        GameNetworkEvent::Disconnected(conn) => {
            println!("[Réseau] Socket fermé : {}", conn.connection_uuid);
            msg_disconnected.write(NetDisconnection { client_id: conn.connection_uuid });
        }
        _ => unreachable!(),
    }
}

/// Gère l'aiguillage des flux de données en fonction de leur Stream ID
fn route_message_events(
    event: GameNetworkEvent,
    msg_input: &mut MessageWriter<PlayerInputEvent>,
    msg_connected: &mut MessageWriter<PlayerConnected>,
    msg_disconnected: &mut MessageWriter<PlayerDisconnected>,
) {
    let GameNetworkEvent::Message { connection, stream, data } = event else { return };

    let client_uuid = connection.connection_uuid;

    match stream.real_stream_id() {
        STREAM_INPUTS => {
            match bincode::deserialize::<PlayerInput>(&data) {
                Ok(input_data) => {
                    msg_input.write(PlayerInputEvent {
                        client_id: client_uuid,
                        input_data
                    });
                }
                Err(e) => eprintln!("Input corrompu de {} : {}", client_uuid, e),
            }
        }
        STREAM_HANDSHAKE => {
            match bincode::deserialize::<NetMessages>(&data) {
                Ok(NetMessages::JOIN (pseudo) ) => {
                    println!("Handshake reçu de {} : {}", client_uuid, pseudo);
                    //dit au jeu de spawn le joueur
                    msg_connected.write(PlayerConnected {
                        client_id: client_uuid,
                        player_name: pseudo,
                    });
                }
                Err(e) => eprintln!("Handshake corrompu de {} : {}", client_uuid, e),

                _ => {}
            }
        }

        _ => {
            eprintln!("Message reçu sur un flux non géré : {}", stream.real_stream_id());
        }
    }
}