// server/src/network.rs

use std::env;
use crate::events;
use crate::game::ClientDirectory;
use bevy::prelude::*;
use bytes::Bytes;
use events::{
    NetConnexion, NetDisconnection, PlayerConnected, PlayerDisconnected, PlayerInputEvent,
};
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use shared_replication::client_server::*;
use shared_replication::{Heartbeat, NetMessages, STREAM_HANDSHAKE, STREAM_INPUTS, ServerInfo};

const INNER_IP_ENV_NAME: &str = "SERVER_LISTEN_IP";
const INNER_PORT_ENV_NAME: &str = "SERVER_LISTEN_PORT";

const ORCH_URL_ENV_NAME: &str = "ORCHESTRATOR_URL";
const SELF_UUID_ENV_NAME: &str = "SERVER_UUID";
const HEARTBEAT_RATE_ENV_NAME: &str = "HEARTBEAT_INTERVAL";
const MAX_PLAYER_PER_SERVER: &str = "MAX_PLAYER_PER_SERVER";

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
    pub connection: Option<game_sockets::GameConnection>, // Sera rempli quand le socket quic sera prêt
    pub timer: Timer,
}

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App) {
        println!("\n[Server] Initializing NetworkPlugin\n");

        // --- ce que le serveur écoute
        let listen_ip: String = env::var(INNER_IP_ENV_NAME).unwrap_or_else(|_| {
            eprintln!("Error: {} environment variable not set", INNER_IP_ENV_NAME);
            "0.0.0.0".to_string()
        });
        let listen_port: u16 = env::var(INNER_PORT_ENV_NAME)
            .unwrap_or_else(|_| {
                eprintln!("Error: {} variable not set", INNER_PORT_ENV_NAME);
                "5000".to_string()
            })
            .parse()
            .unwrap_or_else(|_| {
                eprintln!("Error: {} variable is not a valid u16", INNER_PORT_ENV_NAME);
                5000
            });
        println!("[Server] local listen {}:{}", listen_ip, listen_port);

        let max_players = env::var(MAX_PLAYER_PER_SERVER);

        let max_players = match max_players {
            Ok(port_str) => port_str.parse::<usize>().unwrap_or_else(|_| {
                panic!("Error : {} must be a valid u16", MAX_PLAYER_PER_SERVER);
            }),
            Err(_) => {
                panic!("Error : {} must be set", MAX_PLAYER_PER_SERVER);
            }
        };
        println!("[Server] MAX PLAYERS : {}", max_players);

        let heartbeat_interval = env::var(HEARTBEAT_RATE_ENV_NAME);

        let heartbeat_interval = match heartbeat_interval {
            Ok(interval_str) => interval_str.parse::<u8>().unwrap_or_else(|_| {
                eprintln!("Error : {} must be a valid int8", HEARTBEAT_RATE_ENV_NAME);
                5
            }),
            Err(_) => {
                eprintln!("Error : {} must be set", HEARTBEAT_RATE_ENV_NAME);
                5
            }
        };
        println!(
            "[Server] Heartbeat interval: {} seconds",
            heartbeat_interval
        );

        // retrieving the uuid
        let server_uuid = env::var(SELF_UUID_ENV_NAME);
        let server_uuid = match server_uuid {
            Ok(server_uuid) => match (uuid::Uuid::try_parse(&server_uuid)) {
                Ok(uuid) => uuid,
                Err(E) => {
                    panic!(
                        "Error : {} has error : {}\n\twith : {}",
                        SELF_UUID_ENV_NAME, E, server_uuid
                    );
                }
            },
            Err(_) => {
                panic!("Error : {} must be set.", SELF_UUID_ENV_NAME);
            }
        };

        println!("[Server] Server UUID: {}", server_uuid);

        let orchestrator_url = env::var(ORCH_URL_ENV_NAME);
        let orchestrator_url = match orchestrator_url {
            Ok(url) => url,
            Err(_) => panic!(
                "Error : {} environment variable not set.",
                ORCH_URL_ENV_NAME
            ),
        };
        println!("[Server] Orchestrator URL : {}", orchestrator_url);

        let (orch_ip, orch_port_str) = orchestrator_url.split_once(':').unwrap_or_else(|| {
            panic!(
                "Error :  {} must be in the format IP:PORT (was {})",
                ORCH_URL_ENV_NAME, orchestrator_url
            )
        });
        let orch_port: u16 = orch_port_str
            .parse()
            .expect("Port de l'orchestrateur invalide");

        let orch_peer = GamePeer::new(QuicBackend::new());
        orch_peer
            .connect(orch_ip, orch_port)
            .expect("Impossible de configurer le socket vers l'Orchestrateur");

        let client_peer = GamePeer::new(QuicBackend::new());
        client_peer
            .listen(&*listen_ip, listen_port)
            .expect("Impossible de bind le port QUIC");

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
                    max_players,
                    external_url: listen_ip.to_string(),
                    external_port: listen_port,
                    uuid: server_uuid,
                },
            );

        app.add_message::<PlayerConnected>()
            .add_message::<PlayerDisconnected>()
            .add_message::<PlayerInputEvent>()
            .add_message::<NetConnexion>()
            .add_message::<NetDisconnection>();

        app.add_systems(
            PreUpdate,
            (
                (orchestrator_bridge_system, network_bridge_system),
                handle_disconnexions,
            )
                .chain(),
        );
        app.add_systems(Update, send_heartbeat_system);

        println!("\n[Server] Network plugin initialized.\n");
    }
}

fn handle_disconnexions(
    mut msg_net_deconnexion: MessageReader<NetDisconnection>,
    mut msg_disconnect: MessageWriter<PlayerDisconnected>,
) {
    for msg in msg_net_deconnexion.read() {
        println!("Client {} disconnected", msg.client_id);
        msg_disconnect.write(PlayerDisconnected {
            client_id: msg.client_id,
        });
    }
}

fn orchestrator_bridge_system(
    mut orch: ResMut<OrchestratorManager>,
    mut self_stats : ResMut<ServerStats>,
) {
    while let Ok(Some(event)) = orch.peer.poll() {
        match event {
            GameNetworkEvent::Connected(conn) => {
                println!("[Orchestrator] Connected to orchestrator");
                orch.connection = Some(conn);
            }
            GameNetworkEvent::Message {
                stream,
                connection,
                data,
            } => {
                println!("[Orchestrator] Received message from orchestrator");
                // Handle messages from the orchestrator here
                match stream.real_stream_id() {
                    STREAM_HANDSHAKE => {
                        println!("[Orchestrator] Handshake message received");
                        // Handle handshake messages
                        if let Ok(msg) = bincode::deserialize::<ServerInfo>(&data) {
                            self_stats.external_url = msg.ip;
                            self_stats.external_port = msg.port;
                            self_stats.zone = msg.zone;
                            println!(
                                "[Server] external url is {}:{}",
                                self_stats.external_url, self_stats.external_port
                            );
                            println!("[Server] zone is {}", self_stats.zone);
                        } else {
                            eprintln!("[Orchestrator] Failed to deserialize handshake message");
                        }
                    }
                    _ => {
                        println!(
                            "[Orchestrator] Received message on stream {}",
                            stream.real_stream_id()
                        );
                    }
                }
            }

            GameNetworkEvent::Disconnected { .. } => {
                println!("[Orchestrator] Disconnected from orchestrator");
                orch.connection = None;
            }
            _ => {}
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
            zone: server_info.zone.clone(),
            player_count: client_directory.sessions.len(),
            max_players: server_info.max_players,
        };

        // On sérialise en JSON
        if let Ok(json_bytes) = serde_json::to_vec(&heartbeat) {
            let data = Bytes::from(json_bytes);

            let heartbeat_stream = GameStream::new(
                shared_replication::STREAM_HEARTBEAT,
                GameStreamReliability::Unreliable,
            );

            if let Err(e) = orch.peer.send(conn, &heartbeat_stream, data) {
                eprintln!("[Server] Erreur d'envoi du heartbeat: {:?}", e);
            } else {
                println!(
                    "[Server] Heartbeat envoyé (Joueurs: {}/{})",
                    heartbeat.player_count, heartbeat.max_players
                );
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
            GameNetworkEvent::Connected(conn) => {
                match net.peer
                    .create_stream(conn, GameStreamReliability::Reliable, STREAM_HANDSHAKE) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("[NetworkBridge] failed to create the Handshake stream {:?}", e);
                    }
                }

                route_connection_events(event, &mut msg_net_connexion, &mut msg_net_deconnexion);
            }

            GameNetworkEvent::Disconnected(_) => {
                route_connection_events(event, &mut msg_net_connexion, &mut msg_net_deconnexion);
            }

            // 2. Délégation de la gestion des données
            GameNetworkEvent::Message { .. } => {
                route_message_events(
                    event,
                    &mut msg_input,
                    &mut msg_connected,
                    &mut msg_disconnected,
                );
            }

            // 3. Gestion des erreurs
            GameNetworkEvent::Error { connection, inner } => {
                eprintln!(
                    "Erreur réseau pour {}: {:?}",
                    connection.connection_uuid, inner
                );
            }
            // 4. création de streams ???
            _ => {}
        }
    }
}

/// Gère uniquement les ouvertures et fermetures de socket QUIC
fn route_connection_events(
    event: GameNetworkEvent,
    msg_connected: &mut MessageWriter<NetConnexion>,
    msg_disconnected: &mut MessageWriter<NetDisconnection>,
) {
    match event {
        GameNetworkEvent::Connected(conn) => {
            // Note : À ce stade, le socket est ouvert, mais le joueur n'est pas encore "loggué"
            println!("[Réseau] Socket ouvert : {}", conn.connection_uuid);
            msg_connected.write(NetConnexion {
                client_id: conn.connection_uuid,
            });
        }
        GameNetworkEvent::Disconnected(conn) => {
            println!("[Réseau] Socket fermé : {}", conn.connection_uuid);
            msg_disconnected.write(NetDisconnection {
                client_id: conn.connection_uuid,
            });
        }
        _ => unreachable!(),
    }
}

/// Gère l'aiguillage des flux de données en fonction de leur Stream ID
fn route_message_events(
    event: GameNetworkEvent,
    msg_input: &mut MessageWriter<PlayerInputEvent>,
    msg_connected: &mut MessageWriter<PlayerConnected>,
    _msg_disconnected: &mut MessageWriter<PlayerDisconnected>,
) {
    let GameNetworkEvent::Message {
        connection,
        stream,
        data,
    } = event
    else {
        return;
    };

    let client_uuid = connection.connection_uuid;

    match stream.real_stream_id() {
        STREAM_INPUTS => match bincode::deserialize::<PlayerInput>(&data) {
            Ok(input_data) => {
                msg_input.write(PlayerInputEvent {
                    client_id: client_uuid,
                    input_data,
                });
            }
            Err(e) => eprintln!("Input corrompu de {} : {}", client_uuid, e),
        },
        STREAM_HANDSHAKE => {
            match bincode::deserialize::<NetMessages>(&data) {
                Ok(NetMessages::JOIN(pseudo)) => {
                    println!("Handshake reçu de {} : {}", client_uuid, pseudo);
                    //dit au jeu de spawn le joueur
                    msg_connected.write(PlayerConnected {
                        client_id: client_uuid,
                        player_name: pseudo,
                        stream_used: stream,
                    });
                }
                Err(e) => eprintln!("Handshake corrompu de {} : {}", client_uuid, e),

                _ => {}
            }
        }

        _ => {
            eprintln!(
                "Message reçu sur un flux non géré : {}",
                stream.real_stream_id()
            );
        }
    }
}
