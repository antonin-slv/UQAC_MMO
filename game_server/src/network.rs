// server/src/network.rs

use crate::events;
use crate::game::ClientDirectory;
use bevy::prelude::*;
use bytes::{BufMut, Bytes, BytesMut};
use events::{PlayerConnected, PlayerDisconnected, PlayerInputEvent};
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use shared_replication::broker::{
    BrokerFriends, BrokerMessageHeaders, ClientId, Input, SafeExtract, Topic,
};
use shared_replication::client_server::*;
use shared_replication::{Heartbeat, STREAM_HANDSHAKE};
use std::env;

const INNER_IP_ENV_NAME: &str = "SERVER_LISTEN_IP";
const INNER_PORT_ENV_NAME: &str = "SERVER_LISTEN_PORT";
const BROKER_URL_ENV_NAME: &str = "BROKER_URL";
const SELF_UUID_ENV_NAME: &str = "SERVER_UUID";
const HEARTBEAT_RATE_ENV_NAME: &str = "HEARTBEAT_INTERVAL";
const MAX_PLAYER_PER_SERVER: &str = "MAX_PLAYER_PER_SERVER";
#[derive(Component, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct NetworkId(pub u32);

#[derive(Component)]
pub struct ControlledBy {
    pub client_id: ClientId,
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
    pub topic: Topic,
    uuid: uuid::Uuid,
}

#[derive(Resource)]
pub struct BrockerManager {
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

        let broker_url = env::var(BROKER_URL_ENV_NAME);
        let broker_url = match broker_url {
            Ok(url) => url,
            Err(_) => panic!(
                "Error : {} environment variable not set.",
                BROKER_URL_ENV_NAME
            ),
        };
        println!("[Server] BROKER URL : {}", broker_url);

        let (broker_ip, broker_port_str) = broker_url.split_once(':').unwrap_or_else(|| {
            panic!(
                "Error :  {} must be in the format IP:PORT (was {})",
                BROKER_URL_ENV_NAME, broker_url
            )
        });
        let brocker_port: u16 = broker_port_str
            .parse()
            .expect("Port de l'orchestrateur invalide");

        let brocker_peer = GamePeer::new(QuicBackend::new());
        brocker_peer
            .connect(broker_ip, brocker_port)
            .expect("Impossible de configurer le socket vers l'Orchestrateur");

        app.insert_resource(NetworkIdGenerator::default())
            .insert_resource(BrockerManager {
                peer: brocker_peer,
                connection: None,
                timer: Timer::from_seconds(heartbeat_interval as f32, TimerMode::Repeating),
            })
            .insert_resource(
                //todo : check the values that should go there
                ServerStats {
                    zone: "default".to_string(),
                    total_players: 0,
                    max_players,
                    topic: [0; 32].into(),
                    external_url: listen_ip.to_string(),
                    external_port: listen_port,
                    uuid: server_uuid,
                },
            );

        app.add_message::<PlayerConnected>()
            .add_message::<PlayerDisconnected>()
            .add_message::<PlayerInputEvent>();

        app.add_systems(PreUpdate, network_bridge_system);
        app.add_systems(Update, send_heartbeat_system);

        println!("\n[Server] Network plugin initialized.\n");
    }
}

fn send_heartbeat_system(
    time: Res<Time>,
    mut broker: ResMut<BrockerManager>,
    server_info: Res<ServerStats>,
    client_directory: ResMut<ClientDirectory>,
) {
    broker.timer.tick(time.delta());

    if broker.timer.just_finished() {
        let Some(conn) = &broker.connection else { return };

        // On construit le payload
        let heartbeat = Heartbeat {
            id: server_info.uuid.to_string(),
            zone: server_info.zone.clone(),
            player_count: client_directory.sessions.len(),
            max_players: server_info.max_players,
        };

        // On sérialise en JSON
        if let Ok(json_bytes) = serde_json::to_vec(&heartbeat) {
            let json_bytes = Bytes::from(json_bytes);
            let data_len = json_bytes.len();
            let data_len_u8 = (data_len as u16).to_le_bytes() as [u8; 2];

            let mut data = BytesMut::with_capacity(1 + 2 + data_len);

            data.put_u8(BrokerMessageHeaders::Heartbeat as u8);
            data.put_slice(&data_len_u8);
            data.put_slice(&json_bytes);

            let heartbeat_stream = GameStream::new(
                shared_replication::STREAM_HEARTBEAT,
                GameStreamReliability::Unreliable,
            );

            if let Err(e) = broker.peer.send(conn, &heartbeat_stream, data.freeze()) {
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
    mut broker: ResMut<BrockerManager>,
    mut msg_connected: MessageWriter<PlayerConnected>,
    mut msg_disconnected: MessageWriter<PlayerDisconnected>,
    mut msg_input: MessageWriter<PlayerInputEvent>,
) {
    while let Ok(Some(event)) = broker.peer.poll() {
        match event {
            // 1. Délégation de la gestion des connexions brutes
            GameNetworkEvent::Connected(conn) => {
                broker.connection = Some(conn);
            }

            GameNetworkEvent::Disconnected(_) => {
                broker.connection = None;
                panic!("Disconnected from broker (this is bad)");
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

            GameNetworkEvent::StreamCreated(connexion, game_stream) => {
                println!(
                    "[Server] Stream créé : Connexion {:?}, Stream {:?}",
                    connexion, game_stream
                );
                match game_stream.real_stream_id() {
                    STREAM_HANDSHAKE => {
                        println!("[Server] Handshake stream created by broker (conn: {:?})", connexion);
                        let hello_packet_header = BrokerMessageHeaders::FriendHello as u8;
                        let friend_type = BrokerFriends::Server as u8;
                        let mut data = BytesMut::with_capacity(2);
                        data.put_u8(hello_packet_header);
                        data.put_u8(friend_type);
                        broker.peer
                            .send(&connexion, &game_stream, data.freeze())
                            .unwrap_or_else(|e| {
                                eprintln!("Error sending handshake response: {:?}", e);
                            });

                        println!("[Server] Sent handshake response to broker (conn: {:?})", connexion);
                    }

                    _ => {}
                }
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

    let discard_message = BrokerMessageHeaders::DiscardedMessageBecauseYouKnow as u8;
    let header_byte = data.first().unwrap_or(&discard_message);
    let header = BrokerMessageHeaders::from(*header_byte);

    let process_message = || -> Option<()> {
        match header {
            BrokerMessageHeaders::ClientInput => {
                let client_id = ClientId::extract_from_slice(data.get(1..5)?)?;
                let input: Input = data.get(5..(5 + 16))?.try_into().ok()?;

                let input = PlayerInput::make_from_u8_slice(input.get(0..2)?)?;

                msg_input.write(PlayerInputEvent {
                    client_id,
                    input_data: input,
                });

                Some(())
            }

            BrokerMessageHeaders::SpawnClient => {
                let client_id = ClientId::extract_from_slice(data.get(1..5)?)?;
                let pseudo_len = data.get(5..6)?.first().cloned()? as usize;
                let pseudo_end = 6 + pseudo_len;
                let pseudo = data.get(6..pseudo_end)?;

                let pseudo = std::str::from_utf8(pseudo).ok();
                let pseudo: String = pseudo.unwrap_or_else(|| "Unknown".into()).to_string();

                msg_connected.write(PlayerConnected {
                    stream_used: stream,
                    client_id,
                    player_name: pseudo,
                });

                Some(())
            }

            BrokerMessageHeaders::ClientDisconnect => {
                _msg_disconnected.write(PlayerDisconnected {
                    client_id: ClientId::extract_from_slice(data.get(1..5)?)?,
                });
                Some(())
            }
            _ => None,
        }
    };

    let rslt = process_message();
    if let None = rslt {}
    {
        eprintln!("[Server] Une erreure ou un message non reconnu reçu sur le réseau");
    }

    /*
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
     */
}
