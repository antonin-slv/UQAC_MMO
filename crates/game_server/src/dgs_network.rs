// server/src/network.rs

use crate::events;
use crate::game::ClientDirectory;
use bevy::prelude::*;
use broker_client::{ClientNetworkEvent, MmoNetworkClient};
use broker_protocol::broker_message::NodeId;
use broker_protocol::broker_topics::{Namespace, SecurityDomain, TopicBuilder};
use events::{ChunkAssignedEvent, PlayerConnected, PlayerDisconnected, PlayerInputEvent};
use game_message::msg_client_server::*;
use game_message::msg_dgs::{Heartbeat, HeartbeatMessage, SpawnClientMsg, TakeChunkMessage};
use game_message::msg_servers::{ServerHelloMSG, ServerType};
use game_message::{GameMessageHeaders, GamePayload};
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
    pub client_id: NodeId,
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
    server_broker_id: u32,
}

#[derive(Resource)]
pub struct BrockerManager {
    pub client: MmoNetworkClient,
}
#[derive(Resource)]
pub struct HeartBeatTimer {
    pub timer: Timer,
}

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App) {
        println!("\n[Server] Initializing NetworkPlugin\n");

        // --- Initialisation des variables d'environnement ---
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
                panic!("Error : {} must be a valid usize", MAX_PLAYER_PER_SERVER);
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

        let server_uuid = env::var(SELF_UUID_ENV_NAME);
        let server_uuid = match server_uuid {
            Ok(server_uuid) => match uuid::Uuid::try_parse(&server_uuid) {
                Ok(uuid) => uuid,
                Err(e) => panic!(
                    "Error : {} has error : {}\n\twith : {}",
                    SELF_UUID_ENV_NAME, e, server_uuid
                ),
            },
            Err(_) => panic!("Error : {} must be set.", SELF_UUID_ENV_NAME),
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
        let broker_port: u16 = broker_port_str
            .parse()
            .expect("Port de l'orchestrateur invalide");

        // --- Nouvelle API : Connexion au Broker ---
        let broker_client = MmoNetworkClient::new();
        if broker_client.connect(broker_ip, broker_port).is_err() {
            panic!("Error : Connection To broker failed.");
        }

        app.insert_resource(NetworkIdGenerator::default())
            .insert_resource(BrockerManager {
                client: broker_client,
            })
            .insert_resource(HeartBeatTimer {
                timer: Timer::from_seconds(heartbeat_interval as f32, TimerMode::Repeating),
            })
            .insert_resource(ServerStats {
                zone: "default".to_string(),
                total_players: 0,
                max_players,
                external_url: listen_ip.to_string(),
                external_port: listen_port,
                uuid: server_uuid,
                server_broker_id: 0, // Initialisé à 0, sera mis à jour après le handshake
            });

        app.add_message::<PlayerConnected>()
            .add_message::<PlayerDisconnected>()
            .add_message::<PlayerInputEvent>()
            .add_message::<ChunkAssignedEvent>();

        app.add_systems(PreUpdate, network_bridge_system);
        app.add_systems(Update, send_heartbeat_system);

        println!("\n[Server] Network plugin initialized.\n");
    }
}

fn send_heartbeat_system(
    time: Res<Time>,
    broker: ResMut<BrockerManager>,
    mut timer: ResMut<HeartBeatTimer>,
    server_info: Res<ServerStats>,
    client_directory: ResMut<ClientDirectory>,
) {
    timer.timer.tick(time.delta());

    if timer.timer.just_finished() {
        let heartbeat = Heartbeat {
            id: server_info.uuid.to_string(),
            node_id: broker.client.node_id.unwrap_or(0),
            zone: server_info.zone.clone(),
            player_count: client_directory.sessions.len(),
            max_players: server_info.max_players,
        };

        let heartbeat = HeartbeatMessage { heartbeat };

        let topic_heartbeat =
            TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::Heartbeat).build();
        broker.client.publish_reliable(topic_heartbeat, &heartbeat);
    }
}

fn network_bridge_system(
    mut broker: ResMut<BrockerManager>,
    mut server_info: ResMut<ServerStats>,
    mut msg_connected: MessageWriter<PlayerConnected>,
    mut msg_disconnected: MessageWriter<PlayerDisconnected>,
    mut msg_input: MessageWriter<PlayerInputEvent>,
    mut msg_chunk_assigned: MessageWriter<ChunkAssignedEvent>,
) {
    let broker_connect = &mut broker.client;
    // On utilise poll() de la nouvelle API
    while let Some(event) = broker_connect.poll() {
        match event {
            ClientNetworkEvent::Ready => {
                let my_id = broker_connect.node_id.unwrap_or_else(|| {
                    panic!("Ready event received but node_id is None");
                });
                server_info.server_broker_id = my_id;

                println!("[Server] Handshake validé. Connecté au Broker PubSub.");

                // 1. Abonnement à l'attribution de chunks
                // Le serveur écoute l'orchestrateur pour savoir de quelles cellules il est responsable
                let director_topic =
                    TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::Director).build();

                broker_connect.subscribe(director_topic, 0);
                println!("[Server] Abonnement au topic Director Effectué.");

                let msg = ServerHelloMSG {
                    server_type: ServerType::Server,
                    id: server_info.server_broker_id,
                };

                // On envoie le Hello sur le topic d'authentification des serveurs
                let auth_topic =
                    TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ServerConnection)
                        .build();
                broker_connect.publish_reliable(auth_topic, &msg);
            }
            ClientNetworkEvent::Connected => {
                println!("[Server] Connecté au Broker (Still not ready)...");
            }
            ClientNetworkEvent::Disconnected(disco_id) => {
                if disco_id == server_info.server_broker_id || disco_id == 0 {
                    panic!("Disconnected from broker (this is bad)");
                }
            }

            ClientNetworkEvent::DataReceived {
                client_id,
                stream: _,
                payload,
            } => {
                // Le payload pur arrive ici, expurgé du topic par le broker
                route_message_events(
                    payload,
                    client_id,
                    &mut msg_input,
                    &mut msg_connected,
                    &mut msg_disconnected,
                    &mut msg_chunk_assigned,
                );
            }
        }
    }
}

/// Gère l'aiguillage des flux de données purs (sans logique de Topic, gérée en amont)
fn route_message_events(
    mut payload: GamePayload,
    sender_id: NodeId,
    msg_input: &mut MessageWriter<PlayerInputEvent>,
    msg_connected: &mut MessageWriter<PlayerConnected>,
    msg_disconnected: &mut MessageWriter<PlayerDisconnected>,
    msg_chunk_assigned: &mut MessageWriter<ChunkAssignedEvent>,
) {
    match payload.header {
        GameMessageHeaders::ClientInput => match payload.extract::<PlayerInputMsg>() {
            Ok(msg) => {
                msg_input.write(PlayerInputEvent {
                    client_id: sender_id, // Ou le sender_id du broker !
                    input_data: msg.input_data,
                });
            }
            Err(e) => {
                println!("[Server] Erreur de parsing du message ClientInput : {}", e);
            }
        },

        GameMessageHeaders::SpawnClient => match payload.extract::<SpawnClientMsg>() {
            Ok(msg_input) => {
                msg_connected.write(PlayerConnected {
                    client_id: msg_input.client_id,
                    player_name: msg_input.pseudo.clone(),
                });
            }
            Err(e) => {
                println!("[Server] Erreur de parsing du message SpawnClient : {}", e);
            }
        },

        GameMessageHeaders::ClientDisconnect => match payload.extract::<ClientDisconnectedMsg>() {
            Ok(msg_input) => {
                msg_disconnected.write(PlayerDisconnected {
                    client_id: msg_input.client_id,
                });
            }
            Err(e) => {
                println!(
                    "[Server] Erreur de parsing du message ClientDisconnect : {}",
                    e
                );
            }
        },

        GameMessageHeaders::ChunkHandOff => match payload.extract::<TakeChunkMessage>() {
            Ok(msg_take_chunk) => {
                msg_chunk_assigned.write(ChunkAssignedEvent {
                    chunk: msg_take_chunk.game_chunk,
                });
            }
            Err(e) => {
                println!("[Server] Erreur de parsing du message TakeChunk : {}", e);
            }
        },
        _ => {
            println!(
                "[Server] Unrecognized message header received: {:?} (raw: {:?})",
                payload.header, payload.data
            );
        }
    }
}
