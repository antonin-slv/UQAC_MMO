// server/src/network.rs

use crate::events;
use crate::game::ClientDirectory;
use bevy::prelude::*;
use bytes::{BufMut, Bytes, BytesMut};
use events::{ChunkAssignedEvent, PlayerConnected, PlayerDisconnected, PlayerInputEvent};
use shared_replication::Heartbeat;
use shared_replication::broker_client::{ClientNetworkEvent, MmoNetworkClient};
use shared_replication::broker_message::ClientId;
use shared_replication::broker_topics::{
    BrokerMessageHeaders, Namespace, SecurityDomain, TopicBuilder,
};
use shared_replication::client_server::*;
use shared_replication::servers::ServerType;
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
    uuid: uuid::Uuid,
}

#[derive(Resource)]
pub struct BrockerManager {
    // Remplacement de GamePeer et GameConnection par l'interface unifiée
    pub client: MmoNetworkClient,
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
                timer: Timer::from_seconds(heartbeat_interval as f32, TimerMode::Repeating),
            })
            .insert_resource(ServerStats {
                zone: "default".to_string(),
                total_players: 0,
                max_players,
                external_url: listen_ip.to_string(),
                external_port: listen_port,
                uuid: server_uuid,
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
    mut broker: ResMut<BrockerManager>,
    server_info: Res<ServerStats>,
    client_directory: ResMut<ClientDirectory>,
) {
    broker.timer.tick(time.delta());

    if broker.timer.just_finished() {
        let heartbeat = Heartbeat {
            id: server_info.uuid.to_string(),
            zone: server_info.zone.clone(),
            player_count: client_directory.sessions.len(),
            max_players: server_info.max_players,
        };

        if let Ok(json_bytes) = serde_json::to_vec(&heartbeat) {
            let data_len = json_bytes.len();
            let data_len_u8 = (data_len as u16).to_le_bytes() as [u8; 2];

            let mut data = BytesMut::with_capacity(1 + 2 + data_len);
            data.put_u8(BrokerMessageHeaders::Heartbeat as u8);
            data.put_slice(&data_len_u8);
            data.put_slice(&json_bytes);

            // On publie le heartbeat sur un topic réservé à l'orchestrateur (Director)
            let topic_heartbeat =
                TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::Heartbeat).build();

            broker
                .client
                .publish_unreliable(topic_heartbeat, data.freeze());
        }
    }
}

fn network_bridge_system(
    mut broker: ResMut<BrockerManager>,
    server_info: Res<ServerStats>,
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
                println!("[Server] Handshake validé. Connecté au Broker PubSub.");

                // 1. Abonnement à l'attribution de chunks
                // Le serveur écoute l'orchestrateur pour savoir de quelles cellules il est responsable
                let director_topic =
                    TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::Director).build();

                // Le paramètre 0 signifie que c'est le serveur lui-même qui s'abonne (pas un joueur)
                broker_connect.subscribe(director_topic, 0);

                //pour qu'on lui parle directement.
                let my_topic = TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ServerLine)
                    .append(server_info.uuid.as_bytes())
                    .build();

                broker_connect.subscribe(my_topic, 0);

                println!("[Server] Abonnement au topic Director Effectué.");

                // 2. Envoi du paquet Hello / Registration
                let hello_packet_header = BrokerMessageHeaders::FriendHello as u8;
                let friend_type = ServerType::Server as u8;
                let mut data = BytesMut::with_capacity(2 + 16);
                data.put_u8(hello_packet_header);
                data.put_u8(friend_type);
                data.put_slice(server_info.uuid.as_bytes());

                // On envoie le Hello sur le topic d'authentification des serveurs
                let auth_topic =
                    TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ServerConnection)
                        .build();
                broker_connect.publish_reliable(auth_topic, data.freeze());
            }
            ClientNetworkEvent::Connected => {
                println!("[Server] Connecté au Broker (Still not ready)...");
            }
            ClientNetworkEvent::Disconnected => {
                panic!("Disconnected from broker (this is bad)");
            }

            ClientNetworkEvent::DataReceived { stream: _, payload } => {
                // Le payload pur arrive ici, expurgé du topic par le broker
                route_message_events(
                    payload,
                    broker_connect,
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
    data: Bytes,
    _broker_connect: &MmoNetworkClient,
    msg_input: &mut MessageWriter<PlayerInputEvent>,
    msg_connected: &mut MessageWriter<PlayerConnected>,
    _msg_disconnected: &mut MessageWriter<PlayerDisconnected>,
    msg_chunk_assigned: &mut MessageWriter<ChunkAssignedEvent>,
) {
    if data.is_empty() {
        eprintln!("[Server] Received empty payload from broker, ignoring.");
        return;
    }
    let header_byte = data.first().unwrap();
    let header = BrokerMessageHeaders::from(*header_byte);

    let mut process_message = || -> Option<()> {
        match header {
            BrokerMessageHeaders::ClientInput => {
                println!("[Server] ClientInput message received");
                let client_id = ClientId::from_le_bytes(data.get(1..5)?.try_into().ok()?);
                let input: Input = data.get(5..(5 + 16))?.try_into().ok()?;

                let input = PlayerInput::make_from_u8_slice(input.get(0..2)?)?;
                println!("\t and forwarded to gameplay systems");

                msg_input.write(PlayerInputEvent {
                    client_id,
                    input_data: input,
                });

                Some(())
            }

            BrokerMessageHeaders::SpawnClient => {
                println!("[Server] SpawnClient message received");
                let client_id = ClientId::from_le_bytes(data.get(1..5)?.try_into().ok()?);
                let pseudo_len = data.get(5..6)?.first().cloned()? as usize;
                let pseudo_end = 6 + pseudo_len;
                let pseudo = data.get(6..pseudo_end)?;

                let pseudo = std::str::from_utf8(pseudo).ok();
                let pseudo: String = pseudo.unwrap_or_else(|| "Unknown".into()).to_string();

                msg_connected.write(PlayerConnected {
                    client_id,
                    player_name: pseudo,
                });

                Some(())
            }

            BrokerMessageHeaders::ClientDisconnect => {
                _msg_disconnected.write(PlayerDisconnected {
                    client_id: ClientId::from_le_bytes(data.get(1..5)?.try_into().ok()?),
                });
                Some(())
            } /*
            order.put_u8(BrokerMessageHeaders::TakeChunk as u8);
            order.put_i32_le(0); // Chunk X
            order.put_i32_le(0); // Chunk Y */
            BrokerMessageHeaders::TakeChunk => {
                println!("[Server] TakeChunk message received");
                //on doit se subscribe aux inputs de ce chunk :
                let x = i32::from_le_bytes(data.get(1..5)?.try_into().ok()?);
                let y = i32::from_le_bytes(data.get(5..9)?.try_into().ok()?);

                msg_chunk_assigned.write(ChunkAssignedEvent {
                    chunk: events::GameChunk { x, y },
                });
                Some(())
            }
            _ => {
                println!(
                    "[Server] Unrecognized message header received: {:?} (raw: {})",
                    header, header_byte
                );
                None
            }
        }
    };

    if process_message().is_none() {
        eprintln!("[Server] Une erreur ou un message non reconnu a été reçu sur le réseau");
    }
}
