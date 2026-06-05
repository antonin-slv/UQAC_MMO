use crate::quadtree::{Entity, QuadTree};
use crate::shard_manager::ShardManager;
use broker_client::{ClientNetworkEvent, MmoNetworkClient};
use broker_protocol::broker_message::NodeId;
use broker_protocol::broker_topics::{Namespace, SecurityDomain, TopicBuilder};
use core_types::{GameChunk, Rect, Vec2};
use game_message::msg_client_server::{ClientHelloMsg, ClientWelcomeMsg};
use game_message::msg_dgs::{ChunkHandOff, ChunkHandOffAction, SpawnClientMsg};
use game_message::msg_servers::{ServerHelloMSG, ServerType};
use game_message::{GameMessageHeaders, GamePayload};
use std::env;

const BROKER_URL_ENV_NAME: &str = "BROKER_URL";

pub struct BrokerClient {
    broker_api: MmoNetworkClient,
}

impl BrokerClient {
    pub fn new() -> Self {
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

        let broker_client = MmoNetworkClient::new();
        println!(
            "[Server] Starting broker on : {}:{}",
            broker_ip, broker_port
        );
        if broker_client.connect(broker_ip, broker_port).is_err() {
            panic!("Error : Connection To broker failed.");
        }

        println!("[Server] Connection sent");

        Self {
            broker_api: broker_client,
        }
    }

    pub async fn poll_handle(
        &mut self,
        quad_tree: &mut QuadTree,
        shard_manager: &mut ShardManager,
    ) {
        while let Some(event) = self.broker_api.poll() {
            match event {
                ClientNetworkEvent::Ready => {
                    println!("[Server] Connection ready");

                    // Auth to broker
                    let msg = ServerHelloMSG {
                        server_type: ServerType::Spatial,
                        id: self.broker_api.node_id.unwrap_or_else(|| {
                            panic!(
                                "Orchestrator has no node ID after Ready event, using 0 as fallback"
                            );
                        }),
                    };

                    let auth_topic =
                        TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ServerConnection)
                            .build();
                    self.broker_api.publish_reliable(auth_topic.clone(), &msg);

                    // ================= Subscriptions ===================

                    self.broker_api.subscribe(
                        TopicBuilder::new(
                            SecurityDomain::PrivateReadPublicWrite,
                            Namespace::ClientAuth,
                        )
                        .build(),
                        0,
                    );

                    self.broker_api.subscribe(
                        TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::SpatialServer)
                            .build(),
                        0,
                    );

                    self.broker_api.subscribe(
                        TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ServerConnection)
                            .build(),
                        0,
                    );
                }
                ClientNetworkEvent::Connected => {
                    println!("[Server] Connecté au Broker (Still not ready)...");
                }
                ClientNetworkEvent::Disconnected => {
                    panic!("Disconnected from broker (this is bad)");
                }
                ClientNetworkEvent::DataReceived {
                    client_id,
                    stream: _,
                    mut payload,
                } => match payload.header {
                    GameMessageHeaders::ClientHello => {
                        let msg = payload.extract::<ClientHelloMsg>();
                        if msg.is_err() {
                            println!(
                                "⚠️ [Auth] Message ClientHello mal formé : {}",
                                msg.err().unwrap()
                            );
                            continue;
                        }
                        let msg = msg.unwrap();
                        self.on_new_client_connected(client_id, quad_tree, shard_manager, msg)
                            .await;
                    }
                    GameMessageHeaders::FriendHello => {
                        self.on_server_connected(payload, shard_manager).await;
                    }
                    GameMessageHeaders::ChunkHandOff => {
                        println!("[Orchestrator] Got ChunkHandOff");
                    }
                    _ => {}
                },
            }
        }
    }

    async fn on_server_connected(
        &self,
        mut payload: GamePayload,
        shard_manager: &mut ShardManager,
    ) {
        let friend = payload.extract::<ServerHelloMSG>();
        if friend.is_err() {
            eprintln!(
                "⚠️ [Spatial] Message FriendHello mal formé : {}",
                friend.err().unwrap()
            );
            return;
        }
        let friend = friend.unwrap();

        let server_type = friend.server_type;

        if server_type == ServerType::Server {
            let dgs_net_id = friend.id;
            println!("🗺️ [Spatial] Nouveau DGS détecté : {}", dgs_net_id);
            shard_manager.on_new_dgs(dgs_net_id);
        }
    }

    async fn on_new_client_connected(
        &mut self,
        client_id: NodeId,
        quad_tree: &mut QuadTree,
        shard_manager: &mut ShardManager,
        msg: ClientHelloMsg,
    ) {
        println!("[Orchestrator] On new client connected");

        quad_tree.insert(
            Entity::new(
                client_id,
                Vec2::new(
                    rand::random_range(-300.0..300.0),
                    rand::random_range(-300.0..300.0),
                ),
            ),
            shard_manager,
            self,
        );

        println!(
            "🔑 [Auth] Requête de connexion du client {} (pseudo: {})",
            client_id, msg.pseudo
        );

        let con_ok = true; // AUTHENTIFICATION ICI !

        if !con_ok {
            println!(
                "🔑 [Auth] Rejet de la connexion du client {} (pseudo: {})",
                client_id, msg.pseudo
            );
            self.broker_api.kick_client(client_id);
            return;
        }

        self.broker_api.authorize_client(client_id);

        let shard = shard_manager.get_shard_bounds_for_client(client_id, quad_tree);
        if let Some((dgs_id, bounds)) = shard {
            let chunk = GameChunk {
                x: bounds.min_x as i16,
                y: bounds.min_y as i16,
            };

            let chunk_state =
                TopicBuilder::new(SecurityDomain::PublicReadPrivateWrite, Namespace::Chunk)
                    .append_chunk(&chunk)
                    .build();

            self.broker_api.subscribe(chunk_state, client_id);
            println!(
                "🔑 [Auth] Le Broker a abonné le joueur {} au Chunk {}:{}.",
                client_id, chunk.x, chunk.y
            );

            let server_topic = TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::NodeLine)
                .append_id(dgs_id)
                .build();

            // B) Dire au server de faire spawn :
            let msg = SpawnClientMsg {
                client_id,
                pseudo: msg.pseudo.to_string(),
                chunk: chunk.clone(),
            };

            self.broker_api.publish_reliable(server_topic, &msg);
            let msg = ClientWelcomeMsg { client_id, chunk };
            let client_topic =
                TopicBuilder::new(SecurityDomain::PublicReadPrivateWrite, Namespace::NodeLine)
                    .append_id(client_id)
                    .build();
            self.broker_api.publish_reliable(client_topic, &msg);
        } else {
            eprintln!("[BROKER] Shard not found for spawn player")
        }
    }

    pub fn assign_shard_to_dgs(
        &self,
        dgs_id: NodeId,
        areas: Vec<Rect>,
        old_dgs_ids: Vec<NodeId>,
    ) {
        let topic = TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::NodeLine)
            .append_id(dgs_id)
            .build();

        let chunk_hand_off = ChunkHandOff {
            action: ChunkHandOffAction::ReleaseArea,
            areas,
            old_dgs_ids,
        };

        self.broker_api.publish_reliable(topic, &chunk_hand_off);
    }

    pub fn remove_shard_to_dgs(
        &self,
        dgs_id: NodeId,
        areas: Vec<Rect>,
        old_dgs_ids: Vec<NodeId>,
    ) {
        let topic = TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::NodeLine)
            .append_id(dgs_id)
            .build();

        let chunk_hand_off = ChunkHandOff {
            action: ChunkHandOffAction::ReleaseArea,
            areas,
            old_dgs_ids,
        };

        self.broker_api.publish_reliable(topic, &chunk_hand_off);
    }
}
