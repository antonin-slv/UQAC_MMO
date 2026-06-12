use crate::quadtree::{Entity, QuadTree};
use crate::shard_manager::ShardManager;
use broker_client::{ClientNetworkEvent, MmoNetworkClient};
use broker_protocol::broker_message::{NodeId, NodeIdMetaData};
use broker_protocol::topic_patterns::TopicPattern;
use broker_protocol::topics::{Namespace, SecurityDomain, TopicBuilder};
use core_types::chunks::get_chunk_size;
use core_types::{Rect, Vec2};
use game_message::msg_client_server::{ClientHelloMsg, ClientWelcomeMsg, PersonalSnapshot};
use game_message::msg_dgs::{ChunkHandOff, ChunkHandOffAction, HeartbeatMessage, SpawnClientMsg};
use game_message::msg_entities::NetComponent;
use game_message::msg_servers::{ServerHelloMSG, ServerType, SpawnServerMSG};
use game_message::GameMessageHeaders;
use std::env;

const BROKER_URL_ENV_NAME: &str = "BROKER_URL";

pub struct BrokerClient {
    broker_api: MmoNetworkClient,
    chunk_size: f32,
    max_depth: u8,
    world_size: f32,
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

        let world_size: f32 = env::var("WORLD_SIZE")
            .expect("Env WORLD_SIZE is not set")
            .parse()
            .expect("Env WORLD_SIZE is not a number");
        let max_depth = env::var("QUADTREE_MAX_DEPTH").unwrap().parse().unwrap();

        let chunk_size = get_chunk_size(world_size, max_depth);

        Self {
            broker_api: broker_client,
            world_size,
            max_depth,
            chunk_size,
        }
    }

    pub async fn poll_handle(
        &mut self,
        quad_tree: &mut Option<QuadTree>,
        shard_manager: &mut ShardManager,
    ) -> bool {
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
                        TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ClientAuth).build(),
                        0,
                    );

                    self.broker_api.subscribe(
                        TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ServerConnection)
                            .build(),
                        0,
                    );

                    self.broker_api.subscribe(
                        TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::Heartbeat).build(),
                        0,
                    );

                    let mut world_size = self.world_size;
                    world_size /= 2.0;
                    let map_size = Rect {
                        min_x: -world_size,
                        max_x: world_size,
                        min_y: -world_size,
                        max_y: world_size,
                    };

                    let bounds = map_size.bounding_chunk_aera(self.chunk_size);

                    let topic_pattern = TopicPattern::new()
                        .with_head(Namespace::Chunk, SecurityDomain::PublicReadPrivateWrite)
                        .with_layers(bounds);

                    self.broker_api.batch_subscribe(topic_pattern, 0);
                    return true;
                }
                ClientNetworkEvent::Connected => {
                    println!("[Server] Connecté au Broker (Still not ready)...");
                }
                ClientNetworkEvent::Disconnected(removed_node_id) => {
                    println!("On disconnrction");
                    match self.broker_api.node_id {
                        Some(node_id) => {
                            if node_id == removed_node_id {
                                panic!("Disconnected from broker (this is bad)");
                            } else {
                                if removed_node_id.is_server() {
                                    shard_manager.on_dgs_stopped(removed_node_id);
                                } else if removed_node_id.is_client() {
                                    shard_manager.on_client_disconnected(removed_node_id);
                                }
                            }
                        }
                        None => {
                            panic!("Disconnected from broker (this is bad)");
                        }
                    }
                }
                ClientNetworkEvent::DataReceived {
                    client_id,
                    stream: _,
                    mut payload,
                } => match payload.header {
                    GameMessageHeaders::ClientHello => match payload.extract::<ClientHelloMsg>() {
                        Ok(msg) => {
                            if let Some(quad_tree) = quad_tree {
                                self.on_new_client_connected(
                                    client_id,
                                    quad_tree,
                                    shard_manager,
                                    msg,
                                )
                                .await;
                            }
                        }
                        Err(e) => {
                            println!("⚠️ [Auth] Message ClientHello mal formé : {}", e);
                        }
                    },
                    GameMessageHeaders::FriendHello => match payload.extract::<ServerHelloMSG>() {
                        Ok(friend) => {
                            if let Some(quad_tree) = quad_tree {
                                self.on_server_connected(friend, shard_manager, quad_tree)
                                    .await;
                            }
                        }
                        Err(e) => {
                            println!("[DGS] DGS Connected error {}", e)
                        }
                    },
                    GameMessageHeaders::Heartbeat => match payload.extract::<HeartbeatMessage>() {
                        Ok(heartbeat) => {
                            if let Some(quad_tree) = quad_tree {
                                shard_manager.on_heartbeat_receive(
                                    heartbeat.heartbeat.node_id,
                                    quad_tree,
                                    self,
                                )
                            }
                        }
                        Err(e) => println!("[Orchestrator] Heartbeat error: {}", e),
                    },
                    GameMessageHeaders::Snapshot => match payload.extract::<PersonalSnapshot>() {
                        Ok(snapshot) => {
                            //println!("[Orchestrator] Snapshot received");
                            for entity_snapshot in snapshot.entities {
                                let mut pos = Vec2::new(0.0, 0.0);
                                for comp in entity_snapshot.updates {
                                    if let NetComponent::Position(comp_pos) = comp {
                                        pos = comp_pos;
                                        break;
                                    }
                                }
                                let entity = Entity::new(entity_snapshot.net_id, pos);
                                //println!("Entity ID {}", entity_snapshot.network_id);
                                if let Some(quad_tree) = quad_tree {
                                    quad_tree.insert(entity, shard_manager, self);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[Client] Erreur de parsing du Snapshot : {}", e);
                        }
                    },
                    _ => {}
                },
            }
        }

        false
    }

    pub fn spawn_new_dgs(&self, server_count: usize) {
        let message = SpawnServerMSG {
            server_count: server_count as u8,
        };
        self.broker_api.publish_reliable(
            TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::Director).build(),
            &message,
        );
    }

    async fn on_server_connected(
        &self,
        friend: ServerHelloMSG,
        shard_manager: &mut ShardManager,
        quad_tree: &QuadTree,
    ) {
        let server_type = friend.server_type;

        if server_type == ServerType::Server {
            let dgs_net_id = friend.id;
            println!("🗺️ [Spatial] Nouveau DGS détecté : {}", dgs_net_id);
            shard_manager.on_new_dgs(dgs_net_id, quad_tree, self);
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

        let new_entity_pos = Vec2::new(
            rand::random_range(0.0..self.chunk_size),
            rand::random_range(0.0..self.chunk_size),
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

        let dgs = shard_manager.get_dgs_for_position(new_entity_pos, quad_tree);
        if let Some(dgs) = dgs {
            let chunk = new_entity_pos.get_chunk(self.chunk_size);

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
                .append_id(dgs)
                .build();

            // B) Dire au server de faire spawn :
            let msg = SpawnClientMsg {
                client_id,
                pseudo: msg.pseudo.to_string(),
                chunk: chunk.clone(),
            };

            self.broker_api.publish_reliable(server_topic, &msg);
            let msg = ClientWelcomeMsg {
                client_id,
                chunk,
                chunk_size: self.chunk_size,
            };
            let client_topic =
                TopicBuilder::new(SecurityDomain::PublicReadPrivateWrite, Namespace::NodeLine)
                    .append_id(client_id)
                    .build();
            self.broker_api.publish_reliable(client_topic, &msg);
        } else {
            eprintln!("[BROKER] Shard not found for spawn player")
        }
    }

    pub fn assign_shard_to_dgs(&self, dgs_id: NodeId, areas: Vec<(Rect, Option<NodeId>)>) {
        println!("Assign areas {:?} to DGS {}", areas, dgs_id);
        let topic = TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::NodeLine)
            .append_id(dgs_id)
            .build();

        let chunk_hand_off = ChunkHandOff {
            action: ChunkHandOffAction::TakeArea,
            areas,
        };

        self.broker_api.publish_reliable(topic, &chunk_hand_off);
    }

    pub fn remove_shard_to_dgs(&self, dgs_id: NodeId, areas: Vec<(Rect, Option<NodeId>)>) {
        let topic = TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::NodeLine)
            .append_id(dgs_id)
            .build();

        let chunk_hand_off = ChunkHandOff {
            action: ChunkHandOffAction::ReleaseArea,
            areas,
        };

        self.broker_api.publish_reliable(topic, &chunk_hand_off);
    }
}
