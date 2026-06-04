use crate::quadtree::Entity;
use crate::QuadTreeCommand;
use broker_client::{ClientNetworkEvent, MmoNetworkClient};
use broker_protocol::broker_topics::{
    Namespace, SecurityDomain, TopicBuilder, AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION,
};
use core_types::Vec2;
use game_message::msg_servers::{ServerHelloMSG, ServerType};
use game_message::GameMessageHeaders;
use std::env;
use tokio::sync::mpsc::Sender;
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

    pub async fn poll_handle(&mut self, sender: &Sender<QuadTreeCommand>) {
        while let Some(event) = self.broker_api.poll() {
            match event {
                ClientNetworkEvent::Ready => {
                    println!("[Server] Connection ready");

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
                    println!("[Orchestrator] Handshake 'Hello' envoyé.");

                    let auth_public_listen = TopicBuilder::new(
                        SecurityDomain::PrivateReadPublicWrite,
                        AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION,
                    )
                    .build();
                    self.broker_api.subscribe(auth_public_listen, 0);
                    println!("[Orchestrator] Abonné au topic Auth Public Clients.");

                    let hand_off_responses =
                        TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::SpatialServer)
                            .build();
                    self.broker_api.subscribe(hand_off_responses, 0);
                    println!("[Orchestrator] Abonné au topic réponses des Hands off.");
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
                    payload,
                } => match payload.header {
                    GameMessageHeaders::ClientHello => {
                        if sender
                            .send(QuadTreeCommand::MoveEntity(Entity::new(
                                client_id,
                                Vec2::new(
                                    rand::random_range(-300.0..300.0),
                                    rand::random_range(-300.0..300.0),
                                ),
                            )))
                            .await
                            .is_err()
                        {
                            println!("[Acteur QuadTree] Failed to send move entity (id 3)");
                        }
                        println!("[Orchestrator] Got ClientHello");
                    }
                    GameMessageHeaders::ChunkHandOff
                    => {
                        println!("[Orchestrator] Got ChunkHandOff");
                    }
                    _ => {}
                },
            }
        }
    }
}
