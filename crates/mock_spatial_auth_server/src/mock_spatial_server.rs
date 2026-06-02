use shared_replication::broker_client::{ClientNetworkEvent, MmoNetworkClient};
use shared_replication::broker_topics::{
    AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION, Namespace, SecurityDomain, TopicBuilder,
};
use shared_replication::msg_game_payload::GameMessageHeaders;

use bollard::Docker;
use shared_replication::broker_message::NodeId;
use shared_replication::msg_client_server::{ClientHelloMsg, ClientWelcomeMsg};
use shared_replication::msg_dgs::{GameChunk, SpawnClientMsg, TakeChunkMessage};
use shared_replication::msg_servers::{ServerHelloMSG, ServerType};
use std::env;
use std::time::Duration;

async fn get_ip_of_named_container(docker: &Docker, container_name: &str) -> Option<String> {
    let inspect_result = docker.inspect_container(container_name, None).await;
    if inspect_result.is_err() {
        eprintln!(
            "Error inspecting container '{}': {:?}",
            container_name,
            inspect_result.err()
        );
        return None;
    }
    let inspect_result = inspect_result.unwrap_or(Default::default());
    if let Some(network_settings) = inspect_result.network_settings {
        if let Some(networks) = network_settings.networks {
            if let Some(network_config) = networks.values().next() {
                if let Some(ip_address) = &network_config.ip_address {
                    if !ip_address.is_empty() {
                        return Some(ip_address.clone());
                    }
                }
            }
        }
    }
    None
}
pub async fn run_spatial_auth_server() {
    println!("🌍 [Spatial/Auth] Démarrage du Cerveau Central...");

    let broker_private_port = env::var("BROKER_PRIVATE_PORT")
        .expect("Env BROKER_PRIVATE_PORT is not set")
        .parse()
        .expect("Env BROKER_PRIVATE_PORT is not a u16");

    let host_adress = env::var("HOST_ADDRESS").expect("Env HOST_ADRESS is not set");

    let docker = Docker::connect_with_socket_defaults().expect("Failed to connect to docker");

    let broker_ip: Option<String> = get_ip_of_named_container(&docker, "broker").await;
    if broker_ip.is_none() {
        eprintln!("❌ [Spatial/Auth] Impossible de trouver l'adresse IP du broker ");
        return;
    }
    let broker_ip = broker_ip.unwrap();
    let mut broker_api = MmoNetworkClient::new();
    broker_api
        //todo : faire ça propre
        .connect(broker_ip.as_ref(), broker_private_port)
        .expect("😡 No connexion to broker "); // Se connecte au port Privé (Confiance Totale)

    let mut active_dgs: Vec<NodeId> = Vec::new();

    // Le cerveau écoute les annonces sur le canal d'Authentification (Serveurs et Joueurs)
    let auth_private =
        TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ServerConnection).build();
    let auth_public_listen = TopicBuilder::new(
        SecurityDomain::PrivateReadPublicWrite,
        AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION,
    )
    .build();

    loop {
        while let Some(event) = broker_api.poll() {
            match event {
                ClientNetworkEvent::Ready => {
                    println!("🌍 [Spatial/Auth] Connecté au Broker Privé avec succès.");

                    // 1. S'abonner pour entendre les "Hello" des nouveaux DGS
                    broker_api.subscribe(auth_private.clone(), 0);

                    // 2. S'abonner pour entendre les requêtes de Login des vrais joueurs
                    println!(
                        "[Spatial/auth] Subscribing to {:?}",
                        auth_public_listen.clone()
                    );
                    broker_api.subscribe(auth_public_listen.clone(), 0);
                }
                ClientNetworkEvent::Connected => {
                    println!("🌍 [Spatial/Auth] Connecté au Broker (Still not ready)...");
                }
                ClientNetworkEvent::Disconnected => {
                    println!(
                        "❌ [Spatial/Auth] Perte de connexion au Broker ! Tentative de reconnexion dans 1s..."
                    );
                    //sleeps for 1 seconds
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    let _ = broker_api.connect(host_adress.as_ref(), broker_private_port);
                }
                ClientNetworkEvent::DataReceived {
                    client_id,
                    stream: _,
                    mut payload,
                } => {
                    // ==========================================
                    // 1. UN NOUVEAU SERVEUR DGS SE DÉCLARE
                    // ==========================================
                    match payload.header {
                        GameMessageHeaders::FriendHello => {
                            let friend = payload.extract::<ServerHelloMSG>();
                            if friend.is_err() {
                                eprintln!(
                                    "⚠️ [Spatial] Message FriendHello mal formé : {}",
                                    friend.err().unwrap()
                                );
                                continue;
                            }
                            let friend = friend.unwrap();

                            let server_type = friend.server_type;

                            if server_type == ServerType::Server {
                                let dgs_net_id = friend.id;
                                println!("🗺️ [Spatial] Nouveau DGS détecté : {}", dgs_net_id);
                                active_dgs.push(dgs_net_id);

                                let topic = TopicBuilder::new(
                                    SecurityDomain::PrivateRW,
                                    Namespace::NodeLine,
                                )
                                .append_id(dgs_net_id)
                                .build();

                                if active_dgs.len() == 1 {
                                    let msg = TakeChunkMessage {
                                        game_chunk: GameChunk { x: 0, y: 0 },
                                    };

                                    broker_api.publish_reliable(topic, &msg);
                                    println!(
                                        "🗺️ [Spatial] Ordre 'Prends le Chunk 0:0' envoyé au DGS."
                                    );
                                }
                            } else {
                                let server_name = match server_type {
                                    ServerType::Client => "client",
                                    ServerType::Server => "server",
                                    ServerType::Spatial => "spatial",
                                    ServerType::Orchestrator => "orchestrator",
                                    ServerType::Authentification => "authentification",
                                    ServerType::NotAFriend => "not-friend",
                                };
                                println!(
                                    "⚠️ [Spatial] Un serveur de type '{}' s'est connecté au canal d'authentification, mais ce type n'est pas attendu. Ignoré.",
                                    server_name
                                );
                                continue;
                            }
                        }
                        // ==========================================
                        // 2. UN CLIENT HUMAIN VEUT JOUER (
                        // ==========================================
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
                                broker_api.kick_client(client_id);
                                continue;
                            }

                            broker_api.authorize_client(client_id);

                            let chunk = GameChunk { x: 0, y: 0 };

                            let chunk_0_0_state = TopicBuilder::new(
                                SecurityDomain::PublicReadPrivateWrite,
                                Namespace::Chunk,
                            )
                            .append_chunk(&chunk)
                            .build();

                            broker_api.subscribe(chunk_0_0_state, client_id);
                            println!(
                                "🔑 [Auth] Le Broker a abonné le joueur {} au Chunk 0:0.",
                                client_id
                            );
                            if active_dgs.len() < 1 {
                                continue;
                            }

                            let server_topic =
                                TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::NodeLine)
                                    .append_id(active_dgs[0])
                                    .build();

                            // B) Dire au server de faire spawn :
                            let msg = SpawnClientMsg {
                                client_id,
                                pseudo: msg.pseudo.to_string(),
                                chunk: chunk.clone(),
                            };

                            broker_api.publish_reliable(server_topic, &msg);

                            let msg = ClientWelcomeMsg { client_id, chunk };
                            let client_topic = TopicBuilder::new(
                                SecurityDomain::PublicReadPrivateWrite,
                                Namespace::NodeLine,
                            )
                            .append_id(client_id)
                            .build();
                            broker_api.publish_reliable(client_topic, &msg);
                        }

                        _ => {}
                    }
                }
            }
        }
        // Économie de CPU
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
