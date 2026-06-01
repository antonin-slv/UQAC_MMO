use bytes::{BufMut, BytesMut};
use shared_replication::broker_client::{ClientNetworkEvent, MmoNetworkClient};
use shared_replication::broker_topics::{
    AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION, BrokerMessageHeaders, Namespace, SecurityDomain,
    TopicBuilder,
};
use shared_replication::servers::ServerType;
use std::env;
use std::time::Duration;
use uuid::Uuid;

use bollard::Docker;

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

    let mut active_dgs: Vec<Uuid> = Vec::new();

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
                    broker_api.subscribe(auth_private, 0);

                    // 2. S'abonner pour entendre les requêtes de Login des vrais joueurs
                    broker_api.subscribe(auth_public_listen, 0);
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
                ClientNetworkEvent::DataReceived { stream: _, payload } => {
                    if payload.is_empty() {
                        continue;
                    }

                    let header = payload[0];
                    let header = BrokerMessageHeaders::from(header);
                    // ==========================================
                    // 1. UN NOUVEAU SERVEUR DGS SE DÉCLARE
                    // ==========================================
                    match header {
                        BrokerMessageHeaders::FriendHello => {
                            // Tag(1) + Type(1) + UUID(16)
                            let server_type = payload[1];

                            if server_type == ServerType::Server as u8 {
                                let mut uuid_bytes = [0u8; 16];
                                uuid_bytes.copy_from_slice(&payload[2..18]);
                                let dgs_uuid = Uuid::from_bytes_le(uuid_bytes);

                                println!("🗺️ [Spatial] Nouveau DGS détecté : {}", dgs_uuid);
                                active_dgs.push(dgs_uuid);

                                let topic = TopicBuilder::new(
                                    SecurityDomain::PrivateRW,
                                    Namespace::ServerLine,
                                )
                                .append(uuid_bytes.as_ref())
                                .build();

                                if active_dgs.len() == 1 {
                                    let mut order = BytesMut::new();
                                    order.put_u8(BrokerMessageHeaders::TakeChunk as u8);
                                    order.put_i32_le(0); // Chunk X
                                    order.put_i32_le(0); // Chunk Y

                                    broker_api.publish_reliable(topic, order.freeze());
                                    println!("🗺️ [Spatial] Ordre 'Prends le Chunk 0:0' envoyé au DGS.");
                                }
                            }
                        }
                        // ==========================================
                        // 2. UN CLIENT HUMAIN VEUT JOUER (
                        // ==========================================
                        BrokerMessageHeaders::BrokerBrodcastClientHello => {
                            // Sécurité : Header(1) + ID(4) = 5 octets minimum pour lire l'ID
                            if payload.len() < 5 {
                                eprintln!("⚠️ [Auth] ClientHello trop court");
                                continue;
                            }

                            let client_id = u32::from_le_bytes(payload[1..5].try_into().unwrap());

                            if client_id == 0 {
                                eprintln!(
                                    "⚠️ [Auth] Reçu un ClientHello avec un ID invalide (0). Ignoré."
                                );
                                continue;
                            }
                            let pseudo = if payload.len() > 5 {
                                std::str::from_utf8(&payload[5..]).unwrap_or("NO_PSEUDO")
                            } else {
                                "NO_PSEUDO"
                            };

                            println!(
                                "👤 [Auth] Nouveau client {} avec le pseudo '{}' veut jouer !",
                                client_id, pseudo
                            );

                            broker_api.authorize_client(client_id);

                            let specific_client_topic =
                                TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ClientLine)
                                    .append_entity(client_id)
                                    .build();
                            broker_api.subscribe(specific_client_topic, client_id);

                            let chunk_0_0_state = TopicBuilder::new(
                                SecurityDomain::PublicReadPrivateWrite,
                                Namespace::Chunk,
                            )
                            .append_grid(0, 0)
                            .build();

                            broker_api.subscribe(chunk_0_0_state, client_id);
                            println!(
                                "🔑 [Auth] Le Broker a abonné le joueur {} au Chunk 0:0.",
                                client_id
                            );
                            let server_topic =
                                TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::ServerLine)
                                    .append(active_dgs[0].as_ref())
                                    .build();

                            // B) Dire au server de faire spawn :
                            let mut spawn_msg = BytesMut::new();
                            spawn_msg.put_u8(BrokerMessageHeaders::SpawnClient as u8);
                            spawn_msg.put_u32_le(client_id); // On lui donne son identifiant officiel
                            spawn_msg.put_i32_le(0);
                            spawn_msg.put_i32_le(0);

                            broker_api.publish_reliable(server_topic, spawn_msg.freeze());

                            let mut welcome_msg = BytesMut::new();
                            welcome_msg.put_u8(BrokerMessageHeaders::ClientWelcome as u8);
                            welcome_msg.put_u32_le(client_id);
                            welcome_msg.put_i32_le(0); // Chunk X
                            welcome_msg.put_i32_le(0); // Chunk Y

                            broker_api.publish_reliable(specific_client_topic, welcome_msg.freeze());
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
