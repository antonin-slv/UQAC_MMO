pub mod broker_inner_structs;

use game_sockets::protocols::QuicBackend;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use shared_replication::broker::*;
use std::collections::{HashMap, HashSet};
use std::env;
use crate::broker_inner_structs::{ConnectionOwner, ConnectionRegistry};

const LISTEN_PORT_ENV_NAME: &str = "BROKER_PORT";

fn main() {
    let listen_port = env::var(LISTEN_PORT_ENV_NAME);

    let listen_port = match listen_port {
        Ok(port_str) => port_str.parse::<u16>().unwrap_or_else(|_| {
            panic!("Error : {} must be a valid u16", LISTEN_PORT_ENV_NAME);
        }),
        Err(_) => {
            panic!("Error : {} must be set", LISTEN_PORT_ENV_NAME);
        }
    };
    println!("[Broker] My Port Is : {}", listen_port);

    let socket = GamePeer::new(QuicBackend::new());

    socket
        .listen("127.0.0.1:", listen_port)
        .expect("The socket for the broker could not be created");

    run_broker(socket)
}

fn run_broker(mut peer: GamePeer) {
    // 1. Les tables de routage (État)

    // pas un arbre car flemme (le CPU a la flemme)
    // Qui écoute quoi ? (Topic -> Liste des connexions clients)
    // Utilisé quand un Shard fait un Publish (0x03)
    let mut topic_to_client: HashMap<Topic, HashSet<ClientId>> = HashMap::new();

    // Client_id -> Topic actuel
    // Utilisé pour savoir à quel Shard envoyer un ClientInput (0x05)
    let mut client_id_to_topic: HashMap<ClientId, HashSet<Topic>> = HashMap::new();

    let mut connection_registry: ConnectionRegistry = ConnectionRegistry::new();

    loop {
        while let Ok(Some(event)) = peer.poll() {
            match event {
                GameNetworkEvent::Message {
                    connection,
                    stream,
                    data,
                } => {
                    // Si le paquet est vide, on l'ignore (first renvoie Option, on gère la fallback)
                    let discard_message =
                        BrokerMessageHeaders::DiscardedMessageBecauseYouKnow as u8;
                    let header_byte = data.first().unwrap_or(&discard_message);
                    let header = BrokerMessageHeaders::from(*header_byte);

                    let mut process_message = || -> Option<()> {
                        match header {
                            BrokerMessageHeaders::SubscribeClient => {
                                let client_id = ClientId::extract_from_slice(data.get(1..5)?)?;
                                let topic: Topic = data.get(5..37)?.try_into().ok()?;

                                topic_to_client
                                    .entry(topic.clone())
                                    .or_insert_with(HashSet::new)
                                    .insert(client_id);

                                client_id_to_topic
                                    .entry(client_id)
                                    .or_insert_with(HashSet::new)
                                    .insert(topic);
                            }
                            BrokerMessageHeaders::UnsubscribeClient => {
                                let client_id = ClientId::extract_from_slice(data.get(1..5)?)?;
                                let topic: Topic = data.get(5..37)?.try_into().ok()?;

                                topic_to_client.get_mut(&topic)?.remove(&client_id);
                                if topic_to_client.is_empty() {
                                    topic_to_client.remove(&topic);
                                }

                                client_id_to_topic.get_mut(&client_id)?.remove(&topic);
                                if client_id_to_topic.is_empty() {
                                    client_id_to_topic.remove(&client_id);
                                }
                            }
                            BrokerMessageHeaders::ShardBroadcast => {
                                let topic: Topic = data.get(1..33)?.try_into().ok()?;

                                if let Some(client_ids) = topic_to_client.get(&topic) {
                                    for client_id in client_ids.iter() {
                                        if let Some(conn) =
                                            connection_registry.client_id_to_co.get(client_id)
                                        {
                                            let _ = peer.send(conn, &stream, data.clone());
                                        }
                                    }
                                }
                            }
                            BrokerMessageHeaders::ClientInput => {
                                let client_id = ClientId::extract_from_slice(data.get(1..5)?)?;
                                if *connection_registry.get_client_by_co(&connection)? != client_id
                                {
                                    //securitt check
                                    return None;
                                }

                                if let Some(topics) = client_id_to_topic.get(&client_id) {
                                    //liaison client ->location
                                    for topic in topics {
                                        if let Some(shard_conn) =
                                            connection_registry.server_id_to_co.get(topic)
                                        {
                                            //liaison location -> shard
                                            let _ = peer.send(shard_conn, &stream, data.clone());
                                        }
                                    }
                                }
                            }
                            _ => {
                                println!("[Broker] Unknown message type from {:?}", connection);
                            }
                        }
                        Some(()) // Fin de la closure avec succès
                    };

                    // On exécute la closure. Si elle renvoie None, c'est que le paquet
                    // était trop court, il est simplement ignoré, le serveur survit !
                    if process_message().is_none() {
                        println!(
                            "[Broker] Ignored malformed or truncated packet from {:?}",
                            connection
                        );
                    }
                }

                GameNetworkEvent::Disconnected(conn) => {
                    if let Some(owner) = connection_registry.remove_by_co(&conn) {
                        match owner {
                            ConnectionOwner::Client(client_id) => {
                                client_id_to_topic.remove(&client_id);

                                topic_to_client
                                    .iter_mut()
                                    .for_each(|(_topic, subscribers)| {
                                        subscribers.remove(&client_id);
                                    });

                                println!(
                                    "[Broker] Client {} déconnecté et nettoyé proprement.",
                                    client_id
                                );
                            }
                            ConnectionOwner::Shard(topic) => {
                                // ?
                            }
                            _ => {}
                        }
                    }
                }

                GameNetworkEvent::Connected(conn) => {
                    println!("[Broker] Nouvelle connexion : {:?}", conn);
                }

                _ => {}
            }
        }

        //todo : retirer ça en prod.
        std::thread::sleep(std::time::Duration::from_micros(100));
    }
}
