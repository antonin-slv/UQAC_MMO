use indexmap::IndexSet;
use nohash_hasher::IntSet;
use rustc_hash::FxHashMap;
use std::env;
use uuid::Uuid;

use broker_protocol::broker_message::BrokerMessage::{BroadCastFrom, Broadcast};
use broker_protocol::broker_message::{BrokerMessage, NodeId, NodeIdMetaData, RELIABLE_STREAM_ID};
use broker_protocol::topics;
use broker_protocol::topics::{
    Namespace, SecurityDomain, Topic, TopicBuilder, TopicInterface,
};
use game_sockets::protocols::QuicBackend;
use game_sockets::GameStreamReliability::Reliable;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream};

pub type FastMap<K, V> = FxHashMap<K, V>;
pub type FastSet<K> = IntSet<K>;
pub type FastIterableSet<K> = IndexSet<K>;

const CLIENT_LISTEN_PORT_ENV_NAME: &str = "BROKER_PUBLIC_PORT";
const SERVER_LISTEN_PORT_ENV_NAME: &str = "BROKER_PRIVATE_PORT";

pub struct Broker {
    public_peer: GamePeer,
    private_peer: GamePeer,

    // Générateurs d'ID
    next_client_id: NodeId,
    next_server_id: NodeId,

    uuid_to_node_id: FastMap<Uuid, NodeId>,
    client_connections: FastMap<NodeId, GameConnection>,
    server_connections: FastMap<NodeId, GameConnection>,
    // Pare-feu (Clients uniquement)
    not_authenticated_clients: FastSet<NodeId>,

    // Abonnements Séparés
    client_subscribers: FastMap<Topic, FastIterableSet<NodeId>>,
    server_subscribers: FastMap<Topic, FastIterableSet<NodeId>>,

    // Pour la déconnexion
    node_id_to_topics: FastMap<NodeId, FastIterableSet<Topic>>,
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

impl Broker {
    pub fn new() -> Self {
        let public_port = env::var(CLIENT_LISTEN_PORT_ENV_NAME)
            .unwrap_or_else(|_| "8000".into())
            .parse()
            .unwrap_or(8000);
        let private_port = env::var(SERVER_LISTEN_PORT_ENV_NAME)
            .unwrap_or_else(|_| "8001".into())
            .parse()
            .unwrap_or(8001);

        let public_peer = GamePeer::new(QuicBackend::new());
        let private_peer = GamePeer::new(QuicBackend::new());

        public_peer
            .listen("0.0.0.0", public_port)
            .expect("Impossible de bind le port public");
        private_peer
            .listen("0.0.0.0", private_port)
            .expect("Impossible de bind le port privé");

        println!(
            "🚀 Broker Démarré. Public: {}, Privé (Interne): {}",
            public_port, private_port
        );

        Self {
            public_peer,
            private_peer,
            next_client_id: NodeId::FIRST_CLIENT_ID,
            next_server_id: NodeId::FIRST_SERVER_ID,
            uuid_to_node_id: FastMap::default(),
            client_connections: FastMap::default(),
            server_connections: FastMap::default(),
            not_authenticated_clients: FastSet::default(),

            client_subscribers: FastMap::default(),
            server_subscribers: FastMap::default(),
            node_id_to_topics: FastMap::default(),
        }
    }

    pub fn run(&mut self) {
        loop {
            while let Ok(Some(event)) = self.public_peer.poll() {
                self.handle_public_event(event);
            }
            while let Ok(Some(event)) = self.private_peer.poll() {
                self.handle_private_event(event);
            }
        }
    }

    fn handle_disconnect(&mut self, conn_uuid: Uuid) {
        if let Some(node_id) = self.uuid_to_node_id.remove(&conn_uuid) {
            let map = if node_id.is_server() {
                &mut self.server_connections
            } else {
                &mut self.client_connections
            };
            map.remove(&node_id);

            if let Some(topics) = self.node_id_to_topics.remove(&node_id) {
                for topic in topics {
                    self.node_unsubscribe(node_id, topic);
                }
            }

            let namespace = if node_id.is_server() {
                println!("[RÉSEAU] Serveur déconnecté : ID {:X}", node_id);
                Namespace::ServerConnection
            } else {
                self.not_authenticated_clients.remove(&node_id);
                println!("[RÉSEAU] Client déconnecté : ID {}", node_id);
                Namespace::ClientAuth
            };
            let system_topic = TopicBuilder::new(SecurityDomain::PrivateRW, namespace).build();

            let mock_stream = GameStream::new(RELIABLE_STREAM_ID, Reliable);
            let disconnect_msg = BrokerMessage::NodeDisconnected(node_id);
            self.route_message(disconnect_msg, system_topic, mock_stream);

            // 4. Retrait final des maps de connexions
            if node_id.is_server() {
                self.server_connections.remove(&node_id);
            } else {
                self.client_connections.remove(&node_id);
            }
        }
    }

    // ==========================================
    // RÉSEAU PUBLIC (Clients / Zéro Confiance)
    // ==========================================

    fn handle_public_event(&mut self, event: GameNetworkEvent) {
        match event {
            GameNetworkEvent::Connected(conn) => {
                let id = self.next_client_id;
                self.next_client_id += 1;
                self.uuid_to_node_id.insert(conn.connection_uuid, id);
                self.client_connections.insert(id, conn);
                self.not_authenticated_clients.insert(id);
                let direct_line_topic =
                    TopicBuilder::new(SecurityDomain::PublicReadPrivateWrite, Namespace::NodeLine)
                        .append_id(id)
                        .build();
                self.node_subscribe(id, direct_line_topic);
                println!("[RÉSEAU PUBLIC] Nouveau client connecté : ID {}", id);
            }

            GameNetworkEvent::StreamCreated(connection, stream) => {
                let Some(&node_id) = self.uuid_to_node_id.get(&connection.connection_uuid) else {
                    return;
                };

                if stream.is_reliable() && stream.real_stream_id() == RELIABLE_STREAM_ID {
                    let msg = BrokerMessage::Welcome(node_id);
                    self.public_peer
                        .send(&connection, &stream, msg.serialize())
                        .unwrap_or_else(|e| {
                            eprintln!("[RÉSEAU PUBLIC] Erreur Welcome (Client {}): {}", node_id, e);
                        });
                }
            }
            GameNetworkEvent::Disconnected(conn) => {
                self.handle_disconnect(conn.connection_uuid);
            }
            GameNetworkEvent::Message {
                connection,
                stream,
                data,
            } => {
                let Some(&node_id) = self.uuid_to_node_id.get(&connection.connection_uuid) else {
                    return;
                };

                let message = match BrokerMessage::deserialize(data) {
                    Ok(msg) => msg,
                    Err(e) => {
                        eprintln!(
                            "[RÉSEAU PUBLIC] Erreur de parsing (Client {}): {}",
                            node_id, e
                        );
                        return;
                    }
                };

                match message {
                    BrokerMessage::Subscribe { topic, .. } => {
                        if topic.security_domain() == Some(SecurityDomain::PublicReadPrivateWrite) {
                            self.node_subscribe(node_id, topic);
                        } else {
                            eprintln!("[SÉCURITÉ] Accès LECTURE refusé à {}", node_id);
                        }
                    }
                    BrokerMessage::BatchSubscribe { .. } => {
                        println!(
                            "[RÉSEAU PUBLIC] Client {} abonnement batch. REFUSE",
                            node_id
                        );
                    }
                    BrokerMessage::BatchUnsubscribe { .. } => {
                        println!(
                            "[RÉSEAU PUBLIC] Client {} désinscription batch. REFUSE",
                            node_id
                        );
                    }
                    BrokerMessage::Unsubscribe { topic, .. } => {
                        self.node_unsubscribe(node_id, topic);
                    }
                    BrokerMessage::Publish { topic, payload } => {
                        if topic.security_domain() != Some(SecurityDomain::PrivateReadPublicWrite) {
                            eprintln!("[SÉCURITÉ] Accès ÉCRITURE refusé à {}", node_id);
                            return;
                        }
                        if !self.not_authenticated_clients.contains(&node_id)
                            || topic.namespace()
                                == Some(topics::AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION)
                        {
                            self.route_message(
                                BroadCastFrom {
                                    client_id: node_id,
                                    payload,
                                },
                                topic,
                                stream,
                            );
                        } else {
                            eprintln!(
                                "[SÉCURITÉ] Client {} non authentifié tente de publier !",
                                node_id
                            );
                        }
                    }
                    _ => {
                        eprintln!(
                            "[SÉCURITÉ] Tag invalide ou direct non autorisé venant de {}",
                            node_id
                        );
                    }
                }
            }
            _ => {}
        }
    }

    // ==========================================
    // RÉSEAU PRIVÉ (Shards / Confiance Totale)
    // ==========================================

    fn handle_private_event(&mut self, event: GameNetworkEvent) {
        match event {
            GameNetworkEvent::Connected(conn) => {
                let id = self.next_server_id;
                self.next_server_id += 1;
                self.uuid_to_node_id.insert(conn.connection_uuid, id);
                self.server_connections.insert(id, conn);

                let topic_builder =
                    TopicBuilder::new(SecurityDomain::PrivateReadPublicWrite, Namespace::NodeLine)
                        .append_id(id);
                self.node_subscribe(id, topic_builder.clone().build());
                self.node_subscribe(
                    id,
                    topic_builder
                        .change_security_domain(SecurityDomain::PrivateRW)
                        .build(),
                );
                println!(
                    "[RÉSEAU PRIVÉ] Nouveau Serveur Backend connecté : ID {:X}",
                    id
                );
            }
            GameNetworkEvent::Disconnected(conn) => {
                self.handle_disconnect(conn.connection_uuid);
            }
            GameNetworkEvent::StreamCreated(connection, stream) => {
                let Some(&node_id) = self.uuid_to_node_id.get(&connection.connection_uuid) else {
                    return;
                };
                if stream.is_reliable() && stream.real_stream_id() == RELIABLE_STREAM_ID {
                    let msg = BrokerMessage::Welcome(node_id);
                    self.private_peer
                        .send(&connection, &stream, msg.serialize())
                        .unwrap_or_else(|e| {
                            eprintln!(
                                "[RÉSEAU PRIVÉ] Erreur Welcome (Serveur {:X}): {}",
                                node_id, e
                            );
                        });
                }
            }

            GameNetworkEvent::Message {
                connection,
                stream,
                data,
            } => {
                let Some(&node_id) = self.uuid_to_node_id.get(&connection.connection_uuid) else {
                    return;
                };

                let message = match BrokerMessage::deserialize(data) {
                    Ok(msg) => msg,
                    Err(e) => {
                        eprintln!("[RÉSEAU PRIVÉ] Erreur de parsing : {}", e);
                        return;
                    }
                };

                match message {
                    BrokerMessage::Subscribe { client_id, topic } => {
                        let target_id = if client_id == 0 { node_id } else { client_id };
                        self.node_subscribe(target_id, topic.clone());
                    }
                    BrokerMessage::Unsubscribe { client_id, topic } => {
                        let target_id = if client_id == 0 { node_id } else { client_id };
                        self.node_unsubscribe(target_id, topic);
                    }
                    BrokerMessage::BatchSubscribe { client_id, pattern } => {
                        let target_id = if client_id == 0 { node_id } else { client_id };
                        pattern.unpack_into(|topic| {
                            self.node_subscribe(target_id, topic);
                        });
                    }
                    BrokerMessage::BatchUnsubscribe { client_id, pattern } => {
                        let target_id = if client_id == 0 { node_id } else { client_id };
                        pattern.unpack_into(|topic| {
                            self.node_unsubscribe(target_id, topic);
                        });
                    }
                    BrokerMessage::Publish { topic, payload } => {
                        // Les serveurs sont dignes de confiance. On forward ce qu'ils ont pré-packagé
                        // (Généralement un ProtocolTag::Broadcast ou BroadcastFromClient)
                        self.route_message(Broadcast { payload }, topic, stream);
                    }
                    BrokerMessage::AuthorizeClient(node_id) => {
                        if self.not_authenticated_clients.remove(&node_id) {
                            println!("[RÉSEAU PRIVÉ] Badge accordé à {}", node_id);
                        }
                    }
                    BrokerMessage::KickNode(node_id) => {
                        let map = if node_id.is_server() {
                            &mut self.server_connections
                        } else {
                            &mut self.client_connections
                        };
                        if let Some(conn) = map.get(&node_id) {
                            if node_id.is_server() {
                                let _ = self.private_peer.disconnect(conn);
                            } else {
                                let _ = self.public_peer.disconnect(conn);
                            }
                            println!("🔑 [Auth] Le Nœud {:X} a été éjecté.", node_id);
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    fn node_subscribe(&mut self, node_id: NodeId, topic: Topic) {
        let map = if node_id.is_server() {
            &mut self.server_subscribers
        } else {
            &mut self.client_subscribers
        };
        let vec = map
            .entry(topic.clone())
            .or_insert_with(|| FastIterableSet::with_capacity(16));
        vec.insert(node_id);
        let topics = self
            .node_id_to_topics
            .entry(node_id)
            .or_insert_with(|| FastIterableSet::with_capacity(16));
        topics.insert(topic);
    }

    fn node_unsubscribe(&mut self, node_id: NodeId, topic: Topic) {
        let map = if node_id.is_server() {
            &mut self.server_subscribers
        } else {
            &mut self.client_subscribers
        };
        if let Some(subscribers) = map.get_mut(&topic) {
            if subscribers.swap_remove(&node_id) && subscribers.is_empty() {
                map.remove(&topic);
            }
        }
        if let Some(topics) = self.node_id_to_topics.get_mut(&node_id) {
            topics.swap_remove(&topic);
            if topics.is_empty() {
                self.node_id_to_topics.remove(&node_id);
            }
        }
    }

    /// === LA FONCTION DE ROUTAGE ZERO-COPY ===
    /// Elle prend un buffer pré-formaté et l'injecte directement dans les Sockets.
    fn route_message(&self, message: BrokerMessage, topic: Topic, stream: GameStream) {
        let server_nodes = self.server_subscribers.get(&topic);
        let client_nodes = self.client_subscribers.get(&topic);

        if server_nodes.is_none() || client_nodes.is_none() {
            return;
        }

        let final_bytes = message.serialize();

        // 1. Envoi à tous les serveurs (Private Peer)
        if let Some(server_nodes) = server_nodes {
            for &node_id in server_nodes {
                if let Some(conn) = self.server_connections.get(&node_id) {
                    let _ = self.private_peer.send(conn, &stream, final_bytes.clone());
                }
            }
        }

        // 2. Envoi à tous les clients (Public Peer)
        if let Some(client_nodes) = client_nodes {
            for &node_id in client_nodes {
                if let Some(conn) = self.client_connections.get(&node_id) {
                    let _ = self.public_peer.send(conn, &stream, final_bytes.clone());
                }
            }
        }
    }
}
