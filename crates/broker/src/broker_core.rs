use indexmap::IndexSet;
use nohash_hasher::IntSet;
use rustc_hash::FxHashMap;
use std::env;
use uuid::Uuid;

use broker_protocol::broker_message::BrokerMessage::{BroadCastFrom, Broadcast};
use broker_protocol::broker_message::{BrokerMessage, NodeId, NodeIdMetaData, RELIABLE_STREAM_ID};
use broker_protocol::broker_topics;
use broker_protocol::broker_topics::{
    Namespace, SecurityDomain, Topic, TopicBuilder, TopicInterface,
};
use game_sockets::protocols::QuicBackend;
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

    // Routage Unifié (Clients & Serveurs mélangés)
    uuid_to_node_id: FastMap<Uuid, NodeId>,
    node_id_to_connection: FastMap<NodeId, GameConnection>,

    // Pare-feu (Clients uniquement)
    not_authenticated_clients: FastSet<NodeId>,

    // Abonnements Unifiés
    topic_subscribers: FastMap<Topic, FastIterableSet<NodeId>>,
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
            node_id_to_connection: FastMap::default(),
            not_authenticated_clients: FastSet::default(),
            topic_subscribers: FastMap::default(),
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
            self.node_id_to_connection.remove(&node_id);

            if let Some(topics) = self.node_id_to_topics.remove(&node_id) {
                for topic in topics {
                    self.node_unsubscribe(node_id, topic);
                }
            }

            if node_id.is_server() {
                println!("[RÉSEAU] Serveur déconnecté : ID {:X}", node_id);
            } else {
                self.not_authenticated_clients.remove(&node_id);
                println!("[RÉSEAU] Client déconnecté : ID {}", node_id);
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
                self.node_id_to_connection.insert(id, conn);
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
                    let msg = BrokerMessage::Welcome { node_id };
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
                                == Some(broker_topics::AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION)
                        {
                            self.route_message(
                                BroadCastFrom {
                                    client_id: node_id,
                                    payload,
                                },
                                topic,
                                stream,
                                false,
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
                self.node_id_to_connection.insert(id, conn);

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
                    let msg = BrokerMessage::Welcome { node_id };
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
                    BrokerMessage::Publish { topic, payload } => {
                        // Les serveurs sont dignes de confiance. On forward ce qu'ils ont pré-packagé
                        // (Généralement un ProtocolTag::Broadcast ou BroadcastFromClient)
                        self.route_message(Broadcast { payload }, topic, stream, false);
                    }
                    BrokerMessage::AuthorizeClient { client_id } => {
                        if self.not_authenticated_clients.remove(&client_id) {
                            println!("[RÉSEAU PRIVÉ] Badge accordé à {}", client_id);
                        }
                    }
                    BrokerMessage::KickNode { client_id } => {
                        if let Some(conn) = self.node_id_to_connection.get(&client_id) {
                            if client_id.is_server() {
                                let _ = self.private_peer.disconnect(conn);
                            } else {
                                let _ = self.public_peer.disconnect(conn);
                            }
                            println!("🔑 [Auth] Le Nœud {:X} a été éjecté.", client_id);
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    fn node_subscribe(&mut self, node_id: NodeId, topic: Topic) {
        let vec = self
            .topic_subscribers
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
        if let Some(subscribers) = self.topic_subscribers.get_mut(&topic) {
            if subscribers.swap_remove(&node_id) && subscribers.is_empty() {
                self.topic_subscribers.remove(&topic);
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
    fn route_message(
        &self,
        message: BrokerMessage,
        topic: Topic,
        stream: GameStream,
        debug_print: bool,
    ) {
        if let Some(nodes) = self.topic_subscribers.get(&topic) {
            let final_bytes = message.serialize();

            for &node_id in nodes {
                if let Some(conn) = self.node_id_to_connection.get(&node_id) {
                    if debug_print {
                        println!("\t to {:X}", node_id);
                    }

                    if node_id.is_server() {
                        let _ = self.private_peer.send(conn, &stream, final_bytes.clone());
                    } else {
                        let _ = self.public_peer.send(conn, &stream, final_bytes.clone());
                    }
                }
            }
        }
    }
}
