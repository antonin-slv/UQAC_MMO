use indexmap::IndexSet;
use nohash_hasher::IntSet;
use rustc_hash::FxHashMap;
use std::env;
use uuid::Uuid;

use game_sockets::protocols::QuicBackend;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream};
use shared_replication::broker_message::BrokerMessage::{BroadCastFrom, Broadcast};
use shared_replication::broker_message::{BrokerMessage, NodeId, RELIABLE_STREAM_ID};
use shared_replication::broker_topics;
use shared_replication::broker_topics::{SecurityDomain, Topic, TopicInterface};

pub type FastMap<K, V> = FxHashMap<K, V>;
pub type FastSet<K> = IntSet<K>;
pub type FastIterableSet<K> = IndexSet<K>;
// --- MAGIE DU ROUTAGE UNIFIÉ ---
// Si le bit de poids fort est 1 (0x80000000), c'est un serveur. Sinon, un client.
#[inline]
pub fn is_server(node_id: NodeId) -> bool {
    (node_id & 0x80000000) != 0
}

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
            next_client_id: 1,          // Commence à 1 (0x00000001)
            next_server_id: 0x80000000, // Commence avec le Bit Fort à 1
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
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    // Fonction de nettoyage unifiée (Sert pour les Crashs Serveurs ET Joueurs)
    fn handle_disconnect(&mut self, conn_uuid: Uuid) {
        if let Some(node_id) = self.uuid_to_node_id.remove(&conn_uuid) {
            self.node_id_to_connection.remove(&node_id);

            // Nettoyage rapide des abonnements
            if let Some(topics) = self.node_id_to_topics.remove(&node_id) {
                for topic in topics {
                    self.node_unsubscribe(node_id, topic);
                }
            }

            if is_server(node_id) {
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
                self.node_id_to_topics.insert(id, FastIterableSet::default());
                self.not_authenticated_clients.insert(id);
                println!("[RÉSEAU PUBLIC] Nouveau client connecté : ID {}", id);
            }

            GameNetworkEvent::StreamCreated(connection, stream) => {
                let Some(&node_id) = self.uuid_to_node_id.get(&connection.connection_uuid) else {
                    return;
                };

                if stream.is_reliable() && stream.real_stream_id() == RELIABLE_STREAM_ID {
                    let msg = BrokerMessage::Welcome { node_id };

                    self.public_peer.send(
                        &connection,
                        &stream,
                        msg.serialize(),
                    ).unwrap_or_else(|e| {
                        eprintln!("[RÉSEAU PUBLIC] Erreur en envoyant le message de bienvenue à {}: {}", node_id, e);
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
                    BrokerMessage::Subscribe {
                        client_id: _,
                        topic,
                    } => {
                        if topic.security_domain() == Some(SecurityDomain::PublicReadPrivateWrite) {
                            self.node_subscribe(node_id, topic);
                        } else {
                            eprintln!("[SÉCURITÉ] Accès LECTURE refusé à {}", node_id);
                        }
                    }
                    BrokerMessage::Unsubscribe {
                        client_id: _,
                        topic,
                    } => {
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
                        eprintln!("[SÉCURITÉ] Tag invalide venant de {}", node_id);
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
                self.node_id_to_topics.insert(id, FastIterableSet::default());
                println!(
                    "[RÉSEAU PRIVÉ] Nouveau Serveur Backend connecté : ID {:X}",
                    id
                );

                // IMPORTANT : Il faudra coder ici un message BrokerMessage::WelcomeNode(id)
                // pour dire au serveur quel est son identifiant ClientId assigné.
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

                    self.private_peer.send(
                        &connection,
                        &stream,
                        msg.serialize(),
                    ).unwrap_or_else(|e| {
                        eprintln!("[RÉSEAU PUBLIC] Erreur en envoyant le message de bienvenue à {}: {}", node_id, e);
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
                        // Astuce : Si le serveur envoie 0, il veut s'abonner LUI-MÊME.
                        let target_id = if client_id == 0 { node_id } else { client_id };
                        self.node_subscribe(target_id, topic.clone());
                        println!(
                            "Abonnement : Nœud {:X} sur {:?}",
                            target_id,
                            topic.namespace()
                        );
                    }
                    BrokerMessage::Unsubscribe { client_id, topic } => {
                        let target_id = if client_id == 0 { node_id } else { client_id };
                        self.node_unsubscribe(target_id, topic);
                    }
                    BrokerMessage::Publish { topic, payload } => {
                        self.route_message(Broadcast { payload }, topic, stream, false);
                    }
                    BrokerMessage::AuthorizeClient { client_id } => {
                        if self.not_authenticated_clients.remove(&client_id) {
                            println!("[RÉSEAU PRIVÉ] Badge accordé à {}", client_id);
                        }
                    }
                    BrokerMessage::KickNode { client_id } => {
                        if let Some(conn) = self.node_id_to_connection.get(&client_id) {
                            // On éjecte correctement que ce soit un client (Public) ou un serveur piraté (Privé)
                            if is_server(client_id) {
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

    // ==========================================
    // LOGIQUE INTERNE (Routage et Mémoire)
    // ==========================================

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
            if subscribers.swap_remove(&node_id) {
                if subscribers.is_empty() {
                    self.topic_subscribers.remove(&topic);
                }
            }
        }
        if let Some(topics) = self.node_id_to_topics.get_mut(&node_id) {
            topics.swap_remove(&topic);
        }
    }

    /// La nouvelle fonction de routage magique. Elle lit le bit de poids fort
    /// pour savoir instantanément vers quel peer (Public ou Privé) envoyer la trame.
    fn route_message(
        &self,
        message: BrokerMessage,
        topic: Topic,
        stream: GameStream,
        debug_print: bool,
    ) {
        let final_bytes = message.serialize();

        if let Some(nodes) = self.topic_subscribers.get(&topic) {
            for &node_id in nodes {
                if let Some(conn) = self.node_id_to_connection.get(&node_id) {
                    if debug_print {
                        println!("\t to {:X}", node_id);
                    }

                    // L'Aiguillage à la vitesse de la lumière !
                    if is_server(node_id) {
                        let _ = self.private_peer.send(conn, &stream, final_bytes.clone());
                    } else {
                        let _ = self.public_peer.send(conn, &stream, final_bytes.clone());
                    }
                }
            }
        }
    }
}
