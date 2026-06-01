use nohash_hasher::IntSet;
use rustc_hash::FxHashMap;
use std::env;
use uuid::Uuid;

use game_sockets::protocols::QuicBackend;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream};
use shared_replication::broker_message::BrokerMessage::{BroadCastFromClient, Broadcast};
use shared_replication::broker_message::{BrokerMessage, ClientId};
use shared_replication::broker_topics;
use shared_replication::broker_topics::{SecurityDomain, Topic, TopicInterface};

pub type FastMap<K, V> = FxHashMap<K, V>;
pub type FastSet<K> = IntSet<K>;
#[inline]
fn vec_insert_unique<T: PartialEq>(vec: &mut Vec<T>, item: T) {
    if !vec.contains(&item) {
        vec.push(item);
    }
}

#[inline]
fn vec_remove_item<T: PartialEq>(vec: &mut Vec<T>, item: &T) {
    if let Some(pos) = vec.iter().position(|x| x == item) {
        vec.swap_remove(pos);
    }
}

const CLIENT_LISTEN_PORT_ENV_NAME: &str = "BROKER_PUBLIC_PORT";
const SERVER_LISTEN_PORT_ENV_NAME: &str = "BROKER_PRIVATE_PORT";

pub struct Broker {
    public_peer: GamePeer,
    private_peer: GamePeer,

    // Gestion des connexions clients (Public)
    next_client_id: ClientId,
    uuid_to_client_id: FastMap<Uuid, ClientId>,
    client_id_to_connection: FastMap<ClientId, GameConnection>,

    authenticated_clients: FastSet<ClientId>,

    // L'état du réseau PubSub
    topic_subscribers: FastMap<Topic, Vec<ClientId>>,
    client_id_to_topics: FastMap<ClientId, Vec<Topic>>, // Pour un nettoyage O(1) à la déconnexion

    // Abonnements des serveurs internes (Shards, Auth, Market)
    internal_subscribers: FastMap<Topic, Vec<Uuid>>,
    uuid_to_internal_conn: FastMap<Uuid, GameConnection>,
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
            next_client_id: 1,
            uuid_to_client_id: FastMap::default(),
            client_id_to_connection: FastMap::default(),
            topic_subscribers: FastMap::default(),
            client_id_to_topics: FastMap::default(),
            internal_subscribers: FastMap::default(),
            uuid_to_internal_conn: FastMap::default(),
            authenticated_clients: FastSet::default(),
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

    // ==========================================
    // RÉSEAU PUBLIC (Clients / Zéro Confiance)
    // ==========================================

    fn handle_public_event(&mut self, event: GameNetworkEvent) {
        match event {
            GameNetworkEvent::Connected(conn) => {
                let id = self.next_client_id;
                self.next_client_id += 1;
                self.uuid_to_client_id.insert(conn.connection_uuid, id);
                self.client_id_to_connection.insert(id, conn);
                self.client_id_to_topics.insert(id, Vec::new());
                println!("[RÉSEAU PUBLIC] Nouveau client connecté : ID {}", id);
            }
            GameNetworkEvent::Disconnected(conn) => {
                if let Some(client_id) = self.uuid_to_client_id.remove(&conn.connection_uuid) {
                    self.client_id_to_connection.remove(&client_id);
                    self.authenticated_clients.remove(&client_id);
                    // Nettoyage des abonnements
                    if let Some(topics) = self.client_id_to_topics.remove(&client_id) {
                        for topic in topics {
                            if let Some(subs) = self.topic_subscribers.get_mut(&topic) {
                                vec_remove_item(subs, &client_id);
                            }
                        }
                    }
                }
            }
            GameNetworkEvent::Message {
                connection,
                stream,
                data,
            } => {
                let Some(&client_id) = self.uuid_to_client_id.get(&connection.connection_uuid)
                else {
                    return;
                };

                // 1. Désérialisation
                let message = match BrokerMessage::deserialize(data) {
                    Ok(msg) => msg,
                    Err(e) => {
                        eprintln!(
                            "[RÉSEAU PUBLIC] Erreur de parsing (Client {}): {}",
                            client_id, e
                        );
                        return;
                    }
                };

                // 2. Le Pare-Feu Logique
                match message {
                    BrokerMessage::Subscribe {
                        client_id: _msg_client_id,
                        topic,
                    } => {
                        let domain = topic.security_domain();
                        let domain = match domain {
                            Some(d) => d,
                            None => {
                                eprintln!(
                                    "[SÉCURITÉ] Client {} a tenté de s'abonner à un topic avec un domaine de sécurité invalide : {:?}",
                                    client_id, topic
                                );
                                return;
                            }
                        };
                        if domain == SecurityDomain::PublicReadPrivateWrite {
                            self.client_subscribe(client_id, topic);
                        } else {
                            eprintln!(
                                "[SÉCURITÉ] Client {} accès refusé en LECTURE sur le topic {:?}",
                                client_id, topic
                            );
                        }
                    }
                    BrokerMessage::Unsubscribe {
                        client_id: _msg_client_id,
                        topic,
                    } => {
                        self.client_unsubscribe(client_id, topic);
                    }
                    BrokerMessage::Publish { topic, payload } => {
                        let domain = topic.security_domain();
                        let domain = match domain {
                            Some(d) => d,
                            None => {
                                eprintln!(
                                    "[SÉCURITÉ] Client {} a tenté de s'abonner à un topic avec un domaine de sécurité invalide : {:?}",
                                    client_id, topic
                                );
                                return;
                            }
                        };
                        if domain != SecurityDomain::PrivateReadPublicWrite {
                            eprintln!(
                                "[SÉCURITÉ] Client {} accès refusé en ÉCRITURE sur le topic {:?}",
                                client_id, topic
                            );
                            return;
                        }
                        if self.authenticated_clients.contains(&client_id)
                            || topic.namespace() == Some(broker_topics::AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION)
                        {
                            // Le client a son badge, il peut publier ses inputs
                            self.route_message(
                                BroadCastFromClient { client_id, payload },
                                topic,
                                stream,
                                false,
                            );
                        } else {
                            eprintln!(
                                "[SÉCURITÉ] Client {} a tenté de publier sur {:?} sans être authentifié !",
                                client_id, topic
                            );
                        }
                    }
                    _ => {
                        eprintln!(
                            "[RÉSEAU PUBLIC] Client {} a envoyé un message avec un tag non autorisé pour les clients : {:?}",
                            client_id,
                            message.tag()
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
                self.uuid_to_internal_conn
                    .insert(conn.connection_uuid, conn);
                println!("[RÉSEAU PRIVÉ] Nouveau serveur Backend connecté.");
            }
            GameNetworkEvent::Disconnected(conn) => {
                self.uuid_to_internal_conn.remove(&conn.connection_uuid);
                println!("[RÉSEAU PRIVÉ] Un serveur Backend s'est déconnecté.");
                // Le nettoyage complet des topics internes pourrait être ajouté ici
            }
            GameNetworkEvent::Message {
                connection,
                stream,
                data,
            } => {
                let message = match BrokerMessage::deserialize(data) {
                    Ok(msg) => msg,
                    Err(e) => {
                        eprintln!("[RÉSEAU PRIVÉ] Erreur de parsing : {}", e);
                        return;
                    }
                };

                match message {
                    BrokerMessage::Subscribe { client_id, topic } => {
                        if client_id == 0 {
                            // Le serveur s'abonne lui-même pour écouter ce topic
                            let subs = self.internal_subscribers.entry(topic).or_default();
                            vec_insert_unique(subs, connection.connection_uuid);
                            println!(
                                "Internal subscriber {:?} subscribed to {:?}",
                                connection.connection_uuid, topic
                            );
                        } else {
                            // Force l'abonnement d'un joueur.
                            self.client_subscribe(client_id, topic);
                            println!(
                                "Subscribed {} to {:?} (via {})",
                                client_id, topic, connection.connection_uuid
                            );
                        }
                    }
                    BrokerMessage::Unsubscribe { client_id, topic } => {
                        if client_id == 0 {
                            if let Some(subs) = self.internal_subscribers.get_mut(&topic) {
                                vec_remove_item(subs, &connection.connection_uuid);
                                if subs.is_empty() {
                                    self.internal_subscribers.remove(&topic);
                                }
                            }
                        } else {
                            self.client_unsubscribe(client_id, topic);
                        }
                    }
                    BrokerMessage::Publish { topic, payload } => {
                        // Le paramètre client_id est mis à 0 pour signifier "Envoyé par le système"

                        self.route_message(Broadcast { payload }, topic, stream, false);
                    }

                    BrokerMessage::AuthorizeClient { client_id } => {
                        self.authenticated_clients.insert(client_id);
                        println!(
                            "[RÉSEAU PRIVÉ] Le Broker a accordé le badge de publication au client {}.",
                            client_id
                        );
                    }

                    BrokerMessage::KickClient { client_id } => {
                        if let Some(conn) = self.client_id_to_connection.get(&client_id) {
                            let _ = self.public_peer.disconnect(conn);
                            println!("🔑 [Auth] Le client {} est banni. Éjection !", client_id);
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

    fn client_subscribe(&mut self, client_id: ClientId, topic: Topic) {
        let vec = self
            .topic_subscribers
            .entry(topic)
            .or_insert_with(|| Vec::with_capacity(16));
        vec_insert_unique(vec, client_id);

        let topics = self
            .client_id_to_topics
            .entry(client_id)
            .or_insert_with(|| Vec::with_capacity(16));
        vec_insert_unique(topics, topic);
    }

    fn client_unsubscribe(&mut self, client_id: ClientId, topic: Topic) {
        if let Some(subscribers) = self.topic_subscribers.get_mut(&topic) {
            vec_remove_item(subscribers, &client_id);
            if subscribers.is_empty() {
                self.topic_subscribers.remove(&topic);
            }
        }
        if let Some(topics) = self.client_id_to_topics.get_mut(&client_id) {
            vec_remove_item(topics, &topic);
        }
    }

    /// Extrait la logique de diffusion.
    /// Identifie l'émetteur via sender_id (0 si c'est un serveur backend) pour potentiellement
    /// empêcher le joueur de recevoir en écho ses propres paquets de mouvement.
    fn route_message(
        &self,
        message: BrokerMessage,
        topic: Topic,
        stream: GameStream,
        debug_print: bool,
    ) {
        let final_bytes = message.serialize();

        // 1. Envoyer aux serveurs internes (Shards, IA, Director)
        if let Some(servers) = self.internal_subscribers.get(&topic) {
            for &uuid in servers {
                if let Some(conn) = self.uuid_to_internal_conn.get(&uuid) {
                    if debug_print {
                        println!("\t to {}", uuid);
                    }
                    let _ = self.private_peer.send(conn, &stream, final_bytes.clone());
                }
            }
        }
        // 2. Envoyer aux clients abonnés
        if let Some(clients) = self.topic_subscribers.get(&topic) {
            for &client_id in clients {
                if let Some(conn) = self.client_id_to_connection.get(&client_id) {
                    if debug_print {
                        println!("\t to {}", client_id);
                    }
                    let _ = self.public_peer.send(conn, &stream, final_bytes.clone());
                }
            }
        }
    }
}
