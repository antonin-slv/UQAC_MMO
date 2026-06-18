use crate::broker_state::{IngressEvent, NetworkInterface, SharedBrokerState};
use broker_protocol::broker_message::{BrokerMessage, NodeIdMetaData, RELIABLE_STREAM_ID};
use broker_protocol::topics;
use broker_protocol::topics::{Namespace, SecurityDomain, Topic, TopicBuilder, TopicInterface};
use crossbeam_channel::{unbounded, Receiver};
use game_sockets::{GameNetworkEvent, GamePeer, GamePeerSender, GameStream, GameStreamReliability};
use std::sync::Arc;
use uuid::Uuid;
use game_sockets::protocols::QuicBackend;


pub struct Broker2 {
    public_peer: GamePeer,
    private_peer: GamePeer,
}
impl Broker2 {
    pub fn new(public_port : u16, private_port : u16) -> Self {
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

        Broker2 {
            public_peer,
            private_peer,
        }
    }

    pub fn run(&mut self, with_log: bool) {
        // Création des canaux de communication (Lock-free)
        let (ingress_tx, ingress_rx) = unbounded::<IngressEvent>();

        let mut broker_state = SharedBrokerState::new();
        broker_state.log_enabled = with_log;

        let shared_state = Arc::new(broker_state);

        let public_tx = self.public_peer.get_sender().unwrap();
        let private_tx = self.private_peer.get_sender().unwrap();
        // On lance un pool de workers
        for _ in 0..8 {
            let rx = ingress_rx.clone();
            let state = shared_state.clone();
            let pub_tx = public_tx.clone();
            let priv_tx = private_tx.clone();

            std::thread::spawn(move || {
                Self::worker_loop(rx, state, pub_tx, priv_tx);
            });
        }

        // --- LA BOUCLE PRINCIPALE (I/O Only) ---
        loop {
            let mut idle = true;

            // 1. Lecture Réseau Public → Ingress
            while let Ok(Some(event)) = self.public_peer.poll() {
                idle = false;
                let _ = ingress_tx.send(IngressEvent {
                    interface: NetworkInterface::Public,
                    event,
                });
            }
            // 2. Lecture Réseau Privé → Ingress
            while let Ok(Some(event)) = self.private_peer.poll() {
                idle = false;
                let _ = ingress_tx.send(IngressEvent {
                    interface: NetworkInterface::Private,
                    event,
                });
            }

            if idle {
                std::thread::yield_now();
            }
        }
    }
    fn worker_loop(
        ingress_rx: Receiver<IngressEvent>,
        state: Arc<SharedBrokerState>,
        public_tx: GamePeerSender,
        private_tx: GamePeerSender,
    ) {
        // Cette boucle tourne indépendamment pour chaque thread worker
        while let Ok(ingress) = ingress_rx.recv() {
            let event = ingress.event;
            match ingress.interface {
                NetworkInterface::Private => {
                    Broker2::handle_private_event(event, &state, &public_tx, &private_tx)
                }
                NetworkInterface::Public => {
                    Broker2::handle_public_event(event, &state, &public_tx, &private_tx)
                }
            }
        }
    }

    // ==========================================
    // DÉCONNEXION CENTRALISÉE
    // ==========================================
    fn handle_disconnect(
        conn_uuid: Uuid,
        state: &SharedBrokerState,
        public_tx: &GamePeerSender,
        private_tx: &GamePeerSender,
    ) {
        // "remove" sur DashMap retourne la valeur supprimée sous forme de tuple (K, V)
        if let Some((_, node_id)) = state.uuid_to_node_id.remove(&conn_uuid) {
            if let Some((_, topics)) = state.node_id_to_topics.remove(&node_id) {
                for topic in topics {
                    state.node_unsubscribe(node_id, topic);
                }
            }

            let namespace = if node_id.is_server() {
                println!("[RÉSEAU] Serveur déconnecté : ID {:X}", node_id);
                state.server_connections.remove(&node_id);
                Namespace::ServerConnection
            } else {
                state.not_authenticated_clients.remove(&node_id);
                state.client_connections.remove(&node_id);
                println!("[RÉSEAU] Client déconnecté : ID {}", node_id);
                Namespace::ClientAuth
            };

            let system_topic = TopicBuilder::new(SecurityDomain::PrivateRW, namespace).build();

            let mock_stream = GameStream::new(RELIABLE_STREAM_ID, GameStreamReliability::Reliable);
            let disconnect_msg = BrokerMessage::NodeDisconnected(node_id);

            Self::route_message(
                disconnect_msg,
                system_topic,
                mock_stream,
                state,
                public_tx,
                private_tx,
            );
        }
    }

    // ==========================================
    // RÉSEAU PUBLIC (Clients)
    // ==========================================
    fn handle_public_event(
        event: GameNetworkEvent,
        state: &SharedBrokerState,
        public_tx: &GamePeerSender,
        private_tx: &GamePeerSender,
    ) {
        match event {
            GameNetworkEvent::Connected(conn) => {
                let id = state.generate_client_id();
                state.uuid_to_node_id.insert(conn.connection_uuid, id);
                state.client_connections.insert(id, conn);
                state.not_authenticated_clients.insert(id);

                let direct_line_topic =
                    TopicBuilder::new(SecurityDomain::PublicReadPrivateWrite, Namespace::NodeLine)
                        .append_id(id)
                        .build();
                state.node_subscribe(id, direct_line_topic);
                if state.log_enabled {
                    println!("[RÉSEAU PUBLIC] Nouveau client connecté : ID {}", id);
                }
            }

            GameNetworkEvent::StreamCreated(connection, stream) => {
                let Some(node_id_ref) = state.uuid_to_node_id.get(&connection.connection_uuid)
                else {
                    return;
                };
                let node_id = *node_id_ref;

                if stream.is_reliable() && stream.real_stream_id() == RELIABLE_STREAM_ID {
                    let msg = BrokerMessage::Welcome(node_id);
                    let _ = public_tx.send(&connection, &stream, msg.serialize());
                }
            }

            GameNetworkEvent::Disconnected(conn) => {
                Self::handle_disconnect(conn.connection_uuid, state, public_tx, private_tx);
            }

            GameNetworkEvent::Message {
                connection,
                stream,
                data,
            } => {
                let Some(node_id_ref) = state.uuid_to_node_id.get(&connection.connection_uuid)
                else {
                    return;
                };
                let node_id = *node_id_ref;

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
                            state.node_subscribe(node_id, topic);
                        } else {
                            eprintln!("[SÉCURITÉ] Accès LECTURE refusé à {}", node_id);
                        }
                    }
                    BrokerMessage::Unsubscribe { topic, .. } => {
                        state.node_unsubscribe(node_id, topic);
                    }
                    BrokerMessage::Publish { topic, payload } => {
                        if topic.security_domain() != Some(SecurityDomain::PrivateReadPublicWrite) {
                            eprintln!("[SÉCURITÉ] Accès ÉCRITURE refusé à {}", node_id);
                            return;
                        }
                        if !state.not_authenticated_clients.contains(&node_id)
                            || topic.namespace()
                                == Some(topics::AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION)
                        {
                            Self::route_message(
                                BrokerMessage::BroadCastFrom {
                                    client_id: node_id,
                                    payload,
                                },
                                topic,
                                stream,
                                state,
                                public_tx,
                                private_tx,
                            );
                        } else {
                            eprintln!(
                                "[SÉCURITÉ] Client {} non authentifié tente de publier !",
                                node_id
                            );
                        }
                    }
                    BrokerMessage::BatchSubscribe { .. }
                    | BrokerMessage::BatchUnsubscribe { .. } => {
                        println!("[RÉSEAU PUBLIC] Client {} action batch. REFUSE", node_id);
                    }
                    _ => eprintln!("[SÉCURITÉ] Tag invalide venant de {}", node_id),
                }
            }
            _ => {}
        }
    }

    // ==========================================
    // RÉSEAU PRIVÉ (Servers)
    // ==========================================
    fn handle_private_event(
        event: GameNetworkEvent,
        state: &SharedBrokerState,
        public_tx: &GamePeerSender,
        private_tx: &GamePeerSender,
    ) {
        match event {
            GameNetworkEvent::Connected(conn) => {
                let id = state.generate_server_id();
                state.uuid_to_node_id.insert(conn.connection_uuid, id);
                state.server_connections.insert(id, conn);

                let topic_builder =
                    TopicBuilder::new(SecurityDomain::PrivateReadPublicWrite, Namespace::NodeLine)
                        .append_id(id);
                state.node_subscribe(id, topic_builder.clone().build());
                state.node_subscribe(
                    id,
                    topic_builder
                        .change_security_domain(SecurityDomain::PrivateRW)
                        .build(),
                );
                if state.log_enabled {
                    println!(
                        "[RÉSEAU PRIVÉ] Nouveau Serveur Backend connecté : ID {:X}",
                        id
                    );
                }
            }

            GameNetworkEvent::Disconnected(conn) => {
                Self::handle_disconnect(conn.connection_uuid, state, public_tx, private_tx);
            }

            GameNetworkEvent::StreamCreated(connection, stream) => {
                let Some(node_id_ref) = state.uuid_to_node_id.get(&connection.connection_uuid)
                else {
                    return;
                };
                let node_id = *node_id_ref;

                if stream.is_reliable() && stream.real_stream_id() == RELIABLE_STREAM_ID {
                    let msg = BrokerMessage::Welcome(node_id);
                    let _ = private_tx.send(&connection, &stream, msg.serialize());
                }
            }

            GameNetworkEvent::Message {
                connection,
                stream,
                data,
            } => {
                let Some(node_id_ref) = state.uuid_to_node_id.get(&connection.connection_uuid)
                else {
                    return;
                };
                let node_id = *node_id_ref;

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
                        state.node_subscribe(target_id, topic);
                    }
                    BrokerMessage::Unsubscribe { client_id, topic } => {
                        let target_id = if client_id == 0 { node_id } else { client_id };
                        state.node_unsubscribe(target_id, topic);
                    }
                    BrokerMessage::BatchSubscribe { client_id, pattern } => {
                        let target_id = if client_id == 0 { node_id } else { client_id };
                        pattern.unpack_into(|topic| state.node_subscribe(target_id, topic));
                    }
                    BrokerMessage::BatchUnsubscribe { client_id, pattern } => {
                        let target_id = if client_id == 0 { node_id } else { client_id };
                        pattern.unpack_into(|topic| state.node_unsubscribe(target_id, topic));
                    }
                    BrokerMessage::Publish { topic, payload } => {
                        Self::route_message(
                            BrokerMessage::Broadcast { payload },
                            topic,
                            stream,
                            state,
                            public_tx,
                            private_tx,
                        );
                    }
                    BrokerMessage::AuthorizeClient(client_id) => {
                        if state.not_authenticated_clients.remove(&client_id).is_some() {
                            if state.log_enabled {
                                println!("[RÉSEAU PRIVÉ] Badge accordé à {}", client_id);
                            }
                        }
                    }
                    BrokerMessage::KickNode(target_id) => {
                        let is_server = target_id.is_server();
                        let map = if is_server {
                            &state.server_connections
                        } else {
                            &state.client_connections
                        };

                        if let Some(conn) = map.get(&target_id) {
                            if is_server {
                                let _ = private_tx.disconnect(&conn.value());
                            } else {
                                let _ = public_tx.disconnect(&conn.value());
                            }
                            println!("🔑 [Auth] Le Nœud {:X} a été éjecté.", target_id);
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    fn route_message(
        message: BrokerMessage,
        topic: Topic,
        stream: GameStream,
        state: &SharedBrokerState,
        public_tx: &GamePeerSender,
        private_tx: &GamePeerSender,
    ) {
        let final_bytes = message.serialize();

        if let Some(server_nodes) = state.server_subscribers.get(&topic) {
            for &node_id in server_nodes.value().iter() {
                if let Some(conn) = state.server_connections.get(&node_id) {
                    let _ = private_tx.send(conn.value(), &stream, final_bytes.clone());
                }
            }
        }

        if let Some(client_nodes) = state.client_subscribers.get(&topic) {
            for &node_id in client_nodes.value().iter() {
                if let Some(conn) = state.client_connections.get(&node_id) {
                    let _ = public_tx.send(conn.value(), &stream, final_bytes.clone());
                }
            }
        }
    }
}
