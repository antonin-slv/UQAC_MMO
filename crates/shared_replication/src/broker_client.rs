use crate::broker_message::{BrokerMessage, NodeId, RELIABLE_STREAM_ID};
use crate::broker_topics::Topic;
use crate::msg_game_payload::{GameMessage, GameMessageHeaders, GamePayload};
use bytes::{Buf, BytesMut};
use game_sockets::GameSocketError;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};

/// Les événements simplifiés remontés au moteur de jeu (Bevy)
pub enum ClientNetworkEvent {
    Connected,
    Ready,
    Disconnected,
    /// Les données brutes reçues
    DataReceived {
        client_id: NodeId,
        stream: GameStream,
        payload: GamePayload,
    },
}

pub struct MmoNetworkClient {
    peer: GamePeer,
    connection: Option<GameConnection>,

    // NOUVEAU : Le client stocke son ID officiel distribué par le Broker
    pub node_id: Option<NodeId>,

    // On garde en cache les streams standards pour éviter de les recréer
    stream_unreliable: GameStream,
    stream_reliable: GameStream,
    is_ready: bool,
}

impl MmoNetworkClient {
    /// Initialise le client réseau (valable pour un Shard ou un Joueur)
    pub fn new() -> Self {
        Self {
            peer: GamePeer::new(QuicBackend::new()),
            connection: None,
            node_id: None, // Initialisé à None

            // Définition de nos conventions de streams
            stream_unreliable: GameStream::new(0, GameStreamReliability::Unreliable),
            stream_reliable: GameStream::new(RELIABLE_STREAM_ID, GameStreamReliability::Reliable),
            is_ready: false,
        }
    }

    /// Lance la connexion de manière asynchrone
    pub fn connect(&self, ip: &str, port: u16) -> Result<(), String> {
        match self.peer.connect(ip, port) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("{}", e)),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connection.is_some()
    }

    /// Appelé à chaque frame par le moteur pour dépiler les messages réseau
    pub fn poll(&mut self) -> Option<ClientNetworkEvent> {
        // La boucle permet de sauter les événements inutiles sans renvoyer None
        loop {
            match self.peer.poll() {
                Ok(Some(GameNetworkEvent::Connected(conn))) => {
                    self.connection = Some(conn);

                    // CORRECTION : On utilise bien RELIABLE_STREAM_ID et pas le chiffre "1" en dur,
                    // sinon le Broker n'enverra jamais le message Welcome !
                    let _ = self.peer.create_stream(
                        conn,
                        GameStreamReliability::Reliable,
                        RELIABLE_STREAM_ID,
                    );

                    return Some(ClientNetworkEvent::Connected);
                }

                Ok(Some(GameNetworkEvent::Disconnected(_))) => {
                    self.connection = None;
                    self.is_ready = false;
                    self.node_id = None; // On efface l'ID
                    return Some(ClientNetworkEvent::Disconnected);
                }

                Ok(Some(GameNetworkEvent::Message { stream, data, .. })) => {
                    match BrokerMessage::deserialize(data) {
                        // NOUVEAU : Interception du message Welcome
                        Ok(BrokerMessage::Welcome { node_id }) => {
                            if !self.is_ready {
                                self.node_id = Some(node_id);
                                self.is_ready = true;
                                println!(
                                    "[BrokerClient] Message Welcome reçu. ID officiel : {:X}. Client Ready !",
                                    node_id
                                );
                                return Some(ClientNetworkEvent::Ready);
                            }
                            continue;
                        }

                        Ok(BrokerMessage::Broadcast { mut payload }) => {
                            if payload.is_empty() {
                                continue;
                            }

                            let header = GameMessageHeaders::from(payload.get_u8());

                            return Some(ClientNetworkEvent::DataReceived {
                                client_id: 0, // Vient du système
                                stream,
                                payload: GamePayload {
                                    header,
                                    data: payload,
                                },
                            });
                        }

                        Ok(BrokerMessage::BroadCastFrom {
                            client_id,
                            mut payload,
                        }) => {
                            if payload.is_empty() {
                                continue;
                            }

                            let header = GameMessageHeaders::from(payload.get_u8());

                            return Some(ClientNetworkEvent::DataReceived {
                                client_id,
                                stream,
                                payload: GamePayload {
                                    header,
                                    data: payload,
                                },
                            });
                        }

                        Err(e) => {
                            eprintln!("[BrokerClient] Erreur de parsing du message: {}", e);
                            continue;
                        }
                        _ => continue,
                    }
                }

                Ok(Some(GameNetworkEvent::StreamCreated(_conn, _stream))) => {
                    continue;
                }

                Ok(Some(GameNetworkEvent::StreamClosed(_conn, _stream))) => {
                    continue;
                }

                Ok(Some(GameNetworkEvent::Error { inner: error, .. })) => {
                    match error {
                        GameSocketError::ConnectionError => {
                            self.connection = None;
                            self.is_ready = false;
                            self.node_id = None;
                            return Some(ClientNetworkEvent::Disconnected);
                        }
                        _ => {
                            eprintln!("[BrokerClient] GameSocket error 1 : {:}", error);
                        }
                    }
                    continue;
                }
                Ok(None) => return None, // Le réseau est vraiment vide, on rend la main

                Err(e) => match e {
                    GameSocketError::ConnectionError { .. } => {
                        self.connection = None;
                        self.is_ready = false;
                        self.node_id = None;
                        return Some(ClientNetworkEvent::Disconnected);
                    }
                    _ => {
                        eprintln!("[BrokerClient] GameSocket error 2 : {:}", e);
                        continue;
                    }
                },
            }
        }
    }

    // --- COMMANDES D'ADMINISTRATION ---

    pub fn authorize_client(&self, target_client: NodeId) {
        let msg = BrokerMessage::AuthorizeClient {
            client_id: target_client,
        };
        self.inefficient_send_but_nice_looking(&self.stream_reliable, msg);
    }

    pub fn kick_client(&self, target_client: NodeId) {
        let msg = BrokerMessage::KickNode {
            client_id: target_client,
        };
        self.inefficient_send_but_nice_looking(&self.stream_reliable, msg);
    }

    // --- COMMANDES DE PUBSUB ---

    /// Demande un abonnement. Utilise toujours le stream Fiable.
    /// `target_client` vaut 0 si le client s'abonne lui-même.
    pub fn subscribe(&self, topic: Topic, target_client: NodeId) {
        let msg = BrokerMessage::Subscribe {
            client_id: target_client,
            topic,
        };
        self.inefficient_send_but_nice_looking(&self.stream_reliable, msg);
    }

    /// Annule un abonnement. Utilise toujours le stream Fiable.
    pub fn unsubscribe(&self, topic: Topic, target_client: NodeId) {
        let msg = BrokerMessage::Unsubscribe {
            client_id: target_client,
            topic,
        };
        self.inefficient_send_but_nice_looking(&self.stream_reliable, msg);
    }

    /// Publie une donnée sur un topic. Le choix du stream dépend du besoin.
    pub fn publish_unreliable<T: GameMessage>(&self, topic: Topic, message: &T) {
        // L'unique allocation de tout le processus
        let mut buf = BytesMut::with_capacity(32);
        // On délègue la responsabilité de l'ordre d'écriture au BrokerMessage
        BrokerMessage::write_publish_to(&mut buf, &topic, message);

        self.send_raw(&self.stream_unreliable, buf.freeze());
    }

    /// Publie une donnée critique (ex: Achat, Handoff).
    pub fn publish_reliable<T: GameMessage>(&self, topic: Topic, message: &T) {
        let mut buf = BytesMut::with_capacity(32);
        BrokerMessage::write_publish_to(&mut buf, &topic, message);
        self.send_raw(&self.stream_reliable, buf.freeze());
    }

    // --- FONCTION UTILITAIRE INTERNE ---
    fn send_raw(&self, stream: &GameStream, data: bytes::Bytes) {
        if let Some(conn) = &self.connection {
            let _ = self.peer.send(conn, stream, data);
        } else {
            eprintln!("Tentative d'envoi sans connexion !");
        }
    }
    fn inefficient_send_but_nice_looking(&self, stream: &GameStream, msg: BrokerMessage) {
        if let Some(conn) = &self.connection {
            let serialized = msg.serialize();
            let _ = self.peer.send(conn, stream, serialized);
        } else {
            eprintln!("Tentative d'envoi sans connexion !");
        }
    }
}
