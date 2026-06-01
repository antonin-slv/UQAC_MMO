use crate::broker_message::{BrokerMessage, ClientId};
use crate::broker_topics::Topic;
use bytes::Bytes;
use game_sockets::GameSocketError;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};

const RELIABLE_STREAM_ID: u16 = 1;
/// Les événements simplifiés remontés au moteur de jeu (Bevy)
pub enum ClientNetworkEvent {
    Connected,
    Ready,
    Disconnected,
    /// Les données brutes reçues
    DataReceived {
        stream: GameStream,
        payload: Bytes,
    },
}

pub struct MmoNetworkClient {
    peer: GamePeer,
    connection: Option<GameConnection>,
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
                    let _ = self
                        .peer
                        .create_stream(conn, GameStreamReliability::Reliable, 1);
                    return Some(ClientNetworkEvent::Connected);
                }
                Ok(Some(GameNetworkEvent::Disconnected(_))) => {
                    self.connection = None;
                    return Some(ClientNetworkEvent::Disconnected);
                }
                Ok(Some(GameNetworkEvent::Message { stream, data, .. })) => {
                    match BrokerMessage::deserialize(data) {
                        Ok(BrokerMessage::Broadcast { payload }) => {
                            return Some(ClientNetworkEvent::DataReceived { stream, payload });
                        }
                        Err(e) => {
                            eprintln!("[BrokerClient] Erreur de parsing du message: {}", e);
                            continue; // On passe au paquet suivant !
                        }
                        _ => continue,
                    }
                }
                Ok(Some(GameNetworkEvent::StreamCreated(_conn, stream))) => {
                    if (stream.real_stream_id() == RELIABLE_STREAM_ID)
                        && stream.is_reliable()
                        && !self.is_ready
                        && self.is_connected()
                    {
                        self.is_ready = true;
                        eprintln!("[BrokerClient] Stream fiable prêt, client ready !");
                        return Some(ClientNetworkEvent::Ready);
                    }
                    continue; // On ignore les autres créations de stream
                }
                Ok(Some(GameNetworkEvent::StreamClosed(_conn, _stream))) => {
                    eprintln!("[BrokerClient] stream {:} was closed", _stream.stream_id);
                    continue;
                }
                Ok(Some(GameNetworkEvent::Error { inner: error, .. })) => {
                    match error {
                        GameSocketError::ConnectionError { .. } => {
                            self.connection = None;
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

    // --- API D'ÉMISSION ---
    pub fn actual_game_client_not_server_say_hello(&self, pseudo : String) {
        let msg = BrokerMessage::ClientBrokerHello {
            payload: Bytes::from(pseudo),
        };
        self.send_internal(&self.stream_reliable, msg);
    }

    pub fn authorize_client(&self, target_client: ClientId) {
        let msg = BrokerMessage::AuthorizeClient {
            client_id: target_client,
        };
        self.send_internal(&self.stream_reliable, msg);
    }

    pub fn kick_client(&self, target_client: ClientId) {
        let msg = BrokerMessage::KickClient {
            client_id: target_client,
        };
        self.send_internal(&self.stream_reliable, msg);
    }

    /// Demande un abonnement. Utilise toujours le stream Fiable.
    /// `target_client` vaut 0 si le client s'abonne lui-même.
    pub fn subscribe(&self, topic: Topic, target_client: ClientId) {
        let msg = BrokerMessage::Subscribe {
            client_id: target_client,
            topic,
        };
        self.send_internal(&self.stream_reliable, msg);
    }

    /// Annule un abonnement. Utilise toujours le stream Fiable.
    pub fn unsubscribe(&self, topic: Topic, target_client: ClientId) {
        let msg = BrokerMessage::Unsubscribe {
            client_id: target_client,
            topic,
        };
        self.send_internal(&self.stream_reliable, msg);
    }

    /// Publie une donnée sur un topic. Le choix du stream dépend du besoin.
    pub fn publish_unreliable(&self, topic: Topic, payload: Bytes) {
        let msg = BrokerMessage::Publish { topic, payload };
        self.send_internal(&self.stream_unreliable, msg);
    }

    /// Publie une donnée critique (ex: Achat, Handoff).
    pub fn publish_reliable(&self, topic: Topic, payload: Bytes) {
        let msg = BrokerMessage::Publish { topic, payload };
        self.send_internal(&self.stream_reliable, msg);
    }

    // --- FONCTION UTILITAIRE INTERNE ---

    fn send_internal(&self, stream: &GameStream, msg: BrokerMessage) {
        if let Some(conn) = &self.connection {
            let serialized = msg.serialize();
            let _ = self.peer.send(conn, stream, serialized);
        } else {
            eprintln!("Tentative d'envoi sans connexion !");
        }
    }
}
