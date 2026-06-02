use crate::broker_topics::{Topic, TopicDefaults};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;

pub const RELIABLE_STREAM_ID: u16 = 0;

#[derive(Debug)]
pub enum ProtocolError {
    BufferTooShort {
        expected: usize,
        actual: usize,
        context: &'static str,
    },
    UnknownTag(u8),
    MalformedData(&'static str),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::BufferTooShort {
                expected,
                actual,
                context,
            } => {
                write!(
                    f,
                    "Paquet tronqué [{}]. Attendu: {} octets, Reçu: {}",
                    context, expected, actual
                )
            }
            ProtocolError::UnknownTag(tag) => write!(f, "Tag de protocole inconnu : 0x{:02X}", tag),
            ProtocolError::MalformedData(msg) => write!(f, "Données mal formées : {}", msg),
        }
    }
}
impl std::error::Error for ProtocolError {}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolTag {
    Subscribe = 0x01,
    Unsubscribe = 0x02,
    Publish = 0x03,
    Broadcast = 0x04,
    BroadcastFromClient = 0x05,
    AuthorizeClient = 0x06,
    KickClient = 0x07,
    Welcome = 0x08,
}

impl TryFrom<u8> for ProtocolTag {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Subscribe),
            0x02 => Ok(Self::Unsubscribe),
            0x03 => Ok(Self::Publish),
            0x04 => Ok(Self::Broadcast),
            0x05 => Ok(Self::BroadcastFromClient),
            0x06 => Ok(Self::AuthorizeClient),
            0x07 => Ok(Self::KickClient),
            0x08 => Ok(Self::Welcome),

            _ => Err(ProtocolError::UnknownTag(value)),
        }
    }
}

//ID d'un client connecté au broker
pub type NodeId = u32;

#[derive(Debug, Clone)]
pub enum BrokerMessage {
    Subscribe { client_id: NodeId, topic: Topic },
    Unsubscribe { client_id: NodeId, topic: Topic },
    Publish { topic: Topic, payload: Bytes },
    Broadcast { payload: Bytes },
    BroadCastFrom { client_id: NodeId, payload: Bytes },
    AuthorizeClient { client_id: NodeId },
    KickNode { client_id: NodeId },
    Welcome { node_id: NodeId },
}

impl BrokerMessage {
    // --- HELPERS DEMANDÉS ---

    #[inline]
    /// Retourne le tag binaire associé au message
    pub fn tag(&self) -> ProtocolTag {
        match self {
            Self::Subscribe { .. } => ProtocolTag::Subscribe,
            Self::Unsubscribe { .. } => ProtocolTag::Unsubscribe,
            Self::Publish { .. } => ProtocolTag::Publish,
            Self::Broadcast { .. } => ProtocolTag::Broadcast,
            Self::BroadCastFrom { .. } => ProtocolTag::BroadcastFromClient,
            Self::AuthorizeClient { .. } => ProtocolTag::AuthorizeClient,
            Self::KickNode { .. } => ProtocolTag::KickClient,
            Self::Welcome { .. } => ProtocolTag::Welcome,
        }
    }

    /// Retourne la taille minimale requise (header inclus) pour parser ce type de message
    pub fn min_len(tag: ProtocolTag) -> usize {
        let tl = Topic::topic_length();
        match tag {
            ProtocolTag::Subscribe | ProtocolTag::Unsubscribe => 1 + 4 + tl, // Tag(1) + ID(4) + Topic(16)
            ProtocolTag::Publish => 1 + tl + 2, // Tag(1) + Topic(16) + Len(2) + [Payload]
            ProtocolTag::Broadcast => 1 + 2,    // Tag(1) + Len(2) + [Payload]
            ProtocolTag::BroadcastFromClient => 1 + 4 + 2, //tag + clientID + [Payload]
            ProtocolTag::AuthorizeClient | ProtocolTag::KickClient => 1 + 4, // Tag(1) + ID(4)
            ProtocolTag::Welcome => 1 + 4,
        }
    }

    // --- SÉRIALISATION / DÉSÉRIALISATION (ZÉRO PANIC) ---

    /// Désérialisation sécurisée. Ne crashera jamais.
    pub fn deserialize(mut payload: Bytes) -> Result<Self, ProtocolError> {
        if payload.is_empty() {
            return Err(ProtocolError::BufferTooShort {
                expected: 1,
                actual: 0,
                context: "Lecture du Tag",
            });
        }

        let tag = payload.get_u8();

        let tag = match ProtocolTag::try_from(tag) {
            Ok(tag) => tag,
            Err(e) => return Err(e),
        };
        let real_payload_len = payload.len() + 1;

        let min_len = Self::min_len(tag);

        if real_payload_len < min_len {
            return Err(ProtocolError::BufferTooShort {
                expected: min_len,
                actual: real_payload_len,
                context: "Vérification taille minimale",
            });
        }

        match tag {
            ProtocolTag::Subscribe | ProtocolTag::Unsubscribe => {
                let client_id = payload.get_u32_le();
                let topic = payload.split_to(Topic::topic_length());

                if tag == ProtocolTag::Subscribe {
                    Ok(Self::Subscribe { client_id, topic })
                } else {
                    Ok(Self::Unsubscribe { client_id, topic })
                }
            }
            ProtocolTag::Publish => {
                let topic = payload.split_to(Topic::topic_length());
                let payload_len = payload.get_u16_le() as usize;

                // On vérifie que le payload variable est bien présent en entier
                if payload.len() < payload_len {
                    return Err(ProtocolError::BufferTooShort {
                        expected: min_len + payload_len,
                        actual: real_payload_len,
                        context: "Lecture du payload dynamique (Publish)",
                    });
                }
                let publish_data = payload.split_to(payload_len);
                Ok(Self::Publish {
                    topic,
                    payload: publish_data,
                })
            }

            ProtocolTag::Broadcast => {
                let payload_len = payload.get_u16_le() as usize;

                if payload.len() >= payload_len {
                    let playload = payload.split_to(payload_len);

                    Ok(Self::Broadcast { payload: playload })
                } else {
                    Err(ProtocolError::BufferTooShort {
                        expected: min_len + payload_len,
                        actual: real_payload_len,
                        context: "Lecture du payload dynamique (Broadcast)",
                    })
                }
            }
            ProtocolTag::BroadcastFromClient => {
                let client_id = payload.get_u32_le();
                // 2. La taille (2 octets de 4 à 6)
                let payload_len = payload.get_u16_le() as usize;

                if payload.len() >= payload_len {
                    // 3. Les données (à partir de 6)
                    let playload = payload.split_to(payload_len);

                    Ok(Self::BroadCastFrom {
                        client_id,
                        payload: playload,
                    })
                } else {
                    Err(ProtocolError::BufferTooShort {
                        expected: min_len + payload_len,
                        actual: real_payload_len,
                        context: "Lecture du payload dynamique (BroadcastClientConnected)",
                    })
                }
            }

            ProtocolTag::AuthorizeClient | ProtocolTag::KickClient => {
                let client_id = payload.get_u32_le();
                if tag == ProtocolTag::AuthorizeClient {
                    Ok(Self::AuthorizeClient { client_id })
                } else {
                    Ok(Self::KickNode { client_id })
                }
            }

            ProtocolTag::Welcome => {
                let client_id = payload.get_u32_le();
                if tag == ProtocolTag::Welcome {
                    Ok(Self::Welcome { node_id: client_id })
                } else {
                    Err(ProtocolError::MalformedData("Welcome message"))
                }
            }
        }
    }

    /// Sérialisation hyper performante via `BytesMut`
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(self.tag() as u8);

        match self {
            Self::Subscribe { client_id, topic } | Self::Unsubscribe { client_id, topic } => {
                buf.put_u32_le(*client_id);
                buf.put_slice(topic);
            }
            Self::Publish { topic, payload } => {
                buf.put_slice(topic);
                buf.put_u16_le(payload.len() as u16);
                buf.put_slice(payload);
            }
            Self::Broadcast { payload } => {
                buf.put_u16_le(payload.len() as u16);
                buf.put_slice(payload);
            }
            Self::BroadCastFrom { client_id, payload } => {
                buf.put_u32_le(*client_id);
                buf.put_u16_le(payload.len() as u16);
                buf.put_slice(payload);
            }
            Self::AuthorizeClient { client_id } => {
                buf.put_u32_le(*client_id);
            }
            Self::KickNode { client_id } => {
                buf.put_u32_le(*client_id);
            }
            Self::Welcome { node_id: client_id } => {
                buf.put_u32_le(*client_id);
            }
        }
        buf.freeze()
    }
}
