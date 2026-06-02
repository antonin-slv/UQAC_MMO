use crate::broker_topics::Topic;
use bytes::{BufMut, Bytes, BytesMut};
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
        match tag {
            ProtocolTag::Subscribe | ProtocolTag::Unsubscribe => 1 + 4 + 32, // Tag(1) + ID(4) + Topic(32)
            ProtocolTag::Publish => 1 + 32 + 2, // Tag(1) + Topic(32) + Len(2) + [Payload]
            ProtocolTag::Broadcast => 1 + 2,    // Tag(1) + Len(2) + [Payload]
            ProtocolTag::BroadcastFromClient => 1 + 4 + 2, //tag + clientID + [Payload]
            ProtocolTag::AuthorizeClient | ProtocolTag::KickClient => 1 + 4, // Tag(1) + ID(4)
            ProtocolTag::Welcome => 1 + 4,
        }
    }

    // --- SÉRIALISATION / DÉSÉRIALISATION (ZÉRO PANIC) ---

    /// Désérialisation sécurisée. Ne crashera jamais.
    pub fn deserialize(data: Bytes) -> Result<Self, ProtocolError> {
        if data.is_empty() {
            return Err(ProtocolError::BufferTooShort {
                expected: 1,
                actual: 0,
                context: "Lecture du Tag",
            });
        }

        let tag = ProtocolTag::try_from(data[0])?;
        let min_len = Self::min_len(tag);

        if data.len() < min_len {
            return Err(ProtocolError::BufferTooShort {
                expected: min_len,
                actual: data.len(),
                context: "Vérification taille minimale",
            });
        }

        let payload = &data[1..]; // Safe car on a vérifié is_empty()

        match tag {
            ProtocolTag::Subscribe | ProtocolTag::Unsubscribe => {
                let client_id = u32::from_le_bytes(payload[0..4].try_into().unwrap()); // Unwrap safe car on a vérifié min_len
                let mut topic = Topic::default();
                topic.copy_from_slice(&payload[4..36]); // Unwrap safe car on a vérifié min_len

                if tag == ProtocolTag::Subscribe {
                    Ok(Self::Subscribe { client_id, topic })
                } else {
                    Ok(Self::Unsubscribe { client_id, topic })
                }
            }
            ProtocolTag::Publish => {
                let mut topic = Topic::default();
                topic.copy_from_slice(&payload[0..32]);
                let payload_len = u16::from_le_bytes(payload[32..34].try_into().unwrap()) as usize;

                // On vérifie que le payload variable est bien présent en entier
                if data.len() < min_len + payload_len {
                    return Err(ProtocolError::BufferTooShort {
                        expected: min_len + payload_len,
                        actual: data.len(),
                        context: "Lecture du payload dynamique (Publish)",
                    });
                }
                let publish_data = Bytes::copy_from_slice(&payload[34..34 + payload_len]);
                Ok(Self::Publish {
                    topic,
                    payload: publish_data,
                })
            }

            ProtocolTag::Broadcast => {
                let payload_len = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;

                if data.len() >= min_len + payload_len {
                    let playload = Bytes::copy_from_slice(&payload[2..2 + payload_len]);

                    Ok(Self::Broadcast { payload: playload })
                } else {
                    Err(ProtocolError::BufferTooShort {
                        expected: min_len + payload_len,
                        actual: data.len(),
                        context: "Lecture du payload dynamique (Broadcast)",
                    })
                }
            }
            ProtocolTag::BroadcastFromClient => {
                if data.len() <= min_len {
                    return Err(ProtocolError::BufferTooShort {
                        expected: min_len + 1, // Au moins 1 octet de payload
                        actual: data.len(),
                        context: "Lecture du \"header\" (BroadcastFromClient)",
                    });
                }
                let client_id = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                // 2. La taille (2 octets de 4 à 6)
                let payload_len = u16::from_le_bytes(payload[4..6].try_into().unwrap()) as usize;

                if data.len() >= min_len + payload_len {
                    // 3. Les données (à partir de 6)
                    let playload = Bytes::copy_from_slice(&payload[6..6 + payload_len]);

                    Ok(Self::BroadCastFrom {
                        client_id,
                        payload: playload,
                    })
                } else {
                    Err(ProtocolError::BufferTooShort {
                        expected: min_len + payload_len,
                        actual: data.len(),
                        context: "Lecture du payload dynamique (BroadcastClientConnected)",
                    })
                }
            }

            ProtocolTag::AuthorizeClient | ProtocolTag::KickClient => {
                let client_id = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                if tag == ProtocolTag::AuthorizeClient {
                    Ok(Self::AuthorizeClient { client_id })
                } else {
                    Ok(Self::KickNode { client_id })
                }
            }

            ProtocolTag::Welcome => {
                let client_id = u32::from_le_bytes(payload[0..4].try_into().unwrap());
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
