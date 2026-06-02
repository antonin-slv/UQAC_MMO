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

pub type NodeId = u32;

pub trait NodeIdMetaData {
    const FIRST_SERVER_ID: Self;
    const FIRST_CLIENT_ID: Self;
    const REFERENCE_TO_SENDER: Self;

    fn is_client(&self) -> bool;
    fn is_server(&self) -> bool;
}

impl NodeIdMetaData for NodeId {
    const FIRST_SERVER_ID: Self = 0x80000000;
    const FIRST_CLIENT_ID: Self = 1;
    const REFERENCE_TO_SENDER: Self = 0;
    #[inline]
    fn is_client(&self) -> bool {
        (self & Self::FIRST_SERVER_ID == 0) && (*self != Self::REFERENCE_TO_SENDER)
    }

    #[inline]
    fn is_server(&self) -> bool {
        (self & Self::FIRST_SERVER_ID) != 0
    }
}

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
    #[inline]
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

    pub fn min_len(tag: ProtocolTag) -> usize {
        let tl = Topic::topic_length();
        match tag {
            ProtocolTag::Subscribe | ProtocolTag::Unsubscribe => 1 + 4 + tl,
            ProtocolTag::Publish => 1 + tl + 2,
            ProtocolTag::Broadcast => 1 + 2,
            ProtocolTag::BroadcastFromClient => 1 + 4 + 2,
            ProtocolTag::AuthorizeClient | ProtocolTag::KickClient => 1 + 4,
            ProtocolTag::Welcome => 1 + 4,
        }
    }

    // --- OUTILS DE LECTURE ZERO-COPY ---

    /// Regarde le tag du message encapsulé sans copier ni consommer la mémoire
    pub fn peek_inner_tag(payload: &Bytes) -> Option<ProtocolTag> {
        payload.first().and_then(|&b| ProtocolTag::try_from(b).ok())
    }

    /// Regarde l'ID déclaré dans le message encapsulé sans copier
    pub fn peek_inner_client_id(payload: &Bytes) -> Option<NodeId> {
        if payload.len() >= 5 {
            Some(u32::from_le_bytes(payload[1..5].try_into().unwrap()))
        } else {
            None
        }
    }

    pub fn deserialize(mut payload: Bytes) -> Result<Self, ProtocolError> {
        if payload.is_empty() {
            return Err(ProtocolError::BufferTooShort {
                expected: 1,
                actual: 0,
                context: "Lecture du Tag",
            });
        }

        let tag = ProtocolTag::try_from(payload.get_u8())?;
        let real_payload_len = payload.len() + 1;
        let min_len = Self::min_len(tag);

        if real_payload_len < min_len {
            return Err(ProtocolError::BufferTooShort {
                expected: min_len,
                actual: real_payload_len,
                context: "Taille minimale",
            });
        }

        match tag {
            ProtocolTag::Subscribe | ProtocolTag::Unsubscribe => {
                let client_id = payload.get_u32_le();
                let topic_bytes = payload.split_to(Topic::topic_length());
                let mut topic = [0u8; 16];
                topic.copy_from_slice(&topic_bytes);

                if tag == ProtocolTag::Subscribe {
                    Ok(Self::Subscribe { client_id, topic })
                } else {
                    Ok(Self::Unsubscribe { client_id, topic })
                }
            }
            ProtocolTag::Publish => {
                let topic_bytes = payload.split_to(Topic::topic_length());
                let mut topic = Topic::default_topic();
                topic.copy_from_slice(&topic_bytes);

                let payload_len = payload.get_u16_le() as usize;

                if payload.len() < payload_len {
                    return Err(ProtocolError::BufferTooShort {
                        expected: min_len + payload_len,
                        actual: real_payload_len,
                        context: "Publish dynamique",
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
                    Ok(Self::Broadcast {
                        payload: payload.split_to(payload_len),
                    })
                } else {
                    Err(ProtocolError::BufferTooShort {
                        expected: min_len + payload_len,
                        actual: real_payload_len,
                        context: "Deserialize Broadcast",
                    })
                }
            }
            ProtocolTag::BroadcastFromClient => {
                let client_id = payload.get_u32_le();
                let payload_len = payload.get_u16_le() as usize;

                if payload.len() >= payload_len {
                    Ok(Self::BroadCastFrom {
                        client_id,
                        payload: payload.split_to(payload_len),
                    })
                } else {
                    Err(ProtocolError::BufferTooShort {
                        expected: min_len + payload_len,
                        actual: real_payload_len,
                        context: "BroadcastFromClient",
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
                Ok(Self::Welcome { node_id: client_id })
            }
        }
    }

    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(self.tag() as u8);
        self.write_to(&mut buf);
        buf.freeze()
    }

    pub fn write_to(&self, buf: &mut BytesMut) {
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
            Self::AuthorizeClient { client_id }
            | Self::KickNode { client_id }
            | Self::Welcome { node_id: client_id } => {
                buf.put_u32_le(*client_id);
            }
        }
    }

    pub fn write_subscribe_to(buf: &mut BytesMut, target_client: NodeId, topic: &Topic) {
        buf.put_u8(ProtocolTag::Subscribe as u8);
        buf.put_u32_le(target_client);
        buf.put_slice(topic);
    }

    pub fn write_unsubscribe_from(buf: &mut BytesMut, target_client: NodeId, topic: &Topic) {
        buf.put_u8(ProtocolTag::Unsubscribe as u8);
        buf.put_u32_le(target_client);
        buf.put_slice(topic);
    }

    pub fn write_publish_headers(buf: &mut BytesMut, topic: &Topic) {
        buf.put_u8(ProtocolTag::Publish as u8);
        buf.put_slice(topic);
    }
}
