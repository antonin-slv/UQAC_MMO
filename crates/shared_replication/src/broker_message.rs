use crate::broker_protocol::{ProtocolError, ProtocolTag};
use crate::broker_topics::Topic;
use bytes::{BufMut, Bytes, BytesMut};

//ID d'un client connecté au broker
pub type ClientId = u32;

#[derive(Debug, Clone)]
pub enum BrokerMessage {
    Subscribe { client_id: ClientId, topic: Topic },
    Unsubscribe { client_id: ClientId, topic: Topic },
    Publish { topic: Topic, payload: Bytes },
    Broadcast { payload: Bytes },
    AuthorizeClient { client_id: ClientId },
    KickClient { client_id: ClientId },
    ClientBrokerHello { payload : Bytes},
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
            Self::AuthorizeClient { .. } => ProtocolTag::AuthorizeClient,
            Self::KickClient { .. } => ProtocolTag::KickClient,
            Self::ClientBrokerHello { .. } => ProtocolTag::ClientBrokerHello,
        }
    }

    /// Retourne la taille minimale requise (header inclus) pour parser ce type de message
    pub fn min_len(tag: ProtocolTag) -> usize {
        match tag {
            ProtocolTag::Subscribe | ProtocolTag::Unsubscribe => 1 + 4 + 32, // Tag(1) + ID(4) + Topic(32)
            ProtocolTag::Publish => 1 + 32 + 2, // Tag(1) + Topic(32) + Len(2) + [Payload]
            ProtocolTag::Broadcast => 1 + 2,    // Tag(1) + Len(2) + [Payload]
            ProtocolTag::AuthorizeClient | ProtocolTag::KickClient => 1 + 4, // Tag(1) + ID(4)
            ProtocolTag::ClientBrokerHello => 1, // Client -> Broker Tag(1) + PSEUDO
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

            ProtocolTag::AuthorizeClient | ProtocolTag::KickClient => {
                let client_id = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                if tag == ProtocolTag::AuthorizeClient {
                    Ok(Self::AuthorizeClient { client_id })
                } else {
                    Ok(Self::KickClient { client_id })
                }
            }

            ProtocolTag::ClientBrokerHello => {
                Ok(Self::ClientBrokerHello {
                    payload: payload[..].to_owned().into(),
                })
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
            Self::AuthorizeClient { client_id } => {
                buf.put_u32_le(*client_id);
            }
            Self::KickClient { client_id } => {
                buf.put_u32_le(*client_id);
            }
            Self::ClientBrokerHello { payload } => {
                buf.put_u32_le(payload.len() as u32);
                buf.put_slice(payload);
            }
        }
        buf.freeze()
    }
}
