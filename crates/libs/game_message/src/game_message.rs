use broker_protocol::broker_message::BrokerMessage;
use broker_protocol::topics::Topic;
use bytes::{BufMut, Bytes, BytesMut};

pub mod core_types;
pub mod msg_client_server;
pub mod msg_dgs;
pub mod msg_entities;
pub mod msg_servers;

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq)]
//ce que les clients peuvent recevoir.
pub enum GameMessageHeaders {
    Snapshot = 0x04,    //the shards broadcast the state of the world
    ClientInput = 0x05, //client to Shard

    ClientHello = 0x06,         // Client tells Hello
    ClientWelcome = 0x07,       // Auth to client
    SpawnClient = 0x08,         // Spatial tells DGS to spawn a client
    ClientEntitySpawned = 0x09, //DGS tells client it spawned

    Heartbeat = 0x0B, //from shard => broker then broker => Orchestrator
    SpawnServer = 0x0C,
    FriendHello = 0x0F, //When something that isn't a client says hello.

    //inter shard protocol
    ChunkHandOff = 0x10,

    ChunkDataHandOff = 0x11, //during an handoff, transfers the hidden data.
    EntityHandOff = 0x12,    //transfers the entity

    DiscardedMessageBecauseYouKnow,
}

//create BrokerMessageHeaderFrom u8 :
impl From<u8> for GameMessageHeaders {
    fn from(value: u8) -> Self {
        match value {
            0x04 => GameMessageHeaders::Snapshot,
            0x05 => GameMessageHeaders::ClientInput,

            0x06 => GameMessageHeaders::ClientHello,
            0x07 => GameMessageHeaders::ClientWelcome,
            0x08 => GameMessageHeaders::SpawnClient,
            0x09 => GameMessageHeaders::ClientEntitySpawned,

            0x0B => GameMessageHeaders::Heartbeat,
            0x0C => GameMessageHeaders::SpawnServer,
            0x0F => GameMessageHeaders::FriendHello,

            0x10 => GameMessageHeaders::ChunkHandOff,
            0x11 => GameMessageHeaders::ChunkDataHandOff,
            0x12 => GameMessageHeaders::EntityHandOff,

            _ => GameMessageHeaders::DiscardedMessageBecauseYouKnow,
        }
    }
}

/// Le contrat que chaque message du jeu doit respecter.
/// Il peut être défini n'importe où dans tes fichiers !
pub trait GameMessage: Sized + NetWrite + NetRead + NetWriteTo {
    /// Quel est l'octet d'en-tête de ce message ?
    fn header() -> GameMessageHeaders;
}

pub trait NetWrite {
    /// Transforme la structure en octets
    fn serialize(&self) -> Bytes;
}

pub trait NetRead: Sized {
    /// Reconstruit la structure à partir des octets
    fn deserialize(data: &mut Bytes) -> Result<Self, String>;
}

pub trait NetWriteTo {
    fn write_to(&self, buf: &mut BytesMut);
}
/// L'enveloppe renvoyée par le BrokerClient
#[derive(Debug, Clone)]
pub struct GamePayload {
    pub header: GameMessageHeaders,
    pub data: Bytes,
}

impl GamePayload {
    pub fn extract<T: GameMessage>(&mut self) -> Result<T, String> {
        if self.header == T::header() {
            T::deserialize(&mut self.data)
        } else {
            Err(format!(
                "Erreur d'extraction : Header attendu {:?}, mais le payload contient {:?}",
                T::header(),
                self.header
            ))
        }
    }

    pub fn write_publish_to<T: GameMessage>(buf: &mut BytesMut, topic: &Topic, message: &T) {
        BrokerMessage::write_publish_headers(buf, topic);

        buf.put_u16_le(0);
        let payload_start = buf.len();

        buf.put_u8(T::header() as u8);
        message.write_to(buf);

        // 5. Calcul de la taille réelle et rétro-injection
        let payload_len = (buf.len() - payload_start) as u16;
        buf[payload_start - 2..payload_start].copy_from_slice(&payload_len.to_le_bytes());
    }
}

#[macro_export]
macro_rules! impl_game_message {
    ($msg_type:ty, $header_variant:path) => {
        impl $crate::GameMessage for $msg_type {
            fn header() -> $crate::GameMessageHeaders {
                $header_variant
            }
        }
    };
}

#[macro_export]
macro_rules! impl_bitcode_encode_decode {
    ($msg_type:ty) => {
        impl $crate::NetWriteTo for $msg_type {
            fn write_to(&self, buf: &mut bytes::BytesMut) {
                use bytes::BufMut;

                // bitcode::encode retourne directement un Vec<u8> fortement optimisé.
                // On a juste à le copier dans le BytesMut final.
                let encoded = bitcode::encode(self);
                buf.put_slice(&encoded);
            }
        }

        impl $crate::NetWrite for $msg_type {
            fn serialize(&self) -> bytes::Bytes {
                bytes::Bytes::from(bitcode::encode(self))
            }
        }

        impl $crate::NetRead for $msg_type {
            fn deserialize(data: &mut bytes::Bytes) -> Result<Self, String> {
                use bytes::Buf;

                // bitcode lit le slice entier. On lui passe une référence aux données.
                match bitcode::decode::<Self>(&data) {
                    Ok(msg) => {
                        let len = data.remaining();
                        data.advance(len);

                        Ok(msg)
                    }
                    Err(e) => Err(format!(
                        "Impossible de désérialiser {}: {:?}",
                        stringify!($msg_type),
                        e
                    )),
                }
            }
        }
    };
}
