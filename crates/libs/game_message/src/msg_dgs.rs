use crate::{GameMessage, GameMessageHeaders, NetRead, NetWrite, NetWriteTo};
use broker_protocol::broker_message::NodeId;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use core_types::GameChunk;
use rocket::serde::json::serde_json;
use rocket::serde::{Deserialize, Serialize};

pub struct TakeChunkMessage {
    pub game_chunk: GameChunk,
}

impl NetWriteTo for TakeChunkMessage {
    fn write_to(&self, buf: &mut BytesMut) {
        self.game_chunk.write_to(buf);
    }
}

impl GameMessage for TakeChunkMessage {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::ChunkHandOff
    }
}

impl NetWrite for TakeChunkMessage {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(4);
        self.write_to(&mut buf);
        buf.freeze()
    }
}

impl NetRead for TakeChunkMessage {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        match GameChunk::deserialize(data) {
            Ok(game_chunk) => Ok(Self { game_chunk }),
            Err(e) => Err(format!("TakeChunkMessage : {}", e)),
        }
    }
}

impl NetWriteTo for GameChunk {
    fn write_to(&self, buf: &mut BytesMut) {
        buf.put_i16_le(self.x);
        buf.put_i16_le(self.y);
    }
}

impl NetRead for GameChunk {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        if data.remaining() < 4 {
            return Err("GameChunk: buffer trop court".into());
        }
        Ok(Self {
            x: data.get_i16_le(),
            y: data.get_i16_le(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Heartbeat {
    pub id: String,
    pub zone: String,
    pub player_count: usize,
    pub max_players: usize,
}

pub struct HeartbeatMessage {
    pub heartbeat: Heartbeat,
}

impl NetWriteTo for HeartbeatMessage {
    fn write_to(&self, buf: &mut BytesMut) {
        let length_index = buf.len();
        buf.put_u16_le(0); // On réserve 2 octets (Placeholder pour la longueur finale)

        let original_len = buf.len();
        // On écrit le JSON directement dans le buffer
        let mut writer = buf.writer();
        if serde_json::to_writer(&mut writer, &self.heartbeat).is_err() {
            panic!("Erreur de sérialisation du Heartbeat en JSON");
        }

        let payload_len = (buf.len() - original_len) as u16;

        // On retourne au début pour écraser le placeholder avec la vraie taille !
        buf[length_index..length_index + 2].copy_from_slice(&payload_len.to_le_bytes());
    }
}

impl GameMessage for HeartbeatMessage {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::Heartbeat
    }
}
impl NetWrite for HeartbeatMessage {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.write_to(&mut buf);
        buf.freeze()
    }
}

impl NetRead for HeartbeatMessage {
    fn deserialize(bytes: &mut Bytes) -> Result<Self, String> {
        if bytes.remaining() < 2 {
            return Err("HeartbeatMessage trop court".into());
        }
        let message_len = bytes.get_u16_le() as usize;

        if bytes.remaining() < message_len {
            return Err("HeartbeatMessage : JSON incomplet".into());
        }

        let json_data = bytes.split_to(message_len);
        let heartbeat: Heartbeat = serde_json::from_slice(&json_data)
            .map_err(|e| format!("Erreur de parsing du Heartbeat : {}", e))?;

        Ok(Self { heartbeat })
    }
}

pub struct SpawnClientMsg {
    pub client_id: NodeId,
    pub pseudo: String,
    pub chunk: GameChunk,
}

impl NetWriteTo for SpawnClientMsg {
    fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.client_id);
        self.chunk.write_to(buf);

        let pseudo_bytes = self.pseudo.as_bytes();
        if pseudo_bytes.len() > 255 {
            panic!("Pseudo trop long pour SpawnClientMsg");
        }
        buf.put_u8(pseudo_bytes.len() as u8);
        buf.put_slice(pseudo_bytes);
    }
}

impl GameMessage for SpawnClientMsg {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::SpawnClient
    }
}
impl NetWrite for SpawnClientMsg {
    fn serialize(&self) -> Bytes {
        // Ton code existant ici était déjà parfait avec BytesMut !
        let mut buf = BytesMut::new();
        self.write_to(&mut buf);
        buf.freeze()
    }
}

impl NetRead for SpawnClientMsg {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        if data.remaining() < 4 {
            return Err("SpawnClientMsg trop court".into());
        }

        let client_id = data.get_u32_le();
        let chunk = match GameChunk::deserialize(data) {
            Ok(chunk) => chunk,
            Err(e) => return Err(format!("SpawnClientMsg : {}", e)),
        };
        let pseudo_len = data.get_u8() as usize;

        if data.remaining() < pseudo_len {
            return Err("SpawnClientMsg : pseudo incomplet".into());
        }

        // Remplacement de copy_to_bytes par split_to pour le Zero-Copy
        let pseudo_bytes = data.split_to(pseudo_len);
        let pseudo = std::str::from_utf8(&pseudo_bytes)
            .map_err(|e| format!("SpawnClientMsg : pseudo invalide : {}", e))?
            .to_string();

        Ok(Self {
            client_id,
            pseudo,
            chunk,
        })
    }
}
