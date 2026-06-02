use crate::broker_message::NodeId;
use crate::msg_game_payload::{GameMessage, GameMessageHeaders};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rocket::serde::{Deserialize, Serialize};

pub struct TakeChunkMessage {
    pub chunk_x: i32,
    pub chunk_y: i32,
}

impl GameMessage for TakeChunkMessage {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::TakeChunk
    }

    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_i32_le(self.chunk_x);
        buf.put_i32_le(self.chunk_y);
        buf.freeze()
    }

    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        if data.len() < 8 {
            return Err("TakeChunk trop court".into());
        }
        let chunk_x = data.get_i32_le();
        let chunk_y = data.get_i32_le();
        Ok(Self { chunk_x, chunk_y })
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
impl GameMessage for HeartbeatMessage {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::Heartbeat
    }

    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u16_le(0); // On réserve 2 octets (Placeholder pour la longueur finale)

        // On écrit le JSON directement dans le buffer
        let mut writer = buf.writer();
        if serde_json::to_writer(&mut writer, &self.heartbeat).is_err() {
            return Bytes::new();
        }

        let mut final_buf = writer.into_inner();
        let payload_len = (final_buf.len() - 2) as u16;

        // On retourne au début pour écraser le placeholder avec la vraie taille !
        final_buf[0..2].copy_from_slice(&payload_len.to_le_bytes());

        final_buf.freeze()
    }

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
    pub chunk_x: i32,
    pub chunk_y: i32,
}

impl GameMessage for SpawnClientMsg {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::SpawnClient
    }

    fn serialize(&self) -> Bytes {
        // Ton code existant ici était déjà parfait avec BytesMut !
        let mut buf = BytesMut::new();
        let pseudo_bytes = self.pseudo.as_bytes();
        if pseudo_bytes.len() > 255 {
            panic!("Pseudo trop long pour SpawnClientMsg");
        }
        buf.put_u32_le(self.client_id);
        buf.put_i32_le(self.chunk_x);
        buf.put_i32_le(self.chunk_y);
        buf.put_u8(pseudo_bytes.len() as u8);
        buf.put_slice(pseudo_bytes);
        buf.freeze()
    }

    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        // 4 (id) + 4 (x) + 4 (y) + 1 (len) = 13 octets minimum
        if data.remaining() < 13 {
            return Err("SpawnClientMsg trop court".into());
        }

        let client_id = data.get_u32_le();
        let chunk_x = data.get_i32_le();
        let chunk_y = data.get_i32_le();
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
            chunk_x,
            chunk_y,
        })
    }
}
