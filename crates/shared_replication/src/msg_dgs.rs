use crate::broker_message::ClientId;
use crate::msg_game_payload::{GameMessage, GameMessageHeaders};
use bytes::{BufMut, Bytes, BytesMut};
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

    fn deserialize(data: &Bytes) -> Result<Self, String> {
        if data.len() < 8 {
            return Err("TakeChunk trop court".into());
        }
        let chunk_x = i32::from_le_bytes(data[0..4].try_into().unwrap());
        let chunk_y = i32::from_le_bytes(data[4..8].try_into().unwrap());
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
        if let Ok(json_bytes) = serde_json::to_vec(&self.heartbeat) {
            let data_len = json_bytes.len();
            let data_len_u8 = (data_len as u16).to_le_bytes() as [u8; 2];
            let mut data = BytesMut::with_capacity(2 + data_len);
            data.put_slice(&data_len_u8);
            data.put_slice(&json_bytes);
            data.freeze()
        } else {
            Bytes::new() // En cas d'erreur de sérialisation, on retourne un message vide
        }
    }

    fn deserialize(bytes: &Bytes) -> Result<Self, String> {
        if bytes.len() < 2 {
            return Err("HeartbeatMessage trop court".into());
        }
        let message_len = u16::from_le_bytes(bytes[0..2].try_into().unwrap());//safe car data assez longue

        let heartbeat_data = bytes
            .get(2..(2 + message_len as usize))
            .ok_or("Données de Heartbeat manquantes")?;
        let heartbeat: Heartbeat = serde_json::from_slice(heartbeat_data)
            .map_err(|e| format!("Erreur de parsing du Heartbeat : {}", e))?;
        Ok(Self { heartbeat })
    }
}

pub struct SpawnClientMsg {
    pub client_id: ClientId,
    pub pseudo: String,
    pub chunk_x: i32,
    pub chunk_y: i32,
}

impl GameMessage for SpawnClientMsg {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::SpawnClient
    }

    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32_le(self.client_id);
        let pseudo_bytes = self.pseudo.as_bytes();
        if pseudo_bytes.len() > 255 {
            panic!("Pseudo trop long pour SpawnClientMsg");
        }
        buf.put_i32_le(self.chunk_x);
        buf.put_i32_le(self.chunk_y);
        buf.put_u8(pseudo_bytes.len() as u8);
        buf.put_slice(pseudo_bytes);
        buf.freeze()
    }

    fn deserialize(data: &Bytes) -> Result<Self, String> {
        if data.len() < 13 {
            return Err("SpawnClientMsg trop court".into());
        }
        let client_id = ClientId::from_le_bytes(data[..4].try_into().unwrap());
        let chunk_x = i32::from_le_bytes(data[4..8].try_into().unwrap());
        let chunk_y = i32::from_le_bytes(data[8..12].try_into().unwrap());
        let pseudo_len = data[13] as usize;
        if data.len() < 13 + pseudo_len {
            return Err("SpawnClientMsg : données de pseudo manquantes".into());
        }
        let pseudo = std::str::from_utf8(&data[12..12 + pseudo_len])
            .map_err(|e| format!("SpawnClientMsg : pseudo non valide UTF-8 : {}", e))?
            .to_string();
        Ok(Self {
            client_id,
            pseudo,
            chunk_x,
            chunk_y,
        })
    }
}
