use crate::msg_entities::EntityData;
use crate::{
    impl_bitcode_encode_decode, impl_game_message, GameMessage, GameMessageHeaders, NetRead, NetWrite,
    NetWriteTo,
};
use bitcode::{Decode, Encode};
use broker_protocol::broker_message::NodeId;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use core_types::chunks::{GameChunk, GameChunkAera};
use core_types::Rect;

#[derive(Decode, Encode, Debug, PartialEq)]
pub enum ChunkHandOffAction {
    TakeArea,         //asks a DGS to take a specific area from N someone
    ReadyToTake,      // this DGS tells another DGS : I'll take that (he answers with 1 serialisation of everything)
    ForceReleaseAera, // The spatial server sends it to people when the DGS is unassigned from its shard.
    AeraTook,         // A DGS TELLS THE SPATIAL SERVER it took an aera OR
    AeraGiven,        // A DGS TELLS THE SPATIAL SERVER it Released this aera.
}
impl_bitcode_encode_decode!(ChunkHandOffAction);

#[derive(Decode, Encode, Debug)]
pub struct ChunkHandOff {
    pub action: ChunkHandOffAction,
    pub areas: Vec<(Rect, Option<NodeId>)>,
}
impl_bitcode_encode_decode!(ChunkHandOff);
impl_game_message!(ChunkHandOff, GameMessageHeaders::ChunkHandOff);

#[derive(Debug, Decode, Encode, Clone)]
pub struct Heartbeat {
    pub id: String,
    pub node_id: NodeId,
    pub zone: String,
    pub player_count: usize,
    pub max_players: usize,
}

#[derive(Debug, Decode, Encode, Clone)]
pub struct HeartbeatMessage {
    pub heartbeat: Heartbeat,
}
impl_bitcode_encode_decode!(HeartbeatMessage);
impl_game_message!(HeartbeatMessage, GameMessageHeaders::Heartbeat);

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

#[derive(Debug, Decode, Encode, Clone)]
pub struct ChunkDataHandOff {
    pub origin_aera: GameChunkAera,
    pub old_owner: Option<NodeId>,
    pub data: Vec<EntityData>, //géré directement par les DGS
}
impl_bitcode_encode_decode!(ChunkDataHandOff);
impl_game_message!(ChunkDataHandOff, GameMessageHeaders::ChunkDataHandOff);

#[derive(Debug, Decode, Encode, Clone)]
pub struct EntityHandOff {
    pub data: Vec<EntityData>,
}
impl_bitcode_encode_decode!(EntityHandOff);
impl_game_message!(EntityHandOff, GameMessageHeaders::EntityHandOff);
