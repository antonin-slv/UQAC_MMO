use crate::core_types::SerializedGameChunkAera;
use crate::{
    impl_bitcode_net_message, GameMessage, GameMessageHeaders, NetRead, NetWrite, NetWriteTo,
};
use bitcode::{Decode, Encode};
use broker_protocol::broker_message::NodeId;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use core_types::chunks::GameChunk;
use core_types::Rect;

pub enum ChunkHandOffAction {
    TakeArea,    //asks a DGS to take a specific area from N someone
    ReadyToTake, //  this DGS tells another DGS : I'll take that (he answers with 1 serialisation of everything)
    AreaTook,    //A DGS tells to another DGS : I be ready. Do one last broadcast and then stop.
    ReleaseArea, // NOT IMPLEMENTED YET (TODOWHAT ?)
}

impl NetWrite for ChunkHandOffAction {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.write_to(&mut buf);
        buf.freeze()
    }
}

impl NetWriteTo for ChunkHandOffAction {
    fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u8(match self {
            ChunkHandOffAction::TakeArea => 0u8,
            ChunkHandOffAction::ReadyToTake => 1u8,
            ChunkHandOffAction::AreaTook => 2u8,
            ChunkHandOffAction::ReleaseArea => 3u8,
        });
    }
}

impl NetRead for ChunkHandOffAction {
    fn deserialize(buf: &mut Bytes) -> Result<Self, String> {
        let data = buf.get_u8();
        match data {
            0 => Ok(ChunkHandOffAction::TakeArea),
            1 => Ok(ChunkHandOffAction::AreaTook),
            2 => Ok(ChunkHandOffAction::ReleaseArea),
            _ => Err(format!("Unknown chunk handoff type {}", data)),
        }
    }
}

pub struct ChunkHandOff {
    pub action: ChunkHandOffAction,
    pub old_dgs_ids: Vec<NodeId>, //à chaque fois on a Server -> Server (A prend de B, A envois à B) old_dgs_ids == A.
    pub areas: Vec<Rect>,
}

impl NetWriteTo for ChunkHandOff {
    fn write_to(&self, buf: &mut BytesMut) {
        self.action.write_to(buf);
        buf.put_u8(self.areas.len() as u8);
        buf.put_u8(self.old_dgs_ids.len() as u8);
        for id in self.old_dgs_ids.iter() {
            buf.put_u32_le(*id);
        }
        for chunk in &self.areas {
            chunk.write_to(buf);
        }
    }
}

impl GameMessage for ChunkHandOff {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::ChunkHandOff
    }
}

impl NetWrite for ChunkHandOff {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(5);
        self.write_to(&mut buf);
        buf.freeze()
    }
}

impl NetRead for ChunkHandOff {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        match ChunkHandOffAction::deserialize(data) {
            Ok(action) => {
                let old_dgs_id_len = data.get_u8();
                let mut old_dgs_ids = vec![];
                for _ in 0..old_dgs_id_len {
                    old_dgs_ids.push(data.get_u32_le());
                }

                let areas_len = data.get_u8() as usize;
                let mut areas = Vec::with_capacity(areas_len);
                for _ in 0..areas_len {
                    let err = match Rect::deserialize(data) {
                        Ok(rect) => Ok(areas.push(rect)),
                        Err(e) => Err(format!("ChunkHandOffType : {}", e)),
                    };
                    if err.is_err() {
                        return Err(err.unwrap_err());
                    }
                }

                Ok(Self {
                    action,
                    areas,
                    old_dgs_ids,
                })
            }
            Err(e) => Err(format!("ChunkHandOffType : {}", e)),
        }
    }
}

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
impl_bitcode_net_message!(HeartbeatMessage, GameMessageHeaders::Heartbeat);

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
pub struct EntityStateTransferHandoff {
    pub entity_handoff : bool,
    pub chunk_handoff : bool,
    pub origin_aera: SerializedGameChunkAera,
    pub data: Vec<Vec<u8>>, //géré directement par les DGS
}

impl_bitcode_net_message!(
    EntityStateTransferHandoff,
    GameMessageHeaders::EntityStateTransferHandoff
);
