use bevy::prelude::*;
use broker_protocol::broker_message::NodeId;
use core_types::chunks::{GameChunk, GameChunkAera};
use game_message::msg_dgs::{ChunkHandOff, EntityStateTransferHandoff, SpawnClientMsg};
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;
use game_message::msg_client_server::{PersonalSnapshot, PlayerInput};
use game_message::msg_entities::NetworkEntityId;

pub type FastMap<K, V> = FxHashMap<K, V>;

pub type FastSet<K> = FxHashSet<K>;

#[derive(Message)]
pub struct PlayerConnected {
    pub msg: SpawnClientMsg,
}

#[derive(Message, Debug)]
pub struct PlayerDisconnected {
    pub client_id: NodeId,
}

#[derive(Message, Debug)]
pub struct PlayerInputEvent {
    pub client_id: NodeId,
    pub entity_id: NetworkEntityId,
    pub input_data: PlayerInput,
}
#[derive(Resource, Debug)]
pub struct AssignedChunks {
    pub assigned_chunks: FastSet<GameChunk>,
    pub ghost_chunks: FastSet<GameChunk>,
    pub chunk_size: f32,
}


#[derive(Message, Debug)]
pub struct ChunkHandOffMessage {
    pub message: ChunkHandOff,
}



#[derive(Component, PartialEq, Eq, Clone, Copy, Debug)]
pub enum Authority {
    Authoritative,
    LastAuthFrame,
    Ghost,
}

pub fn to_rect(aera: &GameChunkAera, chunk_size: f32) -> Rect {
    let min_x = aera.x_min as f32;
    let min_y = aera.y_min as f32;
    let max_x = (aera.x_max + 1) as f32;
    let max_y = (aera.y_max + 1) as f32;

    Rect {
        min: Vec2::new(min_x, min_y) * chunk_size,
        max: Vec2::new(max_x, max_y) * chunk_size,
    }
}

pub fn to_rect_chunk(chunk: &GameChunk, chunk_size: f32) -> Rect {
    Rect {
        min: Vec2::new(chunk.x as f32, chunk.y as f32) * chunk_size,
        max: Vec2::new((chunk.x + 1) as f32, (chunk.y + 1) as f32) * chunk_size,
    }
}

#[derive(Message)]
pub struct EntityStateTransferEvent {
    pub message: EntityStateTransferHandoff,
}

#[derive(Resource, Default)]
pub struct PendingTransfersForOther {
    pub aeras: Vec<(GameChunkAera, Option<NodeId>)>,
}

#[derive(Message, Debug)]
pub struct SnapshotReceived {
    pub snapshot: PersonalSnapshot,
}
