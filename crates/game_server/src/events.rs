use bevy::prelude::*;
use broker_protocol::broker_message::NodeId;
use core_types::chunks::{GameChunk};
use core_types::helpers::FastSet;
use game_message::msg_client_server::{InputBuffer, PersonalSnapshot};
use game_message::msg_dgs::{ChunkDataHandOff, ChunkHandOff, EntityHandOff, SpawnClientMsg};
use game_message::msg_entities::{NetworkEntityId};

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
    pub input_data: InputBuffer,
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

#[derive(Message)]
pub struct ChunkTransferEvent {
    pub message: ChunkDataHandOff,
}

#[derive(Message, Debug)]
pub struct EntityTransferEvent {
    pub message : EntityHandOff
}

#[derive(Resource, Default)]
pub struct PendingChunkTransfersForOther {
    pub aeras: Vec<(core_types::Rect, Option<NodeId>)>,
}

#[derive(Message, Debug)]
pub struct SnapshotReceived {
    pub snapshot: PersonalSnapshot,
}

