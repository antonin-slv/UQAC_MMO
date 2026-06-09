use crate::{impl_bitcode_net_message, GameMessageHeaders, NetRead, NetWrite, NetWriteTo};
use bitcode::{Decode, Encode};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use core_types::chunks::{GameChunk, GameChunkAera};
use core_types::{Rect, Vec2};


impl_bitcode_net_message!(Rect, GameMessageHeaders::DiscardedMessageBecauseYouKnow);

impl NetWrite for GameChunk {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(4);
        self.write_to(&mut buf);
        buf.freeze()
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

#[derive(Debug, Decode, Encode, Clone, Copy)]
pub struct SerializedGameChunkAera {
    x_min: i16,
    x_max: i16,
    y_min: i16,
    y_max: i16,
}

impl From<GameChunkAera> for SerializedGameChunkAera {
    fn from(value: GameChunkAera) -> Self {
        Self {
            x_min: value.x_min,
            x_max: value.x_max,
            y_min: value.y_min,
            y_max: value.y_max,
        }
    }
}
impl Into<GameChunkAera> for SerializedGameChunkAera {
    fn into(self) -> GameChunkAera {
        GameChunkAera {
            x_min: self.x_min,
            x_max: self.x_max,
            y_min: self.y_min,
            y_max: self.y_max,
        }
    }
}

impl_bitcode_net_message!(
    SerializedGameChunkAera,
    GameMessageHeaders::DiscardedMessageBecauseYouKnow
);

#[derive(Debug, Decode, Encode, Clone)]
pub struct SerializedVec2 {
    pub x: f32,
    pub y: f32,
}
impl From<Vec2> for SerializedVec2 {
    fn from(value: Vec2) -> Self {
        Self {
            x: value.x,
            y: value.y,
        }
    }
}
