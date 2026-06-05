use crate::{NetRead, NetWrite, NetWriteTo};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use core_types::{GameChunk, Rect};

impl NetWriteTo for Rect {
    fn write_to(&self, buf: &mut BytesMut) {
        buf.put_f32(self.min_x);
        buf.put_f32(self.min_y);
        buf.put_f32(self.max_x);
        buf.put_f32(self.max_y);
    }
}

impl NetWrite for Rect {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        self.write_to(&mut buf);
        buf.freeze()
    }
}

impl NetRead for Rect {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        let rect = Self {
            min_x: data.get_f32(),
            min_y: data.get_f32(),
            max_x: data.get_f32(),
            max_y: data.get_f32(),
        };

        Ok(rect)
    }
}

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