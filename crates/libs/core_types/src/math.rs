use crate::msg_game_payload::{NetRead, NetWrite, NetWriteTo};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Vec2 {
    pub x: f32,
    pub y: f32,
}

#[derive(Debug, Clone, Default)]
pub struct GameChunk {
    pub x: i16,
    pub y: i16,
}

impl Vec2 {
    pub fn new(x: f32, y: f32) -> Self {
        Self { x, y }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rect {
    pub min_x: f32,
    pub min_y: f32,
    pub max_x: f32,
    pub max_y: f32,
}

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

pub type OwnedArea = (u32, Vec<Rect>);

impl Rect {
    pub fn contains(&self, p: Vec2) -> bool {
        p.x >= self.min_x && p.x <= self.max_x && p.y >= self.min_y && p.y <= self.max_y
    }

    pub fn split(&self) -> [Rect; 4] {
        let mid_x = (self.min_x + self.max_x) / 2.0;
        let mid_y = (self.min_y + self.max_y) / 2.0;

        [
            Rect {
                min_x: self.min_x,
                max_x: mid_x,
                min_y: self.min_y,
                max_y: mid_y,
            },
            Rect {
                min_x: mid_x,
                max_x: self.max_x,
                min_y: self.min_y,
                max_y: mid_y,
            },
            Rect {
                min_x: self.min_x,
                max_x: mid_x,
                min_y: mid_y,
                max_y: self.max_y,
            },
            Rect {
                min_x: mid_x,
                max_x: self.max_x,
                min_y: mid_y,
                max_y: self.max_y,
            },
        ]
    }
}
