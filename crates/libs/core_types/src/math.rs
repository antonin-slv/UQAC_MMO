use crate::chunks::GameChunk;

pub mod chunks;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Vec2 {
    pub x: f32,
    pub y: f32,
}


impl Vec2 {
    pub fn new(x: f32, y: f32) -> Self {
        Self { x, y }
    }

    pub fn get_chunk(&self, chunk_size: f32) -> GameChunk {
        GameChunk {
            x: (self.x / chunk_size).floor() as i16,
            y: (self.y / chunk_size).floor() as i16,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rect {
    pub min_x: f32,
    pub min_y: f32,
    pub max_x: f32,
    pub max_y: f32,
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

    pub fn get_chunks(&self, chunk_size: f32) -> Vec<GameChunk> {
        let start_x = (self.min_x / chunk_size).floor() as i16;
        let end_x = (self.max_x / chunk_size).floor() as i16;

        let start_y = (self.min_y / chunk_size).floor() as i16;
        let end_y = (self.max_y / chunk_size).floor() as i16;

        let total_chunks = ((end_x - start_x + 1) * (end_y - start_y + 1)) as usize;
        let mut chunks = Vec::with_capacity(total_chunks);

        for y in start_y..=end_y {
            for x in start_x..=end_x {
                chunks.push(GameChunk { x, y });
            }
        }
        chunks
    }
}

