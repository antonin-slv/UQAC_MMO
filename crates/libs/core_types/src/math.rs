use crate::chunks::{GameChunk, GameChunkAera};
use bitcode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::ops::MulAssign;

pub mod chunks;
pub mod helpers;

#[derive(Clone, Copy, Debug, PartialEq, Encode, Decode)]
pub struct Vec2 {
    pub x: f32,
    pub y: f32,
}

impl MulAssign<f32> for Vec2 {
    fn mul_assign(&mut self, rhs: f32) {
        self.x *= rhs;
        self.y *= rhs;
    }
}
impl Vec2 {
    pub fn new(x: f32, y: f32) -> Self {
        Self { x, y }
    }

    pub fn get_chunk(&self, chunk_size: f32) -> GameChunk {
        get_chunk(self.x, self.y, chunk_size)
    }
}

pub fn get_chunk(x: f32, y: f32, chunk_size: f32) -> GameChunk {
    GameChunk {
        x: (x / chunk_size).floor() as i16,
        y: (y / chunk_size).floor() as i16,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Rect {
    pub min_x: f32,
    pub min_y: f32,
    pub max_x: f32,
    pub max_y: f32,
}

pub type OwnedArea = (u32, Vec<Rect>);

impl Rect {
    pub fn y_is_in(&self, y: f32) -> bool {
        y >= self.min_y && y < self.max_y
    }
    pub fn x_is_in(&self, x: f32) -> bool {
        x >= self.min_x && x < self.max_x
    }
    pub fn contains(&self, p: Vec2) -> bool {
        self.y_is_in(p.y) && self.x_is_in(p.x)
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

    pub fn get_bounding_chunks(&self, chunk_size: f32) -> Vec<GameChunk> {
        self.bounding_chunk_aera(chunk_size)
            .iter()
            .collect::<Vec<_>>()
    }

    pub fn bounding_chunk_aera(&self, chunk_size: f32) -> GameChunkAera {
        let chunk_min = get_chunk(self.min_x, self.min_y, chunk_size);
        let mut chunk_max = get_chunk(self.max_x, self.max_y, chunk_size);

        let frac = (self.max_y / chunk_size).fract();
        if frac <= f32::EPSILON {
            chunk_max.y -= 1;
        }
        let frac = (self.max_x / chunk_size).fract();
        if frac <= f32::EPSILON {
            chunk_max.x -= 1;
        }

        GameChunkAera {
            x_min: chunk_min.x,
            x_max: chunk_max.x,
            y_min: chunk_min.y,
            y_max: chunk_max.y,
        }
    }
}
