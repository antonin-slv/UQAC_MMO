use crate::Rect;

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash, Copy)]
pub struct GameChunk {
    pub x: i16,
    pub y: i16,
}

impl GameChunk {
    pub fn to_core_rect(&self, size: f32) -> Rect {
        let min_x = self.x as f32 * size;
        let min_y = self.y as f32 * size;
        Rect {
            min_x,
            max_x: min_x + size,
            min_y,
            max_y: min_y + size,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Copy)]
pub struct GameChunkAera {
    pub x_min: i16,
    pub x_max: i16,
    pub y_min: i16,
    pub y_max: i16,
}

impl From<GameChunk> for GameChunkAera {
    fn from(chunk: GameChunk) -> Self {
        GameChunkAera {
            x_min: chunk.x,
            x_max: chunk.x,
            y_min: chunk.y,
            y_max: chunk.y,
        }
    }
}
impl GameChunkAera {
    pub fn contains(&self, chunk: GameChunk) -> bool {
        chunk.x >= self.x_min
            && chunk.x <= self.x_max
            && chunk.y >= self.y_min
            && chunk.y <= self.y_max
    }
    pub fn iter(&self) -> GameChunkAeraIterator<'_> {
        GameChunkAeraIterator {
            area: self,
            c_x: self.x_min - 1,
            c_y: self.y_min - 1,
        }
    }

    pub fn to_core_rect(&self, size: f32) -> Rect {
        Rect {
            min_x: self.x_min as f32 * size,
            max_x: self.x_max as f32 * size + size,
            min_y: self.y_min as f32 * size,
            max_y: self.y_max as f32 * size + size,
        }
    }

    pub fn get_borders(&self) -> Vec<GameChunk> {
        let mut border_chunks = Vec::with_capacity(
            (4 + 2 * ((self.x_max - self.x_min + 1) + (self.y_max - self.y_min + 1))) as usize,
        );
        let aera_ghost_zone = GameChunkAera {
            x_min: self.x_min - 1,
            x_max: self.x_max + 1,
            y_min: self.y_min - 1,
            y_max: self.y_min - 1,
        };
        for chunks in aera_ghost_zone.iter() {
            border_chunks.push(chunks);
        }
        let aera_ghost_zone = GameChunkAera {
            x_min: self.x_min - 1,
            x_max: self.x_max + 1,
            y_min: self.y_max + 1,
            y_max: self.y_max + 1,
        };
        for chunks in aera_ghost_zone.iter() {
            border_chunks.push(chunks);
        }

        let aera_ghost_zone = GameChunkAera {
            x_min: self.x_min - 1,
            x_max: self.x_min - 1,
            y_min: self.y_min,
            y_max: self.y_max,
        };
        for chunks in aera_ghost_zone.iter() {
            border_chunks.push(chunks);
        }
        let aera_ghost_zone = GameChunkAera {
            x_min: self.x_max + 1,
            x_max: self.x_max + 1,
            y_min: self.y_min,
            y_max: self.y_max,
        };
        for chunks in aera_ghost_zone.iter() {
            border_chunks.push(chunks);
        }

        border_chunks
    }
}

pub fn get_chunk_size(world_size: f32, max_division: u8) -> f32 {
    let num_division = 2 << max_division;
    world_size / (num_division as f32)
}

pub struct GameChunkAeraIterator<'a> {
    area: &'a GameChunkAera,
    c_x: i16,
    c_y: i16,
}

impl<'a> Iterator for GameChunkAeraIterator<'a> {
    type Item = GameChunk;
    fn next(&mut self) -> Option<Self::Item> {
        if self.c_x < self.area.x_max {
            self.c_x += 1;
        } else {
            self.c_x = self.area.x_min;
            self.c_y += 1;
        }

        if self.c_y > self.area.y_max {
            None
        } else {
            Some(GameChunk {
                x: self.c_x,
                y: self.c_y,
            })
        }
    }
}
