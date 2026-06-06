
#[derive(Debug, Clone, Default)]
pub struct GameChunk {
    pub x: i16,
    pub y: i16,
}

#[derive(Clone, Debug)]
pub struct GameChunkAera {
    pub x_min: i16,
    pub x_max: i16,
    pub y_min: i16,
    pub y_max: i16,
}

pub fn get_chunk_size(world_size: f32, max_division: u8) -> f32 {
    let num_division = 2 << max_division;
    world_size / (num_division as f32)
}
