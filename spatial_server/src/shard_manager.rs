use std::collections::HashMap;

pub struct ShardManager {
    entities: HashMap<u32, u32>,
}

impl ShardManager {
    pub fn new() -> ShardManager {
        Self {
            entities: HashMap::new(),
        }
    }

    pub fn on_new_shard(&mut self, shard_id: u32) {
        println!("on_new_shard: {}", shard_id);
    }

    pub fn on_shard_destroyed(&mut self, shard_id: u32) {
        println!("on_shard_destroyed: {}", shard_id);
    }

    pub fn on_entity_move(&mut self, shard_id: u32, entity_id: u32) {
        self.entities.insert(entity_id, shard_id);
    }

    pub fn get_shard(&self, entity_id: u32) -> Option<u32> {
        self.entities.get(&entity_id).cloned()
    }

    pub fn print(&self) {
        for (entity_id, shard_id) in self.entities.iter() {
            println!("Entity {}: Shard : {:?}", entity_id, shard_id);
        }
    }
}
