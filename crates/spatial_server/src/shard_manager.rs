use crate::quadtree::Entity;
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub struct ShardManager {
    entities: HashMap<u32, u32>,
    shards: HashMap<u32, HashSet<Entity>>,
}

impl ShardManager {
    pub fn new() -> ShardManager {
        Self {
            entities: HashMap::new(),
            shards: HashMap::new(),
        }
    }

    pub fn on_new_shard(&mut self, shard_ids: Vec<u32>) {
        println!("on_new_shard: {:?}", shard_ids);
    }

    pub fn on_shard_destroyed(&mut self, shard_id: Vec<u32>) {
        println!("on_shard_destroyed: {:?}", shard_id);
    }

    pub fn set_entity_shard(&mut self, shard_id: u32, entity: Entity) {
        let old_shard_id = self.get_shard(entity.id);
        if let Some(old_shard_id) = old_shard_id
            && old_shard_id != shard_id
        {
            self.remove_entity_from_shard(old_shard_id, entity.id);
        }
        self.entities.insert(entity.id, shard_id);
        let shard = self.shards.get_mut(&shard_id);
        if let Some(shard) = shard {
            shard.insert(entity);
        } else {
            let mut shard = HashSet::new();
            shard.insert(entity);
            self.shards.insert(shard_id, shard);
        }
    }

    pub fn remove_entity_from_shard(&mut self, shard_id: u32, entity_id: u32) {
        if let Some(shard) = self.shards.get_mut(&shard_id) {
            let entity = shard.iter().find(|e| e.id == entity_id).cloned();
            if let Some(entity) = entity {
                shard.remove(&entity);
            }
        }
    }

    pub fn drain_entities(&mut self, shard_id: u32) -> HashSet<Entity> {
        let shard = self.shards.get(&shard_id);

        let mut entities = HashSet::new();

        if let Some(shard) = shard {
            entities = shard.clone();
            self.shards.remove(&shard_id);
        }

        entities
    }

    pub fn get_shard(&self, entity_id: u32) -> Option<u32> {
        self.entities.get(&entity_id).cloned()
    }

    pub fn count_entity_in_shard(&self, shard_id: u32) -> usize {
        self.shards.get(&shard_id).map_or(0, HashSet::len)
    }

    pub fn get_entities(&self) -> Vec<Entity> {
        let mut entities = Vec::new();

        for shard in self.shards.values() {
            for entity in shard {
                entities.push(entity.clone());
            }
        }

        entities
    }
}
