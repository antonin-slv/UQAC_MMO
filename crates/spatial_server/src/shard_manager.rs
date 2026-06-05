use crate::quadtree::{Entity, QuadTree, ShardId};
use std::collections::{HashMap, HashSet, VecDeque};
use broker_protocol::broker_message::NodeId;
use core_types::{Rect, Vec2};

#[derive(Clone)]
pub struct ShardManager {
    entities: HashMap<NodeId, ShardId>,
    shards: HashMap<ShardId, HashSet<Entity>>,
    active_dgs: HashMap<NodeId, HashSet<ShardId>>,
    shard_without_dgs: VecDeque<ShardId>,
}

impl ShardManager {
    pub fn new() -> ShardManager {
        Self {
            entities: HashMap::new(),
            shards: HashMap::new(),
            active_dgs: HashMap::new(),
            shard_without_dgs: VecDeque::new(),
        }
    }

    pub fn on_new_shard(&mut self, shards: Vec<ShardId>) {
        for i in 0..4 {
            self.shard_without_dgs.push_front(shards[i]);
        }
    }

    pub fn on_shard_destroyed(&mut self, shard_id: Vec<ShardId>) {
        println!("on_shard_destroyed: {:?}", shard_id);
    }

    pub fn on_new_dgs(&mut self, dgs_id: NodeId) {
        let mut shards = HashSet::new();
        if let Some(shard_id) = self.shard_without_dgs.pop_front() {
            shards.insert(shard_id);
        }
        self.active_dgs.insert(dgs_id, shards);
    }

    pub fn set_entity_shard(&mut self, shard_id: ShardId, entity: Entity) {
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

    pub fn remove_entity_from_shard(&mut self, shard_id: ShardId, entity_id: NodeId) {
        if let Some(shard) = self.shards.get_mut(&shard_id) {
            shard.remove(&Entity::new(
                entity_id,
                Vec2::new(0.0, 0.0),
            ));
        }
    }

    pub fn drain_entities(&mut self, shard_id: ShardId) -> HashSet<Entity> {
        let shard = self.shards.get(&shard_id);

        let mut entities = HashSet::new();

        if let Some(shard) = shard {
            entities = shard.clone();
            self.shards.remove(&shard_id);
        }

        entities
    }

    pub fn get_shard(&self, entity_id: NodeId) -> Option<u32> {
        self.entities.get(&entity_id).cloned()
    }

    pub fn get_shard_bounds_for_client(
        &self,
        client_id: NodeId,
        quad_tree: &QuadTree,
    ) -> Option<(NodeId, Rect)> {
        if let Some(shard_id) = self.entities.get(&client_id).cloned() {
            let shard_bounds = quad_tree.get_shard_bounds(&shard_id).unwrap();
            let (dgs, _) = self
                .active_dgs
                .iter()
                .find(|(_, shards)| shards.contains(&shard_id))
                .unwrap();
            return Some((dgs.clone(), shard_bounds));
        }

        None
    }

    pub fn count_entity_in_shard(&self, shard_id: ShardId) -> usize {
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
