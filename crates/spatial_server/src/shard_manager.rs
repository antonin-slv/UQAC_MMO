use crate::broker_client::BrokerClient;
use crate::quadtree::{Entity, QuadTree, ShardId};
use broker_protocol::broker_message::NodeId;
use core_types::{Rect, Vec2};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Clone)]
pub struct ShardManager {
    pub entities: HashMap<NodeId, ShardId>,
    pub shards: HashMap<ShardId, (HashSet<Entity>, Option<NodeId>)>,
    pub active_dgs: HashMap<NodeId, HashSet<ShardId>>,
    pub shard_without_dgs: VecDeque<ShardId>,
    pub dgs_without_shards: VecDeque<NodeId>,
}

impl ShardManager {
    pub fn new() -> ShardManager {
        Self {
            entities: HashMap::new(),
            shards: HashMap::new(),
            active_dgs: HashMap::new(),
            shard_without_dgs: VecDeque::new(),
            dgs_without_shards: VecDeque::new(),
        }
    }

    pub fn on_new_shard(
        &mut self,
        parent: Option<ShardId>,
        shards: Vec<(ShardId, Rect)>,
        broker: &BrokerClient,
    ) {
        println!("New Shards : {:?}", shards);
        let mut old_dgs_id = Vec::new();
        if let Some(parent) = parent {
            if let Some((_, dgs)) = self.shards.get(&parent) {
                if let Some(dgs) = dgs {
                    old_dgs_id.push(dgs.clone());
                }
            }
        }

        for (shard_id, bounds) in shards {
            if let Some(new_dgs) = self.dgs_without_shards.pop_front() {
                broker.assign_shard_to_dgs(new_dgs, vec![bounds], old_dgs_id.clone());
                let mut shard_entities: HashSet<Entity> = HashSet::new();
                if let Some((entities, _)) = self.shards.get(&shard_id) {
                    shard_entities = entities.clone();
                }
                self.shards
                    .insert(shard_id, (shard_entities, Some(new_dgs)));
            } else {
                self.shard_without_dgs.push_back(shard_id);
            }
        }
    }

    pub fn on_shard_destroyed(
        &mut self,
        parent: (ShardId, Rect),
        shards: Vec<(ShardId, Rect)>,
        broker: &BrokerClient,
    ) {
        println!("Destroyed Shards : {:?}", shards);
        for (shard_id, bounds) in shards {
            if let Some(pos) = self
                .shard_without_dgs
                .iter()
                .position(|s_id| *s_id == shard_id)
            {
                self.shard_without_dgs.remove(pos);
                continue;
            }

            if let Some((dgs, _)) = self
                .active_dgs
                .iter_mut()
                .find(|(_, shards)| shards.contains(&shard_id))
            {
                let mut old_dgs_ids = Vec::new();
                let shard = self.shards.get(&parent.0);
                if let Some((_, dgs)) = shard {
                    if let Some(dgs) = dgs {
                        old_dgs_ids.push(dgs.clone());
                    }
                }

                broker.remove_shard_to_dgs(dgs.clone(), vec![bounds], old_dgs_ids);
            }
        }

        self.on_new_shard(None, vec![parent], broker);
    }

    pub fn on_new_dgs(&mut self, dgs_id: NodeId) {
        if let Some(shard_id) = self.shard_without_dgs.pop_front() {
            let mut shards = HashSet::new();
            shards.insert(shard_id);
            self.active_dgs.insert(dgs_id, shards);
        } else {
            self.dgs_without_shards.push_back(dgs_id);
        }
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
        if let Some((shard, _)) = shard {
            shard.insert(entity);
        } else {
            let mut shard = HashSet::new();
            shard.insert(entity);
            self.shards.insert(shard_id, (shard, None));
        }
    }

    pub fn remove_entity_from_shard(&mut self, shard_id: ShardId, entity_id: NodeId) {
        if let Some((shard, _)) = self.shards.get_mut(&shard_id) {
            shard.remove(&Entity::new(entity_id, Vec2::new(0.0, 0.0)));
        }
    }

    pub fn drain_entities(&mut self, shard_id: ShardId) -> HashSet<Entity> {
        let shard = self.shards.get(&shard_id);

        let mut entities = HashSet::new();

        if let Some((shard, _)) = shard {
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
        let shard = self.shards.get(&shard_id);
        if let Some((entities, _)) = shard {
            return entities.len();
        }

        0
    }

    pub fn get_entities(&self) -> Vec<Entity> {
        let mut entities = Vec::new();

        for (shard, _) in self.shards.values() {
            for entity in shard {
                entities.push(entity.clone());
            }
        }

        entities
    }
}
