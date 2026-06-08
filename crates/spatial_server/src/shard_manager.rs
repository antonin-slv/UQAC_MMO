use crate::broker_client::BrokerClient;
use crate::quadtree::{Entity, ShardId};
use broker_protocol::broker_message::NodeId;
use core_types::{Rect, Vec2};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Clone, Debug)]
pub struct Shard {
    pub dgs: Option<NodeId>,
    pub entities: HashSet<Entity>,
}

#[derive(Clone, Debug)]
pub struct ShardManager {
    pub entities: HashMap<NodeId, ShardId>,
    pub shards: HashMap<ShardId, Shard>,
    pub active_dgs: HashMap<NodeId, ShardId>,
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

    pub fn on_heartbeat_receive(&mut self, new_dgs_id: NodeId) {
        if self.active_dgs.contains_key(&new_dgs_id) {
            return;
        }

        if self.dgs_without_shards.contains(&new_dgs_id) {
            return;
        }

        self.on_new_dgs(new_dgs_id);
    }

    pub fn on_new_shard(
        &mut self,
        parent: ShardId,
        shard_id: ShardId,
        bounds: Rect,
        broker: &BrokerClient,
    ) {
        broker.spawn_new_dgs(1);

        println!("On new shard !");

        let mut old_dgs_id = Vec::new();
        if let Some(shard) = self.shards.get_mut(&parent) {
            if let Some(dgs) = shard.dgs {
                old_dgs_id.push(dgs.clone());
            }
        }

        println!("Old DGS ID: {:?}", old_dgs_id);

        if let Some(new_dgs) = self.dgs_without_shards.pop_front() {
            broker.assign_shard_to_dgs(new_dgs, vec![bounds], old_dgs_id.clone());
            if let Some(shard) = self.shards.get_mut(&shard_id) {
                shard.dgs = Some(new_dgs);
            }
        } else {
            println!("Add shard on buffer");
            self.shard_without_dgs.push_back(shard_id);
            self.shards.insert(
                shard_id,
                Shard {
                    entities: HashSet::new(),
                    dgs: None,
                },
            );
        }
    }

    pub fn on_shard_destroyed(
        &mut self,
        parent: ShardId,
        parent_bounds: Rect,
        shards: Vec<ShardId>,
        broker: &BrokerClient,
    ) {
        println!("Destroyed Shards : {:?}", shards);
        for shard_id in shards {
            if let Some(pos) = self
                .shard_without_dgs
                .iter()
                .position(|s_id| *s_id == shard_id)
            {
                self.shard_without_dgs.remove(pos);
                continue;
            }

            if let Some(shard) = self.shards.get(&shard_id)
                && let Some(dgs) = shard.dgs
            {
                let mut old_dgs_ids = Vec::new();
                if let Some(shard) = self.shards.get(&parent) {
                    if let Some(dgs) = shard.dgs {
                        old_dgs_ids.push(dgs.clone());
                    }
                }

                broker.remove_shard_to_dgs(dgs.clone(), vec![parent_bounds], old_dgs_ids);
            }
        }
    }

    pub fn on_new_dgs(&mut self, dgs_id: NodeId) {
        if let Some(shard_id) = self.shard_without_dgs.pop_front() {
            self.active_dgs.insert(dgs_id, shard_id);
            if let Some(shard) = self.shards.get_mut(&shard_id) {
                shard.dgs = Some(dgs_id);
            }
        } else {
            self.dgs_without_shards.push_back(dgs_id);
        }
    }

    pub fn on_dgs_stopped(&mut self, dgs_id: NodeId) {
        println!("DGS Stopped : {:?}", dgs_id);
        if let Some(pos) = self
            .dgs_without_shards
            .iter()
            .position(|dgs| dgs.clone() == dgs_id)
        {
            self.dgs_without_shards.remove(pos);
            return;
        }

        if let Some(shard_id) = self.active_dgs.get_mut(&dgs_id) {
            if let Some(shard) = self.shards.get_mut(shard_id) {
                shard.dgs = None;
                self.shard_without_dgs.push_back(*shard_id);
            }
        }
    }

    pub fn on_client_disconnected(&mut self, client_id: NodeId) {
        println!("Client disconnected : {:?}", client_id);
        let shard_id = self.entities.get(&client_id);
        if let Some(shard) = shard_id {
            println!("Remove client {} from shard {}", client_id, shard);
            self.remove_entity_from_shard(shard.clone(), client_id);
        } else {
            println!("No shard found for client : {}", client_id)
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
        if let Some(shard) = shard {
            shard.entities.insert(entity);
        } else {
            let mut entities = HashSet::new();
            entities.insert(entity);
            self.shards.insert(
                shard_id,
                Shard {
                    entities,
                    dgs: None,
                },
            );
        }
    }

    pub fn remove_entity_from_shard(&mut self, shard_id: ShardId, entity_id: NodeId) {
        if let Some(shard) = self.shards.get_mut(&shard_id) {
            shard
                .entities
                .remove(&Entity::new(entity_id, Vec2::new(0.0, 0.0)));
        }
    }

    pub fn drain_entities(&mut self, shard_id: ShardId) -> HashSet<Entity> {
        let shard = self.shards.get(&shard_id);

        let mut entities = HashSet::new();

        if let Some(shard) = shard {
            entities = shard.entities.clone();
            self.shards.remove(&shard_id);
        }

        entities
    }

    pub fn get_shard(&self, entity_id: NodeId) -> Option<u32> {
        self.entities.get(&entity_id).cloned()
    }

    pub fn get_dgs_for_client(&self, client_id: NodeId) -> Option<NodeId> {
        if let Some(shard_id) = self.entities.get(&client_id).cloned() {
            let shard = self.shards.get(&shard_id);

            if let Some(shard) = shard
                && let Some(dgs) = shard.dgs
            {
                return Some(dgs);
            } else {
                println!("Alors la c'est la barba merde on à pas de server pour le client")
            }
        }

        None
    }

    pub fn count_entity_in_shard(&self, shard_id: ShardId) -> usize {
        if let Some(shard) = self.shards.get(&shard_id) {
            return shard.entities.len();
        }

        0
    }

    pub fn get_entities(&self) -> Vec<Entity> {
        let mut entities = Vec::new();

        for shard in self.shards.values() {
            for entity in shard.entities.iter() {
                entities.push(entity.clone());
            }
        }

        entities
    }
}
