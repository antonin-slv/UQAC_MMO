use crate::broker_client::BrokerClient;
use crate::quadtree::{Entity, QuadTree, ShardId};
use broker_protocol::broker_message::NodeId;
use core_types::{Rect, Vec2};
use game_message::msg_entities::NetworkEntityId;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct Shard {
    pub dgs: Option<NodeId>,
    pub entities: HashSet<Entity>,
}

#[derive(Clone, Debug)]
pub struct EntityMapping {
    shard_id: ShardId,
    client_id: NodeId,
}

#[derive(Clone, Debug)]
pub struct ShardManager {
    pub entities: HashMap<NetworkEntityId, EntityMapping>,
    pub shards: HashMap<ShardId, Shard>,
    pub active_dgs: HashMap<NodeId, Option<ShardId>>,
}

impl ShardManager {
    pub fn new() -> ShardManager {
        Self {
            entities: HashMap::new(),
            shards: HashMap::new(),
            active_dgs: HashMap::new(),
        }
    }

    pub fn on_heartbeat_receive(
        &mut self,
        new_dgs_id: NodeId,
        quad_tree: &QuadTree,
        broker: &BrokerClient,
    ) {
        if let Some(shard) = self.active_dgs.get(&new_dgs_id)
            && shard.is_some()
        {
            return;
        }

        self.on_new_dgs(new_dgs_id, quad_tree, broker);
    }

    pub fn on_new_shard(
        &mut self,
        parent: Option<ShardId>,
        shard_id: ShardId,
        bounds: Rect,
        broker: &BrokerClient,
    ) {
        //broker.spawn_new_dgs(1);

        println!("On new shard ! : {:?}", shard_id);

        let mut old_dgs_id = None;
        if let Some(parent) = parent
            && let Some(shard) = self.shards.get_mut(&parent)
        {
            old_dgs_id = shard.dgs;
        }

        println!("Old DGS ID: {:?}", old_dgs_id);

        if let Some((new_dgs, _)) = self
            .active_dgs
            .iter_mut()
            .find(|(_, shard)| shard.is_none())
        {
            broker.assign_shard_to_dgs(new_dgs.clone(), vec![(bounds, old_dgs_id)]);
            if let Some(shard) = self.shards.get_mut(&shard_id) {
                shard.dgs = Some(new_dgs.clone());
            }
        }

        if let None = self.shards.get(&shard_id) {
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
        for shard_id in shards {
            if let Some(shard) = self.shards.get(&shard_id) {
                if let Some(dgs) = shard.dgs {
                    let mut old_dgs_id = None;
                    if let Some(shard) = self.shards.get(&parent) {
                        old_dgs_id = shard.dgs;
                    }

                    self.active_dgs.insert(dgs.clone(), None);

                    broker.remove_shard_to_dgs(dgs.clone(), vec![(parent_bounds, old_dgs_id)]);
                }
            }

            self.shards.remove(&shard_id);
        }
    }

    pub fn on_new_dgs(&mut self, dgs_id: NodeId, quad_tree: &QuadTree, broker: &BrokerClient) {
        println!("On New DGS ! : {:?}", dgs_id);
        let mut shard_available = None;

        if let Some((shard_id, shard)) = self
            .shards
            .iter_mut()
            .find(|(_, shard)| shard.dgs.is_none())
        {
            shard_available = Some(shard_id.clone());
            shard.dgs = Some(dgs_id);

            println!("New DGS assigned: {:?}", shard.dgs);
            if let Some((bounds, _)) = quad_tree.get_shard_bounds(shard_id) {
                broker.assign_shard_to_dgs(dgs_id, vec![(bounds, None)]);
            } else {
                println!("No bounds found for shard {:?}", shard_id);
            }
        }

        self.active_dgs.insert(dgs_id, shard_available);
    }

    pub fn on_dgs_stopped(&mut self, dgs_id: NodeId) {
        println!("DGS Stopped : {:?}", dgs_id);

        if let Some(Some(shard_id)) = self.active_dgs.get_mut(&dgs_id) {
            if let Some(shard) = self.shards.get_mut(shard_id) {
                shard.dgs = None;
            }
        }
    }

    pub fn on_client_disconnected(&mut self, client_id: NodeId) {
        println!("Client disconnected : {:?}", client_id);
        let shard_id = self
            .entities
            .iter()
            .find(|(_, entity_mapping)| client_id == entity_mapping.client_id.clone());
        if let Some((entity_id, entity_mapping)) = shard_id {
            println!(
                "Remove client {} from shard {}",
                client_id, entity_mapping.shard_id
            );
            self.remove_entity_from_shard(entity_mapping.shard_id.clone(), entity_id.clone());
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
        if let Some(entity_mapping) = self.entities.get_mut(&entity.id) {
            entity_mapping.shard_id = shard_id;
        }
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

    pub fn remove_entity_from_shard(&mut self, shard_id: ShardId, entity_id: NetworkEntityId) {
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

    pub fn get_shard(&self, entity_id: NetworkEntityId) -> Option<ShardId> {
        if let Some(entity) = self.entities.get(&entity_id).cloned() {
            return Some(entity.shard_id);
        }
        None
    }

    pub fn get_dgs_for_position(&self, position: Vec2, quad_tree: &QuadTree) -> Option<NodeId> {
        if let Some(shard_id) = quad_tree.get_shard_of_point(position) {
            if let Some(shard) = self.shards.get(&shard_id)
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
