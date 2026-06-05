use crate::broker_client::BrokerClient;
use crate::shard_manager::ShardManager;
use broker_protocol::broker_message::NodeId;
use core_types::{Rect, Vec2};
use std::env;
use std::hash::{Hash, Hasher};

pub type ShardId = u32;

#[derive(Clone, Copy, Debug)]
pub struct Entity {
    pub id: NodeId,
    pub pos: Vec2,
}

impl Entity {
    pub fn new(id: NodeId, pos: Vec2) -> Self {
        Self { id, pos }
    }
}
impl PartialEq for Entity {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Entity {}

impl Hash for Entity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct QuadTree {
    pub bounds: Rect,
    pub depth: u8,
    pub max_depth: u8,
    pub subdivide_threshold: usize,
    pub merge_threshold: usize,
    pub children: Option<Box<[QuadTree; 4]>>,
    pub shard_id: ShardId,
}

impl QuadTree {
    pub fn new(shard_manager: &mut ShardManager, broker: &BrokerClient) -> Self {
        let mut world_size: f32 = env::var("WORLD_SIZE")
            .expect("Env WORLD_SIZE is not set")
            .parse()
            .expect("Env WORLD_SIZE is not a number");
        world_size /= 2.0;
        let map_size = Rect {
            min_x: -world_size,
            max_x: world_size,
            min_y: -world_size,
            max_y: world_size,
        };

        let max_depth = env::var("QUADTREE_MAX_DEPTH").unwrap().parse().unwrap();
        let subdivide_threshold = env::var("SUBDIVIDE_THRESHOLD").unwrap().parse().unwrap();
        let merge_threshold = env::var("MERGE_THRESHOLD").unwrap().parse().unwrap();

        let mut quadtree = Self::new_internal(
            map_size,
            0,
            max_depth,
            subdivide_threshold,
            merge_threshold,
            0,
        );

        quadtree.subdivide(shard_manager, broker);

        quadtree
    }

    fn new_internal(
        bounds: Rect,
        depth: u8,
        max_depth: u8,
        subdivide_threshold: usize,
        merge_threshold: usize,
        shard_id: ShardId,
    ) -> Self {
        Self {
            bounds,
            depth,
            max_depth,
            subdivide_threshold,
            merge_threshold,
            children: None,
            shard_id,
        }
    }

    pub fn insert(
        &mut self,
        entity: Entity,
        shard_manager: &mut ShardManager,
        broker: &BrokerClient,
    ) -> bool {
        if !self.bounds.contains(entity.pos) {
            return false;
        }

        if let Some(children) = &mut self.children {
            for child in children.iter_mut() {
                if child.insert(entity, shard_manager, broker) {
                    return true;
                }
            }
            return false;
        }

        shard_manager.set_entity_shard(self.shard_id, entity);

        if shard_manager.count_entity_in_shard(self.shard_id) > self.subdivide_threshold
            && self.depth < self.max_depth
        {
            self.subdivide(shard_manager, broker);
        }

        true
    }

    fn subdivide(&mut self, shard_manager: &mut ShardManager, broker: &BrokerClient) {
        let sub_shards = self.generate_sub_shards();

        let mut children = Box::new([
            QuadTree::new_internal(
                sub_shards[0].1,
                self.depth + 1,
                self.max_depth,
                self.subdivide_threshold,
                self.merge_threshold,
                sub_shards[0].0,
            ),
            QuadTree::new_internal(
                sub_shards[1].1,
                self.depth + 1,
                self.max_depth,
                self.subdivide_threshold,
                self.merge_threshold,
                sub_shards[1].0,
            ),
            QuadTree::new_internal(
                sub_shards[2].1,
                self.depth + 1,
                self.max_depth,
                self.subdivide_threshold,
                self.merge_threshold,
                sub_shards[2].0,
            ),
            QuadTree::new_internal(
                sub_shards[3].1,
                self.depth + 1,
                self.max_depth,
                self.subdivide_threshold,
                self.merge_threshold,
                sub_shards[3].0,
            ),
        ]);

        for entity in shard_manager.drain_entities(self.shard_id) {
            for child in children.iter_mut() {
                if child.insert(entity, shard_manager, broker) {
                    break;
                }
            }
        }

        shard_manager.on_new_shard(Some(self.shard_id), sub_shards, broker);
        self.children = Some(children);
    }

    pub fn try_merge(&mut self, shard_manager: &mut ShardManager, broker: &BrokerClient) {
        let entity_count = self.count_entities(shard_manager);
        if let Some(children) = self.children.as_mut() {
            if entity_count < self.merge_threshold && self.depth > 0 {
                self.merge(shard_manager, broker);
            } else {
                for child in children.iter_mut() {
                    child.try_merge(shard_manager, broker);
                }
            }
        }
    }

    fn count_entities(&self, shard_manager: &mut ShardManager) -> usize {
        let mut count = shard_manager.count_entity_in_shard(self.shard_id);
        if let Some(children) = &self.children {
            for child in children.iter() {
                count += child.count_entities(shard_manager);
            }
        }
        count
    }

    fn merge(&mut self, shard_manager: &mut ShardManager, broker: &BrokerClient) {
        if let Some(children) = self.children.as_mut() {
            let mut destroyed_shards: Vec<(ShardId, Rect)> = Vec::new();
            for child in children.iter_mut() {
                for entity in shard_manager.drain_entities(child.shard_id).iter() {
                    shard_manager.set_entity_shard(self.shard_id, entity.clone());
                }
                destroyed_shards.push((child.shard_id, child.bounds));
            }
            shard_manager.on_shard_destroyed(
                (self.shard_id, self.bounds),
                destroyed_shards,
                broker,
            );
            self.children = None;
        }
    }

    fn generate_sub_shards(&mut self) -> Vec<(ShardId, Rect)> {
        let sub_bounds = self.bounds.split();
        let mut shard_ids = Vec::new();
        let offset = self.depth * 2;
        for i in 0..4 {
            let mut child_id = i;
            child_id = child_id << offset;
            child_id |= self.shard_id;
            shard_ids.push((child_id, sub_bounds[i as usize]));
        }
        shard_ids
    }

    pub fn get_shard_bounds(&self, shard_id: &ShardId) -> Option<Rect> {
        if *shard_id == self.shard_id {
            return Some(self.bounds.clone());
        }

        if let Some(children) = &self.children {
            let child_id = ((shard_id >> self.depth * 2) & 3) as usize;
            return children[child_id].get_shard_bounds(shard_id);
        }
        None
    }
}
