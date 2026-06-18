use crate::broker_client::BrokerClient;
use crate::shard_manager::ShardManager;
use core_types::{Rect, Vec2};
use game_message::msg_entities::NetworkEntityId;
use std::collections::HashMap;

use std::env;
use std::hash::{Hash, Hasher};

pub type ShardId = u32;

pub trait ShardIdExt {
    fn parent(self) -> Option<ShardId>;

    fn child_index(self) -> Option<usize>;

    fn child(self, index: u32) -> ShardId;

    fn child_index_towards(self, target: ShardId) -> Option<usize>;
}

impl ShardIdExt for ShardId {
    fn parent(self) -> Option<ShardId> {
        if self == 0 {
            None
        } else {
            Some((self - 1) / 4)
        }
    }

    fn child_index(self) -> Option<usize> {
        if self == 0 {
            None
        } else {
            Some(((self - 1) % 4) as usize)
        }
    }

    fn child(self, index: u32) -> ShardId {
        debug_assert!(index < 4, "Un quadtree n'a que 4 enfants (0-3)");
        self * 4 + 1 + index
    }

    fn child_index_towards(self, target: ShardId) -> Option<usize> {
        let mut current = target;

        while current > self {
            let parent_id = (current - 1) / 4;
            if parent_id == self {
                return Some(((current - 1) % 4) as usize);
            }
            current = parent_id;
        }

        None
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Entity {
    pub id: NetworkEntityId,
    pub pos: Vec2,
}

impl Entity {
    pub fn new(id: NetworkEntityId, pos: Vec2) -> Self {
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

        let result = quadtree.subdivide(shard_manager);
        quadtree.manage_subdivision(&result, shard_manager, broker);

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
    ) {
        let (_, subdivided_shards) = self.insert_private(entity, shard_manager);
        self.manage_subdivision(&subdivided_shards, shard_manager, broker);
    }

    pub fn manage_subdivision(
        &self,
        subdivided_shards: &HashMap<ShardId, ShardId>,
        shard_manager: &mut ShardManager,
        broker: &BrokerClient,
    ) {
        for (new_shard, parent) in subdivided_shards {
            let sub_shard = self.get_shard_bounds(&new_shard);
            if let Some((sub_shard_bounds, is_leaf)) = sub_shard {
                if is_leaf {
                    shard_manager.on_new_shard(
                        Some(parent.clone()),
                        new_shard.clone(),
                        sub_shard_bounds,
                        broker,
                    );
                }
            }
        }
    }

    fn insert_private(
        &mut self,
        entity: Entity,
        shard_manager: &mut ShardManager,
    ) -> (bool, HashMap<ShardId, ShardId>) {
        let mut subdivided_shards = HashMap::new();
        if !self.bounds.contains(entity.pos) {
            return (false, subdivided_shards);
        }

        if let Some(children) = &mut self.children {
            for child in children.iter_mut() {
                let (result, sub) = child.insert_private(entity, shard_manager);
                if result {
                    subdivided_shards.extend(sub);
                    return (true, subdivided_shards);
                }
            }
            return (false, subdivided_shards);
        }

        shard_manager.set_entity_shard(self.shard_id, entity);

        if shard_manager.count_entity_in_shard(self.shard_id) > self.subdivide_threshold
            && self.depth < self.max_depth
        {
            let result = self.subdivide(shard_manager);
            subdivided_shards.extend(result);
        }

        (true, subdivided_shards)
    }

    fn subdivide(&mut self, shard_manager: &mut ShardManager) -> HashMap<ShardId, ShardId> {
        let mut subdivided_shards = HashMap::new();
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

        for child in children.iter() {
            subdivided_shards.insert(child.shard_id, self.shard_id);
        }

        for entity in shard_manager.drain_entities(self.shard_id) {
            for child in children.iter_mut() {
                let (result, sub) = child.insert_private(entity, shard_manager);
                if result {
                    subdivided_shards.extend(sub);
                    break;
                }
            }
        }

        self.children = Some(children);

        subdivided_shards
    }

    pub fn try_merge(
        &mut self,
        shard_manager: &mut ShardManager,
    ) -> HashMap<ShardId, Vec<(ShardId, Rect)>> {
        let mut merges = HashMap::new();
        let entity_count = self.count_entities(shard_manager);

        if self.children.is_some() {
            if entity_count < self.merge_threshold && self.depth > 0 {
                let destroyed = self.merge(shard_manager);
                if !destroyed.is_empty() {
                    merges.insert(self.shard_id, destroyed);
                }
            } else {
                if let Some(children) = self.children.as_mut() {
                    for child in children.iter_mut() {
                        let child_merges = child.try_merge(shard_manager);
                        merges.extend(child_merges);
                    }
                }
            }
        }
        merges
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

    fn merge(&mut self, shard_manager: &mut ShardManager) -> Vec<(ShardId, Rect)> {
        let mut destroyed_shards: Vec<(ShardId, Rect)> = Vec::new();

        self.collect_and_destroy_descendants(shard_manager, self.shard_id, &mut destroyed_shards);

        destroyed_shards
    }

    fn collect_and_destroy_descendants(
        &mut self,
        shard_manager: &mut ShardManager,
        target_shard_id: ShardId,
        destroyed_shards: &mut Vec<(ShardId, Rect)>,
    ) {
        if let Some(mut children) = self.children.take() {
            for child in children.iter_mut() {
                child.collect_and_destroy_descendants(
                    shard_manager,
                    target_shard_id,
                    destroyed_shards,
                );

                for entity in shard_manager.drain_entities(child.shard_id).iter() {
                    shard_manager.set_entity_shard(target_shard_id, entity.clone());
                }
                destroyed_shards.push((child.shard_id, child.bounds));
            }
        }
    }

    fn generate_sub_shards(&mut self) -> Vec<(ShardId, Rect)> {
        let sub_bounds = self.bounds.split();
        let mut shard_ids = Vec::new();
        for i in 0..4 {
            let child_id = self.shard_id.child(i);
            shard_ids.push((child_id, sub_bounds[i as usize]));
        }
        shard_ids
    }

    pub fn get_shard_bounds(&self, shard_id: &ShardId) -> Option<(Rect, bool)> {
        if *shard_id == self.shard_id {
            return Some((self.bounds.clone(), self.children.is_none()));
        }

        if let Some(children) = &self.children {
            if let Some(child_idx) = self.shard_id.child_index_towards(*shard_id) {
                return children[child_idx].get_shard_bounds(shard_id);
            }
        }

        None
    }

    pub fn get_shard_of_point(&self, point: Vec2) -> Option<ShardId> {
        if let Some(children) = &self.children {
            for child in children.iter() {
                if child.bounds.contains(point) {
                    return child.get_shard_of_point(point);
                }
            }
            return None;
        }

        Some(self.shard_id)
    }
}
