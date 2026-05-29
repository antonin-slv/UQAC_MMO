use crate::shard_manager::ShardManager;
pub(crate) use shared_replication::math::{Rect, Vec2};
use std::env;

#[derive(Clone, Copy, Debug)]
pub struct Entity {
    pub id: u32,
    pub pos: Vec2,
}

impl Entity {
    pub fn new(id: u32, pos: Vec2) -> Self {
        Self { id, pos }
    }
}

pub struct QuadTree {
    pub bounds: Rect,
    pub depth: u8,
    pub max_depth: u8,
    pub capacity: usize,
    pub entities: Vec<Entity>,
    pub children: Option<Box<[QuadTree; 4]>>,
    pub shard_id: u32,
}

impl QuadTree {
    pub fn new(shard_manager: &mut ShardManager) -> Self {
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

        let max_depth = env::var("QUADTREE_MAX_DEPTH")
            .expect("Env QUADTREE_MAX_DEPTH is not set")
            .parse()
            .expect("Env QUADTREE_MAX_DEPTH is not a number");

        let quadtree_capacity = env::var("QUADTREE_CAPACITY")
            .expect("Env QUADTREE_CAPACITY is not set")
            .parse()
            .expect("Env QUADTREE_CAPACITY is not a number");

        let mut quadtree = Self::new_internal(map_size, 0, max_depth, quadtree_capacity, 0);

        quadtree.subdivide(shard_manager);

        quadtree
    }

    fn new_internal(
        bounds: Rect,
        depth: u8,
        max_depth: u8,
        capacity: usize,
        shard_id: u32,
    ) -> Self {
        Self {
            bounds,
            depth,
            max_depth,
            capacity,
            entities: Vec::with_capacity(capacity),
            children: None,
            shard_id,
        }
    }

    fn insert(&mut self, entity: Entity, shard_manager: &mut ShardManager) -> bool {
        if !self.bounds.contains(entity.pos) {
            return false;
        }

        if let Some(children) = &mut self.children {
            for child in children.iter_mut() {
                if child.insert(entity, shard_manager) {
                    return true;
                }
            }
            return false;
        }

        self.entities.push(entity);
        shard_manager.on_entity_move(self.shard_id, entity.id);

        if self.entities.len() > self.capacity && self.depth < self.max_depth {
            self.subdivide(shard_manager);
        }

        true
    }

    fn subdivide(&mut self, shard_manager: &mut ShardManager) {
        let sub_bounds = self.bounds.split();

        let shard_ids = self.generate_shard_id();

        let mut children = Box::new([
            QuadTree::new_internal(
                sub_bounds[0],
                self.depth + 1,
                self.max_depth,
                self.capacity,
                shard_ids.get(0).cloned().unwrap_or_default(),
            ),
            QuadTree::new_internal(
                sub_bounds[1],
                self.depth + 1,
                self.max_depth,
                self.capacity,
                shard_ids.get(1).cloned().unwrap_or_default(),
            ),
            QuadTree::new_internal(
                sub_bounds[2],
                self.depth + 1,
                self.max_depth,
                self.capacity,
                shard_ids.get(2).cloned().unwrap_or_default(),
            ),
            QuadTree::new_internal(
                sub_bounds[3],
                self.depth + 1,
                self.max_depth,
                self.capacity,
                shard_ids.get(3).cloned().unwrap_or_default(),
            ),
        ]);

        for entity in self.entities.drain(..) {
            for child in children.iter_mut() {
                if child.insert(entity, shard_manager) {
                    break;
                }
            }
        }

        shard_manager.on_new_shard(shard_ids);

        self.children = Some(children);
    }

    fn try_merge(&mut self, shard_id: u32, shard_manager: &mut ShardManager) -> bool {
        if let Some(children) = self.children.as_mut() {
            let child_id = ((shard_id >> self.depth * 2) & 3) as usize;
            if children[child_id].shard_id == shard_id && self.depth > 0 {
                let mut entity_count = 0;
                for child in children.iter_mut() {
                    entity_count += child.entities.len();
                }

                if entity_count < self.capacity {
                    self.merge(shard_manager);
                    return true;
                }
            } else {
                return children[child_id].try_merge(shard_id, shard_manager);
            }
        }

        println!("Feur");
        false
    }

    fn merge(&mut self, shard_manager: &mut ShardManager) {
        if let Some(children) = self.children.as_mut() {
            let mut destroyed_shards: Vec<u32> = Vec::new();
            for child in children.iter_mut() {
                for entity in child.entities.iter_mut() {
                    shard_manager.on_entity_move(self.shard_id, entity.id);
                    self.entities.push(*entity);
                }

                destroyed_shards.push(child.shard_id)
            }
            shard_manager.on_shard_destroyed(destroyed_shards);

            self.children = None;

            self.try_merge(self.shard_id, shard_manager);
        }
    }

    fn generate_shard_id(&mut self) -> Vec<u32> {
        let mut shard_ids: Vec<u32> = Vec::new();
        let offset = self.depth * 2;
        for i in 0..4 {
            let mut child_id = i;
            child_id = child_id << offset;
            child_id |= self.shard_id;
            shard_ids.push(child_id);
        }
        shard_ids
    }

    pub fn move_entity(&mut self, entity: Entity, shard_manager: &mut ShardManager) {
        let old_shard_id = shard_manager.get_shard(entity.id);
        if let Some(old_shard_id) = old_shard_id {
            self.remove_entity(entity, old_shard_id, shard_manager)
        }

        self.insert(entity, shard_manager);

        if let Some(old_shard_id) = old_shard_id
            && let Some(current_shard_id) = shard_manager.get_shard(entity.id)
            && old_shard_id != current_shard_id
        {
            self.try_merge(old_shard_id, shard_manager);
        }
    }

    fn remove_entity(&mut self, entity: Entity, shard_id: u32, shard_manager: &mut ShardManager) {
        if let Some(children) = self.children.as_mut() {
            let child_id = ((shard_id >> self.depth * 2) & 3) as usize;
            children[child_id].remove_entity(entity, shard_id, shard_manager)
        }

        let entity_pos = self.entities.iter().position(|e| e.id == entity.id);
        if let Some(entity_pos) = entity_pos {
            self.entities.swap_remove(entity_pos);
        }
    }
}
