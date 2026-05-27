use crate::shard_manager::ShardManager;
use std::env;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Vec2 {
    pub x: f32,
    pub y: f32,
}

impl Vec2 {
    pub fn new(x: f32, y: f32) -> Self {
        Self { x, y }
    }

    pub fn zero() -> Self {
        Self::new(0.0, 0.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rect {
    pub min_x: f32,
    pub min_y: f32,
    pub max_x: f32,
    pub max_y: f32,
}

impl Rect {
    pub fn contains(&self, p: Vec2) -> bool {
        p.x >= self.min_x && p.x <= self.max_x && p.y >= self.min_y && p.y <= self.max_y
    }

    pub fn sqr_distance_to_point(&self, p: Vec2) -> f32 {
        let dx = (self.min_x - p.x).max(0.0).max(p.x - self.max_x);
        let dy = (self.min_y - p.y).max(0.0).max(p.y - self.max_y);
        dx * dx + dy * dy
    }

    pub fn split(&self) -> [Rect; 4] {
        let mid_x = (self.min_x + self.max_x) / 2.0;
        let mid_y = (self.min_y + self.max_y) / 2.0;

        [
            Rect {
                min_x: self.min_x,
                max_x: mid_x,
                min_y: self.min_y,
                max_y: mid_y,
            },
            Rect {
                min_x: mid_x,
                max_x: self.max_x,
                min_y: self.min_y,
                max_y: mid_y,
            },
            Rect {
                min_x: self.min_x,
                max_x: mid_x,
                min_y: mid_y,
                max_y: self.max_y,
            },
            Rect {
                min_x: mid_x,
                max_x: self.max_x,
                min_y: mid_y,
                max_y: self.max_y,
            },
        ]
    }
}

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
    depth: u8,
    max_depth: u8,
    capacity: usize,
    entities: Vec<Entity>,
    children: Option<Box<[QuadTree; 4]>>,
    shard_id: u32,
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

        let mut quadtree =
            Self::new_internal(map_size, 0, max_depth, quadtree_capacity, 0, shard_manager);

        quadtree.subdivide(shard_manager);

        quadtree
    }

    fn new_internal(
        bounds: Rect,
        depth: u8,
        max_depth: u8,
        capacity: usize,
        shard_id: u32,
        shard_manager: &mut ShardManager,
    ) -> Self {
        shard_manager.on_new_shard(shard_id);
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

        let mut children = Box::new([
            QuadTree::new_internal(
                sub_bounds[0],
                self.depth + 1,
                self.max_depth,
                self.capacity,
                self.generate_shard_id(0),
                shard_manager,
            ),
            QuadTree::new_internal(
                sub_bounds[1],
                self.depth + 1,
                self.max_depth,
                self.capacity,
                self.generate_shard_id(1),
                shard_manager,
            ),
            QuadTree::new_internal(
                sub_bounds[2],
                self.depth + 1,
                self.max_depth,
                self.capacity,
                self.generate_shard_id(2),
                shard_manager,
            ),
            QuadTree::new_internal(
                sub_bounds[3],
                self.depth + 1,
                self.max_depth,
                self.capacity,
                self.generate_shard_id(3),
                shard_manager,
            ),
        ]);

        for entity in self.entities.drain(..) {
            for child in children.iter_mut() {
                if child.insert(entity, shard_manager) {
                    break;
                }
            }
        }

        self.children = Some(children);
    }

    fn merge(&mut self, shard_id: u32, shard_manager: &mut ShardManager) {
        if let Some(children) = self.children.as_mut() {
            let child_id = ((shard_id >> self.depth * 2) & 3) as usize;
            if children[child_id].shard_id == shard_id && self.depth > 0 {
                let mut entity_count = 0;
                for child in children.iter_mut() {
                    entity_count += child.entities.len();
                }

                if entity_count < 4 {
                    for child in children.iter_mut() {
                        for entity in child.entities.iter_mut() {
                            shard_manager.on_entity_move(self.shard_id, entity.id);
                            self.entities.push(*entity);
                        }

                        shard_manager.on_shard_destroyed(child.shard_id);
                    }

                    self.children = None;
                }
            } else {
                children[child_id].merge(shard_id, shard_manager);
            }
        }
    }

    fn generate_shard_id(&mut self, children_id: u8) -> u32 {
        let offset = self.depth * 2;
        let mut next_id = children_id as u32;
        next_id = next_id << offset;
        next_id |= self.shard_id;
        next_id
    }

    pub fn shards_near(&self, pos: Vec2, margin: f32) -> Vec<u32> {
        let mut results = Vec::new();
        self.collect_shards_near(pos, margin * margin, &mut results);
        results.sort_unstable();
        results.dedup();
        results
    }

    fn collect_shards_near(&self, pos: Vec2, margin_sqr: f32, results: &mut Vec<u32>) {
        if self.bounds.sqr_distance_to_point(pos) > margin_sqr {
            return;
        }
        if let Some(children) = &self.children {
            for child in children.iter() {
                child.collect_shards_near(pos, margin_sqr, results);
            }
        } else {
            results.push(self.shard_id);
        }
    }

    pub fn move_entity(&mut self, entity: Entity, shard_manager: &mut ShardManager) {
        let current_shard_id = shard_manager.get_shard(entity.id);
        if let Some(current_shard_id) = current_shard_id {
            self.remove_entity(entity, current_shard_id, shard_manager)
        }

        self.insert(entity, shard_manager);

        if let Some(old_shard_id) = current_shard_id {
            self.merge(old_shard_id, shard_manager);
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

    pub fn _print_tree(&self) {
        println!("QuadTree Root");
        self._print_internal(String::new(), true);
    }

    fn _print_internal(&self, prefix: String, is_last: bool) {
        let branch = if is_last { "└── " } else { "├── " };

        let bounds_str = format!(
            "[{:.0},{:.0} -> {:.0},{:.0}]",
            self.bounds.min_x, self.bounds.min_y, self.bounds.max_x, self.bounds.max_y
        );

        let mut info = format!(
            "{} (Depth: {} | Shard ID : {}",
            bounds_str, self.depth, self.shard_id
        );
        if self.children.is_some() {
            info.push_str(&format!(
                ", Shard ID: {}, Entities: {})",
                self.shard_id,
                self.entities.len()
            ));
        } else {
            info.push_str(")");
        }

        println!("{}{}{}", prefix, branch, info);

        if let Some(children) = &self.children {
            let new_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });

            for (i, child) in children.iter().enumerate() {
                let is_last_child = i == children.len() - 1;

                let quad_name = match i {
                    0 => "SW: ",
                    1 => "SE: ",
                    2 => "NW: ",
                    _ => "NE: ",
                };

                print!(
                    "{}{}",
                    new_prefix,
                    if is_last_child {
                        "└── "
                    } else {
                        "├── "
                    }
                );
                print!("{}", quad_name);

                let child_prefix = format!(
                    "{}{}",
                    new_prefix,
                    if is_last_child { "    " } else { "│   " }
                );

                child._print_internal(child_prefix, true);
            }
        } else {
            let mut entities = String::new();
            for entity in self.entities.iter() {
                entities.push_str(format!("{} / ", entity.id).as_str());
            }
            print!("{}{}\n", prefix, entities);
        }
    }
}
