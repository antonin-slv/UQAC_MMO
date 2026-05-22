use std::sync::atomic::{AtomicU32, Ordering};

static NEXT_SHARD_ID: AtomicU32 = AtomicU32::new(1);

fn generate_shard_id() -> u32 {
    NEXT_SHARD_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Vec2 {
    pub x: f32,
    pub y: f32,
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

pub struct QuadTree {
    bounds: Rect,
    depth: u8,
    max_depth: u8,
    capacity: usize,
    entities: Vec<Entity>,
    children: Option<Box<[QuadTree; 4]>>,
    shard_id: Option<u32>,
}

impl QuadTree {
    pub fn new(bounds: Rect, capacity: usize, max_depth: u8) -> Self {
        Self::new_internal(bounds, 0, max_depth, capacity)
    }

    fn new_internal(bounds: Rect, depth: u8, max_depth: u8, capacity: usize) -> Self {
        Self {
            bounds,
            depth,
            max_depth,
            capacity,
            entities: Vec::with_capacity(capacity),
            children: None,
            shard_id: Some(generate_shard_id()),
        }
    }

    pub fn insert(&mut self, entity_id: u32, pos: Vec2) -> bool {
        if !self.bounds.contains(pos) {
            return false;
        }

        if let Some(children) = &mut self.children {
            for child in children.iter_mut() {
                if child.insert(entity_id, pos) {
                    return true;
                }
            }
            return false;
        }

        self.entities.push(Entity { id: entity_id, pos });

        if self.entities.len() > self.capacity && self.depth < self.max_depth {
            self.subdivide();
        }

        true
    }

    fn subdivide(&mut self) {
        let sub_bounds = self.bounds.split();

        let mut children = Box::new([
            QuadTree::new_internal(sub_bounds[0], self.depth + 1, self.max_depth, self.capacity),
            QuadTree::new_internal(sub_bounds[1], self.depth + 1, self.max_depth, self.capacity),
            QuadTree::new_internal(sub_bounds[2], self.depth + 1, self.max_depth, self.capacity),
            QuadTree::new_internal(sub_bounds[3], self.depth + 1, self.max_depth, self.capacity),
        ]);

        for entity in self.entities.drain(..) {
            for child in children.iter_mut() {
                if child.insert(entity.id, entity.pos) {
                    break;
                }
            }
        }

        self.children = Some(children);

        self.shard_id = None;
    }

    pub fn shard_for(&self, pos: Vec2) -> Option<u32> {
        if !self.bounds.contains(pos) {
            return None;
        }
        if let Some(children) = &self.children {
            for child in children.iter() {
                if let Some(id) = child.shard_for(pos) {
                    return Some(id);
                }
            }
        }
        self.shard_id
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
        } else if let Some(id) = self.shard_id {
            results.push(id);
        }
    }

    pub fn print_tree(&self) {
        println!("QuadTree Root");
        self.print_internal(String::new(), true);
    }

    fn print_internal(&self, prefix: String, is_last: bool) {
        let branch = if is_last { "└── " } else { "├── " };

        let bounds_str = format!(
            "[{:.0},{:.0} -> {:.0},{:.0}]",
            self.bounds.min_x, self.bounds.min_y, self.bounds.max_x, self.bounds.max_y
        );

        let mut info = format!("{} (Depth: {}", bounds_str, self.depth);
        if let Some(id) = self.shard_id {
            info.push_str(&format!(
                ", Shard ID: {}, Entities: {})",
                id,
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

                child.print_internal(child_prefix, true);
            }
        }
    }
}
