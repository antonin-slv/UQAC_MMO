use crate::quadtree::{QuadTree, Rect, Vec2};
use dotenv::dotenv;
use rand::RngExt;
use std::env;

mod quadtree;

fn main() {
    dotenv().ok();

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

    let quadtree_capacity = env::var("QUADTREE_CAPACITY")
        .expect("Env QUADTREE_CAPACITY is not set")
        .parse()
        .expect("Env QUADTREE_CAPACITY is not a number");

    let max_depth = env::var("QUADTREE_MAX_DEPTH")
        .expect("Env QUADTREE_MAX_DEPTH is not set")
        .parse()
        .expect("Env QUADTREE_MAX_DEPTH is not a number");

    let mut quad_tree = QuadTree::new(map_size, quadtree_capacity, max_depth);

    let entity_id = 12131u32;
    let entity_position = Vec2 { x: 2326., y: 6161. };
    quad_tree.insert(entity_id, entity_position);

    let mut rng = rand::rng();

    for _ in 1..10 {
        let entity_position = Vec2 {
            x: rng.random_range(map_size.min_x..map_size.max_x),
            y: rng.random_range(map_size.min_y..map_size.max_y),
        };
        quad_tree.insert(rng.random(), entity_position);
    }

    if let Some(shard_id) = quad_tree.shard_for(entity_position) {
        println!("Shard for {:?}", shard_id);
    }

    println!("{:?}", quad_tree.shards_near(entity_position, 10.0));

    quad_tree.print_tree()
}
