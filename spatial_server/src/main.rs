use crate::quadtree::{Entity, QuadTree, Vec2};
use crate::shard_manager::ShardManager;
use dotenv::dotenv;

mod quadtree;
mod shard_manager;

fn main() {
    dotenv().ok();

    let mut shard_manager = ShardManager::new();
    let mut quad_tree = QuadTree::new(&mut shard_manager);

    let entities = vec![
        Entity::new(3630, Vec2::new(75.0, 150.0)),
        Entity::new(67, Vec2::new(150.0, 150.0)),
        Entity::new(69, Vec2::new(225.0, 150.0)),
        Entity::new(3630, Vec2::new(-150.0, 150.0)),
    ];

    for entity in entities {
        quad_tree.move_entity(entity, &mut shard_manager);
        quad_tree._print_tree();
    }

    println!("{:?}", quad_tree.shards_near(Vec2::zero(), 150.0));
}
