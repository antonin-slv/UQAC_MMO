use crate::quadtree::QuadTree;

use crate::bevy_renderer::start_renderer;
use crate::shard_manager::ShardManager;
use dotenv::dotenv;

mod bevy_renderer;
mod quadtree;
mod shard_manager;

fn main() {
    dotenv().ok();

    let mut shard_manager = ShardManager::new();
    let quad_tree = QuadTree::new(&mut shard_manager);

    start_renderer(shard_manager, quad_tree);
}
