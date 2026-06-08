#[cfg(feature = "debug_visual")]
use crate::bevy_renderer::bevy_renderer::start_renderer;
use crate::broker_client::BrokerClient;
use crate::quadtree::{Entity, QuadTree};
use crate::shard_manager::ShardManager;
use anyhow::Result;
use dotenv::dotenv;
use std::env;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;

#[cfg(feature = "debug_visual")]
mod bevy_renderer;
mod broker_client;
mod quadtree;
mod shard_manager;

pub enum QuadTreeCommand {
    MoveEntity(Entity),
    TryMerge,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let (quadtree_tx, mut quadtree_rx) = mpsc::channel::<QuadTreeCommand>(4096);

    let (bevy_tx, bevy_rx) = std::sync::mpsc::channel::<(QuadTree, ShardManager)>();

    let quad_tree_handle = tokio::spawn(async move {
        let mut shard_manager = ShardManager::new();
        let mut broker_client = BrokerClient::new();
        let mut quad_tree = QuadTree::new(&mut shard_manager, &broker_client);

        println!("[Acteur QuadTree] Initialisé et à l'écoute...");

        loop {
            while let Ok(command) = quadtree_rx.try_recv() {
                match command {
                    QuadTreeCommand::MoveEntity(entity) => {
                        quad_tree.insert(entity, &mut shard_manager, &broker_client);

                        #[cfg(feature = "debug_visual")]
                        let _ = bevy_tx.send((quad_tree.clone(), shard_manager.clone()));
                    }
                    QuadTreeCommand::TryMerge => {
                        let merged_shards = quad_tree.try_merge(&mut shard_manager);
                        for (shard_id, merged_shards) in merged_shards {
                            let bounds = quad_tree.get_shard_bounds(&shard_id);
                            if let Some((bounds, _)) = bounds {
                                shard_manager.on_shard_destroyed(
                                    shard_id,
                                    bounds,
                                    merged_shards,
                                    &broker_client,
                                );
                            }
                        }

                        #[cfg(feature = "debug_visual")]
                        let _ = bevy_tx.send((quad_tree.clone(), shard_manager.clone()));
                    }
                }
            }

            broker_client
                .poll_handle(&mut quad_tree, &mut shard_manager)
                .await;
        }
    });

    let quadtree_tx_scaler = quadtree_tx.clone();
    let scaler_handle = tokio::spawn(async move {
        let merge_frequency: u64 = env::var("MERGE_FREQUENCY")
            .expect("Env MERGE_FREQUENCY is not set")
            .parse()
            .expect("MERGE_FREQUENCY is not a number");

        let mut ticker = interval(Duration::from_secs(merge_frequency));
        loop {
            ticker.tick().await;

            if quadtree_tx_scaler
                .send(QuadTreeCommand::TryMerge)
                .await
                .is_err()
            {
                println!("[Scaler] Impossible d'enoyer TryMerge, l'acteur est arrêté.");
                break;
            }
        }
    });

    #[cfg(feature = "debug_visual")]
    {
        start_renderer(Mutex::new(bevy_rx));
    }

    tokio::try_join!(quad_tree_handle, scaler_handle)?;

    Ok(())
}
