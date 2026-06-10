#[cfg(feature = "debug_visual")]
pub mod bevy_renderer {
    use crate::quadtree::QuadTree;
    use crate::shard_manager::{Shard, ShardManager};
    use bevy::DefaultPlugins;
    use bevy::app::{App, Startup, Update};
    use bevy::camera::Camera2d;
    use bevy::color::{Color, Srgba};
    use bevy::math::{Isometry2d, Rot2, Vec2};
    use bevy::prelude::{Commands, Gizmos, Res, ResMut, Resource};
    use bevy_text_gizmos::TextGizmos;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::sync::mpsc::Receiver;

    #[derive(Resource)]
    pub struct QuadTreeChannelResource {
        pub rx: Mutex<Receiver<(QuadTree, ShardManager)>>,
    }

    #[derive(Resource)]
    pub struct LocalQuadTreeSnapshot {
        pub quad_tree: Option<QuadTree>,
        pub shard_manager: Option<ShardManager>,
    }

    pub fn start_renderer(rx: Mutex<Receiver<(QuadTree, ShardManager)>>) {
        App::new()
            .add_plugins(DefaultPlugins)
            .insert_resource(QuadTreeChannelResource { rx })
            .insert_resource(LocalQuadTreeSnapshot {
                quad_tree: None,
                shard_manager: None,
            })
            .add_systems(Update, (receive_snapshots, draw_gizmos))
            .add_systems(Startup, setup)
            .run();
    }

    fn setup(mut commands: Commands) {
        commands.spawn(Camera2d);
    }

    fn receive_snapshots(
        channels: ResMut<QuadTreeChannelResource>,
        mut snapshot: ResMut<LocalQuadTreeSnapshot>,
    ) {
        if let Ok(rx_guard) = channels.rx.lock() {
            while let Ok((new_tree, shard_manager)) = rx_guard.try_recv() {
                snapshot.quad_tree = Some(new_tree);
                snapshot.shard_manager = Some(shard_manager);
            }
        }
    }

    fn draw_gizmos(mut gizmos: Gizmos, snapshot: Res<LocalQuadTreeSnapshot>) {
        if let Some(ref quad_tree) = snapshot.quad_tree
            && let Some(ref shard_manager) = snapshot.shard_manager
        {
            draw_quadtree(&mut gizmos, quad_tree, shard_manager);

            for entity in shard_manager.get_entities() {
                let start = Isometry2d::new(Vec2::new(entity.pos.x, entity.pos.y), Rot2::IDENTITY);
                gizmos.circle_2d(start, 5.0, Color::Srgba(Srgba::BLUE));
            }
        }
    }

    fn draw_quadtree(gizmos: &mut Gizmos, quad_tree: &QuadTree, shard_manager: &ShardManager) {
        if let Some(children) = quad_tree.children.as_ref() {
            for child in children.iter() {
                draw_quadtree(gizmos, child, shard_manager);
            }
        } else {
            let size = Vec2::new(
                quad_tree.bounds.max_x - quad_tree.bounds.min_x,
                quad_tree.bounds.max_y - quad_tree.bounds.min_y,
            );
            let start = Isometry2d::new(
                Vec2::new(
                    quad_tree.bounds.min_x + size.x / 2.,
                    quad_tree.bounds.min_y + size.y / 2.,
                ),
                Rot2::IDENTITY,
            );
            gizmos.rect_2d(start, size, Color::Srgba(Srgba::RED));
            gizmos.text_2d(
                start,
                format!(
                    "{}\n{}",
                    quad_tree.shard_id,
                    shard_manager
                        .shards
                        .get(&quad_tree.shard_id)
                        .unwrap_or(&Shard {
                            dgs: Some(0),
                            entities: HashSet::new()
                        })
                        .dgs
                        .unwrap_or(0)
                )
                .as_str(),
                16.0 - quad_tree.depth as f32,
                Vec2::ZERO,
                Color::WHITE,
            );
        }
    }
}
