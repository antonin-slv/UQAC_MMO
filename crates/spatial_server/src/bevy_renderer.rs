#[cfg(feature = "debug_visual")]
pub mod bevy_renderer {
    use crate::quadtree::QuadTree;
    use crate::shard_manager::{Shard, ShardManager};
    use bevy::app::{App, Startup, Update};
    use bevy::camera::Camera2d;
    use bevy::color::{Color, Srgba};
    use bevy::math::{Isometry2d, Rot2, Vec2};
    use bevy::prelude::{Commands, Gizmos, Query, Res, ResMut, Resource, With};
    use bevy::window::{PrimaryWindow, Window};
    use bevy::DefaultPlugins;
    use bevy_text_gizmos::TextGizmos;
    use core_types::chunks::get_chunk_size;
    use core_types::Rect;
    use std::collections::HashSet;
    use std::sync::mpsc::Receiver;
    use std::sync::Mutex;

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

    fn draw_gizmos(
        mut gizmos: Gizmos,
        snapshot: Res<LocalQuadTreeSnapshot>,
        q_window: Query<&Window, With<PrimaryWindow>>,
    ) {
        if let Some(ref quad_tree) = snapshot.quad_tree
            && let Some(ref shard_manager) = snapshot.shard_manager
            && let Ok(window) = q_window.single()
        {
            let world_size = quad_tree.bounds.max_x - quad_tree.bounds.min_x;
            let chunk_size = get_chunk_size(world_size, quad_tree.max_depth);
            let screen_size = Vec2::new(window.width(), window.height());
            let scale = screen_size.min_element() / world_size;

            draw_quadtree(&mut gizmos, quad_tree, shard_manager, chunk_size, scale);

            for entity in shard_manager.get_entities() {
                let start = Isometry2d::new(
                    Vec2::new(entity.pos.x, entity.pos.y) * scale,
                    Rot2::IDENTITY,
                );
                gizmos.circle_2d(start, 5.0, Color::Srgba(Srgba::BLUE));
            }
        }
    }

    fn draw_quadtree(
        gizmos: &mut Gizmos,
        quad_tree: &QuadTree,
        shard_manager: &ShardManager,
        chunk_size: f32,
        scale: f32,
    ) {
        if let Some(children) = quad_tree.children.as_ref() {
            for child in children.iter() {
                draw_quadtree(gizmos, child, shard_manager, chunk_size, scale);
            }
        } else {
            let (start, size) = get_gizmos_area(&quad_tree.bounds, scale);

            // Dessin du rectangle avec les dimensions mises à l'échelle
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
            let (start, size) = get_gizmos_area(&quad_tree.bounds, scale);
            let dgs = shard_manager
                .shards
                .get(&quad_tree.shard_id)
                .unwrap_or(&Shard {
                    dgs: Some(0),
                    entities: HashSet::new(),
                })
                .dgs
                .unwrap_or(0);
            let heartbeat = shard_manager.dgs_data.get(&dgs);
            if let Some((heartbeat, color)) = heartbeat {
                let color = Srgba::new(color.0, color.1, color.2, 1.0);
                gizmos.text_2d(
                    start,
                    format!("{}\n{}\n{}", quad_tree.shard_id, dgs, heartbeat.id).as_str(),
                    13.0 - quad_tree.depth as f32,
                    Vec2::ZERO,
                    color,
                );

                for chunk in heartbeat.chunk_managed.iter() {
                    let area = chunk.to_core_rect(chunk_size);

                    let (start, size) = get_gizmos_area(&area, scale);

                    gizmos.rect_2d(start, size, color);
                }
                gizmos.rect_2d(start, size, Color::Srgba(Srgba::RED));
            }
        }
    }

    fn get_gizmos_area(area: &Rect, scale: f32) -> (Isometry2d, Vec2) {
        let size = Vec2::new(
            (area.max_x - area.min_x) * scale,
            (area.max_y - area.min_y) * scale,
        );

        let center = Vec2::new(
            (area.min_x + (area.max_x - area.min_x) / 2.) * scale,
            (area.min_y + (area.max_y - area.min_y) / 2.) * scale,
        );

        (Isometry2d::from_translation(center), size)
    }
}
