use crate::quadtree;
use crate::quadtree::{Entity, QuadTree};
use crate::shard_manager::ShardManager;
use bevy::DefaultPlugins;
use bevy::app::{App, Startup, Update};
use bevy::camera::Camera2d;
use bevy::color::{Color, Srgba};
use bevy::input::ButtonInput;
use bevy::math::{Isometry2d, Rot2, Vec2};
use bevy::prelude::{
    Commands, Component, Fixed, FixedUpdate, Gizmos, KeyCode, Query, Res, ResMut, Resource, Time,
};
use bevy_text_gizmos::TextGizmos;
use rand::RngExt;
pub(crate) use shared_replication::math::Rect;
use std::env;

#[derive(Resource)]
pub struct ShardManagerResource {
    shard_manager: ShardManager,
}
#[derive(Resource)]
pub struct QuadtreeResource {
    quad_tree: QuadTree,
}

#[derive(Component)]
pub struct Player {
    entity: Entity,
}

pub fn start_renderer(shard_manager: ShardManager, quad_tree: QuadTree) {
    let merge_frequency: f64 = env::var("MERGE_FREQUENCY")
        .expect("Env MERGE_FREQUENCY is not set")
        .parse()
        .expect("MERGE_FREQUENCY is not an float");
    App::new()
        .add_plugins(DefaultPlugins)
        .insert_resource(ShardManagerResource { shard_manager })
        .insert_resource(QuadtreeResource { quad_tree })
        .add_systems(Startup, setup)
        .add_systems(Update, (draw_gizmos, update, movement))
        .insert_resource(Time::<Fixed>::from_seconds(1.0 / merge_frequency))
        .add_systems(FixedUpdate, merge_quadtree)
        .run();
}

fn setup(mut commands: Commands) {
    commands.spawn(Camera2d);
    let mut rng = rand::rng();
    for i in 0..3 {
        commands.spawn(Player {
            entity: Entity::new(
                rng.random(),
                shared_replication::math::Vec2::new((i * 10) as f32, (i * 10) as f32),
            ),
        });
    }
}

fn movement(
    mut player: Query<&mut Player>,
    keyboard_input: Res<ButtonInput<KeyCode>>,
    time: Res<Time>,
) {
    let mut current_input = Vec2::new(0.0, 0.0);
    if keyboard_input.pressed(KeyCode::KeyW) || keyboard_input.pressed(KeyCode::KeyZ) {
        current_input.y += 1.0;
    };
    if keyboard_input.pressed(KeyCode::KeyS) {
        current_input.y -= 1.0;
    }
    if keyboard_input.pressed(KeyCode::KeyA) || keyboard_input.pressed(KeyCode::KeyQ) {
        current_input.x -= 1.0;
    }
    if keyboard_input.pressed(KeyCode::KeyD) {
        current_input.x += 1.0;
    }
    current_input *= 50.0;
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
    for mut player in player.iter_mut() {
        let next_x = player.entity.pos.x + current_input.x * time.delta_secs();
        let next_y = player.entity.pos.y + current_input.y * time.delta_secs();
        if map_size.contains(quadtree::Vec2::new(next_x, next_y)) {
            player.entity.pos.x = next_x;
            player.entity.pos.y = next_y;
        }
    }
}

fn update(
    players: Query<&Player>,
    mut quadtree: ResMut<QuadtreeResource>,
    mut shard_manager: ResMut<ShardManagerResource>,
) {
    for player in players.iter() {
        quadtree
            .quad_tree
            .move_entity(player.entity, &mut shard_manager.shard_manager);
    }
}

fn merge_quadtree(
    mut quadtree: ResMut<QuadtreeResource>,
    mut shard_manager: ResMut<ShardManagerResource>,
) {
    quadtree
        .quad_tree
        .try_merge(&mut shard_manager.shard_manager);
}

fn draw_gizmos(mut gizmos: Gizmos, resource_manager: Res<QuadtreeResource>) {
    draw_quadtree(&mut gizmos, &resource_manager.quad_tree);
}

fn draw_quadtree(gizmos: &mut Gizmos, quad_tree: &QuadTree) {
    if let Some(children) = quad_tree.children.as_ref() {
        for child in children.iter() {
            draw_quadtree(gizmos, child);
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
            format!("{}", quad_tree.shard_id).as_str(),
            16.0 - quad_tree.depth as f32,
            Vec2::ZERO,
            Color::WHITE,
        );

        for entity in quad_tree.entities.iter() {
            let start = Isometry2d::new(Vec2::new(entity.pos.x, entity.pos.y), Rot2::IDENTITY);
            gizmos.circle_2d(start, 5.0, Color::Srgba(Srgba::BLUE));
        }
    }
}
