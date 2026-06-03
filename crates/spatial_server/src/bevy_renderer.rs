use crate::quadtree::{Entity, QuadTree};
use crate::shard_manager::ShardManager;
use crate::QuadTreeCommand;
use bevy::app::{App, Startup, Update};
use bevy::camera::Camera2d;
use bevy::color::{Color, Srgba};
use bevy::input::ButtonInput;
use bevy::math::{Isometry2d, Rot2, Vec2};
use bevy::prelude::{
    Commands, Component, Gizmos, KeyCode, Query, Res, ResMut, Resource, Time,
};
use bevy::DefaultPlugins;
use std::env;
use std::sync::mpsc::Receiver;
use std::sync::Mutex;
use tokio::sync::mpsc::Sender;

#[derive(Resource)]
pub struct QuadTreeChannelResource {
    pub tx: Sender<QuadTreeCommand>,

    pub rx: Mutex<Receiver<(QuadTree, ShardManager)>>,
}

#[derive(Resource)]
pub struct LocalQuadTreeSnapshot {
    pub quad_tree: Option<QuadTree>,
    pub shard_manager: Option<ShardManager>,
}

#[derive(Component)]
pub struct Player {
    entity: Entity,
}

pub fn start_renderer(tx: Sender<QuadTreeCommand>, rx: Mutex<Receiver<(QuadTree, ShardManager)>>) {
    App::new()
        .add_plugins(DefaultPlugins)
        .insert_resource(QuadTreeChannelResource { tx, rx })
        .insert_resource(LocalQuadTreeSnapshot {
            quad_tree: None,
            shard_manager: None,
        })
        .add_systems(Update, (receive_snapshots, draw_gizmos, update, movement))
        .add_systems(Startup, setup)
        .run();
}

fn setup(mut commands: Commands) {
    commands.spawn(Camera2d);
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
    let mut world_size: f32 = env::var("WORLD_SIZE").unwrap().parse().unwrap();
    world_size /= 2.0;
    let map_size = crate::quadtree::Rect {
        min_x: -world_size,
        max_x: world_size,
        min_y: -world_size,
        max_y: world_size,
    };

    for mut player in player.iter_mut() {
        let next_x = player.entity.pos.x + current_input.x * time.delta_secs();
        let next_y = player.entity.pos.y + current_input.y * time.delta_secs();
        if map_size.contains(crate::quadtree::Vec2::new(next_x, next_y)) {
            player.entity.pos.x = next_x;
            player.entity.pos.y = next_y;
        }
    }
}

fn receive_snapshots(
    channels: ResMut<QuadTreeChannelResource>,
    mut snapshot: ResMut<LocalQuadTreeSnapshot>,
) {
    let rx_guard = channels.rx.lock().unwrap();

    while let Ok((new_tree, shard_manager)) = rx_guard.try_recv() {
        snapshot.quad_tree = Some(new_tree);
        snapshot.shard_manager = Some(shard_manager);
    }
}

fn update(players: Query<&Player>, channels: Res<QuadTreeChannelResource>) {
    for player in players.iter() {
        let _ = channels
            .tx
            .try_send(QuadTreeCommand::MoveEntity(player.entity));
    }
}

fn draw_gizmos(mut gizmos: Gizmos, snapshot: Res<LocalQuadTreeSnapshot>) {
    if let Some(ref quad_tree) = snapshot.quad_tree {
        draw_quadtree(&mut gizmos, quad_tree);
    }

    if let Some(ref shard_manager) = snapshot.shard_manager {
        for entity in shard_manager.get_entities() {
            let start = Isometry2d::new(Vec2::new(entity.pos.x, entity.pos.y), Rot2::IDENTITY);
            gizmos.circle_2d(start, 5.0, Color::Srgba(Srgba::BLUE));
        }
    }
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
    }
}
