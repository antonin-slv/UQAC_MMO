mod client_network;
mod launcher;
mod structs;
pub mod client_helper;

use crate::launcher::LauncherPlugin;
use crate::structs::{Chunking, ClientState, LocalPlayer};
use bevy::prelude::*;
use broker_protocol::topics::{Namespace, SecurityDomain, TopicBuilder};
use game_message::msg_client_server::{InputBuffer, PlayerInputMsg, PlayerInput};
use std::collections::VecDeque;

#[derive(Bundle)]
pub struct CameraBundle {
    pub camera: Camera2d,
    pub position: Transform,
}

#[derive(Bundle)]
pub struct RootBundle {
    pub mesh: Mesh2d,
    pub material: MeshMaterial2d<ColorMaterial>,
}

fn main() {
    // 2. Lancement de Bevy
    App::new()
        .add_plugins(DefaultPlugins)
        .add_plugins(client_network::ClientNetworkPlugin)
        .add_plugins(LauncherPlugin)
        .insert_resource(Time::<Fixed>::from_seconds(1.0 / 60.0))
        .insert_resource(Chunking { chunk_size: -0.0 })
        // Systèmes
        .add_systems(Startup, setup_graphics)
        .add_systems(
            FixedUpdate,
            capture_inputs.run_if(in_state(ClientState::InGame)),
        )
        .add_systems(Update, draw_chunks)
        .run();
    //todo : tenter d'envoyer un message de déconnexion (permettra d'un peu limiter la charge serveur) -> soit event soit à la fin de run.
}

fn setup_graphics(mut commands: Commands) {
    commands.spawn(CameraBundle {
        position: Transform::from_xyz(0.0, 50.0, 50.0).looking_at(Vec3::ZERO, Vec3::Y),
        camera: Camera2d::default(),
    });
}

fn capture_inputs(
    keyboard_input: Res<ButtonInput<KeyCode>>,
    broker_client: ResMut<client_network::NetworkManager>,
    local_player: Res<LocalPlayer>,
) {
    // Si on n'est pas encore connecté, on ne fait rien
    if !broker_client.client.is_connected() {
        return;
    }

    if local_player.entity_net_id.is_none() {
        return;
    }

    let mut current_input = PlayerInput::default();

    if keyboard_input.pressed(KeyCode::KeyW) || keyboard_input.pressed(KeyCode::KeyZ) {
        current_input.up = true;
    }
    if keyboard_input.pressed(KeyCode::KeyS) {
        current_input.down = true;
    }
    if keyboard_input.pressed(KeyCode::KeyA) || keyboard_input.pressed(KeyCode::KeyQ) {
        current_input.left = true;
    }
    if keyboard_input.pressed(KeyCode::KeyD) {
        current_input.right = true;
    }

    let mut input_buffer = InputBuffer {
        history: VecDeque::new(),
        max_size: 1,
    };
    input_buffer.push(current_input, 0);

    let msg = PlayerInputMsg {
        entity_id: local_player.entity_net_id.unwrap(),
        emitter_id: local_player.net_id,
        input_data: input_buffer,
    };

    let input_topic = TopicBuilder::new(
        SecurityDomain::PrivateReadPublicWrite,
        Namespace::SpatialInput,
    )
    .append_chunk(&local_player.chunk)
    .build();
    broker_client.client.publish_unreliable(input_topic, &msg);
}

fn draw_chunks(mut gizmos: Gizmos, chunking: Res<Chunking>) {
    let start = Isometry2d::new(
        Vec2::new(chunking.chunk_size, chunking.chunk_size) * 0.5,
        Rot2::IDENTITY,
    );
    let size = Vec2::ONE * chunking.chunk_size;
    gizmos.rect_2d(start, size, Color::Srgba(Srgba::RED));
}
