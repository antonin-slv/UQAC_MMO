mod client_network;
mod launcher;
mod structs;

use crate::launcher::LauncherPlugin;
use crate::structs::{Chunking, ClientState, LocalControlledComponent, LocalPlayer};
use bevy::prelude::*;
use broker_protocol::topics::{Namespace, SecurityDomain, TopicBuilder};
use core_types::get_chunk;
use game_message::msg_client_server::{PlayerInput, PlayerInputMsg};

#[derive(Bundle)]
pub struct CameraBundle {
    pub camera: Camera2d,
    pub position: Transform,
}

#[derive(Bundle)]
pub struct PlayerBundle {
    pub mesh: Mesh2d,
    pub material: MeshMaterial2d<ColorMaterial>,
    pub transform: Transform,
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
    mut local_player: ResMut<LocalPlayer>,
    chunking: Res<Chunking>,
    local_entity_query: Query<&Transform, With<LocalControlledComponent>>,
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
        current_input.set_up(true);
    }
    if keyboard_input.pressed(KeyCode::KeyS) {
        current_input.set_down(true);
    }
    if keyboard_input.pressed(KeyCode::KeyA) || keyboard_input.pressed(KeyCode::KeyQ) {
        current_input.set_left(true);
    }
    if keyboard_input.pressed(KeyCode::KeyD) {
        current_input.set_right(true);
    }

    let msg = PlayerInputMsg {
        entity_id: local_player.entity_net_id.unwrap(),
        emitter_id: local_player.net_id,
        input_data: current_input,
    };

    match local_entity_query.iter().next() {
        Some(local_entity_transform) => {
            let local_chunk = get_chunk(
                local_entity_transform.translation.x,
                local_entity_transform.translation.y,
                chunking.chunk_size,
            );
            local_player.chunk = local_chunk;

            let input_topic = TopicBuilder::new(
                SecurityDomain::PrivateReadPublicWrite,
                Namespace::SpatialInput,
            )
            .append_chunk(&local_player.chunk)
            .build();
            broker_client.client.publish_unreliable(input_topic, &msg);
        }
        None => {
            println!("No local entity found");
        }
    };
}

fn draw_chunks(mut gizmos: Gizmos, chunking: Res<Chunking>) {
    let start = Isometry2d::new(
        Vec2::new(chunking.chunk_size, chunking.chunk_size) * 0.5,
        Rot2::IDENTITY,
    );
    let size = Vec2::ONE * chunking.chunk_size;
    gizmos.rect_2d(
        start,
        size,
        Color::Srgba(Srgba::RED),
    );
    println!("{} / {:?} / {}", chunking.chunk_size, start, size);
}
