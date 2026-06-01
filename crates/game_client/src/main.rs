mod client_network;
mod launcher;
mod structs;

use crate::launcher::LauncherPlugin;
use crate::structs::{ClientState, LocalPlayer};
use bevy::prelude::*;
use shared_replication::broker_topics::{Namespace, SecurityDomain, TopicBuilder};

use shared_replication::msg_client_server::*;

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
        // Systèmes
        .add_systems(Startup, setup_graphics)
        .add_systems(
            FixedUpdate,
            capture_inputs.run_if(in_state(ClientState::InGame)),
        )
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
        client_id: local_player.net_id,
        input_data: current_input,
    };

    let input_topic = TopicBuilder::new(
        SecurityDomain::PrivateReadPublicWrite,
        Namespace::SpatialInput,
    )
    .append_grid(local_player.x_chunk, local_player.y_chunk) //todo : recevoir ses chunk et publier dessus.
    .build();
    broker_client.client.publish_unreliable(input_topic, &msg);
}
