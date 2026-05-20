mod network;
mod structs;
mod launcher;

use bevy::prelude::*;
use bytes::Bytes;
use shared_replication::{PlayerInput, STREAM_INPUTS};
use game_sockets::{ GameStream, GameStreamReliability };
use crate::launcher::LauncherPlugin;
use crate::structs::ClientState;

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
        .add_plugins(network::ClientNetworkPlugin)
        .add_plugins(LauncherPlugin)
        // Systèmes
        .add_systems(Startup, setup_graphics)
        .add_systems(Update, capture_inputs.run_if(in_state(ClientState::InGame)))
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
    net: ResMut<network::NetworkManager>,
    server_conn: Res<network::ServerConnection>,
) {
    // Si on n'est pas encore connecté, on ne fait rien
    let Some(conn) = server_conn.0 else { return };

    let mut current_input = PlayerInput::default();

    if keyboard_input.pressed(KeyCode::KeyW) || keyboard_input.pressed(KeyCode::KeyZ) { current_input.up = true; }
    if keyboard_input.pressed(KeyCode::KeyS) { current_input.down = true; }
    if keyboard_input.pressed(KeyCode::KeyA) || keyboard_input.pressed(KeyCode::KeyQ) { current_input.left = true; }
    if keyboard_input.pressed(KeyCode::KeyD) { current_input.right = true; }

    // On prépare le flux (Unreliable pour les inputs)
    let stream = GameStream::new(STREAM_INPUTS, GameStreamReliability::Unreliable);

    // On sérialise et on envoie directement avec la lib
    // TODO : réplication plus propre (bools-> bitmask)
    if let Ok(bytes) = bincode::serialize(&current_input) {
        let data = Bytes::from(bytes);
        if let Err(e) = net.peer.send(&conn, &stream, data) {
            eprintln!("Erreur lors de l'envoi des inputs: {:?}", e);
        }
    }
}