mod network;
mod structs;
mod launcher;

use bevy::prelude::*;
use bytes::{BufMut, BytesMut};
use shared_replication::{STREAM_INPUTS};
use shared_replication::client_server::*;
use game_sockets::{ GameStream, GameStreamReliability };
use shared_replication::broker::{BrokerMessageHeaders, Input};
use crate::launcher::LauncherPlugin;
use crate::structs::{ClientState, LocalPlayer};

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
        .insert_resource(Time::<Fixed>::from_seconds(1.0 / 60.0))
        // Systèmes
        .add_systems(Startup, setup_graphics)
        .add_systems(FixedUpdate, capture_inputs.run_if(in_state(ClientState::InGame)))
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
    local_player: Res<LocalPlayer>,
) {
    // Si on n'est pas encore connecté, on ne fait rien
    let Some(conn) = server_conn.game_connection else { return };

    let mut current_input = PlayerInput::default();

    if keyboard_input.pressed(KeyCode::KeyW) || keyboard_input.pressed(KeyCode::KeyZ) { current_input.set_up(true); }
    if keyboard_input.pressed(KeyCode::KeyS) { current_input.set_down(true); }
    if keyboard_input.pressed(KeyCode::KeyA) || keyboard_input.pressed(KeyCode::KeyQ) { current_input.set_left(true); }
    if keyboard_input.pressed(KeyCode::KeyD) { current_input.set_right(true); }
    
    // On prépare le flux (Unreliable pour les inputs)
    let stream = GameStream::new(STREAM_INPUTS, GameStreamReliability::Unreliable);

    // On sérialise et on envoie directement avec la lib
    let current_input = PlayerInput::to_u8_slice(&current_input);
    let mut input_for_net : Input = [0; 16].into();
    input_for_net[0..2].copy_from_slice(&current_input);//dégueu... mais il faut avancer.
    let header = BrokerMessageHeaders::ClientInput as u8;
    let id = local_player.net_id;

    let mut packet = BytesMut::with_capacity(1 + 4 + input_for_net.len());
    packet.put_u8(header);
    packet.put_u32_le(id);
    packet.put_slice(&input_for_net);
    if let Err(e) = net.peer.send(&conn, &stream, packet.freeze()) {
        eprintln!("Erreur lors de l'envoi des inputs: {:?}", e);
    }
}