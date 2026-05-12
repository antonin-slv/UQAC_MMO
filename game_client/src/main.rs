use bevy::prelude::*;
use quinn::{ClientConfig, Endpoint};
use std::sync::Arc;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use shared_replication::{PersonalSnapshot, PlayerInput};
// --- COMPOSANTS BEVY ---

#[derive(Component)]
struct NetworkEntity(u64);

#[derive(Resource)]
struct SnapshotReceiver(UnboundedReceiver<PersonalSnapshot>);

#[derive(Bundle)]
pub struct CameraBundle {
    pub camera: Camera2d,
    pub position: Transform,
}

#[derive(Bundle)]
pub struct PlayerBundle {
    // Visuals
    pub mesh: Mesh2d,
    pub material: MeshMaterial2d<ColorMaterial>,
    pub transform: Transform,
}

#[derive(Resource)]
struct InputSender(mpsc::UnboundedSender<PlayerInput>);


fn main() {
    // 1. Canal de communication (Réseau -> Bevy & Bevy -> réseau)
    let (tx_net_to_bevy, rx_net_to_bevy) = mpsc::unbounded_channel::<PersonalSnapshot>();
    let (tx_bevy_to_net, mut rx_bevy_to_net) = mpsc::unbounded_channel::<PlayerInput>();
    // 2. Lancement du thread Réseau (Tokio)
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            println!("[Client] Chargement du certificat...");

            // On lit le certificat généré par le serveur
            let cert_file = std::fs::read("server_cert.der").expect("Certificat introuvable !");
            let mut roots = rustls::RootCertStore::empty();
            roots.add(cert_file.into()).unwrap();

            let client_config = ClientConfig::with_root_certificates(Arc::new(roots));
            let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
            endpoint.set_default_client_config(client_config.expect("REASON"));

            println!("[Client] Connexion au serveur...");
            let connection = endpoint
                .connect("127.0.0.1:5000".parse().unwrap(), "localhost")
                .unwrap()
                .await
                .unwrap();

            println!("[Client] Connecté ! En attente de snapshots...");


            let connection_read = connection.clone();
            let connection_write = connection;

            // --- TÂCHE D'ENVOI (Bevy -> Réseau) ---
            tokio::spawn(async move {
                // On écoute ce que Bevy nous envoie
                while let Some(input) = rx_bevy_to_net.recv().await {
                    // On le transforme en octets
                    if let Ok(bytes) = bincode::serialize(&input) {
                        // Et on l'envoie direct au serveur en UDP !
                        let _ = connection_write.send_datagram(bytes.into());
                    }
                }
            });

            // --- TÂCHE DE RÉCEPTION (Réseau -> Bevy) ---
            loop {
                if let Ok(bytes) = connection_read.read_datagram().await {
                    if let Ok(snapshot) = bincode::deserialize::<PersonalSnapshot>(&bytes) {
                        let _ = tx_net_to_bevy.send(snapshot);
                    }
                }
            }
        });
    });

    // 3. Lancement de Bevy (Affichage)
    App::new()
        .add_plugins(DefaultPlugins)
        .insert_resource(SnapshotReceiver(rx_net_to_bevy))
        .insert_resource(InputSender(tx_bevy_to_net))
        .add_systems(Startup, setup_graphics)
        .add_systems(Update, (process_snapshots, capture_inputs))
        .run();
}

// Configuration d'une caméra simple
fn setup_graphics(mut commands: Commands) {
    commands.spawn(CameraBundle {
        position: Transform::from_xyz(0.0, 50.0, 50.0).looking_at(Vec3::ZERO, Vec3::Y),
        camera: Camera2d::default(),
    });
}

// Le système qui met à jour le monde visuel à partir du réseau
fn process_snapshots(
    mut receiver: ResMut<SnapshotReceiver>,
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
    mut query_net_entities: Query<(Entity, &NetworkEntity, &mut Transform)>,
) {

    let mut latest_snapshot = None;
    while let Ok(snapshot) = receiver.0.try_recv() {
        latest_snapshot = Some(snapshot);
    }

    //return si pas de Snapshot
    let Some(snapshot) = latest_snapshot else { return };

    for net_entity in snapshot.entities {
        // 1. On cherche l'entité existante avec un itérateur
        let existing_entity = query_net_entities
            .iter_mut()
            .find(|(_, existing_id, _)| existing_id.0 == net_entity.network_id);

        if let Some((_, _, mut transform)) = existing_entity {
            transform.translation.x = net_entity.position[0];
            transform.translation.z = net_entity.position[1];
            continue; // EARLY "RETURN"
        }

        // 4. Si on arrive ici, c'est garanti que l'entité n'existait pas !
        println!("Nouvelle entité réseau découverte : {}", net_entity.network_id);

        commands.spawn((
            PlayerBundle {
                mesh: Mesh2d(meshes.add(Circle::new(10.0))),
                material: MeshMaterial2d(materials.add(Color::srgb(0.2, 0.7, 0.9))),
                transform: Transform::from_xyz(net_entity.position[0], 0.5, net_entity.position[1])
            },
            NetworkEntity(net_entity.network_id),
        ));
    }

}


fn capture_inputs(
    keyboard_input: Res<ButtonInput<KeyCode>>,
    sender: Res<InputSender>
) {
    let mut current_input = PlayerInput::default();

    if keyboard_input.pressed(KeyCode::KeyW) { current_input.up = true; }
    if keyboard_input.pressed(KeyCode::KeyZ) { current_input.up = true; }
    if keyboard_input.pressed(KeyCode::KeyS) { current_input.down = true; }
    if keyboard_input.pressed(KeyCode::KeyA) { current_input.left = true; }
    if keyboard_input.pressed(KeyCode::KeyQ) { current_input.left = true; }
    if keyboard_input.pressed(KeyCode::KeyD) { current_input.right = true; }

    sender.0.send(current_input.clone()).expect("TODO: panic message");
}