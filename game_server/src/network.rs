// server/src/network.rs
use bevy::prelude::*;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tokio::sync::mpsc;

use quinn::{Connection, Endpoint, ServerConfig};
use rustls::pki_types::PrivateKeyDer;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use shared_replication::{NetworkEvent, PlayerInput, ServerMessage};
use crate::events;
const SERVER_PORT: &str = "5000";

// Tes ressources Bevy
#[derive(Resource)]
pub struct NetworkReceiver(pub UnboundedReceiver<NetworkEvent>);

#[derive(Resource)]
pub struct NetworkSender(pub UnboundedSender<ServerMessage>);

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App
    ) {
        // 1. Création des channels (
        let (tx_net_to_bevy, rx_net_to_bevy) = mpsc::unbounded_channel::<NetworkEvent>();
        let (tx_bevy_to_net, rx_bevy_to_net) = mpsc::unbounded_channel::<ServerMessage>();

        // 2. Lancement du thread Tokio
        std::thread::spawn(move || {
            network_task(tx_net_to_bevy, rx_bevy_to_net);
        });

        // 3. On insère les ressources dans l'app
        app
            .insert_resource(NetworkReceiver(rx_net_to_bevy))
            .insert_resource(NetworkSender(tx_bevy_to_net))
            .add_message::<events::PlayerConnected>()
            .add_message::<events::PlayerDisconnected>()
            .add_message::<events::PlayerInputEvent>();

        // 2. On ajoute le système Pont
        app.add_systems(PreUpdate, network_to_events_bridge);

        // Autres systèmes réseau ici.
    }
}

#[tokio::main]
/* #[tokio::main] ==
tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { MA FONCTION });

-> On passe par tokio pour créer notre thread réseau + un environnement qu'il peut utiliser
 */
async fn network_task(
    tx_net_to_bevy: UnboundedSender<NetworkEvent>,
    rx_bevy_to_net: UnboundedReceiver<ServerMessage>
){
    println!("[Serveur] Génération du certificat...");
    let server_config = generate_certificat();

    let endpoint = Endpoint::server(server_config, ("0.0.0.0:".to_owned() + SERVER_PORT).parse().unwrap()).unwrap();
    println!("[Serveur] Prêt et en écoute sur UDP {SERVER_PORT}");

    let active_connections = Arc::new(Mutex::new(HashMap::<u64, Connection>::new()));

    // --- Tâche d'ENVOI ---
    let active_connections_tx = active_connections.clone();

    //on créé une tache asynchrone != nouveau thread, donc on est heureux.
    //-> comme #pragma omp task { ...}
    tokio::spawn(async move {
        send_data_loop(active_connections_tx, rx_bevy_to_net).await;
    });

    // --- Tâche de RÉCEPTION ---
    reception_loop(tx_net_to_bevy, endpoint, active_connections).await;
}

pub fn generate_certificat() -> ServerConfig {
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
    let cert = params.self_signed(&key_pair).unwrap();

    let cert_der = cert.der().to_vec();
    let priv_key = key_pair.serialize_der();

    // On sauvegarde le cert pour que notre Client puisse le récupérer
    std::fs::write("server_cert.der", &cert_der).unwrap();

    let cert_chain = vec![cert_der.into()];
    let key = PrivateKeyDer::Pkcs8(priv_key.into());
    let server_config = ServerConfig::with_single_cert(cert_chain, key).unwrap();
    server_config
}

//exiting this function will close the network thread.
async fn reception_loop(
    tx_net_to_bevy: UnboundedSender<NetworkEvent>,
    endpoint: Endpoint,
    active_connections: Arc<Mutex<HashMap<u64, Connection>>>
) {
    let mut next_client_id = 1;
    while let Some(incoming) = endpoint.accept().await {
        let tx_net = tx_net_to_bevy.clone();
        let active_connections_rx = active_connections.clone();
        let client_id = next_client_id;
        next_client_id += 1;

        tokio::spawn(async move {
            match incoming.await {
                Ok(connection) => {
                    active_connections_rx.lock().unwrap().insert(client_id, connection.clone());
                    let _ = tx_net.send(NetworkEvent::PlayerConnected(client_id));

                    loop {
                        match connection.read_datagram().await {
                            Ok(_bytes) => {
                                // WARNING : Si le client envoit autre chose... catastrophe.
                                let player_input : PlayerInput = bincode::deserialize(&_bytes).unwrap_or_else(|_| {
                                    println!("[Serveur] Erreur de désérialisation du message du client {client_id}");
                                    PlayerInput { up: false, down: false, left: false, right: false }
                                });
                                let _ = tx_net.send(NetworkEvent::PlayerInput(client_id, player_input));
                            }
                            Err(_) => {
                                println!("[Serveur] Joueur {} déconnecté", client_id);
                                active_connections_rx.lock().unwrap().remove(&client_id);
                                let _ = tx_net.send(NetworkEvent::PlayerDisconnected(client_id));
                                break;
                            }
                        }
                    }
                }
                Err(e) => eprintln!("[Serveur] Échec de la connexion: {}", e),
            }
        });
    }
}

async fn send_data_loop(
    active_connections_tx: Arc<Mutex<HashMap<u64, Connection>>>,
    mut rx: UnboundedReceiver<ServerMessage>)
{
    while let Some(msg) = rx.recv().await {
        let connections = active_connections_tx.lock().unwrap();
        match msg {
            ServerMessage::SendTo(client_id, bytes) => {
                if let Some(conn) = connections.get(&client_id) {
                    let _ = conn.send_datagram(bytes.into());
                }
            }
            ServerMessage::Broadcast(bytes) => {
                for conn in connections.values() {
                    let _ = conn.send_datagram(bytes.clone().into());
                }
            }
        }
    }
}

fn network_to_events_bridge(
    mut receiver: ResMut<NetworkReceiver>,
    mut ev_connected: MessageWriter<events::PlayerConnected>,
    mut ev_disconnected: MessageWriter<events::PlayerDisconnected>,
    mut ev_input: MessageWriter<events::PlayerInputEvent>,
) {
    // On vide le buffer réseau de cette frame
    while let Ok(net_event) = receiver.0.try_recv() {
        match net_event {
            NetworkEvent::PlayerConnected(id) => {
                ev_connected.write(events::PlayerConnected { client_id: id });
            }
            NetworkEvent::PlayerDisconnected(id) => {
                ev_disconnected.write(events::PlayerDisconnected { client_id: id });
            }
            NetworkEvent::PlayerInput(id, data) => {
                ev_input.write(events::PlayerInputEvent { client_id: id, input_data: data });
            }
        }
    }
}