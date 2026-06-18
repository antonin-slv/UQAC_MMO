use game_sockets::protocols::QuicBackend;
use game_sockets::{GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_reliable_routing_same_stream_id() {
    // 1. Setup du Serveur
    let mut server = GamePeer::new(QuicBackend::new());
    server.listen("127.0.0.1", 9001).unwrap(); // Port 9001 pour éviter les conflits avec l'autre test

    // ========================================================
    // 2. Setup du Client A (L'expéditeur)
    // ========================================================
    let mut client_a = GamePeer::new(QuicBackend::new());
    client_a.connect("127.0.0.1", 9001).unwrap();

    // Serveur intercepte Client A
    let mut server_conn_a = None;
    for _ in 0..20 {
        if let Ok(Some(GameNetworkEvent::Connected(conn))) = server.poll() {
            server_conn_a = Some(conn);
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    let server_conn_a = server_conn_a.expect("Le serveur n'a pas vu le Client A");

    // Client A confirme
    let mut client_a_local_conn = None;
    for _ in 0..19 {
        if let Ok(Some(GameNetworkEvent::Connected(conn))) = client_a.poll() {
            client_a_local_conn = Some(conn);
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    let client_a_local_conn = client_a_local_conn.expect("Client A n'a pas pu se connecter");

    // ========================================================
    // 3. Setup du Client B (Le destinataire)
    // ========================================================
    let mut client_b = GamePeer::new(QuicBackend::new());
    client_b.connect("127.0.0.1", 9001).unwrap();

    // Serveur intercepte Client B
    let mut server_conn_b = None;
    for _ in 0..22 {
        if let Ok(Some(GameNetworkEvent::Connected(conn))) = server.poll() {
            server_conn_b = Some(conn);
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    let server_conn_b = server_conn_b.expect("Le serveur n'a pas vu le Client B");

    // Client B confirme
    let mut client_b_local_conn = None;
    for _ in 0..21 {
        if let Ok(Some(GameNetworkEvent::Connected(conn))) = client_b.poll() {
            client_b_local_conn = Some(conn);
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    let _ = client_b_local_conn.expect("Client B n'a pas pu se connecter");

    // ========================================================
    // 4. Préparation des Streams (ID partagé = 0)
    // ========================================================
    let shared_stream_id = 0;
    let stream_def = GameStream::new(shared_stream_id, GameStreamReliability::Reliable);

    // Client A ouvre le stream vers le Serveur
    client_a.create_stream(client_a_local_conn.clone(), GameStreamReliability::Reliable, shared_stream_id).unwrap();

    // Le Serveur ouvre un stream indépendant (mais de même ID) vers le Client B
    server.create_stream(server_conn_b.clone(), GameStreamReliability::Reliable, shared_stream_id).unwrap();

    // Laisser le temps à QUIC de négocier les streams
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // ========================================================
    // 5. Action : A envoie au Serveur
    // ========================================================
    let text = "Payload_from_A_to_B";
    client_a.send(&client_a_local_conn, &stream_def, bytes::Bytes::from(text)).unwrap();

    // ========================================================
    // 6. Action : Le Serveur route vers B
    // ========================================================
    let mut server_routed = false;
    for _ in 0..20 {
        if let Ok(Some(event)) = server.poll() {
            match event {
                GameNetworkEvent::Message { connection, stream, data } => {
                    // Vérification de sécurité de notre test
                    assert_eq!(connection, server_conn_a, "Le message doit venir de A");
                    assert_eq!(stream.real_stream_id(), shared_stream_id, "Mauvais ID de stream");

                    println!("Serveur: Message reçu de A, routage vers B en cours...");

                    // LE ROUTAGE : On renvoie les mêmes données, sur le même ID de stream, vers la connexion B
                    server.send(&server_conn_b, &stream, data).unwrap();
                    server_routed = true;
                    break;
                }
                _ => {}
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    assert!(server_routed, "Le serveur n'a jamais reçu le message de A pour le router");

    // ========================================================
    // 7. Validation : B reçoit le message
    // ========================================================
    let mut received_by_b = None;
    for _ in 0..20 {
        if let Ok(Some(event)) = client_b.poll() {
            if let GameNetworkEvent::Message { connection: _, stream, data } = event {
                assert_eq!(stream.real_stream_id(), shared_stream_id, "B doit recevoir sur le stream 0");
                received_by_b = Some(String::from_utf8_lossy(&data).to_string());
                println!("Client B: Message reçu avec succès !");
                break;
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    assert_eq!(
        received_by_b.expect("Le Client B n'a jamais reçu le message routé"),
        text
    );
}