use std::{env, thread};
use std::time::Duration;
use broker_client::{ClientNetworkEvent, MmoNetworkClient};
use broker_protocol::topic_patterns::TopicPattern;
use broker_protocol::topics::{Namespace, SecurityDomain, TopicBuilder};

/// Scénario de test : Shard s'abonne à un bloc 2x2 de GameChunks
pub fn run_broker_batch_suite<F>(public_port: u16, private_port: u16, spawn_broker: F)
where
    F: FnOnce() + Send + 'static,
{
    unsafe {
        // 1. Lancement du Broker
        env::set_var("BROKER_PUBLIC_PORT", public_port.to_string());
        env::set_var("BROKER_PRIVATE_PORT", private_port.to_string());
        thread::spawn(move || {
            spawn_broker();
        });
        thread::sleep(Duration::from_millis(200));

        // 2. Connexion du Shard (Privé) et du Joueur (Public)
        let mut client_joueur = MmoNetworkClient::new();
        client_joueur.connect("127.0.0.1", public_port).unwrap();

        let mut client_shard = MmoNetworkClient::new();
        client_shard.connect("127.0.0.1", private_port).unwrap();

        let mut joueur_ready = false;
        let mut shard_ready = false;

        for _ in 0..50 {
            if let Some(ClientNetworkEvent::Ready) = client_joueur.poll() {
                joueur_ready = true;
            }
            if let Some(ClientNetworkEvent::Ready) = client_shard.poll() {
                shard_ready = true;
            }
            if joueur_ready && shard_ready {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        // Le joueur a besoin de son badge d'authentification pour publier
        let joueur_id = client_joueur.node_id.unwrap();
        client_shard.authorize_client(joueur_id);
        thread::sleep(Duration::from_millis(50));

        // ==========================================
        // ACTION 1 : BATCH SUBSCRIBE
        // ==========================================

        // Le Shard s'abonne à un carré de Chunks : X allant de 0 à 1, Y allant de 0 à 1
        // Le Pattern générera 4 topics : (0,0), (0,1), (1,0), (1,1)
        let pattern = TopicPattern::new()
            .with_head(Namespace::Chunk, SecurityDomain::PrivateReadPublicWrite)
            .with_single_layer(0i16..=1i16) // RangeI16 pour X
            .with_single_layer(0i16..=1i16); // RangeI16 pour Y

        client_shard.batch_subscribe(pattern.clone(), 0);
        thread::sleep(Duration::from_millis(50));

        // ==========================================
        // TEST 1 : Publication DANS la zone (Doit être reçu)
        // ==========================================

        // Le joueur publie sur le Chunk (1, 0)
        let topic_in_zone =
            TopicBuilder::new(SecurityDomain::PrivateReadPublicWrite, Namespace::Chunk)
                .append(&1i16.to_le_bytes())
                .append(&0i16.to_le_bytes())
                .build();

        client_joueur.publish_reliable(topic_in_zone, &crate::test_broker::DummyMessage { payload: 1 });

        let mut received_in_zone = false;
        for _ in 0..50 {
            if let Some(ClientNetworkEvent::DataReceived { .. }) = client_shard.poll() {
                received_in_zone = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            received_in_zone,
            "Le Shard aurait dû recevoir le message du Chunk (1,0)"
        );

        // ==========================================
        // TEST 2 : Publication HORS zone (Ne doit PAS être reçu)
        // ==========================================

        // Le joueur publie sur le Chunk (2, 0) - En dehors du pattern !
        let topic_out_of_zone =
            TopicBuilder::new(SecurityDomain::PrivateReadPublicWrite, Namespace::Chunk)
                .append(&2i16.to_le_bytes())
                .append(&0i16.to_le_bytes())
                .build();

        client_joueur.publish_reliable(topic_out_of_zone, &crate::test_broker::DummyMessage { payload: 2 });

        let mut received_out_of_zone = false;
        for _ in 0..20 {
            // Boucle plus courte pour prouver l'absence de message
            if let Some(ClientNetworkEvent::DataReceived { .. }) = client_shard.poll() {
                received_out_of_zone = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !received_out_of_zone,
            "SÉCURITÉ: Le Shard a reçu un message du Chunk (2,0) auquel il n'est pas abonné"
        );

        // ==========================================
        // ACTION 2 : BATCH UNSUBSCRIBE
        // ==========================================

        client_shard.batch_unsubscribe(pattern, 0);
        thread::sleep(Duration::from_millis(50));

        // ==========================================
        // TEST 3 : Publication DANS la zone après désabonnement
        // ==========================================

        // Le joueur republie sur le Chunk (1, 0)
        client_joueur.publish_reliable(topic_in_zone, &crate::test_broker::DummyMessage { payload: 3 });

        let mut received_after_unsub = false;
        for _ in 0..20 {
            if let Some(ClientNetworkEvent::DataReceived { .. }) = client_shard.poll() {
                received_after_unsub = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !received_after_unsub,
            "FUITE: Le Shard a encore reçu le message après le BatchUnsubscribe"
        );
    }
}