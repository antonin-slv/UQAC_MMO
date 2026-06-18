use crate::test_broker::DummyMessage;
use broker_client::{ClientNetworkEvent, MmoNetworkClient};
use broker_protocol::broker_message::NodeIdMetaData;
use broker_protocol::topics::{Namespace, SecurityDomain, TopicBuilder};
use std::time::{Duration, Instant};
use std::{env, thread};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub fn run_broker_validation_suite<F>(public_port: u16, private_port: u16, spawn_broker: F)
where
    F: FnOnce() + Send + 'static,
{
    unsafe {
        // 1. Définition des ports pour que le Broker les lise au démarrage

        // 2. Démarrage du Broker dans un thread isolé
        thread::spawn(move || {
            env::set_var("BROKER_PUBLIC_PORT", public_port.to_string());
            env::set_var("BROKER_PRIVATE_PORT", private_port.to_string());
            spawn_broker();
        });

        // On laisse 200ms au QUIC backend pour bind les ports sur l'OS
        thread::sleep(Duration::from_millis(200));

        // ==========================================
        // PHASE 1 : CONNEXION ET WELCOME
        // ==========================================

        let mut client_joueur = MmoNetworkClient::new();
        client_joueur
            .connect("127.0.0.1", public_port)
            .expect("Erreur connexion client");

        let mut client_shard = MmoNetworkClient::new();
        client_shard
            .connect("127.0.0.1", private_port)
            .expect("Erreur connexion shard");

        let mut joueur_ready = false;
        let mut shard_ready = false;

        // Attente des messages Welcome pour valider la génération des NodeId
        for _ in 0..50 {
            if let Some(ClientNetworkEvent::Ready) = client_joueur.poll() {
                joueur_ready = true;
                assert!(
                    client_joueur.node_id.is_some(),
                    "Le joueur doit avoir un NodeId"
                );
            }
            if let Some(ClientNetworkEvent::Ready) = client_shard.poll() {
                shard_ready = true;
                assert!(
                    client_shard.node_id.unwrap().is_server(),
                    "Le Shard doit avoir un ID Serveur"
                );
            }
            if joueur_ready && shard_ready {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        assert!(
            joueur_ready,
            "Le broker n'a pas envoyé le Welcome au Joueur"
        );
        assert!(shard_ready, "Le broker n'a pas envoyé le Welcome au Shard");

        // ==========================================
        // PHASE 2 : ROUTAGE ET ABONNEMENT
        // ==========================================

        // Création d'un topic test (ex: Un joueur annonce un mouvement au serveur)
        let test_topic = TopicBuilder::new(
            SecurityDomain::PrivateReadPublicWrite,
            Namespace::SpatialInput,
        )
        .append_id(client_joueur.node_id.unwrap())
        .build();

        // 1. Le Shard s'abonne à ce topic
        client_shard.subscribe(test_topic.clone(), 0);
        thread::sleep(Duration::from_millis(50)); // Laisser le temps au broker de traiter l'abonnement

        // 2. Le Joueur s'authentifie (Sinon le pare-feu du broker va rejeter sa publication)
        // Seul un serveur peut autoriser un client
        client_shard.authorize_client(client_joueur.node_id.unwrap());
        thread::sleep(Duration::from_millis(50));

        // 3. Le Joueur publie sur le topic
        let msg = crate::test_broker::DummyMessage { payload: 42 };
        client_joueur.publish_unreliable(test_topic, &msg);

        // ==========================================
        // PHASE 3 : VÉRIFICATION DE LA RÉCEPTION
        // ==========================================

        let mut message_recu = false;
        for _ in 0..50 {
            if let Some(ClientNetworkEvent::DataReceived { client_id, .. }) = client_shard.poll() {
                // Le Broker doit avoir injecté correctement l'ID du joueur expéditeur !
                assert_eq!(
                    client_id,
                    client_joueur.node_id.unwrap(),
                    "L'identité de l'expéditeur a été perdue en route"
                );
                message_recu = true;
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        assert!(message_recu, "Le routage du message a échoué");
    }
}
pub fn run_distributed_stress_test<F>(
    public_port: u16,
    private_port: u16,
    spawn_broker: F,
    num_shards: usize,
    players_per_shard: usize,
    msgs_per_player: usize,
    tickrate_ms: u64,
) where
    F: FnOnce() + Send + 'static,
{
    let total_benchmark_start = Instant::now();

    // ========================================================
    // 1. DÉMARRAGE DU BROKER
    // ========================================================
    let step_start = Instant::now();
    thread::spawn(move || {
        spawn_broker();
    });
    // Attente courte juste pour laisser le socket bind.
    thread::sleep(Duration::from_millis(50));
    println!("⏱ [Étape 1] Broker démarré en {:?}", step_start.elapsed());

    // ========================================================
    // 2. CONNEXION DES SHARDS (Réseau Privé)
    // ========================================================
    let step_start = Instant::now();
    let mut shards = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        let mut shard = MmoNetworkClient::new();
        shard.comment_enabled = false;
        shard.connect("127.0.0.1", private_port).expect("Échec connexion Shard");
        shards.push(shard);
    }
    println!("⏱ [Étape 2] {} Shards instanciés en {:?}", num_shards, step_start.elapsed());

    // ========================================================
    // 3. CONNEXION DES JOUEURS (Réseau Public)
    // ========================================================
    let step_start = Instant::now();
    let total_players = num_shards * players_per_shard;
    let mut players = Vec::with_capacity(total_players);
    for _ in 0..total_players {
        let mut p = MmoNetworkClient::new();
        p.comment_enabled = false;
        p.connect("127.0.0.1", public_port).expect("Échec connexion Joueur");
        players.push(p);
    }
    println!("⏱ [Étape 3] {} Joueurs instanciés en {:?}", total_players, step_start.elapsed());

    // ========================================================
    // 4. HANDSHAKE QUIC (Attente active et sécurisée)
    // ========================================================
    let step_start = Instant::now();
    let handshake_timeout = Duration::from_secs(10);

    for shard in &mut shards {
        let start_wait = Instant::now();
        loop {
            if let Some(ClientNetworkEvent::Ready) = shard.poll() { break; }
            if start_wait.elapsed() > handshake_timeout { panic!("Timeout Handshake Shard !"); }
            thread::sleep(Duration::from_millis(1));
        }
    }
    for p in &mut players {
        let start_wait = Instant::now();
        loop {
            if let Some(ClientNetworkEvent::Ready) = p.poll() { break; }
            if start_wait.elapsed() > handshake_timeout { panic!("Timeout Handshake Joueur !"); }
            thread::sleep(Duration::from_millis(1));
        }
    }
    println!("⏱ [Étape 4] Tous les Handshakes QUIC terminés en {:?}", step_start.elapsed());

    // ========================================================
    // 5. TOPICS, ABONNEMENTS ET AUTORISATIONS
    // ========================================================
    let step_start = Instant::now();
    let mut topics = Vec::with_capacity(num_shards);

    for shard in &mut shards {
        let shard_id = shard.node_id.expect("Shard doit avoir un NodeId");
        let topic = TopicBuilder::new(SecurityDomain::PrivateReadPublicWrite, Namespace::SpatialInput)
            .append_id(shard_id)
            .build();

        shard.subscribe(topic.clone(), 0);
        topics.push(topic);
    }

    // Le Shard 0 valide tous les joueurs
    let mut num = 0;
    for p in players.iter() {
        let player_id = p.node_id.expect("Le joueur doit avoir un NodeId");
        shards[ num % num_shards].authorize_client(player_id);
        num += 1;
    }

    // Petite temporisation active pour vider les buffers d'abonnements/auth
    for shard in &mut shards {
        while let Some(_) = shard.poll() {}
    }
    for p in &mut players {
        while let Some(_) = p.poll() {}
    }
    println!("⏱ [Étape 5] Abonnements et Auth validés en {:?}", step_start.elapsed());

    // ========================================================
    // 🔥 6. BENCHMARK DISTRIBUÉ
    // ========================================================
    let total_messages_expected = total_players * msgs_per_player;
    let message_rate = (1000 * total_players as u64) / tickrate_ms;
    let max_timeout = Duration::from_secs((msgs_per_player as u64 * tickrate_ms / 1000) + 5);

    println!(
        "\n🚀 Lancement du test : {} Joueurs, {} Shards.",
        total_players, num_shards
    );
    println!(
        "🎯 Objectif : {} messages ({} msg/s) | Timeout : {}s",
        total_messages_expected, message_rate, max_timeout.as_secs()
    );

    let bench_start_time = Instant::now();

    // --- Lancement des Threads Lecteurs (Shards) ---
    // Utilisation d'un compteur atomique partagé entre tous les lecteurs
    let received_counter = Arc::new(AtomicUsize::new(0));
    let mut shard_threads = vec![];

    for mut shard in shards {
        let counter_clone = Arc::clone(&received_counter);
        shard_threads.push(thread::spawn(move || {
            let start = Instant::now();
            loop {
                while let Some(ClientNetworkEvent::DataReceived { .. }) = shard.poll() {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                }

                // Condition de sortie locale par timeout pour ne pas zombifier le thread
                if start.elapsed() > max_timeout { break; }
                thread::sleep(Duration::from_micros(5)); // Respiration CPU
            }
        }));
    }

    // --- Lancement des Threads Écrivains (Joueurs) ---
    let mut player_threads = vec![];
    let mut player_idx = 0;

    for mut p in players {
        let assigned_shard_idx = player_idx % num_shards;
        let target_topic = topics[assigned_shard_idx].clone();

        player_threads.push(thread::spawn(move || {
            // Étaler légèrement le démarrage pour éviter un pic irréaliste au millième de seconde près
            thread::sleep(Duration::from_millis(player_idx as u64 % tickrate_ms));

            for i in 0..msgs_per_player {
                p.publish_reliable(target_topic.clone(), &DummyMessage { payload: i as u32 });
                thread::sleep(Duration::from_millis(tickrate_ms));
                // Vider le buffer du client pour éviter l'accumulation
                while let Some(_) = p.poll() {}
            }
        }));
        player_idx += 1;
    }

    // --- Boucle de surveillance principale ---
    loop {
        let current_count = received_counter.load(Ordering::Relaxed);

        if current_count >= total_messages_expected {
            break;
        }

        if bench_start_time.elapsed() > max_timeout {
            println!("❌ TIMEOUT ! Le broker a saturé.");
            println!("📥 Reçu : {} / {}", current_count, total_messages_expected);
            std::process::exit(1); // Arrêt propre au lieu d'un panic brutal
        }

        thread::sleep(Duration::from_millis(50)); // Pas besoin de poll agressivement ici
    }

    let bench_elapsed = bench_start_time.elapsed();

    // Attente propre de la fin des threads joueurs (les shards mourront par timeout ou à la fin du process)
    for handle in player_threads {
        let _ = handle.join();
    }

    println!("\n✅ SUCCÈS ! Tous les messages reçus ({}/{})", total_messages_expected, total_messages_expected);
    println!("⏱ Temps du Benchmark pur : {:?}", bench_elapsed);
    println!("⏱ Temps total d'exécution : {:?}", total_benchmark_start.elapsed());
}
