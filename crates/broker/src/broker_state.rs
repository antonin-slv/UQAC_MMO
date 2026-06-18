use crate::broker_core::FastIterableSet;
use broker_protocol::broker_message::{NodeId, NodeIdMetaData};
use broker_protocol::topics::Topic;
use dashmap::{DashMap, DashSet};
use game_sockets::{GameConnection, GameNetworkEvent};
use std::sync::atomic::{AtomicU32, Ordering};
use rustc_hash::FxBuildHasher;
use uuid::Uuid;

pub type FastDashMap<K, V> = DashMap<K, V, FxBuildHasher>;


pub enum NetworkInterface {
    Public,
    Private,
}

pub struct IngressEvent {
    pub interface: NetworkInterface,
    pub event: GameNetworkEvent,
}

// L'état partagé entre toutes tes tâches de traitement
pub struct SharedBrokerState {
    pub log_enabled: bool,
    pub uuid_to_node_id: FastDashMap<Uuid, NodeId>,
    pub client_connections: FastDashMap<NodeId, GameConnection>,
    pub server_connections: FastDashMap<NodeId, GameConnection>,

    // Pare-feu (DashSet est l'équivalent thread-safe d'un HashSet)
    pub not_authenticated_clients: DashSet<NodeId>,

    // Abonnements
    pub client_subscribers: FastDashMap<Topic, FastIterableSet<NodeId>>,
    pub server_subscribers: FastDashMap<Topic, FastIterableSet<NodeId>>,
    pub node_id_to_topics: FastDashMap<NodeId, FastIterableSet<Topic>>,

    // Compteurs atomiques pour les IDs
    next_client_id: AtomicU32,
    next_server_id: AtomicU32,
}

impl SharedBrokerState {
    pub(crate) fn new() -> SharedBrokerState {
        Self {
            uuid_to_node_id: FastDashMap::default(),
            client_connections: FastDashMap::default(),
            server_connections: FastDashMap::default(),
            not_authenticated_clients: DashSet::new(),
            client_subscribers: FastDashMap::default(),
            server_subscribers: FastDashMap::default(),
            node_id_to_topics: FastDashMap::default(),

            next_client_id: AtomicU32::new(NodeId::FIRST_CLIENT_ID),
            next_server_id: AtomicU32::new(NodeId::FIRST_SERVER_ID),
            log_enabled: true,
        }
    }

    pub fn generate_client_id(&self) -> NodeId {
        self.next_client_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn generate_server_id(&self) -> NodeId {
        self.next_server_id.fetch_add(1, Ordering::Relaxed)
    }

    // --- LOGIQUE D'ABONNEMENT REVISITÉE POUR DASHMAP ---
    pub fn node_subscribe(&self, node_id: NodeId, topic: Topic) {
        let map = if node_id.is_server() {
            &self.server_subscribers
        } else {
            &self.client_subscribers
        };

        // On utilise l'API entry de DashMap pour insérer/mettre à jour en toute sécurité
        map.entry(topic.clone())
            .or_insert_with(|| FastIterableSet::with_capacity(16))
            .value_mut() // Accès mutable au FastIterableSet interne
            .insert(node_id);

        self.node_id_to_topics
            .entry(node_id)
            .or_insert_with(|| FastIterableSet::with_capacity(16))
            .value_mut()
            .insert(topic);
    }

    pub fn node_unsubscribe(&self, node_id: NodeId, topic: Topic) {
        let map = if node_id.is_server() {
            &self.server_subscribers
        } else {
            &self.client_subscribers
        };

        let mut remove_topic = false;
        if let Some(mut subscribers) = map.get_mut(&topic) {
            if subscribers.swap_remove(&node_id) && subscribers.is_empty() {
                remove_topic = true;
            }
        }
        if remove_topic {
            map.remove(&topic);
        }

        let mut remove_node = false;
        if let Some(mut topics) = self.node_id_to_topics.get_mut(&node_id) {
            topics.swap_remove(&topic);
            if topics.is_empty() {
                remove_node = true;
            }
        }
        if remove_node {
            self.node_id_to_topics.remove(&node_id);
        }
    }
}
