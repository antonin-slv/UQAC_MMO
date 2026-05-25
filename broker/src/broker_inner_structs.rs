use std::collections::HashMap;
use game_sockets::GameConnection;
use shared_replication::broker::{ClientId, Topic};

#[derive(Eq, Hash, PartialEq)]
pub enum ConnectionOwner {
    Client(ClientId),
    Shard(Topic),
    Spatial(),
}

pub struct ConnectionRegistry {
    pub all_co_to_id: HashMap<GameConnection, ConnectionOwner>,
    pub client_id_to_co: HashMap<ClientId, GameConnection>,
    pub server_id_to_co: HashMap<Topic, GameConnection>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        ConnectionRegistry {
            all_co_to_id: HashMap::new(),
            client_id_to_co: HashMap::new(),
            server_id_to_co: HashMap::new(),
        }
    }

    pub fn add_server(&mut self, topic: Topic, co: GameConnection) {
        self.all_co_to_id
            .insert(co.clone(), ConnectionOwner::Shard(topic));
        self.server_id_to_co.insert(topic, co);
    }

    pub fn add_client(&mut self, co: GameConnection, id: ClientId) {
        self.client_id_to_co.insert(id, co);
        self.all_co_to_id.insert(co, ConnectionOwner::Client(id));
    }

    pub fn get_by_co(&self, co: &GameConnection) -> Option<&ConnectionOwner> {
        self.all_co_to_id.get(co)
    }
    pub fn get_server_by_co(&self, co: &GameConnection) -> Option<&Topic> {
        match self.get_by_co(co) {
            Some(ConnectionOwner::Shard(topic)) => Some(topic),
            _ => None,
        }
    }
    pub fn get_client_by_co(&self, co: &GameConnection) -> Option<&ClientId> {
        match self.get_by_co(co) {
            Some(ConnectionOwner::Client(id)) => Some(id),
            _ => None,
        }
    }

    pub fn remove_by_co(&mut self, co: &GameConnection) -> Option<ConnectionOwner> {
        match self.all_co_to_id.remove(co) {
            Some(ConnectionOwner::Shard(topic)) => {
                self.server_id_to_co.remove(&topic);
                Some(ConnectionOwner::Shard(topic))
            }
            Some(ConnectionOwner::Client(id)) => {
                self.client_id_to_co.remove(&id);
                Some(ConnectionOwner::Client(id))
            }
            _ => None,
        }
    }

    pub fn remove_client(&mut self, id: ClientId) -> Option<GameConnection> {
        if let Some(co) = self.client_id_to_co.remove(&id) {
            self.all_co_to_id.remove(&co);
            return Some(co);
        }

        None
    }

    pub fn remove_server(&mut self, id: Topic) -> Option<GameConnection> {
        if let Some(co) = self.server_id_to_co.remove(&id) {
            self.all_co_to_id.remove(&co);
            return Some(co);
        }

        None
    }
}