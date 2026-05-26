use game_sockets::GameConnection;
use rustc_hash::FxHashMap;
use shared_replication::broker::{ClientId, Topic};

#[derive(Eq, Hash, PartialEq)]
pub enum ConnectionOwner {
    Client(ClientId),
    Shard(Topic),
    Orchestrator(),
    Spatial(),
}

pub struct ClientIdGenerator {
    next_id: ClientId,
}

impl ClientIdGenerator {
    pub fn next(&mut self) -> ClientId {
        let id = self.next_id;
        self.next_id += 1;
        id - 1
    }
}

pub struct BrokerState {
    id_generator: ClientIdGenerator,

    connection_to_topic: FxHashMap<GameConnection, Vec<Topic>>,
    topic_to_connection: FxHashMap<Topic, Vec<GameConnection>>,

    client_authoritative_location: FxHashMap<ClientId, Topic>,

    all_co_to_id: FxHashMap<GameConnection, ConnectionOwner>,
    client_id_to_co: FxHashMap<ClientId, GameConnection>,
    server_id_to_co: FxHashMap<Topic, GameConnection>,
    pub spatial_server_id: Option<GameConnection>,
    pub orchestrator_server_id: Option<GameConnection>,
}

impl BrokerState {
    pub fn is_trusted(&self, connection: &GameConnection) -> bool {
        match self.all_co_to_id.get(connection) {
            None => false,
            Some(ConnectionOwner::Client(_)) => false,
            Some(ConnectionOwner::Shard(_)) => true,
            Some(ConnectionOwner::Spatial()) => true,
            _ => false,
        }
    }

    pub fn new() -> Self {
        BrokerState {
            id_generator: ClientIdGenerator { next_id: 1 },
            connection_to_topic: FxHashMap::default(),
            topic_to_connection: FxHashMap::default(),

            client_authoritative_location: FxHashMap::default(),
            all_co_to_id: FxHashMap::default(),
            client_id_to_co: FxHashMap::default(),
            server_id_to_co: FxHashMap::default(),
            spatial_server_id: None,
            orchestrator_server_id: None,
        }
    }
    pub fn get_random_server_id(&mut self) -> Option<Topic> {
        self.server_id_to_co.keys().next().cloned()
    }

    pub fn get_random_server_connection(&mut self) -> Option<GameConnection> {
        self.server_id_to_co.values().next().cloned()
    }
    pub fn get_subscribers(&self, p0: &Topic) -> Option<&Vec<GameConnection>> {
        self.topic_to_connection.get(p0)
    }

    pub fn get_subscriptions(&self, p0: &GameConnection) -> Option<&Vec<Topic>> {
        self.connection_to_topic.get(p0)
    }
    pub fn subscribe_client(&mut self, client_id: ClientId, topic: Topic) -> Option<()> {
        let co = self.client_id_to_co.get(&client_id)?;
        self.connection_to_topic
            .entry(co.clone())
            .or_insert_with(Vec::new)
            .push(topic.clone());
        self.topic_to_connection
            .entry(topic.clone())
            .or_insert_with(Vec::new)
            .push(co.clone());
        Some(())
    }

    //returns the old authoritative pocket
    pub fn set_client_authoritative_location(
        &mut self,
        client_id: ClientId,
        topic: Topic,
    ) -> Option<Topic> {
        self.client_authoritative_location.insert(client_id, topic)
    }

    pub fn get_client_authoritative_location(&self, client_id: ClientId) -> Option<&Topic> {
        self.client_authoritative_location.get(&client_id)
    }

    pub fn unsubscribe_client(&mut self, client_id: ClientId, topic: Topic) -> Option<()> {
        let co = self.client_id_to_co.get(&client_id)?;

        let topic_array = self.connection_to_topic.get_mut(co)?;

        let index = topic_array.iter().position(|t| t == &topic)?;
        topic_array.swap_remove(index);

        if topic_array.is_empty() {
            self.connection_to_topic.remove(co);
        }

        let connexions_of_topic = self.topic_to_connection.get_mut(&topic)?;
        let index = connexions_of_topic.iter().position(|t| t == co)?;
        connexions_of_topic.swap_remove(index);
        if connexions_of_topic.is_empty() {
            self.topic_to_connection.remove(&topic);
        }
        Some(())
    }

    pub fn subscribe_connexion(&mut self, co: GameConnection, topic: Topic) {
        //vérification d'unicité :
        let topics = self.connection_to_topic.get_mut(&co);
        if let Some(topics) = topics {
            if topics.contains(&topic) {
                return;
            }
        }
        self.connection_to_topic
            .entry(co.clone())
            .or_insert_with(Vec::new)
            .push(topic.clone());
        self.topic_to_connection
            .entry(topic.clone())
            .or_insert_with(Vec::new)
            .push(co.clone());
    }

    pub fn unsubscribe_connexion_from(&mut self, co: GameConnection, topic: Topic) -> Option<()> {
        let topics = self.connection_to_topic.get_mut(&co)?;
        let index = topics.iter().position(|t| t == &topic)?;
        topics.swap_remove(index);
        let connections = self.topic_to_connection.get_mut(&topic)?;
        let index = connections.iter().position(|c| *c == co)?;
        connections.swap_remove(index);
        Some(())
    }
    pub fn unsubscribe_connexion_all(&mut self, co: GameConnection) -> Option<()> {
        let topics = self.connection_to_topic.remove(&co)?;
        for topic in topics {
            let connections = self.topic_to_connection.get_mut(&topic)?;
            let index = connections.iter().position(|c| *c == co)?;
            connections.swap_remove(index);
            if connections.is_empty() {
                self.topic_to_connection.remove(&topic);
            }
        }
        Some(())
    }

    pub fn add_server(&mut self, topic: Topic, co: GameConnection) {
        self.all_co_to_id
            .insert(co.clone(), ConnectionOwner::Shard(topic));
        self.server_id_to_co.insert(topic, co);
    }

    pub fn get_by_server_id(&self, topic: &Topic) -> Option<&GameConnection> {
        self.server_id_to_co.get(topic)
    }

    pub fn add_client(&mut self, co: GameConnection) -> ClientId {
        let id = self.id_generator.next();
        self.client_id_to_co.insert(id, co);
        self.all_co_to_id.insert(co, ConnectionOwner::Client(id));
        id
    }

    pub fn get_by_client_id(&self, id: &ClientId) -> Option<&GameConnection> {
        self.client_id_to_co.get(&id)
    }

    pub fn get_owner_by_co(&self, co: &GameConnection) -> Option<&ConnectionOwner> {
        self.all_co_to_id.get(co)
    }

    pub fn get_server_by_co(&self, co: &GameConnection) -> Option<&Topic> {
        match self.get_owner_by_co(co) {
            Some(ConnectionOwner::Shard(topic)) => Some(topic),
            _ => None,
        }
    }
    pub fn get_client_by_co(&self, co: &GameConnection) -> Option<&ClientId> {
        match self.get_owner_by_co(co) {
            Some(ConnectionOwner::Client(id)) => Some(id),
            _ => None,
        }
    }

    pub fn get_spatial_server(&self) -> &Option<GameConnection> {
        &self.spatial_server_id
    }

    pub fn add_spatial_server(&mut self, co: GameConnection) {
        if self.spatial_server_id.is_some() {
            eprintln!("Error : Spatial server already added");
            return;
        }
        self.spatial_server_id = Some(co);
        self.all_co_to_id.insert(co, ConnectionOwner::Spatial());
    }

    pub fn get_orchestrator_co(&self) -> &Option<GameConnection> {
        &self.orchestrator_server_id
    }

    pub fn add_orchestrator(&mut self, co: GameConnection) {
        if self.orchestrator_server_id.is_some() {
            eprintln!("Orchestrator already added");
            return;
        }
        self.orchestrator_server_id = Some(co);
        self.all_co_to_id.insert(co, ConnectionOwner::Orchestrator());
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

            Some(ConnectionOwner::Spatial()) => {
                self.spatial_server_id = None;
                Some(ConnectionOwner::Spatial())
            }

            Some(ConnectionOwner::Orchestrator()) => Some(ConnectionOwner::Orchestrator()),

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
