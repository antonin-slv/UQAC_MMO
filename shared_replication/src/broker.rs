pub type Topic = [u8; 32];

pub type Input = [u8; 16];
pub type ClientId = u32;

pub trait SafeExtract {
    /// Tente de lire l'ID depuis une slice, renvoie None si la taille est invalide
    fn extract_from_slice(slice: &[u8]) -> Option<Self>
    where
        Self: Sized;
}

impl SafeExtract for ClientId {
    fn extract_from_slice(slice: &[u8]) -> Option<Self> {
        // try_into vérifie que la slice fait EXACTEMENT 4 octets
        let bytes: [u8; 4] = slice.try_into().ok()?;
        Some(ClientId::from_le_bytes(bytes))
    }
}

//
// BROKER TREE : u8 avec une profondeur de 32
/*
Quand le broker reçoit un paquet :


 ShardLocal : NE/ NW/ SW/ SE/

-SERVER            -> what the server send
    -SPAWN         -> A Server must spawn (topic in parameter)
    -DISPAWN       -> A Server must despawn (topic in parameter)
    -Shard -depth -*ShardLocal* recursive
        -Leaved       ->the server leaved
        -Connected    ->the server connected
        -Broadcast    -> Channel to send all data.
-PLAYER            -> topic of the client
    -HELLO         -> the client say hello to the broker
    -GOODBYE       -> the client say goodbye to the broker


    ///pas pour l'instant
    -Individual -ID1 -ID2 -ID3 -ID4
        -Goodbye       -> client publish here to say goodbye to the broker
        -Subscribe     -> client publish here to subscribe to a topic
        -Unsubscribe   -> client publish here to unsubscribe from a topic

 */

// Ceci est juste un reminder de la structure, ne pas l'utiliser directement.
// vec<u8> == data.
#[allow(dead_code)]
pub enum BrokerMessages {
    Subscribe(ClientId, Topic),
    Unsubscribe(ClientId, Topic),
    Publish(Topic, u16, Vec<u8>),
    Broadcast(u16, Vec<u8>),
    ClientInput(ClientId, Input),

    ClientHello(u8, String),//u8 pour taille de pseudo.
    SpawnClient(ClientId, u8, String), //id, pseudo
    ClientWelcome(ClientId),
    ClientDisconnect(ClientId),
    FriendHello(BrokerFriends),

    //ClientLocation(ClientId, f32, f32),
    Heartbeat(u16, Vec<u8>),
}


#[repr(u8)]
#[derive(Debug)]
pub enum BrokerMessageHeaders {
    Subscribe = 0x01,   //spatial server to broker
    Unsubscribe = 0x02, //spatial server to broker
    Publish = 0x03,     //shard to Clients
    Broadcast = 0x04,   //the shards broadcast the state of the world
    ClientInput = 0x05, //client to Shard

    ClientHello = 0x06,     //client tells hello to broker.
    SpawnClient = 0x07,     //broker tells shard to spawn a client
    ClientWelcome = 0x08,   //broker to client
    ClientDisconnect = 0x09,//broker to ...
    ClientLocation = 0x0A,  //just the location of the player, broadcasted by shards.
    Heartbeat = 0x0B,      //from shard => broker then broker => Orchestrator

    FriendHello = 0x0F,     //When something that isn't a client says hello.

    //inter shard protocol
    HandoffRequest = 0x20,
    HandoffAccept = 0x21,
    HandoffReject = 0x22,
    GhostUpdate = 0x23,
    HandoffComplete = 0x24,

    DiscardedMessageBecauseYouKnow,
}



//create BrokerMessageHeaderFrom u8 :
impl From<u8> for BrokerMessageHeaders {
    fn from(value: u8) -> Self {

        match value {
            0x01 => BrokerMessageHeaders::Subscribe,
            0x02 => BrokerMessageHeaders::Unsubscribe,
            0x03 => BrokerMessageHeaders::Publish,
            0x04 => BrokerMessageHeaders::Broadcast,
            0x05 => BrokerMessageHeaders::ClientInput,

            0x06 => BrokerMessageHeaders::ClientHello,
            0x07 => BrokerMessageHeaders::SpawnClient,
            0x08 => BrokerMessageHeaders::ClientWelcome,
            0x09 => BrokerMessageHeaders::ClientDisconnect,

            0x0A => BrokerMessageHeaders::ClientLocation,
            0x0B => BrokerMessageHeaders::Heartbeat,

            0x0F => BrokerMessageHeaders::FriendHello,

            _ => BrokerMessageHeaders::DiscardedMessageBecauseYouKnow,
        }
    }
}


#[repr(u8)]
pub enum BrokerFriends {
    Server = 0x01,
    Client = 0x02,
    Spatial = 0x03,
    Orchestrator = 0x04,
    NotAFriend
}

impl From<u8> for BrokerFriends {
    fn from(value: u8) -> Self {
        match value {
            0x01 => BrokerFriends::Server,
            0x02 => BrokerFriends::Client,
            0x03 => BrokerFriends::Spatial,
            0x04 => BrokerFriends::Orchestrator,
            _ => BrokerFriends::NotAFriend,
        }
    }
}

