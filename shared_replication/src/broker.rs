use serde::{Deserialize, Serialize};

pub type Topic = [u8; 32];
pub type Input = [u8; 16];
pub type ClientId = u32;

pub trait SafeExtract {
    /// Tente de lire l'ID depuis une slice, renvoie None si la taille est invalide
    fn extract_from_slice(slice: &[u8]) -> Option<Self> where Self: Sized;
}

impl SafeExtract for ClientId {
    fn extract_from_slice(slice: &[u8]) -> Option<Self> {
        // try_into vérifie que la slice fait EXACTEMENT 4 octets
        let bytes: [u8; 4] = slice.try_into().ok()?;
        Some(ClientId::from_le_bytes(bytes))
    }
}

pub enum BrokerMessages {
    Subscribe { client_id: ClientId, topic: Topic }, //émis par le service spatial
    Unsubscribe { client_id: ClientId, topic: Topic }, //émis par le service spatial
    Publish { topic: Topic, payload: Vec<u8> }, //par un shard
    Broadcast { payload: Vec<u8> },                //broker → client
    ClientInput { client_id: ClientId, input: Input }, //client → Broker
    Join { token: Token },
    Welcome { client_id: ClientId },
}
#[repr(u8)]
pub enum BrokerMessageHeaders {
    SubscribeClient = 1,
    UnsubscribeClient = 2,
    Publish = 3,
    ShardBroadcast = 4,
    ClientInput = 5,
    Join = 6,
    Welcome = 7,
    DiscardedMessageBecauseYouKnow
}

//create BrokerMessageHeaderFrom u8 :
impl From<u8> for BrokerMessageHeaders {
    fn from(value: u8) -> Self {
        match value {
            1 => BrokerMessageHeaders::SubscribeClient,
            2 => BrokerMessageHeaders::UnsubscribeClient,
            3 => BrokerMessageHeaders::Publish,
            4 => BrokerMessageHeaders::ShardBroadcast,
            5 => BrokerMessageHeaders::ClientInput,
            6 => BrokerMessageHeaders::Join,
            7 => BrokerMessageHeaders::Welcome,
            _ => BrokerMessageHeaders::DiscardedMessageBecauseYouKnow
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Token {
    //placeholder for authentification token.
}
