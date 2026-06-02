use bytes::{BufMut, Bytes, BytesMut};

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq)]
//ce que les clients peuvent recevoir.
pub enum GameMessageHeaders {
    Snapshot = 0x04,    //the shards broadcast the state of the world
    ClientInput = 0x05, //client to Shard

    ClientHello = 0x06, //Broker broadcast the client Hello.
    SpawnClient = 0x07,               //broker tells shard to spawn a client
    ClientWelcome = 0x08,             //broker to client
    ClientDisconnect = 0x09,          //broker to ...
    Heartbeat = 0x0B,                 //from shard => broker then broker => Orchestrator

    FriendHello = 0x0F, //When something that isn't a client says hello.

    //inter shard protocol
    TakeChunk = 0x10,

    DiscardedMessageBecauseYouKnow,
}

//create BrokerMessageHeaderFrom u8 :
impl From<u8> for GameMessageHeaders {
    fn from(value: u8) -> Self {
        match value {
            0x04 => GameMessageHeaders::Snapshot,
            0x05 => GameMessageHeaders::ClientInput,

            0x06 => GameMessageHeaders::ClientHello,
            0x07 => GameMessageHeaders::SpawnClient,
            0x08 => GameMessageHeaders::ClientWelcome,
            0x09 => GameMessageHeaders::ClientDisconnect,

            0x0B => GameMessageHeaders::Heartbeat,

            0x0F => GameMessageHeaders::FriendHello,

            0x10 => GameMessageHeaders::TakeChunk,

            _ => GameMessageHeaders::DiscardedMessageBecauseYouKnow,
        }
    }
}

/// Le contrat que chaque message du jeu doit respecter.
/// Il peut être défini n'importe où dans tes fichiers !
pub trait GameMessage: Sized {
    /// Quel est l'octet d'en-tête de ce message ?
    fn header() -> GameMessageHeaders;

    /// Transforme la structure en octets (SANS l'en-tête)
    fn serialize(&self) -> Bytes;

    /// Reconstruit la structure à partir des octets (SANS l'en-tête)
    fn deserialize(data: &mut Bytes) -> Result<Self, String>;
}

/// L'enveloppe renvoyée par le BrokerClient
#[derive(Debug, Clone)]
pub struct GamePayload {
    pub header: GameMessageHeaders,
    pub data: Bytes, // Les données pures, zero-copy !
}

impl GamePayload {
    /// Utilisé par MmoNetworkClient pour empaqueter avant envoi
    pub fn to_network_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(1 + self.data.len());
        buf.put_u8(self.header.clone() as u8);
        buf.put_slice(&self.data);
        buf.freeze()
    }
    pub fn extract<T: GameMessage>(&mut self) -> Result<T, String> {
        if self.header == T::header() {
            T::deserialize(&mut self.data)
        } else {
            Err(format!(
                "Erreur d'extraction : Header attendu {:?}, mais le payload contient {:?}",
                T::header(),
                self.header
            ))
        }
    }
}
