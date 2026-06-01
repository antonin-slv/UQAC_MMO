use std::fmt;

#[derive(Debug)]
pub enum ProtocolError {
    BufferTooShort { expected: usize, actual: usize, context: &'static str },
    UnknownTag(u8),
    MalformedData(&'static str),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::BufferTooShort { expected, actual, context } => {
                write!(f, "Paquet tronqué [{}]. Attendu: {} octets, Reçu: {}", context, expected, actual)
            }
            ProtocolError::UnknownTag(tag) => write!(f, "Tag de protocole inconnu : 0x{:02X}", tag),
            ProtocolError::MalformedData(msg) => write!(f, "Données mal formées : {}", msg),
        }
    }
}
impl std::error::Error for ProtocolError {}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolTag {
    Subscribe = 0x01,
    Unsubscribe = 0x02,
    Publish = 0x03,
    Broadcast = 0x04,
    AuthorizeClient = 0x05,
    KickClient = 0x06,
    ClientBrokerHello = 0x07, //for a client to connect to the broker.
}

impl TryFrom<u8> for ProtocolTag {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Subscribe),
            0x02 => Ok(Self::Unsubscribe),
            0x03 => Ok(Self::Publish),
            0x04 => Ok(Self::Broadcast),
            0x05 => Ok(Self::AuthorizeClient),
            0x06 => Ok(Self::KickClient),
            0x07 => Ok(Self::ClientBrokerHello),

            _ => Err(ProtocolError::UnknownTag(value)),
        }
    }
}
