use core_types::GameChunk;

pub const AUTH_FREE_NAMESPACE_FOR_CLIENTS_CONNEXION: Namespace = Namespace::ClientAuth;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum SecurityDomain {
    PublicReadPrivateWrite = 0x01, //worldstate (Everybody can read)
    PrivateReadPublicWrite = 0x02, //client connexion n input
    PrivateRW = 0x03,              //server-sever com
}

impl PartialEq for SecurityDomain {
    fn eq(&self, other: &Self) -> bool {
        *self as u8 == *other as u8
    }
}

impl TryFrom<u8> for SecurityDomain {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::PublicReadPrivateWrite),
            0x02 => Ok(Self::PrivateReadPublicWrite),
            0x03 => Ok(Self::PrivateRW),
            _ => Err("Domaine de sécurité inconnu"),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Namespace {
    ServerConnection = 0x04, // when a server connects he tells it here.
    ClientAuth = 0x05,       // client to server auth
    Chunk = 0x06,            // event linked to a specifi chunk + localisation
    SpatialInput = 0x11,     // Réception des inputs (lié à un chunk)
    SpatialServer = 0x12,     // Réception des inputs (lié à un chunk)
    Director = 0x30,         // general messages for servers (like an orchestra director)
    Heartbeat = 0x31,        // Heartbeat (from shard to broker, then broker to orchestrator)

    NodeLine = 0x32, // Direct comm to a Node
}

impl TryFrom<u8> for Namespace {
    type Error = &'static str;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x04 => Ok(Self::ServerConnection),
            0x05 => Ok(Self::ClientAuth),
            0x06 => Ok(Self::Chunk),
            0x11 => Ok(Self::SpatialInput),
            0x12 => Ok(Self::SpatialServer),
            0x30 => Ok(Self::Director),
            0x31 => Ok(Self::Heartbeat),
            0x32 => Ok(Self::NodeLine),

            _ => Err("Namespace inconnu"),
        }
    }
}

//la même chose mais en correcte :
pub trait TopicInterface {
    fn security_domain(&self) -> Option<SecurityDomain>;

    fn namespace(&self) -> Option<Namespace>;
}
pub type Topic = [u8; TOPIC_LENGTH as usize];

pub trait TopicDefaults {
    fn default_topic() -> Self;
    fn topic_length() -> usize;
}
impl TopicDefaults for Topic {
    fn default_topic() -> Self {
        [0u8; 16] // Plus besoin de Bytes::from_static
    }
    fn topic_length() -> usize {
        TOPIC_LENGTH as usize
    }
}
const TOPIC_LENGTH: u8 = 16;
impl TopicInterface for Topic {
    fn security_domain(&self) -> Option<SecurityDomain> {
        let domain_val = (self[0] >> 6) & 0b00000011;
        SecurityDomain::try_from(domain_val).ok()
    }

    fn namespace(&self) -> Option<Namespace> {
        let namespace_val = self[0] & 0b00111111;
        Namespace::try_from(namespace_val).ok()
    }
}

// --- Le Builder pour créer des topics sans erreur ---
#[derive(Debug, Clone)]
pub struct TopicBuilder {
    buffer: Topic,
    cursor: usize,
}

impl TopicBuilder {
    pub fn new(domain: SecurityDomain, namespace: Namespace) -> Self {
        let first_byte = ((domain as u8) << 6) | ((namespace as u8) & 0b00111111);
        let mut buffer = [0u8; 16];
        buffer[0] = first_byte;
        Self { buffer, cursor: 1 } // On a écrit le premier octet
    }

    pub fn append_chunk(mut self, p0: &GameChunk) -> Self {
        let mut pos = self.cursor;
        self.buffer[pos..pos + 2].copy_from_slice(&p0.x.to_le_bytes());
        pos += 2;
        self.buffer[pos..pos + 2].copy_from_slice(&p0.y.to_le_bytes());
        self.cursor = pos + 2;
        self
    }
    pub fn append_id(mut self, entity: u32) -> Self {
        // Écriture directe sans allocation dynamique
        let bytes = entity.to_le_bytes();
        self.buffer[self.cursor..self.cursor + 4].copy_from_slice(&bytes);
        self.cursor += 4;
        self
    }
    pub fn change_namespace(mut self, namespace: Namespace) -> Self {
        let namespace_val = namespace as u8;
        self.buffer[0] = (self.buffer[0] & 0b11000000) | (namespace_val & 0b00111111);
        self
    }

    pub fn change_security_domain(mut self, security_domain: SecurityDomain) -> Self {
        let secu_val = security_domain as u8;
        self.buffer[0] = (self.buffer[0] & 0b00111111) | ((secu_val << 6) & 0b11000000);
        self
    }

    pub fn build(self) -> Topic {
        self.buffer
    }

    pub fn append(mut self, sub_topic: &[u8]) -> Self {
        let slice_len = sub_topic.len();
        if self.cursor + slice_len > self.buffer.len() {
            panic!("Sub-topic trop long pour le buffer de topic !");
        }
        self.buffer[self.cursor..self.cursor + slice_len].copy_from_slice(sub_topic);
        self.cursor += slice_len;
        self
    }
}
