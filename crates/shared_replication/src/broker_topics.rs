use std::cmp::min;

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
    Director = 0x30,         // general messages for servers (like an orchestra director)
    Heartbeat = 0x31,        // Heartbeat (from shard to broker, then broker to orchestrator)

    ServerLine = 0x32, // Direct comm to a server
    ClientLine = 0x33, // Direct line to a client
}

impl TryFrom<u8> for Namespace {
    type Error = &'static str;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x04 => Ok(Self::ServerConnection),
            0x05 => Ok(Self::ClientAuth),
            0x06 => Ok(Self::Chunk),
            0x11 => Ok(Self::SpatialInput),
            0x30 => Ok(Self::Director),
            0x31 => Ok(Self::Heartbeat),
            0x32 => Ok(Self::ServerLine),

            _ => Err("Namespace inconnu"),
        }
    }
}

//la même chose mais en correcte :
pub trait TopicInterface {
    fn security_domain(&self) -> Option<SecurityDomain>;
    fn default() -> Self;

    fn namespace(&self) -> Option<Namespace>;
}
pub type Topic = [u8; 32];
impl TopicInterface for Topic {
    fn security_domain(&self) -> Option<SecurityDomain> {
        SecurityDomain::try_from(self[0]).ok()
    }
    fn namespace(&self) -> Option<Namespace> {
        Namespace::try_from(self[1]).ok()
    }

    fn default() -> Self {
        [0u8; 32]
    }
}

// --- Le Builder pour créer des topics sans erreur ---

pub struct TopicBuilder {
    buffer: Topic,
    current_index: u8,
}

impl TopicBuilder {
    pub fn new(domain: SecurityDomain, namespace: Namespace) -> Self {
        let mut buffer = [0u8; 32];
        buffer[0] = domain as u8;
        buffer[1] = namespace as u8;
        Self {
            buffer,
            current_index: 2,
        }
    }

    pub fn append_entity(mut self, entity: u32) -> Self {
        let entity_bytes = entity.to_le_bytes();
        let ci = self.current_index as usize;
        self.buffer[ci..(ci + 4)].copy_from_slice(&entity_bytes);
        self.current_index = (ci + 4) as u8;
        self
    }
    /// Ajoute le niveau de détail (utile pour l'AOI)
    pub fn append_lod(mut self, lod: u16) -> Self {
        let lod_bytes = lod.to_le_bytes();
        let ci = self.current_index as usize;
        self.buffer[ci..(ci + 2)].copy_from_slice(&lod_bytes);
        self.current_index = (ci + 2) as u8;
        self
    }

    /// Ajoute des coordonnées spatiales (X, Y) pour la grille d'AOI
    pub fn append_grid(mut self, cell_x: i32, cell_y: i32) -> Self {
        let x_bytes = cell_x.to_le_bytes();
        let y_bytes = cell_y.to_le_bytes();
        let mut ci = self.current_index as usize;
        // On place X de l'octet 4 à 7, et Y de 8 à 11
        self.buffer[ci..(ci + 4)].copy_from_slice(&x_bytes);
        ci += 4;
        self.buffer[ci..(ci + 4)].copy_from_slice(&y_bytes);
        self.current_index = (ci + 4) as u8;
        self
    }
    pub fn append(mut self, sub_topic: &[u8]) -> Self {
        let length = sub_topic.len();
        let idx = self.current_index as usize;

        let remaing_space = 32 - idx;

        let fill_in = min(length, remaing_space);

        self.buffer[idx..idx + fill_in].copy_from_slice(&sub_topic[..fill_in]);
        self.current_index += fill_in as u8;
        self
    }

    /// Finalise la construction
    pub fn build(self) -> Topic {
        self.buffer
    }
}
