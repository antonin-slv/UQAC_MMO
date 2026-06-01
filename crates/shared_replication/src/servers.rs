#[repr(u8)]
pub enum ServerType {
    Client = 0x01,
    Server = 0x02,
    Spatial = 0x03,
    Orchestrator = 0x04,
    Authentification = 0x05,
    NotAFriend
}

impl From<u8> for ServerType {
    fn from(value: u8) -> Self {
        match value {
            0x01 => ServerType::Server,
            0x02 => ServerType::Client,
            0x03 => ServerType::Spatial,
            0x04 => ServerType::Orchestrator,
            0x05 => ServerType::Authentification,
            _ => ServerType::NotAFriend,
        }
    }
}