use crate::{GameMessage, GameMessageHeaders, NetRead, NetWrite, NetWriteTo};
use broker_protocol::broker_message::NodeId;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::PartialEq;

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ServerType {
    Client = 0x01,
    Server = 0x02,
    Spatial = 0x03,
    Orchestrator = 0x04,
    Authentification = 0x05,
    NotAFriend,
}

impl From<u8> for ServerType {
    fn from(value: u8) -> Self {
        match value {
            0x01 => ServerType::Client,
            0x02 => ServerType::Server,
            0x03 => ServerType::Spatial,
            0x04 => ServerType::Orchestrator,
            0x05 => ServerType::Authentification,
            _ => ServerType::NotAFriend,
        }
    }
}

pub struct ServerHelloMSG {
    pub server_type: ServerType,
    pub id: NodeId,
}

impl NetWrite for ServerHelloMSG {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(1 + 4);
        self.write_to(&mut buf);
        buf.freeze()
    }
}

impl NetRead for ServerHelloMSG {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        // Il faut s'assurer qu'il reste au moins 5 octets (1 pour le type, 4 pour l'ID)
        if data.remaining() < 5 {
            return Err("ServerHelloMSG trop court".into());
        }

        let server_type = ServerType::from(data.get_u8());
        if server_type == ServerType::NotAFriend {
            return Err("ServerHelloMSG : type de serveur inconnu".into());
        }

        let id = data.get_u32_le();
        Ok(Self { server_type, id })
    }
}

impl NetWriteTo for ServerHelloMSG {
    fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u8(self.server_type as u8);
        buf.put_u32_le(self.id);
    }
}

impl GameMessage for ServerHelloMSG {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::FriendHello
    }
}
