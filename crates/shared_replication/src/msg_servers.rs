use crate::msg_game_payload::{GameMessage, GameMessageHeaders};
use bytes::Bytes;
use std::cmp::PartialEq;
use crate::broker_message::NodeId;

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

/*
let hello_packet_header = GameMessageHeaders::FriendHello as u8;
let friend_type = ServerType::Server as u8;
let mut data = BytesMut::with_capacity(2 + 16);
data.put_u8(hello_packet_header);
data.put_u8(friend_type);
data.put_slice(server_info.uuid.as_bytes());
*/
pub struct ServerHelloMSG {
    pub server_type: ServerType,
    pub id: NodeId,
}

impl GameMessage for ServerHelloMSG {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::FriendHello
    }

    fn serialize(&self) -> Bytes {
        let mut data = Vec::with_capacity(2 + 16);
        data.push(self.server_type as u8);
        data.extend_from_slice(&self.id.to_le_bytes());
        Bytes::from(data)
    }

    fn deserialize(data: &Bytes) -> Result<Self, String> {
        if data.len() < 2 {
            return Err("ServerHelloMSG trop court".into());
        }
        let server_type = ServerType::from(data[0]);
        if server_type == ServerType::NotAFriend {
            return Err("ServerHelloMSG : type de serveur inconnu".into());
        }
        if data.len() < 1 + 4 {
            Err("ServerHelloMSG : données d'identification trop courtes".into())
        } else {
            let id = u32::from_le_bytes(data[1..5].try_into().unwrap());
            Ok(Self { server_type, id })
        }
    }
}
