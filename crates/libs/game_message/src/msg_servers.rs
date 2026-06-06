use crate::{impl_bitcode_net_message, GameMessage, GameMessageHeaders, NetRead, NetWrite, NetWriteTo};
use broker_protocol::broker_message::NodeId;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::PartialEq;
use bitcode::{Decode, Encode};

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Encode, Decode)]
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

#[derive(Encode, Decode)]
pub struct ServerHelloMSG {
    pub server_type: ServerType,
    pub id: NodeId,
}


impl_bitcode_net_message!(ServerHelloMSG, GameMessageHeaders::FriendHello);

pub struct SpawnServerMSG {
    pub server_count: u8,
}

impl NetRead for SpawnServerMSG {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        Ok(Self {
            server_count: data.get_u8(),
        })
    }
}

impl NetWriteTo for SpawnServerMSG {
    fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u8(self.server_count);
    }
}

impl NetWrite for SpawnServerMSG {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(1);
        self.write_to(&mut buf);
        buf.freeze()
    }
}

impl GameMessage for SpawnServerMSG {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::SpawnServer
    }
}
