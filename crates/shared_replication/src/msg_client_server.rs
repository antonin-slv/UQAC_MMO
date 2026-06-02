// -- les différents streams de données

use crate::broker_message::NodeId;
use crate::msg_dgs::GameChunk;
use crate::msg_game_payload::{GameMessage, GameMessageHeaders, NetRead, NetWrite, NetWriteTo};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rocket::serde::{Deserialize, Serialize};

//
// CLIENT SERVER COMMUNICATION
//
pub type Input = [u8; 16];

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub struct EntitySnapshot {
    pub network_id: NodeId,
    pub position: [f32; 2],
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PersonalSnapshot {
    pub entities: Vec<EntitySnapshot>,
}

pub struct SnapshotMsg {
    pub snapshot: PersonalSnapshot,
}

impl NetWrite for SnapshotMsg {
    fn serialize(&self) -> Bytes {
        let mut writer = BytesMut::new().writer();
        if let Err(e) = bincode::serialize_into(&mut writer, &self.snapshot) {
            eprintln!("Erreur bincode: {}", e);
            return Bytes::new();
        }
        writer.into_inner().freeze()
    }
}

impl NetRead for SnapshotMsg {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        let mut reader = data.reader();
        match bincode::deserialize_from(&mut reader) {
            Ok(snapshot) => Ok(SnapshotMsg { snapshot }),
            Err(e) => Err(format!("Impossible de désérialiser le snapshot: {}", e)),
        }
    }
}

impl GameMessage for SnapshotMsg {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::Snapshot
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct PlayerInput(pub u16);

pub struct PlayerInputMsg {
    pub client_id: NodeId,
    pub input_data: PlayerInput,
}

impl GameMessage for PlayerInputMsg {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::ClientInput
    }
}

impl NetWrite for PlayerInputMsg {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(6);
        buf.put_u32_le(self.client_id);
        buf.put_u16_le(self.input_data.0); // On écrit directement le u16 !
        buf.freeze()
    }
}

impl NetRead for PlayerInputMsg {
    fn deserialize(bytes: &mut Bytes) -> Result<Self, String> {
        if bytes.remaining() < 6 {
            return Err(format!(
                "PlayerInputMsg doit faire 6 octets, reste {}",
                bytes.remaining()
            ));
        }
        let client_id = bytes.get_u32_le();
        let input_data = PlayerInput(bytes.get_u16_le());

        Ok(Self {
            client_id,
            input_data,
        })
    }
}
impl PlayerInput {
    pub fn make_from_u8_slice(slice: &[u8]) -> Option<Self> {
        if slice.len() != 2 {
            return None;
        }
        let bytes: [u8; 2] = slice.try_into().ok()?;
        Some(PlayerInput(u16::from_le_bytes(bytes)))
    }

    pub fn to_u8_slice(&self) -> [u8; 2] {
        self.0.to_le_bytes()
    }
    pub fn get_bit(&self, pos: u8) -> bool {
        (self.0 >> pos) & 1 == 1
    }
    pub fn is_up(&self) -> bool {
        self.get_bit(0)
    }
    pub fn is_down(&self) -> bool {
        self.get_bit(1)
    }
    pub fn is_left(&self) -> bool {
        self.get_bit(2)
    }

    pub fn is_right(&self) -> bool {
        self.get_bit(3)
    }

    //set pos-th bit to bit value. (0 being right)
    pub fn set_bit(&mut self, bit: bool, pos: u8) {
        self.0 = (self.0 & !(1u16 << pos)) | ((bit as u16) << pos);
    }

    pub fn set_up(&mut self, up: bool) {
        self.set_bit(up, 0);
    }

    pub fn set_down(&mut self, down: bool) {
        self.set_bit(down, 1);
    }
    pub fn set_left(&mut self, left: bool) {
        self.set_bit(left, 2);
    }

    pub fn set_right(&mut self, right: bool) {
        self.set_bit(right, 3);
    }
}

pub struct ClientDisconnectedMsg {
    pub client_id: u32,
}

impl GameMessage for ClientDisconnectedMsg {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::ClientDisconnect
    }
}
impl NetWrite for ClientDisconnectedMsg {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(4);
        buf.put_u32_le(self.client_id);
        buf.freeze()
    }
}

impl NetRead for ClientDisconnectedMsg {
    fn deserialize(bytes: &mut Bytes) -> Result<Self, String> {
        if bytes.remaining() < 4 {
            return Err("ClientDisconnectedMsg trop court".into());
        }
        Ok(Self {
            client_id: bytes.get_u32_le(),
        })
    }
}
pub struct ClientWelcomeMsg {
    pub client_id: u32,
    pub chunk: GameChunk,
}

impl NetWrite for ClientWelcomeMsg {
    fn serialize(&self) -> Bytes {
        let mut data = BytesMut::with_capacity(12);
        data.put_u32_le(self.client_id);
        self.chunk.write_to(&mut data);
        data.freeze()
    }
}

impl NetRead for ClientWelcomeMsg {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        if data.len() != 8 {
            return Err(format!(
                "ClientWelcomeMsg doit être de 8 octets, trouvé {}",
                data.len()
            ));
        }
        let client_id = data.get_u32_le();
        let chunk = match GameChunk::deserialize(data) {
            Ok(chunk) => chunk,
            Err(e) => return Err(format!("ClientWelcomeMsg : {}", e)),
        };
        Ok(Self { client_id, chunk })
    }
}

impl GameMessage for ClientWelcomeMsg {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::ClientWelcome
    }
}

pub struct ClientHelloMsg {
    pub pseudo: String,
}

impl GameMessage for ClientHelloMsg {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::ClientHello
    }
}

impl NetWrite for ClientHelloMsg {
    fn serialize(&self) -> Bytes {
        let pseudo_bytes = self.pseudo.as_bytes();
        let mut buf = BytesMut::with_capacity(2 + pseudo_bytes.len());
        buf.put_u16_le(pseudo_bytes.len() as u16);
        buf.put_slice(pseudo_bytes);
        buf.freeze()
    }
}

impl NetRead for ClientHelloMsg {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        if data.remaining() < 2 {
            return Err("ClientHelloMsg trop court".into());
        }

        let pseudo_len = data.get_u16_le() as usize;
        if data.remaining() < pseudo_len {
            return Err("ClientHelloMsg : données de pseudo manquantes".into());
        }

        let pseudo_bytes = data.split_to(pseudo_len);
        let pseudo = std::str::from_utf8(&pseudo_bytes)
            .map_err(|e| format!("ClientHelloMsg : pseudo non valide UTF-8: {}", e))?
            .to_string(); // La seule allocation inévitable pour construire la String finale

        Ok(Self { pseudo })
    }
}
