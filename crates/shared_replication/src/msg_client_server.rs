// -- les différents streams de données

use crate::broker_message::NodeId;
use crate::msg_game_payload::{GameMessage, GameMessageHeaders};
use bytes::Bytes;
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

impl GameMessage for SnapshotMsg {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::Snapshot
    }
    fn serialize(&self) -> Bytes {
        match bincode::serialize(&self.snapshot) {
            Ok(snapshot_as_bytes) => Bytes::from(snapshot_as_bytes),
            Err(e) => {
                eprintln!("Erreur de sérialisation bincode: {}", e);
                Bytes::new()
            }
        }
    }

    fn deserialize(bytes: &Bytes) -> Result<Self, String> {
        if let Ok(snapshot) = bincode::deserialize::<PersonalSnapshot>(&bytes[..]) {
            Ok(SnapshotMsg { snapshot })
        } else {
            Err("Could Not deser snapshot".to_string())
        }
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

    fn serialize(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(6);
        bytes.extend_from_slice(&self.client_id.to_le_bytes());
        bytes.extend_from_slice(&self.input_data.to_u8_slice());

        Bytes::from(bytes)
    }

    fn deserialize(bytes: &Bytes) -> Result<Self, String> {
        if bytes.len() != 6 {
            return Err(format!(
                "PlayerInputMsg doit être de 6 octets, trouvé {}",
                bytes.len()
            ));
        }
        let client_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let input_data = PlayerInput::make_from_u8_slice(&bytes[4..6])
            .ok_or("PlayerInputMsg : données d'entrée mal formées")?;
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
    fn serialize(&self) -> Bytes {
        Bytes::from(self.client_id.to_le_bytes().to_vec())
    }
    fn deserialize(bytes: &Bytes) -> Result<Self, String> {
        if bytes.len() != 4 {
            return Err(format!(
                "ClientDisconnectedMsg doit être de 4 octets, trouvé {}",
                bytes.len()
            ));
        }
        let client_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        Ok(Self { client_id })
    }
}


pub struct ClientWelcomeMsg {
    pub client_id: u32,
    pub chunk_x : i32,
    pub chunk_y : i32
}

impl GameMessage for ClientWelcomeMsg {
    fn header() -> GameMessageHeaders { GameMessageHeaders::ClientWelcome }

    fn serialize(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(12);
        bytes.extend_from_slice(&self.client_id.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_x.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_y.to_le_bytes());
        Bytes::from(bytes)
    }

    fn deserialize(data: &Bytes) -> Result<Self, String> {
        if data.len() != 12 {
            return Err(format!(
                "ClientWelcomeMsg doit être de 12 octets, trouvé {}",
                data.len()
            ));
        }
        let client_id = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let chunk_x = i32::from_le_bytes(data[4..8].try_into().unwrap());
        let chunk_y = i32::from_le_bytes(data[8..12].try_into().unwrap());
        Ok(Self { client_id, chunk_x, chunk_y })
    }
}

pub struct ClientHelloMsg {
    pub pseudo : String,
}

impl GameMessage for ClientHelloMsg {
    fn header() -> GameMessageHeaders { GameMessageHeaders::ClientHello }

    fn serialize(&self) -> Bytes {
        let pseudo_bytes = self.pseudo.as_bytes();
        let mut bytes = Vec::with_capacity(2 + pseudo_bytes.len());
        let pseudo_len_u16 = (pseudo_bytes.len() as u16).to_le_bytes();
        bytes.extend_from_slice(&pseudo_len_u16);
        bytes.extend_from_slice(pseudo_bytes);
        Bytes::from(bytes)
    }

    fn deserialize(data: &Bytes) -> Result<Self, String> {
        if data.len() < 2 {
            return Err("ClientHelloMsg trop court pour contenir la longueur du pseudo".into());
        }
        let pseudo_len = u16::from_le_bytes(data[0..2].try_into().unwrap()) as usize;
        if data.len() < 2 + pseudo_len {
            return Err(format!(
                "ClientHelloMsg trop court pour contenir le pseudo (attendu {} octets, trouvé {})",
                pseudo_len,
                data.len() - 2
            ));
        }
        let pseudo = String::from_utf8(data[2..2 + pseudo_len].to_vec())
            .map_err(|e| format!("ClientHelloMsg : pseudo non valide UTF-8: {}", e))?;
        Ok(Self { pseudo })
    }
}