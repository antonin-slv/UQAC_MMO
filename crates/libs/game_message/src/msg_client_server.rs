// -- les différents streams de données

use crate::msg_entities::{EntityData, NetworkEntityId};
use crate::{impl_bitcode_encode_decode, impl_game_message, GameMessage, GameMessageHeaders, NetRead, NetWrite, NetWriteTo};
use bitcode::{Decode, Encode};
use broker_protocol::broker_message::NodeId;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use core_types::chunks::GameChunk;
use core_types::helpers::Tick;
use std::collections::VecDeque;

//
// CLIENT SERVER COMMUNICATION
//
#[derive(Encode, Decode, Clone, Debug)]
pub struct PersonalSnapshot {
    pub entities: Vec<EntityData>,
}
impl_bitcode_encode_decode!(PersonalSnapshot);
impl_game_message!(PersonalSnapshot, GameMessageHeaders::Snapshot);

#[derive(Encode, Decode, Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct PlayerInput {
    pub up: bool,
    pub down: bool,
    pub left: bool,
    pub right: bool,
    pub attack: bool,
    pub shoot: bool,
    pub build: bool,
}

/// Le Component Bevy attaché au Joueur (Local) ou à sa représentation serveur.
#[derive(Clone, Encode, Decode, Debug, Default, PartialEq)]
pub struct InputBuffer {
    /// L'historique récent des inputs (ex: les 60 dernières frames)
    pub history: VecDeque<(Tick, PlayerInput)>,
    /// La taille maximale du buffer pour éviter les fuites de mémoire
    pub max_size: u8,
}

impl InputBuffer {
    pub fn new(max_size: u8) -> Self {
        Self {
            history: VecDeque::with_capacity(max_size as usize),
            max_size,
        }
    }

    /// Ajoute un nouvel input et vire les plus anciens si on dépasse max_size
    pub fn push(&mut self, input: PlayerInput, time: Tick) {
        if self.history.len() >= self.max_size as usize {
            self.history.pop_front();
        }
        self.history.push_back((time, input));
    }

    pub fn recv_other(&mut self, other_buffer : InputBuffer) {
        self.max_size = other_buffer.max_size;
        self.history = other_buffer.history;
    }

    pub fn get_last_input(&self) -> Option<(Tick, PlayerInput)> {
        match self.history.iter().last() {
            Some(last_input) => Some(*last_input),
            None => None,
        }
    }
}

#[derive(Encode, Decode, Clone, Debug, Default, PartialEq)]
pub struct PlayerInputMsg {
    pub emitter_id: NodeId,
    pub entity_id: NetworkEntityId,
    pub input_data: InputBuffer,
}
impl_bitcode_encode_decode!(PlayerInputMsg);
impl_game_message!(PlayerInputMsg, GameMessageHeaders::ClientInput);

pub struct ClientWelcomeMsg {
    pub client_id: NodeId,
    pub chunk: GameChunk,
    pub chunk_size: f32,
}

impl NetWrite for ClientWelcomeMsg {
    fn serialize(&self) -> Bytes {
        let mut data = BytesMut::new();
        self.write_to(&mut data);
        data.freeze()
    }
}

impl NetRead for ClientWelcomeMsg {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        if data.len() != 12 {
            return Err(format!(
                "ClientWelcomeMsg doit être de 12 octets, trouvé {}",
                data.len()
            ));
        }
        let client_id = data.get_u32_le();
        let chunk = match GameChunk::deserialize(data) {
            Ok(chunk) => chunk,
            Err(e) => return Err(format!("ClientWelcomeMsg : {}", e)),
        };
        let chunk_size = data.get_f32_le();
        Ok(Self {
            client_id,
            chunk,
            chunk_size,
        })
    }
}

impl NetWriteTo for ClientWelcomeMsg {
    fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.client_id);
        self.chunk.write_to(buf);
        buf.put_f32_le(self.chunk_size);
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

impl NetWriteTo for ClientHelloMsg {
    fn write_to(&self, buf: &mut BytesMut) {
        let pseudo_bytes = self.pseudo.as_bytes();
        buf.put_u16_le(pseudo_bytes.len() as u16);
        buf.put_slice(pseudo_bytes);
    }
}

impl GameMessage for ClientHelloMsg {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::ClientHello
    }
}

impl NetWrite for ClientHelloMsg {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.write_to(&mut buf);
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
