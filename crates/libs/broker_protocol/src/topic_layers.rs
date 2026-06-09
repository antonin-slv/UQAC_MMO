use crate::broker_message::ProtocolError;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use core_types::chunks::{GameChunk, GameChunkAera};

pub trait IntoTopicLayers {
    fn into_layers(self) -> Vec<TopicLayer>;
}

//GAMEPLAY OBJECTS IMPLEMENTATION


impl IntoTopicLayers for GameChunkAera {
    fn into_layers(self) -> Vec<TopicLayer> {
        vec![
            TopicLayer::RangeI16(self.x_min, self.x_max),
            TopicLayer::RangeI16(self.y_min, self.y_max),
        ]
    }
}

impl<T: IntoTopicLayer> IntoTopicLayers for Vec<T> {
    fn into_layers(self) -> Vec<TopicLayer> {
        self.into_iter().map(|item| item.into_layer()).collect()
    }
}
impl IntoTopicLayer for GameChunk {
    fn into_layer(self) -> TopicLayer {
        let le_x = self.x.to_le_bytes();
        let le_y = self.y.to_le_bytes();
        TopicLayer::Fixed(Vec::from([le_x[0], le_x[1], le_y[0], le_y[1]]))
    }
}
impl IntoTopicLayer for &GameChunk {
    fn into_layer(self) -> TopicLayer {
        (*self).into_layer()
    }
}

//CLASS DEFINITIONS

#[derive(Debug, Clone)]
pub enum TopicLayer {
    /// Ajoute des octets fixes exacts (ex: le base_byte, ou un sous-topic fixe)
    Fixed(Vec<u8>),
    RangeU8(u8, u8),
    RangeU16(u16, u16),
    RangeU32(u32, u32),
    RangeI8(i8, i8),
    RangeI16(i16, i16),
    /// Génère une boucle sur une liste d'octets (ex: [Movement, Combat])
    ListU8(Vec<u8>),
}

impl TopicLayer {
    // Sérialisation ultra-compacte sur le réseau
    pub fn serialize(&self, buf: &mut BytesMut) {
        match self {
            Self::Fixed(bytes) => {
                buf.put_u8(0x01);
                buf.put_u8(bytes.len() as u8);
                buf.put_slice(bytes);
            }
            Self::RangeU8(min, max) => {
                buf.put_u8(0x02);
                buf.put_u8(*min);
                buf.put_u8(*max);
            }
            Self::RangeU16(min, max) => {
                buf.put_u8(0x03);
                buf.put_u16_le(*min);
                buf.put_u16_le(*max);
            }
            Self::RangeU32(min, max) => {
                buf.put_u8(0x04);
                buf.put_u32_le(*min);
                buf.put_u32_le(*max);
            }
            Self::RangeI8(min, max) => {
                buf.put_u8(0x05);
                buf.put_i8(*min);
                buf.put_i8(*max);
            }
            Self::RangeI16(min, max) => {
                buf.put_u8(0x06);
                buf.put_i16_le(*min);
                buf.put_i16_le(*max);
            }
            Self::ListU8(bytes) => {
                buf.put_u8(0x07);
                buf.put_u8(bytes.len() as u8);
                buf.put_slice(bytes);
            }
        }
    }

    pub fn deserialize(payload: &mut Bytes) -> Result<Self, ProtocolError> {
        match payload.get_u8() {
            0x01 => {
                // Fixed
                if payload.remaining() < 1 {
                    return Err(ProtocolError::MalformedData("Taille manquante pour Fixed"));
                }
                let len = payload.get_u8() as usize;
                if payload.remaining() < len {
                    return Err(ProtocolError::MalformedData(
                        "Données manquantes pour Fixed",
                    ));
                }
                Ok(Self::Fixed(payload.split_to(len).to_vec()))
            }
            0x02 => {
                // RangeU8
                if payload.remaining() < 2 {
                    return Err(ProtocolError::MalformedData(
                        "Données manquantes pour RangeU8",
                    ));
                }
                Ok(Self::RangeU8(payload.get_u8(), payload.get_u8()))
            }
            0x03 => {
                // RangeU16
                if payload.remaining() < 4 {
                    return Err(ProtocolError::MalformedData(
                        "Données manquantes pour RangeU16",
                    ));
                }
                Ok(Self::RangeU16(payload.get_u16_le(), payload.get_u16_le()))
            }
            0x04 => {
                // RangeU32
                if payload.remaining() < 8 {
                    return Err(ProtocolError::MalformedData(
                        "Données manquantes pour RangeU32",
                    ));
                }
                Ok(Self::RangeU32(payload.get_u32_le(), payload.get_u32_le()))
            }
            0x05 => {
                // RangeI8 (Corrigé : c'était I16)
                if payload.remaining() < 2 {
                    return Err(ProtocolError::MalformedData(
                        "Données manquantes pour RangeI8",
                    ));
                }
                Ok(Self::RangeI8(payload.get_i8(), payload.get_i8()))
            }
            0x06 => {
                // RangeI16 (Corrigé : c'était ListU8)
                if payload.remaining() < 4 {
                    return Err(ProtocolError::MalformedData(
                        "Données manquantes pour RangeI16",
                    ));
                }
                Ok(Self::RangeI16(payload.get_i16_le(), payload.get_i16_le()))
            }
            0x07 => {
                // ListU8 (Ajouté)
                if payload.remaining() < 1 {
                    return Err(ProtocolError::MalformedData("Taille manquante pour ListU8"));
                }
                let len = payload.get_u8() as usize;
                if payload.remaining() < len {
                    return Err(ProtocolError::MalformedData(
                        "Données manquantes pour ListU8",
                    ));
                }
                Ok(Self::ListU8(payload.split_to(len).to_vec()))
            }
            _ => Err(ProtocolError::MalformedData("Tag TopicLayer inconnu")),
        }
    }
}

pub trait IntoTopicLayer {
    fn into_layer(self) -> TopicLayer;
}

impl IntoTopicLayer for std::ops::RangeInclusive<u8> {
    fn into_layer(self) -> TopicLayer {
        TopicLayer::RangeU8(*self.start(), *self.end())
    }
}

impl IntoTopicLayer for std::ops::RangeInclusive<u16> {
    fn into_layer(self) -> TopicLayer {
        TopicLayer::RangeU16(*self.start(), *self.end())
    }
}
impl IntoTopicLayer for std::ops::RangeInclusive<u32> {
    fn into_layer(self) -> TopicLayer {
        TopicLayer::RangeU32(*self.start(), *self.end())
    }
}
impl IntoTopicLayer for std::ops::RangeInclusive<i8> {
    fn into_layer(self) -> TopicLayer {
        TopicLayer::RangeI8(*self.start(), *self.end())
    }
}
impl IntoTopicLayer for std::ops::RangeInclusive<i16> {
    fn into_layer(self) -> TopicLayer {
        TopicLayer::RangeI16(*self.start(), *self.end())
    }
}
