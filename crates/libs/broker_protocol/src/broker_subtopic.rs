use crate::broker_message::ProtocolError;
use crate::broker_subtopics::IntoTopicLayers;
use crate::broker_topics::{Namespace, SecurityDomain, Topic, TopicBuilder, TopicDefaults};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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

#[derive(Debug, Clone)]
pub struct TopicPattern(pub Vec<TopicLayer>);

impl TopicPattern {
    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u8(self.0.len() as u8); // Nombre de couches
        for layer in &self.0 {
            layer.serialize(buf);
        }
    }

    pub fn deserialize(payload: &mut Bytes) -> Result<Self, ProtocolError> {
        if payload.remaining() < 1 {
            return Err(ProtocolError::BufferTooShort {
                expected: 1,
                actual: payload.remaining(),
                context: "TopicPattern Count",
            });
        }
        let count = payload.get_u8() as usize;
        let initial_capacity = std::cmp::min(count, 16);
        let mut layers = Vec::with_capacity(initial_capacity);
        for _ in 0..count {
            layers.push(TopicLayer::deserialize(payload)?);
        }
        Ok(Self(layers))
    }

    /// L'entrée principale pour le Broker
    pub fn unpack_into<F>(&self, callback: F)
    where
        F: FnMut(Topic),
    {
        let mut cb = callback;
        // On lance la récursion à l'index 0
        let base_builder = TopicBuilder::default();
        self.unpack_recursive(base_builder, 0, &mut cb);
    }

    /// LA MAGIE ZERO-COST : La récursion remplace les N boucles imbriquées
    fn unpack_recursive<F>(&self, current_builder: TopicBuilder, layer_idx: usize, cb: &mut F)
    where
        F: FnMut(Topic),
    {
        macro_rules! unpack_range {
            ($min:expr, $max:expr) => {
                for val in *$min..=*$max {
                    // Le compilateur résoudra le bon to_le_bytes() pour chaque type
                    let next_builder = current_builder.clone().append(&val.to_le_bytes());
                    self.unpack_recursive(next_builder, layer_idx + 1, cb);
                }
            };
        }
        // Condition d'arrêt : on a traversé toutes les couches, le Topic est prêt
        if layer_idx == self.0.len() {
            match current_builder.safe_build() {
                Ok(topic) => cb(topic),
                Err(err) => eprintln!("Erreur lors de la construction du topic : {}", err),
            }
            return;
        }

        // On exécute l'instruction courante
        match &self.0[layer_idx] {
            TopicLayer::Fixed(bytes) => {
                let next_builder = current_builder.clone().append(bytes);
                self.unpack_recursive(next_builder, layer_idx + 1, cb);
            }
            TopicLayer::RangeU8(min, max) => unpack_range!(min, max),
            TopicLayer::RangeU16(min, max) => unpack_range!(min, max),
            TopicLayer::RangeU32(min, max) => unpack_range!(min, max),
            TopicLayer::RangeI8(min, max) => unpack_range!(min, max),
            TopicLayer::RangeI16(min, max) => unpack_range!(min, max),

            TopicLayer::ListU8(list) => {
                // Création dynamique de la boucle pour un tableau de sous-topics
                for &val in list {
                    let next_builder = current_builder.clone().append(&[val]);
                    self.unpack_recursive(next_builder, layer_idx + 1, cb);
                }
            }
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

impl TopicPattern {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Ajoute une couche fixe (Fixed)
    pub fn with_fixed(mut self, bytes: impl Into<Vec<u8>>) -> Self {
        self.0.push(TopicLayer::Fixed(bytes.into()));
        self
    }

    /// Ajoute une liste d'octets (ListU8)
    pub fn with_list(mut self, list: impl Into<Vec<u8>>) -> Self {
        self.0.push(TopicLayer::ListU8(list.into()));
        self
    }

    pub fn with_range(mut self, range: impl IntoTopicLayer) -> Self {
        self.0.push(range.into_layer());
        self
    }

    /// Permet d'injecter directement n'importe quel type implémentant IntoTopicLayers (ex: GameChunkAera)
    pub fn with_layers(mut self, layers: impl IntoTopicLayers) -> Self {
        self.0.extend(layers.into_layers());
        self
    }

    pub fn with_head(mut self, namespace: Namespace, security_domain: SecurityDomain) -> Self {
        self.0
            .push(TopicLayer::Fixed(vec![Topic::security_namespace_as_u8(
                security_domain,
                namespace,
            )]));
        self
    }
}

impl Default for TopicPattern {
    fn default() -> Self {
        Self::new()
    }
}
