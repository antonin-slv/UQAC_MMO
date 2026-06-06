use crate::broker_message::ProtocolError;
use crate::topic_layers::{IntoTopicLayer, IntoTopicLayers, TopicLayer};
use crate::topics::{Namespace, SecurityDomain, Topic, TopicBuilder, TopicDefaults};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
