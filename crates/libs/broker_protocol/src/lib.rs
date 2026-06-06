
pub mod topics;
pub mod broker_message;
pub mod topic_layers;
pub mod topic_patterns;

#[cfg(test)]
mod tests {
    use core_types::chunks::GameChunkAera;
    use crate::topic_patterns::{TopicPattern};
    use crate::topics::{Namespace, SecurityDomain, Topic, TopicDefaults};

    #[test]
    pub fn test_generic_unpacker() {
        let area = GameChunkAera { x_min: 0, x_max: 2, y_min: 0, y_max: 2 };
        let first_layer = Topic::security_namespace_as_u8(SecurityDomain::PublicReadPrivateWrite, Namespace::Chunk);

        let min = 0;
        let max = 1;
        let pattern_area = TopicPattern::new()
            .with_fixed(vec![first_layer])
            .with_layers(area)
            .with_range(0u8..=2u8)
            .with_fixed(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]); // Liste d'octets à la fin

        pattern_area.unpack_into(|topic| {
            println!("{:?}", topic);
        });
    }
}