
use core_types::chunks::{GameChunkAera};
use crate::broker_subtopic::TopicLayer;

pub trait IntoTopicLayers {
    fn into_layers(self) -> Vec<TopicLayer>;
}

// Exemple 1 : Un GameChunkArea devient 2 boucles I16
impl IntoTopicLayers for GameChunkAera {
    fn into_layers(self) -> Vec<TopicLayer> {
        vec![
            TopicLayer::RangeI16(self.x_min, self.x_max),
            TopicLayer::RangeI16(self.y_min, self.y_max),
        ]
    }
}

