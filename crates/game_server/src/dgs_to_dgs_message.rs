use bitcode::{Decode, Encode};

#[derive(Debug, Decode, Encode, Clone)]
pub struct FullEntityState {
    pub network_entity_id: u64,
    pub owner_node_id: u32,
    pub position: [f32; 2],
    pub velocity: [f32; 2], // Pour ne pas casser le mouvement
    pub meta_data : FullEntityMetaData,
}
#[derive(Debug, Decode, Encode, Clone)]
pub struct FullEntityMetaData {
    pub entity_type: u16, // 0 = player, 1 = bullet, etc. A définir selon les besoins du jeu
    pub health: u16, // Pour les entités qui ont de la santé
    pub extra_data: Vec<u8>, // Pour stocker des données spécifiques à certains types d'entités
}

