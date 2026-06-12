use crate::impl_bitcode_encode_decode;
use crate::msg_client_server::InputBuffer;
use bitcode::{Decode, Encode};
use broker_protocol::broker_message::NodeId;
use core_types::Vec2;

pub type NetworkEntityId = u64;

#[repr(u8)]
#[derive(Encode, Decode, Clone, Copy, Debug)]
pub enum EntityType {
    Player,
    Zombie,
    Turret,
    Wall,
    Projectile,
}

// 3. L'énumération des Composants
#[derive(Encode, Decode, Clone, Debug)]
pub enum NetComponent {
    Position(Vec2),
    Velocity(Vec2),

    Health(u16),

    // États spécifiques
    TargetAngle(f32), // Pour la rotation des tourelles ou des joueurs
    ControlledBy(NodeId),
    EntityId(NetworkEntityId),
    Type(EntityType),
    Inputs(Vec<InputBuffer>),
}

// 4. Les Messages Réseau globaux
#[derive(Encode, Decode, Clone, Debug)]
pub struct EntityData {
    pub net_id: NetworkEntityId,
    pub owner_id: NodeId,
    pub updates: Vec<NetComponent>,
}

impl_bitcode_encode_decode!(EntityData);
