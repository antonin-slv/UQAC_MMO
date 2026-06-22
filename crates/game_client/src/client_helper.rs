use bevy::prelude::{Commands, Component, Entity, EntityCommands, Transform};
use game_message::msg_client_server::InputBuffer;
use game_message::msg_entities::{EntityType, NetComponent};

#[derive(Component)]
pub struct EntityTypeComponent(pub EntityType);

#[derive(Component)]
struct InputComponent {
    _input_buffer: InputBuffer
}

pub fn insert_net_component(entity_cmds: &mut EntityCommands, net_comp: &NetComponent) {
    match net_comp {
        NetComponent::Position(pos) => {
            entity_cmds.insert(Transform::from_xyz(pos.x, pos.y, 0.0));
        }
        NetComponent::Type(entity_type) => {
            entity_cmds.insert(EntityTypeComponent(*entity_type));
        }
        NetComponent::Inputs(buffers) => {
            entity_cmds.insert(InputComponent {
                _input_buffer: buffers.clone(),
            });
        }
        NetComponent::Velocity(_vel) => {}
        NetComponent::Health(_health) => {}
        NetComponent::ControlledBy(_controller_id) => {}
        _ => {}
    }
}

/// Traduit un composant réseau pour une entité EXISTANTE (Mise à jour structurelle)
/// Utilisé pour les composants qui ne changent pas à chaque frame.
pub fn update_net_component(entity: Entity, net_comp: &NetComponent, commands: &mut Commands) {
    match net_comp {
        NetComponent::Health(_health) => {
            // commands.entity(entity).insert(LocalHealth { current: health.value });
        }
        NetComponent::Type(entity_type) => {
            commands
                .entity(entity)
                .insert(EntityTypeComponent(*entity_type));
        }
        // La position est gérée directement dans le système pour éviter 1 frame de lag
        _ => {}
    }
}
