use bevy::prelude::{Commands, Component, Entity, EntityCommands, Transform};
use game_message::msg_entities::{EntityType, NetComponent};

#[derive(Component)]
pub struct EntityComponent(pub EntityType);

pub fn insert_net_component(entity_cmds: &mut EntityCommands, net_comp: &NetComponent) {
    match net_comp {
        NetComponent::Position(pos) => {
            // On mappe les floats du réseau vers le Transform de Bevy
            entity_cmds.insert(Transform::from_xyz(pos.x, pos.y, 0.0));
        }
        NetComponent::Velocity(_vel) => {
            // Exemple : tu as un composant local Velocity
            // entity_cmds.insert(LocalVelocity(Vec2::new(vel.x, vel.y)));
        }
        NetComponent::Health(_health) => {
            // entity_cmds.insert(LocalHealth { current: health.value });
        }
        NetComponent::ControlledBy(_controller_id) => {}
        _ => {} // Gérer les autres cas ou les ignorer
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
                .insert(EntityComponent(*entity_type));
        }
        // La position est gérée directement dans le système pour éviter 1 frame de lag
        _ => {}
    }
}
