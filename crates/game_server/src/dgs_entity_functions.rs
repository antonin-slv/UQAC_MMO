use crate::dgs_network::{ControlledBy, NetworkIdComponent};
use crate::game::{ClientDirectory, EntityDirectory};
use bevy::prelude::{
    Bundle, Commands, Component, Entity, EntityCommands, GlobalTransform, Query, Transform, With,
};
use broker_protocol::broker_message::NodeId;
use game_message::msg_entities::{EntityData, EntityType, NetComponent, NetworkEntityId};


#[derive(Component)]
pub struct EntityComponent(pub EntityType);

#[derive(Component, PartialEq, Eq, Clone, Copy, Debug)]
pub enum Authority {
    Authoritative,
    LastAuthFrame,
    Ghost,
}

#[derive(Bundle)]
pub struct EntityBundlebase {
    pub net_id: NetworkIdComponent,
    pub controlled_by: ControlledBy,
    pub global_transform: GlobalTransform,
    pub authority: Authority,
}

impl EntityBundlebase {
    pub fn new(net_id: NetworkEntityId, owner_id: NodeId, authority: Authority) -> Self {
        Self {
            net_id: NetworkIdComponent(net_id),
            controlled_by: ControlledBy {
                client_id: owner_id,
            }, // On assigne la propriété
            global_transform: GlobalTransform::default(),
            authority,
        }
    }
}

pub fn spawn_or_update_entity(
    mut commands: &mut Commands,
    authority: Authority,
    client_directory: &mut ClientDirectory,
    entity_directory: &mut EntityDirectory,
    current_in_ecs_entities: &mut Query<
        (Entity, &mut Transform, &mut Authority),
        With<ControlledBy>,
    >,
    recv_entity: &EntityData,
) -> Entity {
    if let Some(current_entity) = entity_directory.entities.get(&recv_entity.net_id) {
        if let Ok((entt, mut transform, mut auth)) =
            current_in_ecs_entities.get_mut(*current_entity)
        {
            // On a trouvé LA bonne entité
            if *auth == Authority::Ghost {
                *auth = authority; //si on la contrôle déjà... pas touche, sinon on hérite de ce qu'on nous donne.
            }
            for component in &recv_entity.updates {
                match component {
                    NetComponent::Position(pos) => {
                        transform.translation.x = pos.x as f32;
                        transform.translation.y = pos.y as f32;
                    }
                    _ => {
                        update_net_component(*current_entity, &component, &mut commands);
                    }
                }
            }
            return entt;
        }
    }

    println!(
        "\t\thad to spawn {} (controlled by {})",
        recv_entity.net_id, recv_entity.owner_id
    );
    // on arrive ici si l'entité est nouvelle pour nous.

    spawn_entity(
        &mut commands,
        authority,
        client_directory,
        entity_directory,
        &recv_entity,
    )
}

pub fn spawn_entity(
    commands: &mut Commands,
    authority: Authority,
    client_directory: &mut ClientDirectory,
    entity_directory: &mut EntityDirectory,
    recv_entity: &EntityData,
) -> Entity {
    let player_bundle = EntityBundlebase::new(recv_entity.net_id, recv_entity.owner_id, authority);

    let mut spawned_id = commands.spawn(player_bundle);

    for component in &recv_entity.updates {
        insert_net_component(&mut spawned_id, component);
    }
    client_directory
        .sessions
        .entry(recv_entity.owner_id)
        .or_insert_with(Vec::new)
        .push(recv_entity.net_id);

    entity_directory
        .entities
        .insert(recv_entity.net_id, spawned_id.id());
    spawned_id.id()
}

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
        NetComponent::ControlledBy(controller_id) => {
            entity_cmds.insert(ControlledBy {
                client_id: *controller_id,
            });
        }
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
