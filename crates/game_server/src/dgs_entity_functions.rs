use crate::game::{ClientDirectory, EntityDirectory};
use bevy::prelude::{
    Bundle, Commands, Component, Entity, EntityCommands, GlobalTransform, Query, Transform, With,
};
use broker_protocol::broker_message::NodeId;
use game_message::msg_client_server::InputBuffer;
use game_message::msg_entities::{EntityData, EntityType, NetComponent, NetworkEntityId};

#[derive(Component, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct NetworkIdComponent(pub NetworkEntityId);

impl Into<NetComponent> for NetworkIdComponent {
    fn into(self) -> NetComponent {
        NetComponent::EntityId(self.0)
    }
}
#[derive(Component, Copy, Clone)]
pub struct ControlledBy {
    pub client_id: NodeId,
}

impl Into<NetComponent> for ControlledBy {
    fn into(self) -> NetComponent {
        NetComponent::ControlledBy(self.client_id)
    }
}

#[derive(Component)]
pub struct EntityTypeComponent(pub EntityType);

impl Into<NetComponent> for EntityTypeComponent {
    fn into(self) -> NetComponent {
        NetComponent::Type(self.0)
    }
}

#[derive(Component)]
pub struct Player;
impl Into<NetComponent> for Player {
    fn into(self) -> NetComponent {
        NetComponent::Type(EntityType::Player)
    }
}
#[derive(Component)]
pub struct Wall;

impl Into<NetComponent> for Wall {
    fn into(self) -> NetComponent {
        NetComponent::Type(EntityType::Wall)
    }
}

#[derive(Component)]
pub struct Projectile;

impl Into<NetComponent> for Projectile {
    fn into(self) -> NetComponent {
        NetComponent::Type(EntityType::Projectile)
    }
}

#[derive(Component)]
pub struct Zombie;
impl Into<NetComponent> for Zombie {
    fn into(self) -> NetComponent {
        NetComponent::Type(EntityType::Zombie)
    }
}

#[derive(Component)]
pub struct Turret;

impl Into<NetComponent> for Turret {
    fn into(self) -> NetComponent {
        NetComponent::Type(EntityType::Turret)
    }
}
#[derive(Component, PartialEq, Eq, Clone, Copy, Debug)]
pub enum Authority {
    Authoritative,
    LastAuthFrame,
    Ghost,
}

#[derive(Component, PartialEq, Clone, Debug)]
pub struct InputComponent {
    pub input_buffer: InputBuffer,
}

impl Into<NetComponent> for InputComponent {
    fn into(self) -> NetComponent {
        NetComponent::Inputs(self.input_buffer.clone())
    }
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
        (With<ControlledBy>, With<NetworkIdComponent>),
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
        "[GameServer] had to spawn {} (controlled by {})",
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
    let entity_bundle = EntityBundlebase::new(recv_entity.net_id, recv_entity.owner_id, authority);

    let mut spawned_id = commands.spawn(entity_bundle);

    let mut has_input = false;
    let mut must_get_input = false;
    for component in &recv_entity.updates {
        match component {
            NetComponent::Inputs(_) => has_input = true,
            NetComponent::Type(t_igrec_pe) => {
                if t_igrec_pe.has_input() {
                    must_get_input = true;
                }
            }

            _ => {}
        }
    }
    if !has_input && must_get_input {
        let comp = InputComponent {
            input_buffer: InputBuffer::default(),
        };
        insert_net_component(&mut spawned_id, &(comp.into()))
    }

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
        NetComponent::Type(entity) => {
            entity_cmds.insert(EntityTypeComponent(*entity));
            match entity {
                EntityType::Player => entity_cmds.insert(Player),
                EntityType::Projectile => entity_cmds.insert(Projectile),
                EntityType::Wall => entity_cmds.insert(Wall),
                EntityType::Turret => entity_cmds.insert(Turret),
                EntityType::Zombie => entity_cmds.insert(Zombie),
            };
        }
        NetComponent::Inputs(buffers) => {
            entity_cmds.insert(InputComponent {
                input_buffer: buffers.clone(),
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
                .insert(EntityTypeComponent(*entity_type));
        }
        NetComponent::Inputs(input_buffer) => {
            commands.entity(entity).insert(InputComponent {
                input_buffer: input_buffer.clone(),
            });
        }
        // La position est gérée directement dans le système pour éviter 1 frame de lag
        _ => {}
    }
}
