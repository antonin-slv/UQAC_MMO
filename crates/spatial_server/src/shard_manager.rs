use crate::broker_client::BrokerClient;
use crate::quadtree::{Entity, QuadTree, ShardId, ShardIdExt};
use broker_protocol::broker_message::NodeId;
use core_types::{Rect, Vec2};
use game_message::msg_dgs::Heartbeat;
use game_message::msg_entities::NetworkEntityId;
use rand::prelude::IteratorRandom;
use rand::random_range;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq)]
pub enum ShardState {
    Active,
    PendingSplit,
    PendingDestroy,
    PendingReady,
}

#[derive(Clone, Debug)]
pub struct Shard {
    pub dgs: Option<NodeId>,
    pub entities: HashSet<Entity>,
    pub state: ShardState,
}

#[derive(Clone, Debug)]
pub struct ShardManager {
    pub entities: HashMap<NetworkEntityId, ShardId>,
    pub shards: HashMap<ShardId, Shard>,
    pub active_dgs: HashMap<NodeId, Option<ShardId>>,
    pub dgs_data: HashMap<NodeId, (Heartbeat, (f32, f32, f32))>,
    pub pending_merges: HashMap<ShardId, Rect>,
}

impl ShardManager {
    pub fn new() -> ShardManager {
        Self {
            entities: HashMap::new(),
            shards: HashMap::new(),
            active_dgs: HashMap::new(),
            dgs_data: HashMap::new(),
            pending_merges: HashMap::new(),
        }
    }

    pub fn on_heartbeat_receive(
        &mut self,
        heartbeat: Heartbeat,
        quad_tree: &QuadTree,
        broker: &BrokerClient,
    ) {
        let data = self.dgs_data.entry(heartbeat.node_id).or_insert((
            heartbeat.clone(),
            (
                random_range(0.0..1.0),
                random_range(0.0..1.0),
                random_range(0.0..1.0),
            ),
        ));
        data.0 = heartbeat.clone();

        if let Some(shard) = self.active_dgs.get_mut(&heartbeat.node_id)
            && shard.is_some()
        {
            return;
        }

        self.on_new_dgs(heartbeat.node_id, quad_tree, broker);
    }

    pub fn on_new_shard(
        &mut self,
        parent: Option<ShardId>,
        shard_id: ShardId,
        bounds: Rect,
        broker: &BrokerClient,
    ) {
        println!("On new shard ! : {:?}", shard_id);

        let mut old_dgs_id = None;
        let mut current_ancestor = parent;
        while let Some(ancestor_id) = current_ancestor {
            if let Some(ancestor_shard) = self.shards.get_mut(&ancestor_id) {
                ancestor_shard.state = ShardState::PendingSplit;
                if ancestor_shard.dgs.is_some() {
                    old_dgs_id = ancestor_shard.dgs.clone();
                    break; // On a trouvé le DGS légitime, on s'arrête
                }
            }
            current_ancestor = ancestor_id.parent();
        }

        if old_dgs_id.is_none() {
            println!("SpatialServer : New Shard Spawned but no DGS ancestor found")
        }

        if let Some((new_dgs, shard_assignment)) = self
            .active_dgs
            .iter_mut()
            .find(|(_, shard)| shard.is_none())
        {
            let new_dgs_id = new_dgs.clone();

            *shard_assignment = Some(shard_id);

            broker.assign_shard_to_dgs(new_dgs_id.clone(), vec![(bounds, old_dgs_id)]);

            self.shards.insert(
                shard_id,
                Shard {
                    entities: HashSet::new(),
                    dgs: Some(new_dgs_id),
                    state: ShardState::PendingReady,
                },
            );
        } else {
            println!(
                "⏳ Aucun DGS libre. Shard {:?} mis en file d'attente.",
                shard_id
            );

            self.shards.insert(
                shard_id,
                Shard {
                    entities: HashSet::new(),
                    dgs: None,
                    state: ShardState::PendingReady,
                },
            );
        }
    }

    pub fn on_merge_request(
        &mut self,
        parent_id: ShardId,
        children: Vec<(ShardId, Rect)>,
        broker: &BrokerClient,
    ) {
        println!(
            "🔄 [Merge In-Place] Demande de fusion complexe pour le parent {:?}",
            parent_id
        );

        // 1. 🧹 LE FILTRE : On sépare les vrais serveurs (feuilles) des nœuds logiques (branches)
        let mut real_dgs_children = Vec::new();

        for (child_id, child_bounds) in children {
            if let Some(child_shard) = self.shards.get(&child_id) {
                if child_shard.dgs.is_some() {
                    // C'est une feuille avec un vrai serveur de jeu
                    real_dgs_children.push((child_id, child_bounds));
                } else {
                    // C'est un nœud intermédiaire fantôme. On le nettoie silencieusement.
                    self.shards.remove(&child_id);
                }
            }
        }

        if real_dgs_children.is_empty() {
            return;
        }

        // 2. 👑 L'ÉLECTION : On promeut le premier VRAI serveur
        let (promoted_child_id, _promoted_bounds) = real_dgs_children.remove(0);

        let promoted_dgs = self
            .shards
            .get(&promoted_child_id)
            .and_then(|s| s.dgs.clone())
            .expect("L'enfant promu devrait avoir un DGS actif ici !");

        self.shards.remove(&promoted_child_id);

        self.shards.insert(
            parent_id,
            Shard {
                entities: HashSet::new(),
                dgs: Some(promoted_dgs.clone()),
                state: ShardState::PendingReady,
            },
        );
        self.active_dgs
            .insert(promoted_dgs.clone(), Some(parent_id));

        // 3. 🧲 L'ABSORPTION MASSIVE : On gère tous les sous-serveurs restants
        let mut areas_to_take = Vec::new();

        for (child_id, child_bounds) in real_dgs_children {
            if let Some(child_shard) = self.shards.get_mut(&child_id) {
                child_shard.state = ShardState::PendingDestroy;
                areas_to_take.push((child_bounds.clone(), child_shard.dgs.clone()));
                self.pending_merges.insert(child_id, child_bounds);
            }
        }

        // 4. 🚀 L'ORDRE RÉSEAU
        if !areas_to_take.is_empty() {
            println!(
                "👑 [Merge In-Place] Le DGS {} est promu Parent ! Il va absorber {} sous-zones de profondeurs variables.",
                promoted_dgs,
                areas_to_take.len()
            );
            broker.assign_shard_to_dgs(promoted_dgs, areas_to_take);
        } else {
            if let Some(parent_shard) = self.shards.get_mut(&parent_id) {
                parent_shard.state = ShardState::Active;
            }
        }
    }
    pub fn on_area_took(
        &mut self,
        stolen_dgs: Option<NodeId>,
        bounds_of_area: Rect,
        quad_tree: &QuadTree,
    ) {
        let merged_child_id = self
            .pending_merges
            .iter()
            .find_map(|(&id, &rect)| (rect == bounds_of_area).then_some(id));

        if let Some(child_id) = merged_child_id {
            self.handle_merge_area_took(child_id, bounds_of_area);
        } else {
            self.handle_split_area_took(stolen_dgs, bounds_of_area, quad_tree);
        }
    }

    fn handle_merge_area_took(&mut self, child_id: ShardId, bounds_of_area: Rect) {
        self.pending_merges.remove(&child_id);

        if let Some(child_shard) = self.shards.remove(&child_id) {
            if let Some(child_dgs) = child_shard.dgs {
                self.active_dgs.insert(child_dgs, None); // Libération du serveur enfant
            }

            println!(
                "✅ [Merge] Sous-zone {:?} absorbée. Serveur enfant libéré.",
                bounds_of_area
            );

            // Vérification de la complétion du merge pour le parent
            if let Some(parent_id) = child_id.parent() {
                let still_has_children = self
                    .pending_merges
                    .keys()
                    .any(|id| id.parent() == Some(parent_id));

                if !still_has_children {
                    if let Some(parent_shard) = self.shards.get_mut(&parent_id) {
                        parent_shard.state = ShardState::Active;
                        println!(
                            "🎉 [Merge] Téléchargement total réussi ! Parent {} Actif.",
                            parent_id
                        );
                    }
                }
            }
        }
    }

    fn handle_split_area_took(
        &mut self,
        stolen_dgs: Option<NodeId>,
        bounds_of_area: Rect,
        quad_tree: &QuadTree,
    ) {
        // On cherche le nouveau shard en attente qui correspond à cette zone
        let target_shard_id = self.shards.iter().find_map(|(&shard_id, shard)| {
            if shard.state == ShardState::PendingReady {
                if let Some((shard_bounds, _)) = quad_tree.get_shard_bounds(&shard_id) {
                    if shard_bounds == bounds_of_area {
                        return Some(shard_id);
                    }
                }
            }
            None
        });

        if let Some(child_shard_id) = target_shard_id {
            // On active l'enfant et on récupère l'ID du parent
            if let Some(child_shard) = self.shards.get_mut(&child_shard_id) {
                child_shard.state = ShardState::Active;
                println!("✅ [Handoff] La sous-zone {:?} est désormais Active sur le DGS {:?} !", bounds_of_area, stolen_dgs);
            }

            let mut current_parent_to_check = child_shard_id.parent();

            // Remontée en cascade
            while let Some(parent_id) = current_parent_to_check {
                if self.check_if_node_fully_transferred(parent_id) {
                    println!(
                        "🎉 [Handoff] Toutes les sous-zones du parent {} ont confirmé (AreaTook). Destruction du parent.",
                        parent_id
                    );

                    // On retire le parent qui a fini son split
                    if let Some(parent_shard) = self.shards.remove(&parent_id) {
                        if let Some(parent_dgs) = parent_shard.dgs {
                            self.active_dgs.insert(parent_dgs, None); // On libère le serveur
                        }
                    }
                    current_parent_to_check = parent_id.parent();
                } else {
                    break;
                }
            }
        } else {
            eprintln!(
                "⚠️ [Handoff] AreaTook reçu du DGS {:?}, mais aucun shard en attente trouvé pour les bounds {:?}",
                stolen_dgs, bounds_of_area
            );
        }
    }
    fn check_if_node_fully_transferred(&self, parent_id: ShardId) -> bool {
        let mut has_descendants = false;

        let mut stack = vec![parent_id];

        while let Some(current_id) = stack.pop() {
            for i in 0..4 {
                let child_id = current_id.child(i);

                if let Some(child_shard) = self.shards.get(&child_id) {
                    has_descendants = true;

                    if child_shard.state == ShardState::PendingReady {
                        return false;
                    }
                    stack.push(child_id);
                }
            }
        }

        has_descendants
    }

    pub fn on_new_dgs(&mut self, dgs_id: NodeId, quad_tree: &QuadTree, broker: &BrokerClient) {
        if let Some(Some(_)) = self.active_dgs.get(&dgs_id) {
            return;
        }

        let mut waiting_shard_info = None;

        if let Some((&shard_id, shard)) = self.shards.iter_mut().find(|(_, shard)| {
            shard.dgs.is_none()
                && shard.state != ShardState::PendingSplit
                && shard.state != ShardState::PendingDestroy
        }) {
            shard.dgs = Some(dgs_id.clone());
            waiting_shard_info = Some((shard_id, shard_id.parent()));
        }

        if let Some((shard_id, parent_id)) = waiting_shard_info {
            println!(
                "✅ Nouveau DGS assigné au shard en attente : {:?}",
                shard_id
            );

            let mut old_dgs_id = None;
            let mut current_ancestor = parent_id;

            while let Some(ancestor_id) = current_ancestor {
                if let Some(ancestor_shard) = self.shards.get(&ancestor_id) {
                    if ancestor_shard.dgs.is_some() {
                        old_dgs_id = ancestor_shard.dgs.clone();
                        break; // Trouvé !
                    }
                }
                current_ancestor = ancestor_id.parent();
            }

            if let Some((bounds, _)) = quad_tree.get_shard_bounds(&shard_id) {
                broker.assign_shard_to_dgs(dgs_id.clone(), vec![(bounds, old_dgs_id)]);
            } else {
                println!("No bounds found for shard {:?}", shard_id);
            }

            self.active_dgs.insert(dgs_id, Some(shard_id));
        } else {
            self.active_dgs.insert(dgs_id, None);
        }
    }

    pub fn on_dgs_stopped(&mut self, dgs_id: NodeId) {
        println!("DGS Stopped : {:?}", dgs_id);

        if let Some(Some(shard_opt)) = self.active_dgs.remove(&dgs_id) {
            if let Some(shard) = self.shards.get_mut(&shard_opt) {
                shard.dgs = None;
            }
        }
    }

    pub fn set_entity_shard(&mut self, shard_id: ShardId, entity: Entity) {
        let old_shard_id = self.get_shard(entity.id);

        if let Some(old_shard_id) = old_shard_id
            && old_shard_id != shard_id
        {
            self.remove_entity_from_shard(old_shard_id, entity.id);
        }

        self.entities.insert(entity.id, shard_id);

        let shard = self.shards.get_mut(&shard_id);
        if let Some(shard) = shard {
            shard.entities.replace(entity);
        } else {
            let mut entities = HashSet::new();
            entities.insert(entity);
            self.shards.insert(
                shard_id,
                Shard {
                    entities,
                    state: ShardState::Active,
                    dgs: None,
                },
            );
        }
    }

    pub fn remove_entity_from_shard(&mut self, shard_id: ShardId, entity_id: NetworkEntityId) {
        if let Some(shard) = self.shards.get_mut(&shard_id) {
            shard
                .entities
                .remove(&Entity::new(entity_id, Vec2::new(0.0, 0.0)));
        }
    }

    pub fn drain_entities(&mut self, shard_id: ShardId) -> HashSet<Entity> {
        if let Some(shard) = self.shards.get_mut(&shard_id) {
            let entities = shard.entities.clone();
            shard.entities.clear();
            return entities;
        }
        HashSet::new()
    }

    pub fn get_shard(&self, entity_id: NetworkEntityId) -> Option<ShardId> {
        self.entities.get(&entity_id).cloned()
    }

    pub fn get_dgs_for_position(&self, position: Vec2, quad_tree: &QuadTree) -> Option<NodeId> {
        if let Some(shard_id) = quad_tree.get_shard_of_point(position) {
            let mut current_shard_id = shard_id;
            while let Some(shard) = self.shards.get(&current_shard_id) {
                if shard.state == ShardState::PendingReady {
                    if shard.dgs.is_some() {
                        return shard.dgs.clone();
                    }
                    // pas encore de server => on remonte
                    if let Some(parent_id) = current_shard_id.parent() {
                        current_shard_id = parent_id;
                        continue;
                    }
                }

                if shard.dgs.is_some() {
                    return shard.dgs.clone();
                }
            }
        }

        eprintln!(
            "⚠️ [Routage] Aucun serveur trouvé pour la position {:?}",
            position
        );
        None
    }

    pub fn count_entity_in_shard(&self, shard_id: ShardId) -> usize {
        if let Some(shard) = self.shards.get(&shard_id) {
            return shard.entities.len();
        }

        0
    }

    pub fn _get_entities(&self) -> Vec<Entity> {
        let mut entities = Vec::new();

        for shard in self.shards.values() {
            for entity in shard.entities.iter() {
                entities.push(entity.clone());
            }
        }

        entities
    }

    pub fn get_random_spawn_point(&self) -> Option<Vec2> {
        let mut rng = rand::rng();

        let all_entities: Vec<&Entity> = self
            .shards
            .values()
            .flat_map(|shard| shard.entities.iter())
            .collect();

        let random_entity = all_entities.iter().cloned().choose(&mut rng)?;

        Some(random_entity.pos)
    }
}
