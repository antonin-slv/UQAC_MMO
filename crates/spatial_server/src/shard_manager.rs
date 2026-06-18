use crate::broker_client::BrokerClient;
use crate::quadtree::{Entity, QuadTree, ShardId, ShardIdExt};
use broker_protocol::broker_message::NodeId;
use core_types::{Rect, Vec2};
use game_message::msg_dgs::Heartbeat;
use game_message::msg_entities::NetworkEntityId;
use rand::random_range;
use rand::RngExt;
use rand::prelude::IteratorRandom;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq)]
pub enum ShardState {
    Active,
    PendingSplit, // En train de se diviser, mais garde le contrôle
    PendingDestroy, //en train de se faire casser la figure (le parent se merge)
    PendingReady, // Créé et avec un DGS, mais attend la fin du/des transfert P2P
}

#[derive(Clone, Debug)]
pub struct Shard {
    pub dgs: Option<NodeId>,
    pub entities: HashSet<Entity>,
    pub state: ShardState,
    pub parent_id: Option<ShardId>,
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
        //broker.spawn_new_dgs(1);

        println!("On new shard ! : {:?}", shard_id);

        let mut old_dgs_id = None;
        if let Some(parent_id) = parent {
            if let Some(parent_shard) = self.shards.get_mut(&parent_id) {
                old_dgs_id = parent_shard.dgs;
                parent_shard.state = ShardState::PendingSplit;
            }
        }

        if let Some((new_dgs, shard_assignment)) = self
            .active_dgs
            .iter_mut()
            .find(|(_, shard)| shard.is_none())
        {
            let new_dgs_id = new_dgs.clone();

            *shard_assignment = Some(shard_id);

            // Envoi au DGS de la demande de transfert (P2P via TakeArea)
            broker.assign_shard_to_dgs(new_dgs_id.clone(), vec![(bounds, old_dgs_id)]);

            self.shards.insert(
                shard_id,
                Shard {
                    entities: HashSet::new(),
                    dgs: Some(new_dgs_id),
                    state: ShardState::PendingReady,
                    parent_id: parent,
                },
            );
        } else {
            println!(
                "⏳ Aucun DGS libre. Shard {:?} mis en file d'attente.",
                shard_id
            );
            // Fallback sécuritaire (à améliorer plus tard)
            self.shards.insert(
                shard_id,
                Shard {
                    entities: HashSet::new(),
                    dgs: None,
                    state: ShardState::PendingReady,
                    parent_id: parent,
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
        println!("🔄 [Merge] Demande de fusion pour le parent {:?}", parent_id);
        let mut areas_to_take = Vec::new();

        // 1. On fige les enfants et on les enregistre dans l'Overlay
        for (child_id, child_bounds) in children {
            if let Some(child_shard) = self.shards.get_mut(&child_id) {
                child_shard.state = ShardState::PendingDestroy;
                areas_to_take.push((child_bounds.clone(), child_shard.dgs.clone()));
                self.pending_merges.insert(child_id, child_bounds);
            }
        }

        // 2. On trouve le gros serveur parent qui va tout absorber
        if let Some((new_dgs, shard_assignment)) = self
            .active_dgs
            .iter_mut()
            .find(|(_, shard)| shard.is_none())
        {
            let new_dgs_id = new_dgs.clone();
            *shard_assignment = Some(parent_id);

            println!("✅ [Merge] DGS {} prêt à absorber les sous-zones.", new_dgs_id);
            broker.assign_shard_to_dgs(new_dgs_id.clone(), areas_to_take);

            self.shards.insert(
                parent_id,
                Shard {
                    entities: HashSet::new(),
                    dgs: Some(new_dgs_id),
                    state: ShardState::PendingReady,
                    parent_id: parent_id.parent()
                },
            );
        } else {
            println!("⏳ [Merge] Aucun DGS libre. Parent {:?} en file d'attente.", parent_id);
            self.shards.insert(
                parent_id,
                Shard {
                    entities: HashSet::new(),
                    dgs: None,
                    state: ShardState::PendingReady,
                    parent_id: parent_id.parent(),
                },
            );
        }
    }

    // Nouvelle version : Le DGS s'identifie avec son NodeId et la zone (Rect) qu'il a fini de charger
    pub fn on_area_took(&mut self, dgs_id: NodeId, bounds: Rect, quad_tree: &QuadTree) {

        // vérif si c'est un merge
        let mut merged_child_id = None;
        for (&child_id, &rect) in self.pending_merges.iter() {
            if rect == bounds {
                merged_child_id = Some(child_id);
                break;
            }
        }

        //execution merge
        if let Some(child_id) = merged_child_id {
            self.pending_merges.remove(&child_id); // Retire la zone de la file d'attente

            if let Some(child_shard) = self.shards.remove(&child_id) { // On supprime le shard mort

                // MAGIE : Le serveur DGS de l'enfant est vide, on le remet dans le pool des serveurs libres !
                if let Some(child_dgs) = child_shard.dgs {
                    self.active_dgs.insert(child_dgs, None);
                }

                println!("✅ [Merge] Sous-zone {:?} absorbée. Serveur enfant libéré.", bounds);

                // On regarde si toutes les zones de ce parent ont été absorbées
                if let Some(parent_id) = child_shard.parent_id {
                    let still_has_children = self.pending_merges.keys().any(|&id| {
                        self.shards.get(&id).map_or(false, |s| s.parent_id == Some(parent_id))
                    });

                    if !still_has_children {
                        if let Some(parent_shard) = self.shards.get_mut(&parent_id) {
                            parent_shard.state = ShardState::Active;
                            println!("🎉 [Merge] Téléchargement total réussi ! Parent {} Actif.", parent_id);
                        }
                    }
                }
            }
            return; // C'était un Merge, on arrête ici !
        }


        //gestion du split.
        let mut target_shard_id = None;

        // 1. Trouver à quel ShardId correspond ce DGS et ce Rect
        for (&shard_id, shard) in self.shards.iter() {
            if shard.dgs == Some(dgs_id.clone()) {
                if let Some((shard_bounds, _)) = quad_tree.get_shard_bounds(&shard_id) {
                    if shard_bounds == bounds {
                        target_shard_id = Some(shard_id);
                        break; // On a trouvé, on arrête la boucle
                    }
                }
            }
        }

        // 2. Si on a identifié le shard, on procède au Handoff
        if let Some(child_shard_id) = target_shard_id {
            let mut parent_id_to_check = None;

            if let Some(child_shard) = self.shards.get_mut(&child_shard_id) {
                if child_shard.state == ShardState::PendingReady {
                    child_shard.state = ShardState::Active;
                    parent_id_to_check = child_shard.parent_id;
                    println!("✅ [Handoff] La sous-zone {:?} est désormais Active sur le DGS {} !", bounds, dgs_id);
                }
            }

            // 3. Vérification de la destruction du parent
            if let Some(parent_id) = parent_id_to_check {
                let is_fully_transferred = self.check_if_parent_fully_transferred(parent_id);

                if is_fully_transferred {
                    println!("🎉 [Handoff] Toutes les sous-zones du parent {} ont confirmé (AreaTook). Destruction du parent.", parent_id);
                    self.shards.remove(&parent_id);
                }
            }
        } else {
            eprintln!(
                "⚠️ [Handoff] AreaTook reçu du DGS {}, mais aucun shard en attente trouvé pour les bounds {:?}",
                dgs_id, bounds
            );
        }
    }

    fn check_if_parent_fully_transferred(&self, parent_id: ShardId) -> bool {
        let mut has_children = false;

        for shard in self.shards.values() {
            if shard.parent_id == Some(parent_id) {
                has_children = true;
                // Si au moins un enfant est encore en attente, le transfert n'est pas fini
                if shard.state == ShardState::PendingReady {
                    return false;
                }
            }
        }

        // Retourne vrai seulement s'il avait des enfants ET qu'aucun n'est en attente
        has_children
    }

    pub fn on_shard_destroyed(
        &mut self,
        parent: ShardId,
        parent_bounds: Rect,
        shards: Vec<ShardId>,
        broker: &BrokerClient,
    ) {
        for shard_id in shards {
            if let Some(shard) = self.shards.get(&shard_id) {
                if let Some(dgs) = shard.dgs {
                    let mut old_dgs_id = None;
                    if let Some(shard) = self.shards.get(&parent) {
                        old_dgs_id = shard.dgs;
                    }

                    self.active_dgs.insert(dgs.clone(), None);

                    broker.remove_shard_to_dgs(dgs.clone(), vec![(parent_bounds, old_dgs_id)]);
                }
            }

            self.shards.remove(&shard_id);
        }
    }

    pub fn on_new_dgs(&mut self, dgs_id: NodeId, quad_tree: &QuadTree, broker: &BrokerClient) {
        let mut waiting_shard_info = None;

        // 1. Chercher s'il y a un shard qui attend un serveur
        if let Some((&shard_id, shard)) = self
            .shards
            .iter_mut()
            .find(|(_, shard)| shard.dgs.is_none() && shard.state != ShardState::PendingSplit)
        {
            shard.dgs = Some(dgs_id.clone());
            waiting_shard_info = Some((shard_id, shard.parent_id));
        }

        // 2. Si on a trouvé un shard, on procède à l'assignation
        if let Some((shard_id, parent_id)) = waiting_shard_info {
            println!(
                "✅ Nouveau DGS assigné au shard en attente : {:?}",
                shard_id
            );

            // On retrouve le DGS du parent pour que le nouveau DGS puisse lui voler les entités (P2P)
            let mut old_dgs_id = None;
            if let Some(p_id) = parent_id {
                if let Some(parent_shard) = self.shards.get(&p_id) {
                    old_dgs_id = parent_shard.dgs.clone();
                }
            }

            if let Some((bounds, _)) = quad_tree.get_shard_bounds(&shard_id) {
                // On passe bien old_dgs_id (qui n'est plus None)
                broker.assign_shard_to_dgs(dgs_id.clone(), vec![(bounds, old_dgs_id)]);
            } else {
                println!("No bounds found for shard {:?}", shard_id);
            }

            self.active_dgs.insert(dgs_id, Some(shard_id));
        } else {
            // Aucun shard n'était en attente, le serveur rentre dans le pool des serveurs inactifs
            self.active_dgs.insert(dgs_id, None);
        }
    }

    pub fn on_dgs_stopped(&mut self, dgs_id: NodeId) {
        println!("DGS Stopped : {:?}", dgs_id);

        if let Some(Some(shard_id)) = self.active_dgs.get_mut(&dgs_id) {
            if let Some(shard) = self.shards.get_mut(shard_id) {
                shard.dgs = None;
            }
        }
    }

    pub fn on_area_released(&mut self, dgs_id: NodeId) {
        for (_, shard) in self.shards.iter_mut() {
            if let Some(shard_dgs) = shard.dgs
                && shard_dgs == dgs_id
            {
                shard.dgs = None;
            }
        }
    }

    pub fn on_client_disconnected(&mut self, client_id: NodeId) {
        //println!("Client disconnected : {:?}", client_id);
        //let shard_id = self
        //    .entities
        //    .iter()
        //    .find(|(_, entity_mapping)| client_id == entity_mapping.client_id.clone());
        //if let Some((entity_id, entity_mapping)) = shard_id {
        //    println!(
        //        "Remove client {} from shard {}",
        //        client_id, entity_mapping.shard_id
        //    );
        //    self.remove_entity_from_shard(entity_mapping.shard_id.clone(), entity_id.clone());
        //} else {
        //    println!("No shard found for client : {}", client_id)
        //}
    }

    pub fn set_entity_shard(&mut self, shard_id: ShardId, entity: Entity) {
        let old_shard_id = self.get_shard(entity.id);
        //println!("Entities : {:?}", self.entities);
        //println!(
        //    "New Shard iD : {} / Old Shard ID: {:?}",
        //    shard_id, old_shard_id
        //);
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
                    parent_id: None,
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

        // OVERLAY DE FUSION : On intercepte les joueurs qui sont dans des zones en train de mourir
        for (&child_id, rect) in self.pending_merges.iter() {
            if rect.contains(position) {
                if let Some(shard) = self.shards.get(&child_id) {
                    return shard.dgs; // On renvoie le serveur du petit enfant en sursis
                }
            }
        }

        if let Some(shard_id) = quad_tree.get_shard_of_point(position) {
            let mut current_shard_id = shard_id;

            // On remonte l'arbre si la zone est en pleine transition
            while let Some(shard) = self.shards.get(&current_shard_id) {
                if shard.state == ShardState::PendingReady {
                    // L'enfant n'est pas encore prêt, on remonte à l'ancien propriétaire (parent)
                    if let Some(parent) = shard.parent_id {
                        if let Some(parent_shard) = self.shards.get(&parent) {
                            if parent_shard.dgs.is_some() {
                                current_shard_id = parent;
                                continue;
                            }
                        }
                    }
                }

                // Si le shard est Active ou PendingSplit, c'est lui le vrai patron actuel
                return shard.dgs;
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

    pub fn get_entities(&self) -> Vec<Entity> {
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

        // 1. Collecter toutes les entités existantes
        let all_entities: Vec<&Entity> = self
            .shards
            .values()
            .flat_map(|shard| shard.entities.iter())
            .collect();

        // 2. Choisir une entité aléatoire si la liste n'est pas vide
        let random_entity = all_entities.iter().cloned().choose(&mut rng)?;

        let position = Vec2::new(
            rng.random_range(-10.0..10.0) + random_entity.pos.x,
            rng.random_range(-10.0..10.0) + random_entity.pos.y,
        );

        Some(position)
    }
}
