use crate::broker_client::BrokerClient;
use crate::shard_manager::ShardManager;
use broker_protocol::broker_message::NodeId;
use core_types::{Rect, Vec2};
use std::collections::HashMap; // Ajout de l'import
use std::env;
use std::hash::{Hash, Hasher};

pub type ShardId = u32;

#[derive(Clone, Copy, Debug)]
pub struct Entity {
    pub id: NodeId,
    pub pos: Vec2,
}

impl Entity {
    pub fn new(id: NodeId, pos: Vec2) -> Self {
        Self { id, pos }
    }
}
impl PartialEq for Entity {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Entity {}

impl Hash for Entity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct QuadTree {
    pub bounds: Rect,
    pub depth: u8,
    pub max_depth: u8,
    pub subdivide_threshold: usize,
    pub merge_threshold: usize,
    pub children: Option<Box<[QuadTree; 4]>>,
    pub shard_id: ShardId,
}

impl QuadTree {
    pub fn new(shard_manager: &mut ShardManager, broker: &BrokerClient) -> Self {
        let mut world_size: f32 = env::var("WORLD_SIZE")
            .expect("Env WORLD_SIZE is not set")
            .parse()
            .expect("Env WORLD_SIZE is not a number");
        world_size /= 2.0;
        let map_size = Rect {
            min_x: -world_size,
            max_x: world_size,
            min_y: -world_size,
            max_y: world_size,
        };

        let max_depth = env::var("QUADTREE_MAX_DEPTH").unwrap().parse().unwrap();
        let subdivide_threshold = env::var("SUBDIVIDE_THRESHOLD").unwrap().parse().unwrap();
        let merge_threshold = env::var("MERGE_THRESHOLD").unwrap().parse().unwrap();

        let mut quadtree = Self::new_internal(
            map_size,
            0,
            max_depth,
            subdivide_threshold,
            merge_threshold,
            0,
        );

        let result = quadtree.subdivide(shard_manager);
        quadtree.manage_subdivision(&result, shard_manager, broker);

        quadtree
    }

    fn new_internal(
        bounds: Rect,
        depth: u8,
        max_depth: u8,
        subdivide_threshold: usize,
        merge_threshold: usize,
        shard_id: ShardId,
    ) -> Self {
        Self {
            bounds,
            depth,
            max_depth,
            subdivide_threshold,
            merge_threshold,
            children: None,
            shard_id,
        }
    }

    pub fn insert(
        &mut self,
        entity: Entity,
        shard_manager: &mut ShardManager,
        broker: &BrokerClient,
    ) {
        let (_, subdivided_shards) = self.insert_private(entity, shard_manager);
        self.manage_subdivision(&subdivided_shards, shard_manager, broker);
    }

    pub fn manage_subdivision(
        &self,
        subdivided_shards: &HashMap<ShardId, ShardId>,
        shard_manager: &mut ShardManager,
        broker: &BrokerClient,
    ) {
        println!("Managed subdivisions : {:?}", subdivided_shards);
        for (new_shard, parent) in subdivided_shards {
            let sub_shard = self.get_shard_bounds(&new_shard);
            if let Some((sub_shard_bounds, is_leaf)) = sub_shard {
                if is_leaf {
                    shard_manager.on_new_shard(
                        parent.clone(),
                        new_shard.clone(),
                        sub_shard_bounds,
                        broker,
                    );
                }
            }
        }
    }

    fn insert_private(
        &mut self,
        entity: Entity,
        shard_manager: &mut ShardManager,
    ) -> (bool, HashMap<ShardId, ShardId>) {
        let mut subdivided_shards = HashMap::new();
        if !self.bounds.contains(entity.pos) {
            return (false, subdivided_shards);
        }

        if let Some(children) = &mut self.children {
            for child in children.iter_mut() {
                let (result, sub) = child.insert_private(entity, shard_manager);
                if result {
                    subdivided_shards.extend(sub);
                    return (true, subdivided_shards);
                }
            }
            return (false, subdivided_shards);
        }

        shard_manager.set_entity_shard(self.shard_id, entity);

        if shard_manager.count_entity_in_shard(self.shard_id) > self.subdivide_threshold
            && self.depth < self.max_depth
        {
            let result = self.subdivide(shard_manager);
            subdivided_shards.extend(result);
        }

        (true, subdivided_shards)
    }

    fn subdivide(&mut self, shard_manager: &mut ShardManager) -> HashMap<ShardId, ShardId> {
        let mut subdivided_shards = HashMap::new();
        let sub_shards = self.generate_sub_shards();

        let mut children = Box::new([
            QuadTree::new_internal(
                sub_shards[0].1,
                self.depth + 1,
                self.max_depth,
                self.subdivide_threshold,
                self.merge_threshold,
                sub_shards[0].0,
            ),
            QuadTree::new_internal(
                sub_shards[1].1,
                self.depth + 1,
                self.max_depth,
                self.subdivide_threshold,
                self.merge_threshold,
                sub_shards[1].0,
            ),
            QuadTree::new_internal(
                sub_shards[2].1,
                self.depth + 1,
                self.max_depth,
                self.subdivide_threshold,
                self.merge_threshold,
                sub_shards[2].0,
            ),
            QuadTree::new_internal(
                sub_shards[3].1,
                self.depth + 1,
                self.max_depth,
                self.subdivide_threshold,
                self.merge_threshold,
                sub_shards[3].0,
            ),
        ]);

        for child in children.iter() {
            subdivided_shards.insert(child.shard_id, self.shard_id);
        }

        for entity in shard_manager.drain_entities(self.shard_id) {
            for child in children.iter_mut() {
                let (result, sub) = child.insert_private(entity, shard_manager);
                if result {
                    subdivided_shards.extend(sub);
                    break;
                }
            }
        }

        self.children = Some(children);

        subdivided_shards
    }

    // NOUVELLE VERSION DE TRY_MERGE
    pub fn try_merge(
        &mut self,
        shard_manager: &mut ShardManager,
    ) -> HashMap<ShardId, Vec<ShardId>> {
        let mut merges = HashMap::new();
        let entity_count = self.count_entities(shard_manager);

        if self.children.is_some() {
            // L'approche "top-down" garantit l'optimisation voulue : on attrape le parent le plus haut.
            if entity_count < self.merge_threshold && self.depth > 0 {
                let destroyed = self.merge(shard_manager);
                if !destroyed.is_empty() {
                    merges.insert(self.shard_id, destroyed);
                }
            } else {
                // Si on ne peut pas merge ce niveau, on regarde si les enfants le peuvent.
                if let Some(children) = self.children.as_mut() {
                    for child in children.iter_mut() {
                        let child_merges = child.try_merge(shard_manager);
                        merges.extend(child_merges); // On compile toutes les fusions qui se passent en bas
                    }
                }
            }
        }
        merges
    }

    fn count_entities(&self, shard_manager: &mut ShardManager) -> usize {
        let mut count = shard_manager.count_entity_in_shard(self.shard_id);
        if let Some(children) = &self.children {
            for child in children.iter() {
                count += child.count_entities(shard_manager);
            }
        }
        count
    }

    // NOUVELLE VERSION DE MERGE
    fn merge(&mut self, shard_manager: &mut ShardManager) -> Vec<ShardId> {
        let mut destroyed_shards: Vec<(ShardId, Rect)> = Vec::new();

        // On délègue la récolte à une fonction récursive pour bien vider la totalité de l'arbre
        self.collect_and_destroy_descendants(shard_manager, self.shard_id, &mut destroyed_shards);

        let merged_ids = destroyed_shards.iter().map(|(id, _)| *id).collect();

        merged_ids
    }

    // NOUVEAU : Fonction récursive pour détruire et drainer tous les niveaux enfants
    fn collect_and_destroy_descendants(
        &mut self,
        shard_manager: &mut ShardManager,
        target_shard_id: ShardId,
        destroyed_shards: &mut Vec<(ShardId, Rect)>,
    ) {
        // En utilisant `.take()`, on met implicitement `self.children = None`
        if let Some(mut children) = self.children.take() {
            for child in children.iter_mut() {
                // On descend d'abord profondément pour aplatir les petits-enfants
                child.collect_and_destroy_descendants(
                    shard_manager,
                    target_shard_id,
                    destroyed_shards,
                );

                // Puis on draine les entités de l'enfant courant vers le parent cible (target)
                for entity in shard_manager.drain_entities(child.shard_id).iter() {
                    shard_manager.set_entity_shard(target_shard_id, entity.clone());
                }
                destroyed_shards.push((child.shard_id, child.bounds));
            }
        }
    }

    fn generate_sub_shards(&mut self) -> Vec<(ShardId, Rect)> {
        let sub_bounds = self.bounds.split();
        let mut shard_ids = Vec::new();
        let offset = self.depth * 2;
        for i in 0..4 {
            let mut child_id = i;
            child_id = child_id << offset;
            child_id |= self.shard_id;
            shard_ids.push((child_id, sub_bounds[i as usize]));
        }
        shard_ids
    }

    pub fn get_shard_bounds(&self, shard_id: &ShardId) -> Option<(Rect, bool)> {
        if let Some(children) = &self.children {
            let child_id = ((shard_id >> self.depth * 2) & 3) as usize;
            return children[child_id].get_shard_bounds(shard_id);
        }

        if *shard_id == self.shard_id {
            return Some((self.bounds.clone(), self.children.is_none()));
        }
        None
    }
}
