use crate::dgs_network::{BrockerManager, NetworkIdComponent};
use crate::dgs_to_dgs_message::{FullEntityMetaData, FullEntityState};
use crate::events;
use crate::events::{
    AssignedChunks, Authority, EntityStateTransferEvent, FastSet, PendingTransfersForOther,
};
use crate::game::{ClientDirectory, ControlledBy};
use bevy::app::Plugin;
use bevy::prelude::*;
use bevy::prelude::{Commands, MessageReader, ResMut, SystemSet};
use broker_protocol::topic_patterns::TopicPattern;
use broker_protocol::topics::SecurityDomain::{PrivateReadPublicWrite, PublicReadPrivateWrite};
use broker_protocol::topics::{Namespace, SecurityDomain, Topic, TopicBuilder, TopicDefaults};
use core_types::chunks::{get_chunk_size, GameChunk, GameChunkAera};
use game_message::core_types::SerializedGameChunkAera;
use game_message::msg_dgs::{ChunkHandOff, ChunkHandOffAction, EntityStateTransferHandoff};
use std::env;

pub struct ChunkPlugin;

#[derive(SystemSet, Debug, Hash, PartialEq, Eq, Clone)]
pub struct ChunksAuthorityLogicSet;

impl Plugin for ChunkPlugin {
    fn build(&self, app: &mut App) {
        let world_size = match env::var("WORLD_SIZE") {
            Ok(val) => val.parse::<f32>().unwrap(),
            Err(_) => panic!("Please set WORLD_SIZE environment variable"),
        };

        let max_tree_depth = match env::var("QUADTREE_MAX_DEPTH") {
            Ok(val) => val.parse::<u8>().unwrap(),
            Err(_) => panic!("Please set QUADTREE_MAX_DEPTH environment variable"),
        };

        app.insert_resource(AssignedChunks {
            assigned_chunks: FastSet::default(),
            ghost_chunks: FastSet::default(),
            chunk_size: get_chunk_size(world_size, max_tree_depth),
        })
        .insert_resource(PendingTransfersForOther { aeras: Vec::new() })
        .add_message::<events::SnapshotReceived>()
        .add_message::<events::ChunkHandOffMessage>()
        .add_message::<EntityStateTransferEvent>()
        .add_systems(
            Update,
            (
                handle_snapshot_received,
                handle_chunks_handoff_message,
                handle_state_transfer,
            )
                .chain()
                .in_set(ChunksAuthorityLogicSet),
        )
        .add_systems(
            PostUpdate,
            execute_handoff_transfers.in_set(ChunksAuthorityLogicSet),
        );
    }
}
fn handle_snapshot_received(
    mut command: Commands,
    mut snapshot_messages: MessageReader<events::SnapshotReceived>,
    client_directory: Res<ClientDirectory>,
    mut entity_query: Query<(&mut Transform, &mut Authority), With<ControlledBy>>,
) {
    for snp_msg in snapshot_messages.read() {
        for entity in snp_msg.snapshot.entities.iter() {
            if let Some(current_entities_of_node) = client_directory.sessions.get(&entity.owner_id)
            {
                if let Some(node_current_entity) = current_entities_of_node
                    .iter()
                    .find(|(_, net_id)| *net_id == entity.network_id)
                {
                    if let Ok((mut transform, auth)) = entity_query.get_mut(node_current_entity.0) {
                        if *auth == Authority::Authoritative {
                            continue;
                        }
                        *transform = Transform::from_translation(Vec3::new(
                            entity.position[0],
                            entity.position[1],
                            0.0,
                        ));
                    }
                }
            } else {
                command.spawn((
                    NetworkIdComponent(entity.network_id),
                    Transform::from_translation(Vec3::new(
                        entity.position[0],
                        entity.position[1],
                        0.0,
                    )),
                    Authority::Ghost,
                    ControlledBy {
                        client_id: entity.owner_id,
                    },
                ));
            }
        }
    }
}

// ce que fait cette fonction :
/*
   Si on a un TakeAera :
       - S'abonne aux inputs + replications de tout les chunks liés + leur bordures
       - On s'abonne aux Handoff de tt ces chunks, et on désabonne l'ancien propriétaire de ces handoff (comme ça, c'est "atomique")
       - On prévient le copain qu'on est prêt.
   Si on a un ReadyToTake :
       - On ajoute la zone à nos pending_transfers (On enverra la data à la fin de la frame)
*/

fn handle_chunks_handoff_message(
    mut ev_chunk: MessageReader<events::ChunkHandOffMessage>,
    mut chunk_directory: ResMut<AssignedChunks>,
    broker: ResMut<BrockerManager>,
    mut pending_transfers: ResMut<PendingTransfersForOther>,
) {
    let AssignedChunks {
        chunk_size,
        ghost_chunks,
        assigned_chunks,
        ..
    } = &mut *chunk_directory;

    for ev in ev_chunk.read() {
        match ev.message.action {
            // PHASE 1: Le Director nous dit de prendre une zone
            ChunkHandOffAction::TakeArea => {
                for i in 0..ev.message.areas.len() {
                    let aera = ev.message.areas[i].clone();
                    let old_owner = ev.message.old_dgs_ids[i].clone();
                    let chunk_aera = aera.as_chunk_aera(*chunk_size);

                    let extended_chunk_aera = GameChunkAera {
                        x_min: chunk_aera.x_min - 1,
                        x_max: chunk_aera.x_max + 1,
                        y_min: chunk_aera.y_min - 1,
                        y_max: chunk_aera.y_max + 1,
                    };
                    for chunk in extended_chunk_aera.iter() {
                        if !assigned_chunks.contains(&chunk) {
                            ghost_chunks.insert(chunk);
                        }
                    }

                    let input_root = Topic::security_namespace_as_u8(
                        PrivateReadPublicWrite,
                        Namespace::SpatialInput,
                    );
                    let chunk_data_root =
                        Topic::security_namespace_as_u8(PublicReadPrivateWrite, Namespace::Chunk);
                    // On s'abonne pour commencer à écouter (Ghost)
                    let all_subscribes_to_do = TopicPattern::new()
                        .with_list(vec![input_root, chunk_data_root])
                        .with_layers(extended_chunk_aera); //ici on s'abonne directement aux ghost ZONES ET aux zones normales
                    broker.client.batch_subscribe(all_subscribes_to_do, 0);

                    let chunk_entity_hand_off_topic = TopicPattern::new()
                        .with_head(Namespace::ChunkEntityHandOff, PublicReadPrivateWrite)
                        .with_layers(chunk_aera);
                    broker
                        .client
                        .batch_subscribe(chunk_entity_hand_off_topic.clone(), 0);
                    broker
                        .client
                        .batch_unsubscribe(chunk_entity_hand_off_topic, old_owner);

                    // On prévient l'ancien propriétaire qu'on est prêt à recevoir les entités
                    let topic_old_owner =
                        TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::NodeLine)
                            .append_id(old_owner)
                            .build();

                    let ready_message = ChunkHandOff {
                        action: ChunkHandOffAction::ReadyToTake,
                        areas: vec![aera],
                        old_dgs_ids: vec![old_owner], // On envoie notre ID
                    };
                    broker
                        .client
                        .publish_reliable(topic_old_owner, &ready_message);
                }
            }

            // PHASE 2: Un autre serveur est prêt à prendre notre zone (Nous sommes le Source)
            ChunkHandOffAction::ReadyToTake => {
                for i in 0..ev.message.areas.len() {
                    let aera = ev.message.areas[i].clone();
                    let target_dgs = ev.message.old_dgs_ids[i].clone();

                    pending_transfers
                        .aeras
                        .push((aera.as_chunk_aera(*chunk_size), target_dgs));
                }
            }
            ChunkHandOffAction::AreaTook => { /* Pour le spatial server */ }
            ChunkHandOffAction::ReleaseArea => { /* ... */ }
        }
    }
}

// PHASE 3: On reçoit les entités transférées (Nous sommes le Target)
//2 CAS : entity HandOFF ou chunk handoff (on transmet des entités d'un chunk ou UN CHUNK ET SON AUTHORITE)
fn handle_state_transfer(
    mut ev_transfer: MessageReader<EntityStateTransferEvent>,
    mut commands: Commands,
    mut chunk_directory: ResMut<AssignedChunks>,
    mut client_directory: ResMut<ClientDirectory>,
    mut current_in_ecs_entities: Query<(&mut Transform, &mut Authority), With<ControlledBy>>,
    broker: ResMut<BrockerManager>, // query pour mettre à jour des fantômes existants ou en spawner de nouveaux
) {
    if ev_transfer.is_empty() {
        return;
    }

    let mut taken_aeras: Vec<core_types::Rect> = Vec::new();
    for ev in ev_transfer.read() {
        let is_chunk_handoff = ev.message.chunk_handoff;
        println!("[Handoff] Réception de l'état des entités. Prise d'autorité absolue.");

        let orgin_aera: GameChunkAera = ev.message.origin_aera.into();

        if is_chunk_handoff {
            let chunks_pattern = TopicPattern::new()
                .with_head(Namespace::Chunk, PublicReadPrivateWrite)
                .with_layers(orgin_aera);
            broker.client.batch_unsubscribe(chunks_pattern, 0);
        }

        // 1. Désérialiser ev.message.data
        let entities: Vec<FullEntityState> = ev
            .message
            .data
            .iter()
            .filter_map(|data| match bitcode::decode::<FullEntityState>(data) {
                Ok(state) => Some(state),
                Err(e) => {
                    println!("Erreur de désérialisation d'une entité transférée : {}", e);
                    None
                }
            })
            .collect();

        'iter_through_recv_entities: for received_entity in entities {
            let recv_position = Vec3::new(
                received_entity.position[0],
                received_entity.position[1],
                0.0,
            );
            if let Some(current_in_map_entity) = client_directory
                .sessions
                .get(&received_entity.owner_node_id)
            {
                //On itère dans les entité que l'on connait que le node controle
                //on met à jours ssi on a pas l'autorité
                if let Some(current_entity) = current_in_map_entity
                    .iter()
                    .find(|(_, net_id)| *net_id == received_entity.network_entity_id)
                {
                    if let Ok((mut transform, mut auth)) =
                        current_in_ecs_entities.get_mut(current_entity.0)
                    {
                        // On a trouvé LA bonne entité
                        if *auth != Authority::Authoritative {
                            *transform = Transform::from_translation(recv_position);
                            *auth = Authority::Authoritative;
                        }
                        continue 'iter_through_recv_entities;
                    }
                }
            }
            // on arrive ici si l'entité est nouvelle pour nous.
            let spawned_id = commands
                .spawn((
                    Transform::from_translation(recv_position),
                    Authority::Authoritative,
                    ControlledBy {
                        client_id: received_entity.owner_node_id,
                    },
                    NetworkIdComponent(received_entity.network_entity_id),
                ))
                .id();

            client_directory
                .sessions
                .entry(received_entity.owner_node_id)
                .or_insert_with(Vec::new)
                .push((spawned_id, received_entity.network_entity_id));
        }
        if is_chunk_handoff {
            //si on récupère un chunk, on les stocks dans le tableaux
            for chunk in orgin_aera.iter() {
                chunk_directory.ghost_chunks.remove(&chunk);
                chunk_directory.assigned_chunks.insert(chunk);
            }
            taken_aeras.push(orgin_aera.to_core_rect(chunk_directory.chunk_size));
        }
    }

    let spatial_server_topic =
        TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::SpatialServer).build();

    let handoff_complete_message = &ChunkHandOff {
        action: ChunkHandOffAction::AreaTook,
        old_dgs_ids: vec![],
        areas: taken_aeras,
    };
    broker
        .client
        .publish_reliable(spatial_server_topic, handoff_complete_message);
}

/*
 === PARTIE 2 Du handoff :
 - on vide les pending_tranfers
 - On envoit les data détaillées au nouveau serveur
 - On se désinscrit des Inputs des chunks consernés
 - On retire les chunks concernés de la liste de nos chunks.
(PLUS TARD :
    -> il y aura un dernier broadcast vers les joueurs avec les données de ce DGS (milieu du post update)
    -> Les chunks n'étant plus assigné, toutes les entités non comprises dans nos chunk actifs passeront en ghost (fin du post update)
    -> L'autre chunk aura complètement pris le relais (après son handle_state_tranfer)
)
 */
fn execute_handoff_transfers(
    mut pending_transfers: ResMut<PendingTransfersForOther>,
    broker: ResMut<BrockerManager>,
    mut entity_query: Query<(Entity, &Transform, &ControlledBy, &mut Authority)>,
    mut chunk_directory: ResMut<AssignedChunks>,
) {
    if pending_transfers.aeras.is_empty() {
        return;
    }

    let mut killed_ghost_chunks: FastSet<GameChunk> = FastSet::default();

    for (chunk_aera, target_dgs) in pending_transfers.aeras.drain(..) {
        for border_chunk in chunk_aera.get_borders().iter() {
            killed_ghost_chunks.insert(*border_chunk);
        }

        let rect = events::to_rect(&chunk_aera, chunk_directory.chunk_size);

        let mut transfer_data = Vec::new();
        // 1. On rassemble et on rétrograde en une seule passe
        for (_, transform, owner, authority) in entity_query.iter_mut() {
            let x_y = transform.translation.xy();
            if rect.contains(x_y) && *authority == Authority::Authoritative {
                let meta_data = FullEntityMetaData {
                    entity_type: 0,
                    health: 0,
                    extra_data: vec![],
                };
                let entity_state = FullEntityState {
                    network_entity_id: 0,
                    owner_node_id: owner.client_id,
                    position: x_y.to_array(),
                    velocity: [0.0, 0.0],
                    meta_data,
                };

                let serialized = bitcode::encode(&entity_state);
                transfer_data.push(serialized);
                // On est dans le début du PostUpdate. On a prévenu le destinataire.
                // On va broadcast une dernière fois, puis libérer l'autorité avec le mécanisme naturel (pas chez moi == pas autorité)
            }
        }

        // 2. On envoie le paquet final
        let handoff_msg = EntityStateTransferHandoff {
            entity_handoff: false,
            chunk_handoff: true,
            origin_aera: SerializedGameChunkAera::from(chunk_aera),
            data: transfer_data,
        };

        let topic_target = TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::NodeLine)
            .append_id(target_dgs)
            .build();

        broker.client.publish_reliable(topic_target, &handoff_msg);

        for chunk in chunk_aera.iter() {
            chunk_directory.assigned_chunks.remove(&chunk);
        }
    }
    //
    for chunk in &chunk_directory.assigned_chunks {
        for border_chunk in GameChunkAera::from(*chunk).get_borders() {
            killed_ghost_chunks.remove(&border_chunk);
        }
    }
    chunk_directory
        .ghost_chunks
        .retain(|current_chunk| !killed_ghost_chunks.contains(current_chunk));

    // 3. Se désabonner des topics Input de chaque chunk perdu.
    let vec_of_killed_ghost_chunks: Vec<GameChunk> = killed_ghost_chunks.into_iter().collect();

    for chunk_batch in vec_of_killed_ghost_chunks.chunks(200) {
        //on le fait 200 par 200 pour éviter de dépasser les 1400 octets max de l'UDP... Ce serait surement mieux de le cacher dans la fonction du client.
        let chunk_topic_pattern = TopicPattern::new()
            .with_head(Namespace::SpatialInput, PrivateReadPublicWrite)
            .with_layers(chunk_batch.to_vec());

        broker.client.batch_unsubscribe(chunk_topic_pattern, 0);
    }
}
