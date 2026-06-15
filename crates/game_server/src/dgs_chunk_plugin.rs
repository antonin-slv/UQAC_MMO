use crate::dgs_entity_functions::{
    spawn_or_update_entity, Authority, ControlledBy, NetworkIdComponent,
};
use crate::dgs_network::BrockerManager;
use crate::events;
use crate::events::{
    AssignedChunks, ChunkTransferEvent, EntityTransferEvent, FastSet, PendingChunkTransfersForOther,
};
use crate::game::{ClientDirectory, EntityDirectory};
use bevy::app::Plugin;
use bevy::ecs::relationship::RelationshipSourceCollection;
use bevy::prelude::*;
use bevy::prelude::{Commands, MessageReader, ResMut, SystemSet};
use broker_protocol::broker_message::NodeId;
use broker_protocol::topic_patterns::TopicPattern;
use broker_protocol::topics::{Namespace, SecurityDomain, Topic, TopicBuilder, TopicDefaults};
use core_types::chunks::{get_chunk_size, GameChunk, GameChunkAera};
use game_message::msg_dgs::{ChunkDataHandOff, ChunkHandOff, ChunkHandOffAction};
use game_message::msg_entities::{EntityData, NetComponent};
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
        .insert_resource(PendingChunkTransfersForOther { aeras: Vec::new() })
        .add_message::<events::SnapshotReceived>()
        .add_message::<events::ChunkHandOffMessage>()
        .add_message::<ChunkTransferEvent>()
        .add_systems(
            PreUpdate,
            (
                handle_snapshot_received,
                handle_chunks_handoff_message,
                handle_chunk_transfer,
                handle_entity_handoff_transfers,
            )
                .chain()
                .in_set(ChunksAuthorityLogicSet),
        )
        .add_systems(
            PostUpdate,
            execute_outgoing_chunk_handoff_transfers.in_set(ChunksAuthorityLogicSet),
        );
    }
}
fn handle_snapshot_received(
    mut command: Commands,
    mut snapshot_messages: MessageReader<events::SnapshotReceived>,
    mut client_directory: ResMut<ClientDirectory>,
    mut entity_directory: ResMut<EntityDirectory>,
    mut current_in_ecs_entities: Query<
        (Entity, &mut Transform, &mut Authority),
        With<ControlledBy>,
    >,
) {
    for ev in snapshot_messages.read() {
        for entity in &ev.snapshot.entities {
            let _ = spawn_or_update_entity(
                &mut command,
                Authority::Ghost,
                &mut client_directory,
                &mut entity_directory,
                &mut current_in_ecs_entities,
                entity,
            );
        }
    }
}

fn handle_entity_handoff_transfers(
    mut ev_transfer: MessageReader<EntityTransferEvent>,
    mut commands: Commands,
    mut client_directory: ResMut<ClientDirectory>,
    mut entity_directory: ResMut<EntityDirectory>,
    mut current_in_ecs_entities: Query<
        (Entity, &mut Transform, &mut Authority),
        With<ControlledBy>,
    >,
) {
    for ev in ev_transfer.read() {
        for entity in &ev.message.data {
            let _ = spawn_or_update_entity(
                &mut commands,
                Authority::Authoritative,
                &mut client_directory,
                &mut entity_directory,
                &mut current_in_ecs_entities,
                entity,
            );
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
    mut commands: Commands,
    mut ev_chunk: MessageReader<events::ChunkHandOffMessage>,
    mut chunk_directory: ResMut<AssignedChunks>,
    broker: ResMut<BrockerManager>,
    mut pending_transfers: ResMut<PendingChunkTransfersForOther>,

    mut client_directory: ResMut<ClientDirectory>,
    mut entity_directory: ResMut<EntityDirectory>,

    mut current_in_ecs_entities: Query<
        (Entity, &mut Transform, &mut Authority),
        With<ControlledBy>,
    >,
) {
    for ev in ev_chunk.read() {
        println!("Received chunk hand-off event: {:?}", ev);
        match ev.message.action {
            // PHASE 1: Le Director nous dit de prendre une zone
            ChunkHandOffAction::TakeArea => {
                for (aera, old_owner) in ev.message.areas.iter() {
                    println!(
                        "[GameServer] recv : \t take {:?} from {:?}",
                        aera, old_owner
                    );

                    let taken_chunk_aera = aera.bounding_chunk_aera(chunk_directory.chunk_size);

                    // 1. SÉPARATION STRICTE : CŒUR vs FRONTIÈRES
                    let mut new_border_chunks: Vec<GameChunk> = Vec::new();
                    // Mise à jour de notre base de ghost chunks locale
                    for border_chunk in taken_chunk_aera.get_borders(1).iter() {
                        if !chunk_directory.assigned_chunks.contains(border_chunk) {
                            new_border_chunks.push(*border_chunk);
                            chunk_directory.ghost_chunks.insert(*border_chunk);
                        }
                    }

                    // 2. ABONNEMENTS DU CŒUR (CORE CHUNKS)
                    let mut core_topics = vec![
                        Topic::security_namespace_as_u8(
                            SecurityDomain::PrivateReadPublicWrite,
                            Namespace::SpatialInput,
                        ),
                        Topic::security_namespace_as_u8(
                            SecurityDomain::PrivateRW,
                            Namespace::ChunkEntityHandOff,
                        ),
                    ];

                    if old_owner.is_some() {
                        core_topics.push(Topic::security_namespace_as_u8(
                            SecurityDomain::PublicReadPrivateWrite,
                            Namespace::Chunk,
                        ));
                    }

                    let core_pattern = TopicPattern::new()
                        .with_list(core_topics)
                        .with_layers(taken_chunk_aera); // On cible uniquement le cœur

                    broker.client.batch_subscribe(core_pattern, 0);

                    // 3. ABONNEMENTS DES FRONTIÈRES (GHOST CHUNKS)
                    let border_topics = vec![
                        Topic::security_namespace_as_u8(
                            SecurityDomain::PrivateReadPublicWrite,
                            Namespace::SpatialInput,
                        ),
                        Topic::security_namespace_as_u8(
                            SecurityDomain::PublicReadPrivateWrite,
                            Namespace::Chunk,
                        ),
                    ];

                    let border_pattern = TopicPattern::new()
                        .with_list(border_topics)
                        .with_single_layer(new_border_chunks); // uniquement couronne extérieure

                    broker.client.batch_subscribe(border_pattern, 0);

                    // 4. HANDSHAKE AVEC L'ANCIEN PROPRIÉTAIRE
                    if let Some(old_owner_id) = old_owner {
                        // Désabonnement "atomique" de l'ancien propriétaire sur le topic de transfert du cœur
                        let chunk_entity_hand_off_topic = TopicPattern::new()
                            .with_head(Namespace::ChunkEntityHandOff, SecurityDomain::PrivateRW)
                            .with_layers(taken_chunk_aera);
                        broker
                            .client
                            .batch_unsubscribe(chunk_entity_hand_off_topic, *old_owner_id);

                        // Signal "ReadyToTake" envoyé en P2P (NodeLine)
                        let topic_old_owner =
                            TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::NodeLine)
                                .append_id(*old_owner_id)
                                .build();

                        let ready_message = ChunkHandOff {
                            action: ChunkHandOffAction::ReadyToTake,
                            areas: vec![(*aera, broker.client.node_id)],
                        };
                        broker
                            .client
                            .publish_reliable(topic_old_owner, &ready_message);
                    } else {
                        // 5. COLD START (Zone vierge)
                        let message = ChunkDataHandOff {
                            origin_aera: taken_chunk_aera,
                            old_owner: None,
                            data: vec![],
                        };
                        println!(
                            "Faking ChunkTransferEvent :\n\t{:?}\n\t{:?}",
                            aera, message.origin_aera
                        );

                        let aera_taken = handle_specific_chunk_data_handoff_message(
                            &mut commands,
                            &mut chunk_directory,
                            &mut client_directory,
                            &mut entity_directory,
                            &mut current_in_ecs_entities,
                            &broker,
                            &message,
                        );

                        publish_aera_took(&broker, vec![aera_taken]);
                    }
                }
            }

            // PHASE 2: Un autre serveur est prêt à prendre notre zone (Nous sommes le Source)
            ChunkHandOffAction::ReadyToTake => {
                let mut message = "Received ReadyToTake message".to_string();
                for msg in ev.message.areas.iter() {
                    message = format!("{}\n\t give  {:?} to {:?}", message, msg.0, msg.1);

                    pending_transfers.aeras.push(*msg);
                }
                println!("[GameServer][{:?}]{}", broker.client.node_id, message);
            }
            ChunkHandOffAction::AreaTook => { /* Pour le spatial server */ }
            ChunkHandOffAction::ReleaseArea => { /* ... */ }
        }
    }
}

// PHASE 3: On reçoit les entités transférées (Nous sommes le Target)
//        , à ce niveau, tout les chunks concernés sont déjà en ghost.
//2 CAS : entity HandOFF ou chunk handoff (on transmet des entités d'un chunk ou UN CHUNK ET SON AUTHORITE)
fn handle_chunk_transfer(
    mut ev_transfer: MessageReader<ChunkTransferEvent>,
    mut commands: Commands,
    mut chunk_directory: ResMut<AssignedChunks>,
    mut client_directory: ResMut<ClientDirectory>,
    mut entity_directory: ResMut<EntityDirectory>,
    mut current_in_ecs_entities: Query<
        (Entity, &mut Transform, &mut Authority),
        With<ControlledBy>,
    >,
    broker: ResMut<BrockerManager>, // query pour mettre à jour des fantômes existants ou en spawner de nouveaux
) {
    if ev_transfer.is_empty() {
        return;
    }

    let mut taken_aeras = Vec::new();
    for ev in ev_transfer.read() {
        println!(
            "[Handoff] Received Chunk transfer Origin area: {:?}. Old owner: {:?}. Number of entities: {}",
            ev.message.origin_aera,
            ev.message.old_owner,
            ev.message.data.len()
        );
        let aera = handle_specific_chunk_data_handoff_message(
            &mut commands,
            &mut chunk_directory,
            &mut client_directory,
            &mut entity_directory,
            &mut current_in_ecs_entities,
            &broker,
            &ev.message,
        );

        taken_aeras.push(aera);
    }

    if !taken_aeras.is_empty() {
        publish_aera_took(broker.as_ref(), taken_aeras);
    }
}

fn publish_aera_took(
    broker: &BrockerManager,
    taken_aeras: Vec<(core_types::Rect, Option<NodeId>)>,
) {
    let spatial_server_topic =
        TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::SpatialServer).build();

    let handoff_complete_message = ChunkHandOff {
        action: ChunkHandOffAction::AreaTook,
        areas: taken_aeras,
    };
    broker
        .client
        .publish_reliable(spatial_server_topic, &handoff_complete_message);
}

// !!! YOU MUST CALL publish_aeraa_took after this !!!
#[must_use]
fn handle_specific_chunk_data_handoff_message(
    mut commands: &mut Commands,
    chunk_directory: &mut AssignedChunks,
    mut client_directory: &mut ClientDirectory,
    mut entity_directory: &mut EntityDirectory,
    mut current_in_ecs_entities: &mut Query<
        (Entity, &mut Transform, &mut Authority),
        With<ControlledBy>,
    >,
    broker: &ResMut<BrockerManager>,
    message: &ChunkDataHandOff,
) -> (core_types::Rect, Option<NodeId>) {
    let orgin_aera: GameChunkAera = message.origin_aera;

    if message.old_owner.is_some() {
        let chunks_pattern = TopicPattern::new()
            .with_head(Namespace::Chunk, SecurityDomain::PublicReadPrivateWrite)
            .with_layers(orgin_aera);
        broker.client.batch_unsubscribe(chunks_pattern, 0);
    }

    for recv_entities in &message.data {
        if spawn_or_update_entity(
            &mut commands,
            Authority::Authoritative,
            &mut client_directory,
            &mut entity_directory,
            &mut current_in_ecs_entities,
            &recv_entities,
        )
        .is_empty()
        {
            //problèmes ?
        }
    }
    for chunk in orgin_aera.iter() {
        chunk_directory.assigned_chunks.insert(chunk);
        chunk_directory.ghost_chunks.remove(&chunk); // On le retire des fantômes !
    }
    // on prépare la déclaration de la prise des chunks.
    (
        orgin_aera.to_core_rect(chunk_directory.chunk_size),
        message.old_owner,
    )
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
    -> L'autre chunk aura complètement pris le relais (après son handle_state_transfer)
)
 */
fn execute_outgoing_chunk_handoff_transfers(
    mut pending_transfers: ResMut<PendingChunkTransfersForOther>,
    broker: ResMut<BrockerManager>,
    mut entity_query: Query<(
        Entity,
        &NetworkIdComponent,
        &Transform,
        &ControlledBy,
        &mut Authority,
    )>,
    mut chunk_directory: ResMut<AssignedChunks>,
) {
    if pending_transfers.aeras.is_empty() {
        return;
    }

    let mut killed_ghost_chunks: FastSet<GameChunk> = FastSet::default();

    let handoff_base_topic =
        Topic::security_namespace_as_u8(SecurityDomain::PrivateRW, Namespace::ChunkEntityHandOff);

    let mut lost_aeras_inner = Vec::new();

    for (real_aera, target_dgs) in pending_transfers.aeras.drain(..1) {
        let chunk_aera = real_aera.bounding_chunk_aera(chunk_directory.chunk_size);
        println!(
            "Executing handoff transfer for area {:?} to DGS {:?}",
            chunk_aera, target_dgs
        );

        let Some(target_dgs) = target_dgs else {
            continue;
        };

        for border_chunk in chunk_aera.get_borders(1).iter() {
            killed_ghost_chunks.insert(*border_chunk);
        }

        let rect = chunk_aera.to_core_rect(chunk_directory.chunk_size);

        let mut transfer_data = Vec::new();
        // 1. On rassemble et on rétrograde en une seule passe
        for (_, net_id, transform, owner, mut authority) in entity_query.iter_mut() {
            let x_y = core_types::Vec2::new(transform.translation.x, transform.translation.y);
            if rect.contains(x_y) && *authority == Authority::Authoritative {
                let pos = NetComponent::Position(x_y);
                let entity_data = EntityData {
                    net_id: net_id.0,
                    owner_id: owner.client_id,
                    updates: vec![pos],
                };
                *authority = Authority::LastAuthFrame;
                transfer_data.push(entity_data);
                // On est dans le début du PostUpdate. On a prévenu le destinataire.
                // On va broadcast une dernière fois, puis libérer l'autorité avec le mécanisme naturel (pas chez moi == pas autorité)
            }
        }

        // 2. On envoie le paquet final
        let handoff_msg = ChunkDataHandOff {
            origin_aera: chunk_aera,
            old_owner: broker.client.node_id,
            data: transfer_data,
        };

        let topic_target = TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::NodeLine)
            .append_id(target_dgs)
            .build();

        broker.client.publish_reliable(topic_target, &handoff_msg);

        //3. on préviens le spatial server :
        let spatial_handoff_msg = ChunkHandOff {
            action: ChunkHandOffAction::ReleaseArea,
            areas: vec![(real_aera, broker.client.node_id)],
        };

        let topic_spatial_server =
            TopicBuilder::new(SecurityDomain::PrivateRW, Namespace::SpatialServer).build();

        broker
            .client
            .publish_reliable(topic_spatial_server, &spatial_handoff_msg);

        for chunk in chunk_aera.iter() {
            chunk_directory.assigned_chunks.remove(&chunk);
        }

        lost_aeras_inner.push(GameChunkAera {
            x_min: chunk_aera.x_min + 1,
            y_min: chunk_aera.y_min + 1,
            y_max: chunk_aera.y_max - 1,
            x_max: chunk_aera.x_max - 1,
        });

        let reg_chunk_topic_pattern = TopicPattern::new() // on ne reçoit plus les handoff sur ces chunks.
            .with_list(vec![handoff_base_topic])
            .with_layers(chunk_aera);
        broker.client.batch_unsubscribe(reg_chunk_topic_pattern, 0);
    }

    //  | | |
    //  | | |    inner c'est juste celui du milieu.
    //  | | |
    let mut regular_chunks_unsubscribe_ = FastSet::default();
    for aeras in lost_aeras_inner.iter() {
        for inner_borders in aeras.get_borders_as_aera(1) {
            for inner_border_chunk in inner_borders.iter() {
                regular_chunks_unsubscribe_.insert(inner_border_chunk.clone());
            }
        }
    }

    let chunk_as_vec: Vec<GameChunk> = chunk_directory.assigned_chunks.iter().copied().collect();
    let new_border = GameChunk::get_borders_of(chunk_as_vec.as_ref(), 1);
    for chunk in new_border {
        killed_ghost_chunks.remove(&chunk);
        regular_chunks_unsubscribe_.remove(&chunk);
    }
    chunk_directory
        .ghost_chunks
        .retain(|current_chunk| !killed_ghost_chunks.contains(current_chunk));

    // 3. Se désabonner des topics Input de chaque chunk perdu.
    let vec_of_killed_ghost_chunks: Vec<GameChunk> = killed_ghost_chunks.into_iter().collect();
    let chunk_state_base_topic =
        Topic::security_namespace_as_u8(SecurityDomain::PublicReadPrivateWrite, Namespace::Chunk);
    let input_base_topic = Topic::security_namespace_as_u8(
        SecurityDomain::PrivateReadPublicWrite,
        Namespace::SpatialInput,
    );
    let roots_for_ghost = vec![input_base_topic, chunk_state_base_topic];

    for chunk_batch in vec_of_killed_ghost_chunks.chunks(200) {
        //on le fait 200 par 200 pour éviter de dépasser les 1400 octets max de l'ethernet... Ce serait surement mieux de le cacher dans la fonction du client.
        let chunk_topic_pattern = TopicPattern::new()
            .with_list(roots_for_ghost.clone())
            .with_single_layer(chunk_batch.to_vec());

        broker.client.batch_unsubscribe(chunk_topic_pattern, 0);
    }

    //on se désincrit
    let root_for_regular = vec![input_base_topic];
    for safe_regular_unsubscribe in lost_aeras_inner.iter() {
        let chunk_topic_pattern = TopicPattern::new()
            .with_list(root_for_regular.clone())
            .with_layers(*safe_regular_unsubscribe);
        broker.client.batch_unsubscribe(chunk_topic_pattern, 0);
    }

    let regular_chunks_unsubscribe_: Vec<GameChunk> =
        regular_chunks_unsubscribe_.iter().copied().collect();
    let chunk_topic_pattern = TopicPattern::new()
        .with_list(root_for_regular)
        .with_single_layer(regular_chunks_unsubscribe_);
    broker.client.batch_unsubscribe(chunk_topic_pattern, 0);
}
