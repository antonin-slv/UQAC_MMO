pub mod broker_inner_structs;

use crate::broker_inner_structs::{BrokerState, ConnectionOwner};
use bytes::{BufMut, BytesMut};
use dotenv::dotenv;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use shared_replication::STREAM_HANDSHAKE;
use shared_replication::broker::*;
use std::env;

const CLIENT_LISTEN_PORT_ENV_NAME: &str = "BROKER_PUBLIC_PORT";
const SERVER_LISTEN_PORT_ENV_NAME: &str = "BROKER_PRIVATE_PORT";

fn read_env_var<T: std::str::FromStr>(name: &str) -> Option<T> {
    let var = env::var(name);

    match var {
        Ok(port_str) => match port_str.parse() {
            Ok(port) => Some(port),
            Err(_) => {
                eprintln!("Error : {} Parse error", name);
                None
            }
        },
        Err(e) => {
            eprintln!("Error : {} : {}", name, e);
            None
        }
    }
}

fn main() {
    dotenv().ok();
    let listen_port = read_env_var(CLIENT_LISTEN_PORT_ENV_NAME);
    let client_port = match listen_port {
        Some(port) => port,
        None => {
            panic!("It made me panic");
        }
    };
    println!("[Broker] Port (fort client) Is : {}", client_port);

    let server_port = read_env_var(SERVER_LISTEN_PORT_ENV_NAME);
    let server_port = match server_port {
        Some(port) => port,
        None => {
            panic!("It made me panic");
        }
    };
    println!("[Broker] Port (fort trusted comrades) Is : {}", server_port);

    let client_socket = GamePeer::new(QuicBackend::new());

    client_socket
        .listen("0.0.0.0", client_port)
        .expect("The brocker socket for clients could not be created");

    let safe_socket = GamePeer::new(QuicBackend::new());
    safe_socket
        .listen("0.0.0.0", server_port)
        .expect("The brocker socket for trusted associate could not be created");

    run_broker(client_socket, safe_socket);
}

fn run_broker(mut client_peer: GamePeer, mut safe_peer: GamePeer) {
    // 1. Les tables de routage (État)

    // pas un arbre car flemme
    let mut broker_state: BrokerState = BrokerState::new();

    loop {
        while let Ok(Some(event)) = client_peer.poll() {
            broker_tick(&mut client_peer, &mut broker_state, event, false);
        }

        while let Ok(Some(event)) = safe_peer.poll() {
            broker_tick(&mut safe_peer, &mut broker_state, event, true);
        }

        //todo : retirer ça en prod.
        std::thread::sleep(std::time::Duration::from_micros(100));
        
    }
}

fn broker_tick(
    peer: &mut GamePeer,
    broker_state: &mut BrokerState,
    event: GameNetworkEvent,
    is_safe: bool,
) {
    match event {
        GameNetworkEvent::Disconnected(conn) => {
            println!("[Broker] Disconnected : {:?}", conn);
            if let Some(owner) = broker_state.remove_by_co(&conn) {
                match owner {
                    ConnectionOwner::Client(_client_id) => {
                        let Some(auth_loc) =
                            broker_state.get_client_authoritative_location(_client_id)
                        else {
                            return;
                        };

                        let Some(auth_loc) = broker_state.get_by_server_id(&auth_loc) else {
                            return;
                        };
                        let header = BrokerMessageHeaders::ClientDisconnect as u8;

                        let mut disconnect_packet = BytesMut::with_capacity(5);

                        disconnect_packet.put_u8(header);
                        disconnect_packet.put_u32_le(_client_id);

                        let stream =
                            GameStream::new(STREAM_HANDSHAKE, GameStreamReliability::Unreliable);
                        let _ = peer.send(auth_loc, &stream, disconnect_packet.freeze());
                    }
                    ConnectionOwner::Shard(topic) => {
                        if let Some(_conn) = broker_state.get_subscribers(&topic) {
                            eprintln!(
                                "[Broker] ERREUR : Shard pour topic {:?} déconnecté SANS supression du topic ?",
                                topic
                            );
                            //todo : treat the error at runtime ?
                        }
                    }

                    ConnectionOwner::Spatial() => {
                        panic!("Spatial Server should never disconnected !");
                    }

                    ConnectionOwner::Orchestrator() => {
                        panic!("Orchestrator should never disconnected !");
                    }
                }

                // todo : faire quelque chose de plus élégant que de supprimer tout les abonnements de cette connexion ?
                broker_state.unsubscribe_connexion_all(conn);
            }
        }

        GameNetworkEvent::Connected(conn) => {
            println!("[Broker] Nouvelle connexion : {:?}", conn);
            match peer.create_stream(conn, GameStreamReliability::Reliable, STREAM_HANDSHAKE) {
                Ok(_) => {}
                Err(e) => {
                    println!(
                        "[NetworkBridge] failed to create the Handshake stream {:?}",
                        e
                    );
                }
            }
        }

        GameNetworkEvent::Message {
            connection,
            stream,
            data,
        } => {
            // Si le paquet est vide, on l'ignore
            let discard_message = BrokerMessageHeaders::DiscardedMessageBecauseYouKnow as u8;
            let header_byte = data.first().unwrap_or(&discard_message);
            let header = BrokerMessageHeaders::from(*header_byte);

            let mut process_message = || -> Option<()> {
                match header {
                    BrokerMessageHeaders::Subscribe => {
                        let client_id = ClientId::extract_from_slice(data.get(1..5)?)?; // gets the packet said client id
                        if !is_safe {
                            let connection_owner = broker_state.get_owner_by_co(&connection)?; //verify if we know the connection

                            if let &ConnectionOwner::Client(owner_as_client_id) = connection_owner {
                                //si c'est un client, on vérifie que c'est bien lui-même qu'il abonne.
                                if owner_as_client_id != client_id {
                                    return None; //le client n'a pas l'authorité.
                                }
                            }
                        }

                        let topic: Topic = data.get(5..37)?.try_into().ok()?;

                        broker_state.subscribe_client(client_id, topic);
                        //todo : gérer ça proprement.
                        broker_state.set_client_authoritative_location(client_id, topic);
                    }
                    BrokerMessageHeaders::Unsubscribe => {
                        let client_id = ClientId::extract_from_slice(data.get(1..5)?)?;

                        if !is_safe {
                            let connection_owner = broker_state.get_owner_by_co(&connection)?;
                            if let &ConnectionOwner::Client(owner_as_client_id) = connection_owner {
                                if owner_as_client_id != client_id {
                                    return None; //le client n'a pas l'authorité.
                                }
                            }
                        }

                        let topic: Topic = data.get(5..37)?.try_into().ok()?;

                        broker_state.unsubscribe_client(client_id, topic);
                    }
                    BrokerMessageHeaders::Publish => {
                        //faire des sécurités.
                        let publish_data = data.clone();
                        let topic = publish_data.get(1..32)?.try_into().ok()?;
                        let data_len_le_bytes = publish_data.get(33..35)?;
                        let data_len =
                            u16::from_le_bytes(data_len_le_bytes.try_into().ok()?) as usize;

                        //byte 1 : Broadcast packet / byte 2/3 : packet Len / next bytes : data
                        let mut broadcast_packet = BytesMut::with_capacity(1 + 2 + data_len);
                        broadcast_packet.put_u8(BrokerMessageHeaders::Broadcast as u8);
                        broadcast_packet.put_slice(data_len_le_bytes);
                        broadcast_packet.put_slice(publish_data.get(35..(35 + data_len))?);

                        let broadcast_data = broadcast_packet.freeze();

                        if let Some(client_ids) = broker_state.get_subscribers(&topic) {
                            for conn in client_ids {
                                let _ = peer.send(conn, &stream, broadcast_data.clone());
                            }
                        }
                    }
                    BrokerMessageHeaders::ClientHello => {
                        println!("[Broker] Received ClientHello from {:?}", connection);

                        //le client a envoyé ce paquet
                        let pseudo_len: &u8 = data.get(1)?;
                        let pseudo = data.get(2..(2 + *pseudo_len as usize))?;
                        //todo : check pseudo + token.

                        let client_id = broker_state.add_client(connection);

                        //in theory, we would call the spatial server before.

                        // todo : test / debug / proto / sale / nulle

                        // we do the welcome paquet for the client - - - - - - - - - - - - - - - - - --
                        let mut welcome_paquet = BytesMut::with_capacity(1 + 4);
                        welcome_paquet.put_u8(BrokerMessageHeaders::ClientWelcome as u8);
                        welcome_paquet.put_u32_le(client_id);

                        if peer
                            .send(&connection, &stream, welcome_paquet.freeze())
                            .is_err()
                        {
                            eprintln!(
                                "[Broker] Failed to send welcome packet to client {:?}",
                                connection
                            );
                            return None;
                        }
                        // sends the spawn paquet for the server - - - - - - - - - - - - - - - - -
                        let Some(topic_choosen) = broker_state.get_random_server_id() else {
                            eprintln!(
                                "[Broker] No shard available to handle the client {:?}",
                                connection
                            );
                            return None;
                        };

                        let mut spawn_client_paquet = BytesMut::new();
                        spawn_client_paquet.put_u8(BrokerMessageHeaders::SpawnClient as u8);
                        spawn_client_paquet.put_u32_le(client_id);
                        spawn_client_paquet.put_u8(*pseudo_len);
                        spawn_client_paquet.put_slice(pseudo);

                        broker_state.subscribe_client(client_id, topic_choosen);
                        broker_state.set_client_authoritative_location(client_id, topic_choosen);

                        let serv_connexion = broker_state.get_by_server_id(&topic_choosen)?;

                        let _ = peer.send(serv_connexion, &stream, spawn_client_paquet.freeze());
                    }

                    BrokerMessageHeaders::ClientInput => {
                        //todo : faire cette vérif est important ?
                        let client_id = ClientId::extract_from_slice(data.get(1..5)?)?;
                        if *broker_state.get_client_by_co(&connection)? != client_id {
                            //security check
                            return None;
                        }

                        let topic = broker_state.get_client_authoritative_location(client_id)?;
                        //todo : changer ça ? ici on part du principe que les identifiants des serveurs sont exactement leur topic.
                        if let Some(shard_conn) = broker_state.get_by_server_id(topic) {
                            //liaison location -> shard
                            let _ = peer.send(shard_conn, &stream, data.clone());
                        }
                    }

                    BrokerMessageHeaders::Heartbeat => {
                        println!("[Broker] Heartbeat received!");
                        if !is_safe {
                            println!("\t nuts ! it wasn't safe.");
                            return None;
                        }
                        let orchestrator_connexion = (*broker_state.get_orchestrator_co())?;
                        match peer.send(&orchestrator_connexion, &stream, data.clone()) {
                            Ok(_) => (),
                            Err(e) => {
                                println!(
                                    "[Broker] Failed to forward heartbeat to orchestrator: {:?}",
                                    e
                                );
                            }
                        }
                    }

                    BrokerMessageHeaders::FriendHello => {
                        println!(
                            "[Broker] FriendHello received from connection {:?}",
                            connection
                        );
                        if !is_safe {
                            println!("\t nuts ! it wasn't safe.");
                            return None;
                        }

                        let friend_type: u8 = data.get(1..2)?.first().copied()?;
                        let friend_type = BrokerFriends::try_from(friend_type).ok()?;

                        match friend_type {
                            BrokerFriends::Server => {
                                println!("[Broker] received friend_hello from server");
                                //todo : make something correct here
                                let default_topic = [0; 32];
                                broker_state.add_server(default_topic, connection);
                            }
                            BrokerFriends::Orchestrator => {
                                println!("[Broker] received friend_hello from orchestrator");
                                broker_state.add_orchestrator(connection);
                            }
                            BrokerFriends::Spatial => {
                                println!("[Broker] received friend_hello from spatial");
                                broker_state.add_spatial_server(connection);
                            }

                            _ => {
                                println!(
                                    "[Broker] received friend_hello with unknown friend type: {:?}",
                                    friend_type as u8
                                );
                            }
                        }
                    }

                    _ => {
                        println!("[Broker] Unknown message type from {:?}", connection);
                    }
                }
                Some(()) // Fin de la closure avec succès
            };

            // On exécute la closure. Si elle renvoie None, c'est que le paquet
            // était trop court, il est simplement ignoré, le serveur survit !

            let process_rslt = process_message();
            match process_rslt {
                None => {
                    println!(
                        "[Broker] Discarded message from {:?} because of invalid format or unauthorized action",
                        connection
                    );
                }
                Some(_) => {
                }
            }
        }

        _ => {}
    }
}
