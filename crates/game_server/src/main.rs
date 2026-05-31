mod events;
mod game;
mod dgs_network;
mod snapshot;

use crate::game::GameLogicPlugin;
use crate::dgs_network::NetworkPlugin;
use bevy::app::{App, ScheduleRunnerPlugin};
use bevy::prelude::*;
use dotenv::dotenv;
use std::env;
use std::time::Duration;

const DEFAULT_SERV_FREQUENCY: u16 = 60;
const SERV_FREQUENCY_ENV_NAME: &str = "SERV_FREQUENCY";

fn main() {
    dotenv().ok();

    let serv_frequency = env::var(SERV_FREQUENCY_ENV_NAME);
    let serv_frequency = match serv_frequency {
        Ok(freq) => freq.parse::<u16>().unwrap_or_else(|_| {
            eprintln!("Error: SERV_FREQUENCY was not a valid u16");
            DEFAULT_SERV_FREQUENCY
        }),
        Err(_) => {
            eprintln!("Error: SERV_FREQUENCY must be provided");
            DEFAULT_SERV_FREQUENCY
        }
    };
    println!("[Server] Server frequency: {} Hz", serv_frequency);

    // Lancement de Bevy (Boucle principale)
    App::new()
        .add_plugins(
            MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(
                1.0 / (serv_frequency as f64),
            ))),
        )
        .add_plugins(NetworkPlugin)
        .add_plugins(GameLogicPlugin)
        .add_plugins(snapshot::SnapshotPlugin)
        .run();
}
