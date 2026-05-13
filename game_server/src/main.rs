
mod snapshot;
mod network;
mod game;
mod events;

use bevy::prelude::*;
use bevy::app::{App, ScheduleRunnerPlugin};
use std::time::Duration;
use dotenv::dotenv;
use std::env;

use crate::network::{NetworkPlugin};
use crate::game::GameLogicPlugin;

const DEFAULT_SERV_FREQUENCY : u16 = 60;
const SERV_FREQUENCY_ENV_NAME: &str = "SERV_FREQUENCY";


fn main() {
    dotenv().ok();

    let serv_frequency = env::var(SERV_FREQUENCY_ENV_NAME);
    println!("Serving frequency from {:?}s...", serv_frequency);
    let serv_frequency = match serv_frequency {
        Ok(freq) => freq.parse::<u16>().unwrap_or({
            eprintln!("Error: SERV_FREQUENCY was not a valid u16. Defaulting to {} Hz.", DEFAULT_SERV_FREQUENCY);
            DEFAULT_SERV_FREQUENCY
        }),
        Err(_) => {
            eprintln!("Error: SERV_FREQUENCY must be provided : Default to {} HZ", DEFAULT_SERV_FREQUENCY);
            DEFAULT_SERV_FREQUENCY
        },
    };

    // Lancement de Bevy (Boucle principale)
    App::new()
        .add_plugins(MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(
            Duration::from_secs_f64(1.0 / (serv_frequency as f64)),
        )))
        .add_plugins(NetworkPlugin)
        .add_plugins(GameLogicPlugin)
        .add_plugins(snapshot::SnapshotPlugin)
        .run();
}