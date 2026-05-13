
mod snapshot;
mod network;
mod game;
mod events;

use bevy::prelude::*;
use bevy::app::{App, ScheduleRunnerPlugin};
use std::time::Duration;

use network::{NetworkPlugin};
use crate::game::GameLogicPlugin;

fn main() {
    // 3. Lancement de Bevy (Boucle principale)
    App::new()
        .add_plugins(MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(
            // Le serveur tourne à 60 ticks par seconde (Tick rate).
            Duration::from_secs_f64(1.0 / 60.0),
        )))
        .add_plugins(NetworkPlugin)
        .add_plugins(GameLogicPlugin)
        .add_plugins(snapshot::SnapshotPlugin)
        .run();
}