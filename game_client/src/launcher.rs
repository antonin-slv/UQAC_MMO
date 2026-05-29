use bevy::prelude::*;
use bevy_egui::{egui, EguiContexts, EguiPlugin};
use bevy::tasks::{IoTaskPool, Task};
use futures_lite::future;
use crate::network::NetworkManager;
use crate::structs::{ClientState, LocalPlayer};
use shared_replication::{LoginResponse, Login, Register};

// Les données tapées par l'utilisateur
#[derive(Resource)]
struct LoginUiData {
    username_input: String,
    password_input: String,
    error_message: Option<String>,
}

impl Default for LoginUiData {
    fn default() -> Self {
        Self {
            username_input: String::new(),
            password_input: String::new(),
            error_message: None,
        }
    }
}

// Un composant pour suivre la requête HTTP en arrière-plan
#[derive(Component)]
struct LoginTask(Task<Result<LoginResponse, String>>);

fn ui_login_menu(
    mut contexts: EguiContexts,
    mut ui_data: ResMut<LoginUiData>,
    mut commands: Commands,
    mut next_state: ResMut<NextState<ClientState>>,
    mut local_player: ResMut<LocalPlayer>
) {
    let Ok(ctx) = contexts.ctx_mut() else {
        println!("Impossible d'obtenir le contexte Egui");
        return;
    };

    egui::Window::new("Connexion au MMORPG")
        .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0]) // Centré à l'écran
        .collapsible(false)
        .resizable(false)
        .show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.heading("Bienvenue");
                ui.add_space(20.0);
            });

            ui.horizontal(|ui| {
                ui.label("Nom de compte :");
                ui.text_edit_singleline(&mut ui_data.username_input);
            });

            ui.horizontal(|ui| {
                ui.label("Mot de passe :");
                // text_edit_singleline avec mot de passe masqué
                ui.add(egui::TextEdit::singleline(&mut ui_data.password_input).password(true));
            });

            ui.add_space(10.0);

            // Bouton de connexion
            if ui.button("Se Connecter").clicked() {
                let username = ui_data.username_input.clone();
                let password = ui_data.password_input.clone();
                println!("Tentative de connexion avec {} / {}", username, password);

                local_player.pseudo = Some(username.clone());

                let thread_pool = IoTaskPool::get();
                //1. on tente de se co au gatekeeper
                let task = thread_pool.spawn(async move {
                    // 1. On crée un mini-moteur Tokio "jetable" isolé sur ce thread
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Impossible de créer le runtime Tokio");

                    // 2. On exécute ta requête reqwest à l'intérieur de ce moteur
                    rt.block_on(async move {
                        let client = reqwest::Client::new();
                        let res = client
                            .post("http://127.0.0.1:3630/gate-keeper/login")
                            .json(&Login { username, password })
                            .send()
                            .await;

                        match res {
                            Ok(response) if response.status().is_success() => {
                                let data = response.json::<LoginResponse>().await.map_err(|e| e.to_string())?;
                                Ok(data)
                            }
                            Ok(response) => Err(format!("Erreur: {}", response.status())),
                            Err(e) => Err(e.to_string()),
                        }
                    }) // Fin du bloc Tokio
                });

                // 2. On attache la Task à une entité Bevy pour la surveiller
                commands.spawn(LoginTask(task));

                // 3. On change l'état pour afficher un écran de chargement
                next_state.set(ClientState::Connecting);
            }

            if ui.button("S'enregistrer").clicked() {
                let username = ui_data.username_input.clone();
                let password = ui_data.password_input.clone();
                println!("Tentative de connexion avec {} / {}", username, password);

                local_player.pseudo = Some(username.clone());

                let thread_pool = IoTaskPool::get();
                //1. on tente de se co au gatekeeper
                let task = thread_pool.spawn(async move {
                    // 1. On crée un mini-moteur Tokio "jetable" isolé sur ce thread
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Impossible de créer le runtime Tokio");

                    // 2. On exécute ta requête reqwest à l'intérieur de ce moteur
                    rt.block_on(async move {
                        let client = reqwest::Client::new();
                        let res = client
                            .post("http://127.0.0.1:3630/gate-keeper/register")
                            .json(&Register { username, password })
                            .send()
                            .await;

                        match res {
                            Ok(response) if response.status().is_success() => {
                                let data = response.json::<LoginResponse>().await.map_err(|e| e.to_string())?;
                                Ok(data)
                            }
                            Ok(response) => Err(format!("Erreur: {}", response.status())),
                            Err(e) => Err(e.to_string()),
                        }
                    }) // Fin du bloc Tokio
                });

                // 2. On attache la Task à une entité Bevy pour la surveiller
                commands.spawn(LoginTask(task));

                // 3. On change l'état pour afficher un écran de chargement
                next_state.set(ClientState::Connecting);
            }

            // Affichage des erreurs
            if let Some(err) = &ui_data.error_message {
                ui.add_space(10.0);
                ui.colored_label(egui::Color32::RED, err);
            }
        });
}
fn handle_login_task(
    mut commands: Commands,
    mut tasks: Query<(Entity, &mut LoginTask)>,
    mut ui_data: ResMut<LoginUiData>,
    mut next_state: ResMut<NextState<ClientState>>,
    net: ResMut<NetworkManager>,
) {
    for (entity, mut task) in tasks.iter_mut() {
        // block_on avec poll (now_or_never) ne bloque pas le jeu !
        if let Some(result) = future::block_on(future::poll_once(&mut task.0)) {

            // La requête HTTP est terminée, on détruit la tâche
            commands.entity(entity).despawn();

            match result {
                Ok(login_response) => {
                    println!("Succès ! ID Joueur: {}", login_response.player_id);
                    println!("Connexion au serveur cible: {}:{}", login_response.server.ip, login_response.server.port);

                    // --- On tente de se connecter au server ici --- cf network pour la suite.
                    net.peer
                        .connect(login_response.server.ip.as_str(), login_response.server.port)
                        .expect("Échec de la connexion au serveur de jeu");

                    // Et on passe en jeu
                    next_state.set(ClientState::Connecting);
                }
                Err(e) => {
                    // Échec, on affiche l'erreur et on retourne au menu
                    ui_data.error_message = Some(e);
                    next_state.set(ClientState::LoginMenu);
                }
            }
        }
    }
}

pub struct LauncherPlugin;

impl Plugin for LauncherPlugin {
    fn build(&self, app: &mut App) {
        app.add_plugins(EguiPlugin::default())
            .init_state::<ClientState>()
            .init_resource::<LoginUiData>()
            // Le menu ne s'affiche que dans l'état LoginMenu
            .add_systems(bevy_egui::EguiPrimaryContextPass, ui_login_menu.run_if(in_state(ClientState::LoginMenu)))
            .add_systems(Update, handle_login_task.run_if(in_state(ClientState::Connecting)));
    }
}