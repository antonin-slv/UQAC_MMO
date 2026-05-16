use anyhow::Result;
use bollard::Docker;
use bollard::container::{
    Config, CreateContainerOptions, InspectContainerOptions, StartContainerOptions,
    StopContainerOptions,
};
use bollard::models::HostConfig;
use std::env;

pub struct DockerManager {
    docker: Docker,
    self_ip: String,
}

impl DockerManager {
    pub async fn new() -> Result<Self> {
        let docker = Docker::connect_with_socket_defaults()?;

        let self_ip = Self::get_container_ip(&docker).await?;

        let manager = Self { docker, self_ip };

        Ok(manager)
    }

    pub async fn spawn_container(&self, id: &String) -> Result<String> {
        let valid_env = vec![
            format!(
                "HEARTBEAT_INTERVAL={}",
                env::var("HEARTBEAT_INTERVAL").expect("Env HEARTBEAT_INTERVAL must be set")
            ),
            format!(
                "SERV_FREQUENCY={}",
                env::var("SERV_FREQUENCY").expect("Env SERV_FREQUENCY must be set")
            ),
            "SERVER_EXT_IP=127.0.0.1".to_string(),
            "SERVER_EXT_PORT=5000".to_string(),
            format!("SERVER_UUID={}", id),
            format!("ORCHESTRATOR_URL={}:3631", self.self_ip),
            format!(
                "MAX_PLAYER_PER_SERVER={}",
                env::var("MAX_PLAYER_PER_SERVER").expect("Env MAX_PLAYER_PER_SERVER must be set")
            ),
        ];

        let image = env::var("GAME_SERVER_IMAGE").expect("Env GAME_SERVER_IMAGE is not set");

        let config = Config {
            image: Some(image),
            tty: Some(true),
            host_config: Some(HostConfig {
                auto_remove: Some(true),
                network_mode: Some("mmo_network".to_string()),
                ..Default::default()
            }),
            env: Some(valid_env),

            ..Default::default()
        };

        let name = format!("server-{}", id);

        self.docker
            .create_container(
                Some(CreateContainerOptions {
                    name: name.clone(),
                    platform: None,
                }),
                config,
            )
            .await?;

        self.docker
            .start_container(name.as_str(), None::<StartContainerOptions<String>>)
            .await?;

        let container_data = self
            .docker
            .inspect_container(name.as_str(), None::<InspectContainerOptions>)
            .await?;

        let server_ip = container_data
            .network_settings
            .and_then(|ns| ns.networks)
            .and_then(|nets| {
                nets.values()
                    .next()
                    .and_then(|endpoint| endpoint.ip_address.clone())
            })
            .filter(|ip| !ip.is_empty());

        Ok(server_ip.expect("Feur"))
    }

    pub async fn terminate_container(&self, id: &String) -> Result<()> {
        let stop_options = StopContainerOptions { t: 10 };

        let name = format!("server-{}", id);

        self.docker
            .stop_container(name.as_str(), Some(stop_options))
            .await?;

        self.docker.remove_container(name.as_str(), None).await?;

        Ok(())
    }

    async fn get_container_ip(docker: &Docker) -> Result<String> {
        // 1. Inspecter le container à partir de son nom
        let inspect_result = docker.inspect_container("orchestrator", None).await?;

        // 2. Naviguer dans les métadonnées réseau du container
        if let Some(network_settings) = inspect_result.network_settings {
            // Option A : Si vos containers sont sur le réseau par défaut ("bridge")
            if let Some(ip_address) = network_settings.ip_address {
                if !ip_address.is_empty() {
                    return Ok(ip_address);
                }
            }

            // Option B : Si vous utilisez un réseau personnalisé dans votre Docker Compose
            if let Some(networks) = network_settings.networks {
                // On prend la première IP disponible dans la liste des réseaux connectés
                if let Some(network_config) = networks.values().next() {
                    if let Some(ip_address) = &network_config.ip_address {
                        if !ip_address.is_empty() {
                            return Ok(ip_address.clone());
                        }
                    }
                }
            }
        }

        Ok("127.0.0.1".to_string())
    }
}
