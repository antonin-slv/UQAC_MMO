use anyhow::Result;
use bollard::config::ContainerCreateBody;
use bollard::models::{HostConfig, PortBinding};
use bollard::query_parameters::{
    CreateContainerOptions, InspectContainerOptions, StartContainerOptions, StopContainerOptions,
};
use bollard::Docker;
use std::collections::HashMap;
use std::env;

pub struct DockerManager {
    docker: Docker,
    self_ip: String,
    pub broker_ip: String,
    pub broker_private_port: u16,
}

impl DockerManager {
    pub async fn new() -> Result<Self> {
        let docker = Docker::connect_with_socket_defaults()?;

        let self_ip = Self::get_ip_of_named_container(&docker, "orchestrator").await?;

        let broker_ip = Self::get_ip_of_named_container(&docker, "broker").await?;
        let broker_private_port = env::var("BROKER_PRIVATE_PORT")
            .expect("Env BROKER_PRIVATE_PORT is not set")
            .parse()
            .expect("Env BROKER_PRIVATE_PORT is not a u16");
        let manager = Self {
            docker,
            self_ip,
            broker_ip,
            broker_private_port,
        };

        Ok(manager)
    }

    pub async fn spawn_container(&self, id: String) -> Result<u16> {
        let valid_env = vec![
            format!(
                "WORLD_SIZE={}",
                env::var("WORLD_SIZE").expect("Env WORLD_SIZE must be set")
            ),
            format!(
                "QUADTREE_MAX_DEPTH={}",
                env::var("QUADTREE_MAX_DEPTH").expect("Env QUADTREE_MAX_DEPTH must be set")
            ),
            format!(
                "HEARTBEAT_INTERVAL={}",
                env::var("HEARTBEAT_INTERVAL").expect("Env HEARTBEAT_INTERVAL must be set")
            ),
            format!(
                "SERV_FREQUENCY={}",
                env::var("SERV_FREQUENCY").expect("Env SERV_FREQUENCY must be set")
            ),
            "SERVER_LISTEN_IP=0.0.0.0".to_string(),
            "SERVER_LISTEN_PORT=5000".to_string(),
            format!("SERVER_UUID={}", id),
            format!(
                "ORCHESTRATOR_URL={}:{}",
                self.self_ip,
                env::var("ORCH_PORT").expect("Env ORCH_PORT must be set")
            ),
            format!(
                "MAX_PLAYER_PER_SERVER={}",
                env::var("MAX_PLAYER_PER_SERVER").expect("Env MAX_PLAYER_PER_SERVER must be set")
            ),
            format!("BROKER_URL={}:{}", self.broker_ip, self.broker_private_port),
        ];

        let image = env::var("GAME_SERVER_IMAGE").expect("Env GAME_SERVER_IMAGE is not set");

        let mut port_bindings = HashMap::new();
        port_bindings.insert(
            "5000/udp".to_string(),
            Some(vec![PortBinding {
                host_port: Some("0".to_string()), // Demande explicitement à docker un nouveau port
                host_ip: Some("0.0.0.0".to_string()),
            }]),
        );
        let config = ContainerCreateBody {
            image: Some(image),
            tty: Some(true),
            host_config: Some(HostConfig {
                auto_remove: Some(true),
                network_mode: Some("mmo_network".to_string()),
                port_bindings: Some(port_bindings),
                ..Default::default()
            }),
            env: Some(valid_env),
            exposed_ports: Some(vec!["5000/udp".to_string()]),
            ..Default::default()
        };

        let name = format!("server-{}", id);

        self.docker
            .create_container(
                Some(CreateContainerOptions {
                    name: Some(name.clone()),
                    ..Default::default()
                }),
                config,
            )
            .await?;

        self.docker
            .start_container(name.as_str(), None::<StartContainerOptions>)
            .await?;

        // --- CORRECTION : Attente active du port assigné par Docker ---
        let mut attempts = 0;
        let assigned_port = loop {
            let container_data = self
                .docker
                .inspect_container(name.as_str(), None::<InspectContainerOptions>)
                .await?;

            let maybe_port = container_data
                .network_settings
                .and_then(|ns| ns.ports)
                .and_then(|ports| ports.get("5000/udp").cloned())
                .and_then(|bindings| bindings)
                .and_then(|bindings_list| bindings_list.first().cloned())
                .and_then(|binding| binding.host_port)
                .and_then(|port_str| port_str.parse::<u16>().ok());

            if let Some(port) = maybe_port {
                break port; // Le port est prêt, on sort de la boucle !
            }

            attempts += 1;
            if attempts > 10 {
                return Err(anyhow::anyhow!(
                    "Le conteneur {} a démarré mais Docker n'a jamais exposé son port UDP après 1 seconde.",
                    name
                ));
            }

            // On attend 100ms avant de ré-inspecter le conteneur
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        };

        println!(
            "[DockerManager] Port dynamique assigné pour {}: {}",
            name, assigned_port
        );
        Ok(assigned_port)
    }

    pub async fn terminate_container(&self, id: &String) -> Result<()> {
        let stop_options = StopContainerOptions {
            t: Some(10),
            ..Default::default()
        };

        let name = format!("server-{}", id);

        self.docker
            .stop_container(name.as_str(), Some(stop_options))
            .await?;

        self.docker.remove_container(name.as_str(), None).await?;

        Ok(())
    }

    async fn get_ip_of_named_container(docker: &Docker, container_name: &str) -> Result<String> {
        let inspect_result = docker.inspect_container(container_name, None).await?;

        if let Some(network_settings) = inspect_result.network_settings {
            if let Some(networks) = network_settings.networks {
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
