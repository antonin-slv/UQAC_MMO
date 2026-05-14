use anyhow::{Error, Result};
use bollard::Docker;
use bollard::container::{
    Config, CreateContainerOptions, InspectContainerOptions, StartContainerOptions,
    StopContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::models::HostConfig;
use futures_util::StreamExt;
use std::env;

pub struct DockerManager {
    docker: Docker,
}

impl DockerManager {
    pub async fn new() -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()?;

        let manager = Self { docker };

        let options = Some(CreateImageOptions {
            from_image: env::var("GAME_SERVER_IMAGE").expect("Env GAME_SERVER_IMAGE is not set"),
            ..Default::default()
        });

        let mut pull_stream = manager.docker.create_image(options, None, None);

        while let Some(result) = pull_stream.next().await {
            if let Err(e) = result {
                eprintln!("Erreur pendant le pull : {}", e);
                return Err(Error::from(e));
            }
        }

        Ok(manager)
    }

    pub async fn spawn_container(&self, id: &String) -> Result<String> {
        let config = Config {
            image: Some(env::var("GAME_SERVER_IMAGE").expect("Env GAME_SERVER_IMAGE is not set")),
            tty: Some(true),
            host_config: Some(HostConfig {
                ..Default::default()
            }),
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
}
