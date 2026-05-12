use anyhow::Result;
use redis::AsyncTypedCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env;

#[derive(Serialize, Deserialize, Debug)]
pub struct GameServer {
    pub name: String,
    pub address: String,
    pub players_online: u32,
    pub players_max: u32,
}

impl GameServer {
    pub fn new(name: String, players_max: u32) -> Self {
        Self {
            name,
            address: "".to_string(),
            players_max,
            players_online: 0,
        }
    }
}

pub struct ServerManager {
    client: redis::Client,
}

impl ServerManager {
    pub fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        Ok(Self { client })
    }

    pub async fn update_server(&self, server: &GameServer) -> Result<()> {
        let mut con = self.client.get_multiplexed_async_connection().await?;

        let key = format!("server:{}", server.name);

        let data = serde_json::to_string(server).map_err(anyhow::Error::msg)?;

        con.set(&key, data).await?;

        con.sadd("active_servers", &server.name).await?;

        Ok(())
    }

    pub async fn get_server(&self, server_name: String) -> Result<Option<GameServer>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;

        let key = format!("server:{}", server_name);
        let data = con.get(key).await?;

        if let Some(s) = data {
            if let Ok(server) = serde_json::from_str::<GameServer>(&s) {
                return Ok(Some(server));
            }
        }

        Ok(None)
    }

    pub async fn get_all_servers(&self) -> Result<Vec<GameServer>> {
        let mut con = self.client.get_multiplexed_async_connection().await?;

        let server_names: HashSet<String> = con.smembers("active_servers").await?;

        let mut servers = Vec::new();

        for name in server_names {
            let key = format!("server:{}", name);
            let data: Option<String> = con.get(&key).await?;

            if let Some(s) = data {
                if let Ok(server) = serde_json::from_str(&s) {
                    servers.push(server);
                }
            }
        }

        Ok(servers)
    }

    pub async fn get_available_server(&self) -> Result<Option<GameServer>> {
        let servers = self.get_all_servers().await?;

        for server in servers {
            if server.players_online < server.players_max {
                return Ok(Some(server));
            }
        }

        Ok(None)
    }

    pub async fn remove_server(&self, server_name: &String) -> Result<()> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let key = format!("server:{}", server_name);

        con.del(key).await?;

        con.srem("active_servers", &server_name).await?;

        Ok(())
    }

    pub async fn create_server(&self) -> Result<GameServer> {
        let mut con = self.client.get_multiplexed_async_connection().await?;

        let data = con.get("next_server_sequence").await?;
        let next_server_sequence = if let Some(s) = data {
            s.parse().expect("Euh c'est pas un nombre ça Michel")
        } else {
            0
        };

        let new_server = GameServer::new(
            format!("game-instance-{}", next_server_sequence),
            env::var("MAX_PLAYER_PER_SERVER")
                .expect("Env MAX_PLAYER_PER_SERVER is not set")
                .parse()
                .expect("Max player per server is not a valid number"),
        );

        con.set("next_server_sequence", next_server_sequence + 1)
            .await?;

        self.update_server(&new_server).await?;

        Ok(new_server)
    }

    pub async fn cleanup(&self) -> Result<()> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        con.flushall().await?;
        Ok(())
    }
}
