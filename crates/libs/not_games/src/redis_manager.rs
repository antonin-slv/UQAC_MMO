use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis::{AsyncTypedCommands, ExpireOption};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GameServer {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub area: String,
    pub players_online: u32,
    pub players_max: u32,
}

impl GameServer {
    pub fn new(id: String, players_max: u32, port: u16) -> Self {
        Self {
            id,
            address: env::var("HOST_ADDRESS").expect("Env HOST_ADDRESS is not set"),
            players_max,
            players_online: 0,
            port,
            area: "NA".to_string(),
        }
    }
}

pub struct RedisManager {
    client: redis::Client,
    conn: MultiplexedConnection,
}

impl RedisManager {
    pub async fn new() -> Result<Self> {
        let redis_url = &env::var("REDIS_ADDRESS").expect("Env REDIS_ADDRESS is not set");
        let client = redis::Client::open(redis_url.as_str())?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Self { client, conn })
    }

    pub async fn update_server(&self, server: &GameServer) -> Result<()> {
        let mut con = self.conn.clone();

        let data = serde_json::to_string(server).map_err(anyhow::Error::msg)?;

        let server_ttl: i64 = env::var("HEARTBEAT_INTERVAL")
            .expect("Env HEARTBEAT_INTERVAL is not set")
            .parse()
            .expect("Env HEARTBEAT_INTERVAL is not an integer");

        con.hset("server", server.id.as_str(), data).await?;
        con.hexpire(
            "server",
            server_ttl * 3,
            ExpireOption::NONE,
            server.id.as_str(),
        )
            .await?;

        Ok(())
    }

    pub async fn get_server(&self, id: String) -> Result<Option<GameServer>> {
        let mut con = self.conn.clone();

        let data = con.hget("server", id.as_str()).await?;

        if let Some(s) = data {
            if let Ok(server) = serde_json::from_str::<GameServer>(&s) {
                return Ok(Some(server));
            }
        }

        Ok(None)
    }

    pub async fn get_all_servers(&self) -> Result<Vec<GameServer>> {
        let mut con = self.conn.clone();

        let server_data: HashMap<String, String> = con.hgetall("server").await?;

        let mut servers = Vec::new();

        for (_, server) in server_data {
            if let Ok(server) = serde_json::from_str::<GameServer>(&server) {
                servers.push(server);
            }
        }

        Ok(servers)
    }

    pub async fn get_available_servers(&self) -> Result<Vec<GameServer>> {
        let mut servers = self.get_all_servers().await?;

        servers.retain(|server| server.players_online < server.players_max);

        Ok(servers)
    }

    pub async fn remove_server(&self, id: &String) -> Result<()> {
        let mut con = self.conn.clone();

        con.hdel("server", id).await?;

        Ok(())
    }

    pub async fn create_server(&self, id: String, port: u16) -> Result<GameServer> {
        let new_server = GameServer::new(
            id,
            env::var("MAX_PLAYER_PER_SERVER")
                .expect("Env MAX_PLAYER_PER_SERVER is not set")
                .parse()
                .expect("Max player per server is not a valid number"),
            port,
        );

        self.update_server(&new_server).await?;

        Ok(new_server)
    }
}
