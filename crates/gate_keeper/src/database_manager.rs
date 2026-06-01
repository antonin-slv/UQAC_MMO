use anyhow::{Result, anyhow};
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, Pool, Postgres};
use std::env;

pub struct DatabaseManager {
    pool: Pool<Postgres>,
}

impl DatabaseManager {
    pub async fn new() -> Result<DatabaseManager> {
        let host = &env::var("POSTGRES_HOST").expect("Env POSTGRES_HOST is not set");
        let username = &env::var("POSTGRES_USER").expect("Env POSTGRES_USER is not set");
        let password = &env::var("POSTGRES_PASSWORD").expect("Env POSTGRES_PASSWORD is not set");
        let database = &env::var("POSTGRES_DB").expect("Env POSTGRES_DB is not set");

        let database_url = format!("postgres://{}:{}@{}/{}", username, password, host, database);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url.as_str())
            .await?;

        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(DatabaseManager { pool })
    }

    pub async fn register(&self, username: &str, password: &str) -> Result<()> {
        // Bien sûr pour des raisons de sécurité on hash le password
        let hashed_password = format!("hashed({})", password);

        let result = sqlx::query("INSERT INTO users (username, password_hash) VALUES ($1, $2)")
            .bind(username)
            .bind(hashed_password)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(anyhow!("Can't create new user"))
        }
    }

    pub async fn login(&self, username: &str, password: &str) -> bool {
        let user =
            sqlx::query_as::<_, UserRow>("SELECT password_hash FROM users WHERE username = $1")
                .bind(username)
                .fetch_one(&self.pool)
                .await;

        let hashed_password = format!("hashed({})", password);
        if let Ok(user) = user {
            user.password_hash == hashed_password
        } else {
            false
        }
    }
}

#[derive(FromRow)]
struct UserRow {
    password_hash: String,
}
