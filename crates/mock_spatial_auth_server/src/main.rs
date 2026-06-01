use dotenv::dotenv;
use crate::mock_spatial_server::run_spatial_auth_server;

pub mod mock_spatial_server;

#[tokio::main]
async fn main() {
    dotenv().ok();
    run_spatial_auth_server().await
}
