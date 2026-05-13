#[macro_use]
extern crate rocket;

use dotenv::dotenv;
use std::env;

#[launch]
fn rocket() -> _ {
    dotenv().ok();

    rocket::build().mount("/gate-keeper", routes![login])
}

#[post("/login")]
async fn login() -> String {
    let gatekeeper_address =
        &env::var("GATEKEEPER_ADDRESS").expect("Env GATEKEEPER_ADDRESS is not set");
    let gatekeeper_port: u16 = env::var("GATEKEEPER_PORT")
        .expect("Env GATEKEEPER_PORT is not set")
        .parse()
        .expect("Env GATEKEEPER_PORT is not a valid number");

    let orchestrator_address = format!(
        "http://{}:{}/orchestrator",
        gatekeeper_address, gatekeeper_port
    );
    let response = reqwest::get(format!("{}/connect", orchestrator_address)).await;

    if let Ok(response) = response
        && response.status().is_success()
    {
        let body = response.text().await;
        if let Ok(body) = body {
            return body;
        }
    }

    "Orchestrator failed to login.".to_string()
}
