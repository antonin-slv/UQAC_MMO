use crate::broker::Broker;
use std::env;

mod broker;
mod broker_state;

const CLIENT_LISTEN_PORT_ENV_NAME: &str = "BROKER_PUBLIC_PORT";
const SERVER_LISTEN_PORT_ENV_NAME: &str = "BROKER_PRIVATE_PORT";

fn main() {
    let public_port = env::var(CLIENT_LISTEN_PORT_ENV_NAME)
        .unwrap_or_else(|_| "8000".into())
        .parse()
        .unwrap_or(8000);
    let private_port = env::var(SERVER_LISTEN_PORT_ENV_NAME)
        .unwrap_or_else(|_| "8001".into())
        .parse()
        .unwrap_or(8001);
    let mut broker = Broker::new(public_port, private_port);

    broker.run(true);
}
