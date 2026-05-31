use crate::broker_core::Broker;

pub mod broker_core;

fn main() {

    let mut broker = Broker::new();

    broker.run();
}
