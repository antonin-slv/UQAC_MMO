use bytes::{Buf, BufMut, Bytes, BytesMut};
use broker::broker_core::Broker;
use broker::broker_impl::Broker2;
use crate::broker_hard_copy::test_batch::*;
use crate::broker_hard_copy::test_broker_base::*;
use game_message::{GameMessage, GameMessageHeaders, NetRead, NetWrite, NetWriteTo};
#[test]
fn test_legacy_broker_parity() {
    // Ports dédiés pour ne pas croiser les tests
    let pub_port = 8100;
    let priv_port = 8101;

    let lambda = move || {
        let mut broker = Broker::new(pub_port, priv_port);
        broker.run(false); // Boucle bloquante mono-thread
    };
    run_broker_validation_suite(pub_port, priv_port, lambda);
}

#[test]
fn test_broker_batch() {
    let pub_port = 8102;
    let priv_port = 8103;

    let lambda = move || {
        let mut broker = Broker::new(pub_port, priv_port);
        broker.run(false); // Boucle bloquante mono-thread
    };
    run_broker_batch_suite(pub_port, priv_port, lambda);
}

#[test]
fn benchmark_legacy_broker() {
    println!("--- BENCHMARK BROKER V1 (Synchrone) ---");
    let pub_port = 8104;
    let priv_port = 8105;
    let lambda = move || {
        let mut broker = Broker::new(pub_port, priv_port);
        broker.run(false); // Boucle bloquante mono-thread
    };
    run_distributed_stress_test(pub_port, priv_port, lambda, 10, 100, 500, 8);
}
#[test]
fn test_async_broker2_parity() {
    let pub_port = 8200;
    let priv_port = 8201;

    let lambda = move || {
        let mut broker2 = Broker2::new(pub_port, priv_port);
        broker2.run(false); // Boucle avec workers asynchrones
    };
    run_broker_validation_suite(pub_port, priv_port, lambda);
}
#[test]
fn test_broker2_batch() {
    let pub_port = 8202;
    let priv_port = 8203;

    let lambda = move || {
        let mut broker2 = Broker2::new(pub_port, priv_port);
        broker2.run(false); // Boucle avec workers asynchrones
    };
    run_broker_batch_suite(pub_port, priv_port, lambda);
}

#[test]
fn benchmark_async_broker2() {
    println!("--- BENCHMARK BROKER V2 (Asynchrone + Workers) ---");

    let pub_port = 8204;
    let priv_port = 8205;
    let lambda = move || {
        let mut broker2 = Broker2::new(pub_port, priv_port);
        broker2.run(false); // Boucle avec workers asynchrones
    };
    run_distributed_stress_test(pub_port, priv_port, lambda, 10, 100, 500, 8);
}

pub(crate) struct DummyMessage {
    pub(crate) payload: u32,
}

impl NetWrite for DummyMessage {
    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32(self.payload);
        buf.freeze()
    }
}

impl NetRead for DummyMessage {
    fn deserialize(data: &mut Bytes) -> Result<Self, String> {
        let payload = data.get_u32();
        Ok(DummyMessage { payload })
    }
}

impl NetWriteTo for DummyMessage {
    fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u32(self.payload);
    }
}

impl GameMessage for DummyMessage {
    fn header() -> GameMessageHeaders {
        GameMessageHeaders::DiscardedMessageBecauseYouKnow
    }
}
