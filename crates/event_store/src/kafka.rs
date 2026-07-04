//! A [`CapturedEntrySink`] that publishes a compact, header-carrying record to Kafka for every
//! durably-captured event-store entry.
//!
//! This exists purely for external correlation: a downstream consumer (e.g. a hypergraph bridge)
//! already receives the main, Kafka-egressed event stream via the message bus's own
//! `MessageBusExternalEgress` (see `nautilus-infrastructure`'s `kafka` feature) — that stream
//! carries `topic`/`type`/`encoding`/`payload` but no `correlation_id`/`causation_id`, since those
//! live in this crate's `Headers`, not on `BusMessage`. This sink publishes a small side-channel
//! record keyed by `identity` (the same value a type's registered identity extractor computes,
//! e.g. `OrderFilled::event_id` — see `capture/builtins.rs`) so a consumer can join the two
//! streams on an id domain event payloads already carry, without a second identity scheme.
//!
//! Mirrors `nautilus-infrastructure`'s Kafka message bus backing's task shape (a dedicated
//! publish task draining an unbounded channel, fire-and-forget delivery futures) rather than
//! depending on that crate directly — `nautilus-infrastructure` does not depend on
//! `nautilus-event-store` today, and pulling this crate's `nautilus-system` dependency in the
//! other direction would make a backing-implementations crate depend on the full kernel stack for
//! no reason other than sharing a small producer task.

use std::time::Duration;

use nautilus_common::{
    live::get_runtime,
    logging::{log_task_error, log_task_started, log_task_stopped},
};
use nautilus_core::UUID4;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer as _},
};
use serde::Serialize;

use crate::{capture::CapturedEntrySink, writer::EntryDraft};

const TASK_NAME: &str = "event-store-kafka-causality-sink";

/// Configuration for [`KafkaCapturedEntrySink`].
#[derive(Debug, Clone)]
pub struct KafkaCapturedEntrySinkConfig {
    /// Comma-separated Kafka bootstrap servers, e.g. `"localhost:9092"`.
    pub brokers: String,
    /// The Kafka topic causality records are published to.
    pub topic: String,
    /// The client ID reported to the broker. If `None`, librdkafka's default is used.
    pub client_id: Option<String>,
    /// The producer delivery timeout (milliseconds).
    pub message_timeout_ms: u64,
}

impl Default for KafkaCapturedEntrySinkConfig {
    fn default() -> Self {
        Self {
            brokers: "127.0.0.1:9092".to_string(),
            topic: "hg.nautilus.causality.v1".to_string(),
            client_id: None,
            message_timeout_ms: 5_000,
        }
    }
}

#[derive(Serialize)]
struct CausalityRecord {
    /// The registered identity extractor's value for this entry's type, if any (see the module
    /// doc comment) — the correlation key a consumer joins against the main event stream.
    identity: Option<UUID4>,
    topic: String,
    payload_type: String,
    correlation_id: Option<UUID4>,
    causation_id: Option<UUID4>,
    ts_init: u64,
}

/// Publishes a [`CausalityRecord`] to Kafka for every entry [`BusCaptureAdapter`](crate::capture::BusCaptureAdapter)
/// durably submits.
///
/// `on_captured` only enqueues onto an in-process channel — it never blocks or performs I/O
/// itself, matching [`CapturedEntrySink`]'s no-blocking, no-bus-reentrancy contract. A dedicated
/// background task drains the channel and does the actual Kafka publish.
pub struct KafkaCapturedEntrySink {
    // `Option` behind a std (sync) mutex so `close()` can drop the sender to end the publish
    // task's `rx.recv()` loop -- a plain `UnboundedSender` field could be sent into but never
    // torn down without consuming `self`. Locking is uncontended and cheap; `on_captured` cannot
    // be async (see `CapturedEntrySink`'s contract), so this must be a std, not tokio, mutex.
    tx: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<CausalityRecord>>>,
    handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl std::fmt::Debug for KafkaCapturedEntrySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(KafkaCapturedEntrySink))
            .finish_non_exhaustive()
    }
}

impl KafkaCapturedEntrySink {
    /// Connects to Kafka and spawns the background publish task.
    ///
    /// # Errors
    ///
    /// Returns an error if the Kafka producer cannot be constructed.
    pub fn connect(config: KafkaCapturedEntrySinkConfig) -> anyhow::Result<Self> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<CausalityRecord>();
        let handle = get_runtime().spawn(async move {
            if let Err(e) = publish_records(rx, config).await {
                log_task_error(TASK_NAME, &e);
            }
        });
        Ok(Self {
            tx: std::sync::Mutex::new(Some(tx)),
            handle: tokio::sync::Mutex::new(Some(handle)),
        })
    }

    /// Drops the sender (ending the publish task's receive loop) and awaits the task, so any
    /// already-queued records get a chance to flush before returning. Safe to call more than
    /// once -- a repeat call finds nothing left to do.
    pub async fn close(&self) {
        self.tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        if let Some(handle) = self.handle.lock().await.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
    }
}

impl CapturedEntrySink for KafkaCapturedEntrySink {
    fn on_captured(&self, identity: Option<UUID4>, entry: &EntryDraft) {
        let record = CausalityRecord {
            identity,
            topic: entry.topic.to_string(),
            payload_type: entry.payload_type.to_string(),
            correlation_id: entry.headers.correlation_id,
            causation_id: entry.headers.causation_id,
            ts_init: u64::from(entry.ts_init),
        };
        let guard = self.tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(tx) = guard.as_ref()
            && let Err(e) = tx.send(record)
        {
            log::error!("Failed to enqueue causality record: {e}");
        }
    }
}

async fn publish_records(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<CausalityRecord>,
    config: KafkaCapturedEntrySinkConfig,
) -> anyhow::Result<()> {
    log_task_started(TASK_NAME);

    let mut client = ClientConfig::new();
    client.set("bootstrap.servers", &config.brokers);
    if let Some(client_id) = &config.client_id {
        client.set("client.id", client_id);
    }
    client.set("message.timeout.ms", config.message_timeout_ms.to_string());
    let producer: FutureProducer = client
        .create()
        .map_err(|e| anyhow::anyhow!("failed to create Kafka producer: {e}"))?;

    while let Some(record) = rx.recv().await {
        let key = record
            .identity
            .map_or_else(|| record.topic.clone(), |id| id.to_string());

        let payload = match serde_json::to_vec(&record) {
            Ok(payload) => payload,
            Err(e) => {
                log::error!("Failed to serialize causality record: {e}");
                continue;
            }
        };

        let kafka_record = FutureRecord::to(&config.topic).key(&key).payload(&payload);

        // Fire-and-forget, matching the main Kafka egress backing's pattern: librdkafka batches
        // internally, so hand the delivery future to a detached task rather than awaiting it
        // here.
        match producer.send_result(kafka_record) {
            Ok(delivery) => {
                tokio::spawn(async move {
                    match delivery.await {
                        Ok(Ok(_)) => {}
                        Ok(Err((e, _))) => log::error!("Kafka causality delivery failed: {e}"),
                        Err(_canceled) => {
                            log::error!("Kafka causality delivery future canceled (producer dropped)");
                        }
                    }
                });
            }
            Err((e, _)) => log::error!("Failed to enqueue Kafka causality record: {e}"),
        }
    }

    let _ = producer.flush(Duration::from_secs(5));
    log_task_stopped(TASK_NAME);
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_default_config() {
        let config = KafkaCapturedEntrySinkConfig::default();
        assert_eq!(config.brokers, "127.0.0.1:9092");
        assert_eq!(config.topic, "hg.nautilus.causality.v1");
        assert_eq!(config.client_id, None);
        assert_eq!(config.message_timeout_ms, 5_000);
    }
}
