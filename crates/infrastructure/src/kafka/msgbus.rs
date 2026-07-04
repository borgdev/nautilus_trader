// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Kafka-backed message bus backing for the system.
//!
//! # Architecture
//!
//! Mirrors the Redis backing's task shape (see `redis::msgbus`): a dedicated publish task drains
//! an unbounded `tokio::sync::mpsc` channel and hands messages to a Kafka `FutureProducer`; an
//! optional stream task consumes configured external topics via a `StreamConsumer` and forwards
//! decoded messages into the channel `take_receiver()` exposes; an optional heartbeat task
//! publishes a `health:heartbeat` message at a fixed interval like every other backing.
//!
//! Egress writes every message to one Kafka topic per node (see `super::get_topic_key`), carrying
//! the internal Nautilus topic, payload type, and encoding as Kafka record headers, with the
//! internal topic also used as the partition key so per-topic ordering is preserved. Ingress
//! reads those same headers back to reconstruct a [`BusMessage`].

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use nautilus_common::{
    live::get_runtime,
    logging::{log_task_error, log_task_started, log_task_stopped},
    msgbus::{
        BusMessage, BusPayloadType, MessageBusBacking, MessageBusBackingFactory, MessageBusConfig,
        switchboard::CLOSE_TOPIC,
    },
};
use nautilus_core::UUID4;
use nautilus_model::identifiers::TraderId;
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::{Header, Headers as _, Message, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer as _},
};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::get_topic_key;

const MSGBUS_PUBLISH: &str = "msgbus-publish";
const MSGBUS_STREAM: &str = "msgbus-stream";
const MSGBUS_HEARTBEAT: &str = "msgbus-heartbeat";
const HEARTBEAT_TOPIC: &str = "health:heartbeat";
const HEADER_TOPIC: &str = "topic";
const HEADER_TYPE: &str = "type";
const HEADER_ENCODING: &str = "encoding";
/// How often the stream task polls its stop signal between Kafka receives.
const STREAM_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Configuration for a Kafka-backed message bus backing.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.infrastructure",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.infrastructure")
)]
pub struct KafkaMessageBusConfig {
    /// Comma-separated Kafka bootstrap servers, e.g. `"localhost:9092"`.
    pub brokers: String,
    /// The client ID reported to the broker. If `None`, librdkafka's default is used.
    pub client_id: Option<String>,
    /// The consumer group ID used for the ingress side. If `None`, librdkafka's default is used.
    pub group_id: Option<String>,
    /// The producer delivery timeout (milliseconds).
    pub message_timeout_ms: u64,
    /// The `security.protocol` passed straight through to librdkafka (e.g. `"SASL_SSL"`).
    /// If `None`, the connection is unauthenticated and unencrypted.
    pub security_protocol: Option<String>,
    /// The `sasl.mechanism` passed straight through to librdkafka (e.g. `"PLAIN"`, `"SCRAM-SHA-256"`).
    pub sasl_mechanism: Option<String>,
    /// The SASL username, if SASL is configured.
    pub sasl_username: Option<String>,
    /// The SASL password, if SASL is configured.
    pub sasl_password: Option<String>,
}

impl Debug for KafkaMessageBusConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted = self.sasl_password.as_ref().map(|_| "***");
        f.debug_struct(stringify!(KafkaMessageBusConfig))
            .field("brokers", &self.brokers)
            .field("client_id", &self.client_id)
            .field("group_id", &self.group_id)
            .field("message_timeout_ms", &self.message_timeout_ms)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field("sasl_password", &redacted)
            .finish()
    }
}

impl Default for KafkaMessageBusConfig {
    fn default() -> Self {
        Self {
            brokers: "127.0.0.1:9092".to_string(),
            client_id: None,
            group_id: None,
            message_timeout_ms: 5_000,
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        }
    }
}

impl KafkaMessageBusConfig {
    fn apply_common(&self, client: &mut ClientConfig) {
        client.set("bootstrap.servers", &self.brokers);

        if let Some(client_id) = &self.client_id {
            client.set("client.id", client_id);
        }
        if let Some(protocol) = &self.security_protocol {
            client.set("security.protocol", protocol);
        }
        if let Some(mechanism) = &self.sasl_mechanism {
            client.set("sasl.mechanism", mechanism);
        }
        if let Some(username) = &self.sasl_username {
            client.set("sasl.username", username);
        }
        if let Some(password) = &self.sasl_password {
            client.set("sasl.password", password);
        }
    }

    fn build_producer(&self) -> anyhow::Result<FutureProducer> {
        let mut client = ClientConfig::new();
        self.apply_common(&mut client);
        client.set("message.timeout.ms", self.message_timeout_ms.to_string());
        client
            .create()
            .map_err(|e| anyhow::anyhow!("failed to create Kafka producer: {e}"))
    }

    fn build_consumer(&self, fallback_group_id: &str) -> anyhow::Result<StreamConsumer> {
        let mut client = ClientConfig::new();
        self.apply_common(&mut client);
        client.set(
            "group.id",
            self.group_id.as_deref().unwrap_or(fallback_group_id),
        );
        client.set("enable.auto.commit", "true");
        client.set("auto.offset.reset", "latest");
        client
            .create()
            .map_err(|e| anyhow::anyhow!("failed to create Kafka consumer: {e}"))
    }
}

use std::fmt::Debug;

/// Factory for constructing Kafka message bus backings.
#[derive(Debug, Clone)]
pub struct KafkaMessageBusFactory {
    config: KafkaMessageBusConfig,
}

impl KafkaMessageBusFactory {
    /// Creates a new [`KafkaMessageBusFactory`] from the given Kafka configuration.
    #[must_use]
    pub const fn new(config: KafkaMessageBusConfig) -> Self {
        Self { config }
    }
}

impl MessageBusBackingFactory for KafkaMessageBusFactory {
    fn create(
        &self,
        trader_id: TraderId,
        instance_id: UUID4,
        config: MessageBusConfig,
    ) -> anyhow::Result<Box<dyn MessageBusBacking>> {
        Ok(Box::new(KafkaMessageBusBacking::new(
            trader_id,
            instance_id,
            &config,
            &self.config,
        )?))
    }
}

pub struct KafkaMessageBusBacking {
    /// The trader ID for this message bus backing.
    pub trader_id: TraderId,
    /// The instance ID for this message bus backing.
    pub instance_id: UUID4,
    pub_tx: tokio::sync::mpsc::UnboundedSender<BusMessage>,
    pub_handle: Option<tokio::task::JoinHandle<()>>,
    stream_rx: Option<tokio::sync::mpsc::Receiver<BusMessage>>,
    stream_handle: Option<tokio::task::JoinHandle<()>>,
    stream_signal: Arc<AtomicBool>,
    heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
    heartbeat_signal: Arc<AtomicBool>,
}

impl Debug for KafkaMessageBusBacking {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(KafkaMessageBusBacking))
            .field("trader_id", &self.trader_id)
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}

impl KafkaMessageBusBacking {
    /// Creates a new [`KafkaMessageBusBacking`] instance for the given `trader_id`, `instance_id`, and `config`.
    ///
    /// # Errors
    ///
    /// Returns an error if the heartbeat interval is configured as zero seconds.
    pub fn new(
        trader_id: TraderId,
        instance_id: UUID4,
        config: &MessageBusConfig,
        backing: &KafkaMessageBusConfig,
    ) -> anyhow::Result<Self> {
        if config.heartbeat_interval_secs == Some(0) {
            anyhow::bail!("heartbeat_interval_secs must be greater than 0");
        }

        let egress_topic = get_topic_key(trader_id, instance_id, config);
        let external_topics = config.external_streams.clone().unwrap_or_default();
        let heartbeat_interval_secs = config.heartbeat_interval_secs;

        let (pub_tx, pub_rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();

        let producer_config = backing.clone();
        let pub_handle = Some(get_runtime().spawn(async move {
            if let Err(e) = publish_messages(pub_rx, egress_topic, producer_config).await {
                log_task_error(MSGBUS_PUBLISH, &e);
            }
        }));

        let stream_signal = Arc::new(AtomicBool::new(false));
        let (stream_rx, stream_handle) = if external_topics.is_empty() {
            (None, None)
        } else {
            let stream_signal_clone = stream_signal.clone();
            let (stream_tx, stream_rx) = tokio::sync::mpsc::channel::<BusMessage>(100_000);
            let consumer_config = backing.clone();
            let fallback_group_id = format!("nautilus-{trader_id}-{instance_id}");
            (
                Some(stream_rx),
                Some(get_runtime().spawn(async move {
                    if let Err(e) = stream_messages(
                        stream_tx,
                        consumer_config,
                        fallback_group_id,
                        external_topics,
                        stream_signal_clone,
                    )
                    .await
                    {
                        log_task_error(MSGBUS_STREAM, &e);
                    }
                })),
            )
        };

        let heartbeat_signal = Arc::new(AtomicBool::new(false));
        let heartbeat_handle = if let Some(heartbeat_interval_secs) = heartbeat_interval_secs {
            let signal = heartbeat_signal.clone();
            let pub_tx_clone = pub_tx.clone();

            Some(get_runtime().spawn(async move {
                run_heartbeat(heartbeat_interval_secs, signal, pub_tx_clone).await;
            }))
        } else {
            None
        };

        Ok(Self {
            trader_id,
            instance_id,
            pub_tx,
            pub_handle,
            stream_rx,
            stream_handle,
            stream_signal,
            heartbeat_handle,
            heartbeat_signal,
        })
    }
}

impl MessageBusBacking for KafkaMessageBusBacking {
    /// Returns whether the message bus backing publishing channel is closed.
    fn is_closed(&self) -> bool {
        self.pub_tx.is_closed()
    }

    /// Queues a serialized bus message for external publication.
    fn publish(&self, message: BusMessage) {
        if let Err(e) = self.pub_tx.send(message) {
            log::error!("Failed to send message: {e}");
        }
    }

    fn take_receiver(&mut self) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
        self.stream_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("Stream receiver already taken"))
    }

    /// Closes the message bus backing.
    fn close(&mut self) {
        log::debug!("Closing");

        self.stream_signal.store(true, Ordering::Relaxed);
        self.heartbeat_signal.store(true, Ordering::Relaxed);

        if !self.pub_tx.is_closed() {
            let msg = BusMessage::new_close();

            if let Err(e) = self.pub_tx.send(msg) {
                log::warn!("Failed to send close message: {e:?}");
            }
        }

        // Keep close sync for now to avoid async trait method
        tokio::task::block_in_place(|| {
            get_runtime().block_on(async {
                self.close_async().await;
            });
        });

        log::debug!("Closed");
    }
}

impl KafkaMessageBusBacking {
    pub async fn close_async(&mut self) {
        await_handle(self.pub_handle.take(), MSGBUS_PUBLISH).await;
        await_handle(self.stream_handle.take(), MSGBUS_STREAM).await;
        await_handle(self.heartbeat_handle.take(), MSGBUS_HEARTBEAT).await;
    }
}

async fn await_handle(handle: Option<tokio::task::JoinHandle<()>>, task_name: &str) {
    if let Some(handle) = handle
        && let Err(e) = tokio::time::timeout(Duration::from_secs(2), handle).await
    {
        log::warn!("Timed out awaiting task '{task_name}': {e}");
    }
}

/// Publishes messages received on `rx` to the given Kafka `topic`, using `backing` for the
/// producer configuration.
///
/// # Errors
///
/// Returns an error if constructing the Kafka producer fails.
pub async fn publish_messages(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<BusMessage>,
    topic: String,
    backing: KafkaMessageBusConfig,
) -> anyhow::Result<()> {
    log_task_started(MSGBUS_PUBLISH);

    let producer = backing.build_producer()?;

    while let Some(msg) = rx.recv().await {
        if msg.topic == CLOSE_TOPIC {
            log::debug!("Received close message");
            break;
        }

        let encoding = msg.encoding.to_string();
        let key = msg.topic.to_string();
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: HEADER_TOPIC,
                value: Some(msg.topic.as_bytes()),
            })
            .insert(Header {
                key: HEADER_TYPE,
                value: Some(msg.payload_type.as_str().as_bytes()),
            })
            .insert(Header {
                key: HEADER_ENCODING,
                value: Some(encoding.as_bytes()),
            });

        let record = FutureRecord::to(&topic)
            .key(&key)
            .payload(msg.payload.as_ref())
            .headers(headers);

        // Fire-and-forget: librdkafka batches internally, so we hand off the delivery future to
        // a detached task rather than awaiting it here, to keep pace with the channel.
        match producer.send_result(record) {
            Ok(delivery) => {
                tokio::spawn(async move {
                    match delivery.await {
                        Ok(Ok(_)) => {}
                        Ok(Err((e, _))) => log::error!("Kafka delivery failed: {e}"),
                        Err(_canceled) => {
                            log::error!("Kafka delivery future canceled (producer dropped)");
                        }
                    }
                });
            }
            Err((e, _)) => log::error!("Failed to enqueue Kafka message: {e}"),
        }
    }

    // Best-effort flush so in-flight sends land before the task exits.
    let _ = producer.flush(Duration::from_secs(5));

    log_task_stopped(MSGBUS_PUBLISH);
    Ok(())
}

/// Consumes messages from the given Kafka `topics` and sends them over the provided `tx` channel.
///
/// # Errors
///
/// Returns an error if constructing the Kafka consumer or subscribing to `topics` fails.
pub async fn stream_messages(
    tx: tokio::sync::mpsc::Sender<BusMessage>,
    backing: KafkaMessageBusConfig,
    fallback_group_id: String,
    topics: Vec<String>,
    stream_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    log_task_started(MSGBUS_STREAM);

    let consumer = backing.build_consumer(&fallback_group_id)?;
    let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer
        .subscribe(&topic_refs)
        .map_err(|e| anyhow::anyhow!("failed to subscribe to Kafka topics {topic_refs:?}: {e}"))?;

    log::debug!("Listening to Kafka topics: [{}]", topic_refs.join(", "));

    loop {
        if stream_signal.load(Ordering::Relaxed) {
            log::debug!("Received stream terminate signal");
            break;
        }

        let received = match tokio::time::timeout(STREAM_POLL_INTERVAL, consumer.recv()).await {
            Ok(Ok(msg)) => msg,
            Ok(Err(e)) => {
                log::error!("Kafka receive error: {e}");
                continue;
            }
            Err(_elapsed) => continue, // no message within the poll interval, recheck the signal
        };

        let Some(bus_msg) = decode_bus_message(&received) else {
            log::warn!(
                "Skipping undecodable Kafka message on '{}' (missing headers or payload)",
                received.topic()
            );
            continue;
        };

        if tx.send(bus_msg).await.is_err() {
            log::debug!("Ingress receiver dropped, stopping stream task");
            break;
        }
    }

    log_task_stopped(MSGBUS_STREAM);
    Ok(())
}

fn decode_bus_message(msg: &rdkafka::message::BorrowedMessage<'_>) -> Option<BusMessage> {
    let headers = msg.headers()?;

    let mut topic = None;
    let mut type_name = None;
    let mut encoding_name = None;

    for i in 0..headers.count() {
        let header = headers.get(i);
        let value = header.value.map(|v| String::from_utf8_lossy(v).into_owned());
        match header.key {
            HEADER_TOPIC => topic = value,
            HEADER_TYPE => type_name = value,
            HEADER_ENCODING => encoding_name = value,
            _ => {}
        }
    }

    let topic = topic?;
    let type_name = type_name?;
    let encoding = encoding_name?.parse().ok()?;
    let payload = Bytes::copy_from_slice(msg.payload()?);

    Some(BusMessage::new(
        Ustr::from(&topic),
        BusPayloadType::from_name(&type_name),
        payload,
        encoding,
    ))
}

async fn run_heartbeat(
    heartbeat_interval_secs: u16,
    signal: Arc<AtomicBool>,
    pub_tx: tokio::sync::mpsc::UnboundedSender<BusMessage>,
) {
    log_task_started("heartbeat");
    log::debug!("Heartbeat at {heartbeat_interval_secs} second intervals");

    let heartbeat_interval = Duration::from_secs(u64::from(heartbeat_interval_secs));
    let check_interval = Duration::from_millis(100);

    let heartbeat_timer = tokio::time::interval(heartbeat_interval);
    let check_timer = tokio::time::interval(check_interval);
    tokio::pin!(heartbeat_timer);
    tokio::pin!(check_timer);

    loop {
        if signal.load(Ordering::Relaxed) {
            log::debug!("Received heartbeat terminate signal");
            break;
        }

        tokio::select! {
            _ = heartbeat_timer.tick() => {
                let heartbeat = create_heartbeat_msg();
                if let Err(e) = pub_tx.send(heartbeat) {
                    // We expect an error if the channel is closed during shutdown
                    log::debug!("Error sending heartbeat: {e}");
                }
            },
            _ = check_timer.tick() => {}
        }
    }

    log_task_stopped("heartbeat");
}

fn create_heartbeat_msg() -> BusMessage {
    let payload = Bytes::from(chrono::Utc::now().to_rfc3339().into_bytes());
    BusMessage::with_str_topic(
        HEARTBEAT_TOPIC,
        BusPayloadType::Custom(Ustr::default()),
        payload,
        nautilus_common::enums::SerializationEncoding::default(),
    )
}

#[cfg(test)]
mod tests {
    use rstest::*;

    use super::*;

    #[rstest]
    fn test_default_kafka_message_bus_config() {
        let config = KafkaMessageBusConfig::default();

        assert_eq!(config.brokers, "127.0.0.1:9092");
        assert_eq!(config.client_id, None);
        assert_eq!(config.group_id, None);
        assert_eq!(config.message_timeout_ms, 5_000);
        assert_eq!(config.security_protocol, None);
    }

    #[rstest]
    fn test_get_topic_key_defaults() {
        let trader_id = TraderId::from("TRADER-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            streams_prefix: "stream".to_string(),
            ..Default::default()
        };

        let key = super::super::get_topic_key(trader_id, instance_id, &config);
        assert_eq!(key, "trader-TRADER-001.stream");
    }
}
