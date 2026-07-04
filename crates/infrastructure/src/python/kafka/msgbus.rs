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

//! Python bindings for the Kafka message bus backing.
//!
//! `DatabaseConfig` (the Python-facing config shape shared across backings: `host`, `port`,
//! `username`, `password`, `ssl`, ...) is mapped onto [`KafkaMessageBusConfig`]'s own
//! Kafka-idiomatic fields here, rather than reusing those field names on the Rust-native config
//! itself — `brokers` is a list, not a single host/port pair, and SASL/TLS has more shape than a
//! single `ssl` flag once you go past the common case. Rust-native callers (the bridge/orchestrator
//! side) use [`KafkaMessageBusConfig`] directly and are unaffected by this mapping.

use bytes::Bytes;
use futures::{pin_mut, stream::StreamExt};
use nautilus_common::{
    enums::SerializationEncoding,
    msgbus::{BusMessage, BusPayloadType, MessageBusBacking, MessageBusConfig},
    python::config_error_to_pyvalue_err,
};
use nautilus_core::{
    UUID4,
    python::{IntoPyObjectNautilusExt, call_python, to_pyruntime_err, to_pyvalue_err},
};
use nautilus_model::identifiers::TraderId;
use pyo3::{prelude::*, pybacked::PyBackedBytes};
use serde_json::Value;
use ustr::Ustr;

use crate::kafka::msgbus::{KafkaMessageBusBacking, KafkaMessageBusConfig};

#[derive(Debug)]
#[pyclass(
    name = "KafkaMessageBusBacking",
    module = "nautilus_trader.core.nautilus_pyo3.infrastructure"
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.infrastructure")]
pub struct PyKafkaMessageBusBacking {
    inner: KafkaMessageBusBacking,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyKafkaMessageBusBacking {
    #[new]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "PyBackedBytes is required for generated Python bytes stubs"
    )]
    fn py_new(
        trader_id: TraderId,
        instance_id: UUID4,
        config_json: PyBackedBytes,
    ) -> PyResult<Self> {
        let (config, backing) = parse_config(config_json.as_ref())?;
        let inner = KafkaMessageBusBacking::new(trader_id, instance_id, &config, &backing)
            .map_err(to_pyvalue_err)?;
        Ok(Self { inner })
    }

    #[pyo3(name = "is_closed")]
    fn py_is_closed(&self) -> bool {
        MessageBusBacking::is_closed(&self.inner)
    }

    #[pyo3(name = "publish")]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "PyBackedBytes is required for generated Python bytes stubs"
    )]
    fn py_publish(&self, topic: &str, payload: PyBackedBytes) {
        let message = BusMessage::new(
            Ustr::from(topic),
            BusPayloadType::Custom(Ustr::default()),
            Bytes::copy_from_slice(payload.as_ref()),
            SerializationEncoding::default(),
        );
        MessageBusBacking::publish(&self.inner, message);
    }

    #[pyo3(name = "stream")]
    fn py_stream<'py>(
        &mut self,
        callback: Py<PyAny>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let stream_rx = self.inner.get_stream_receiver().map_err(to_pyruntime_err)?;
        let stream = KafkaMessageBusBacking::stream(stream_rx);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            pin_mut!(stream);
            while let Some(msg) = stream.next().await {
                Python::attach(|py| call_python(py, &callback, msg.into_py_any_unwrap(py)));
            }
            Ok(())
        })
    }

    #[pyo3(name = "close")]
    fn py_close(&mut self) {
        MessageBusBacking::close(&mut self.inner);
    }
}

fn parse_config(config_json: &[u8]) -> PyResult<(MessageBusConfig, KafkaMessageBusConfig)> {
    let mut value: Value = serde_json::from_slice(config_json).map_err(to_pyvalue_err)?;
    let backing = parse_backing_config(&mut value)?;
    let config = serde_json::from_value::<MessageBusConfig>(value).map_err(to_pyvalue_err)?;
    config.validate().map_err(config_error_to_pyvalue_err)?;

    Ok((config, backing))
}

fn parse_backing_config(value: &mut Value) -> PyResult<KafkaMessageBusConfig> {
    let Value::Object(config) = value else {
        return Err(to_pyvalue_err("MessageBusConfig must be a JSON object"));
    };

    let Some(database) = config.remove("database") else {
        return Ok(KafkaMessageBusConfig::default());
    };

    let database = match database {
        Value::Null => return Ok(KafkaMessageBusConfig::default()),
        Value::Object(database) => database,
        _ => {
            return Err(to_pyvalue_err(
                "MessageBusConfig.database must be a JSON object",
            ));
        }
    };

    if let Some(database_type) = database.get("type") {
        match database_type {
            Value::String(t) if t == "kafka" => {}
            Value::String(t) => {
                return Err(to_pyvalue_err(format!(
                    "MessageBusConfig.database.type must be 'kafka', was '{t}'"
                )));
            }
            other => {
                return Err(to_pyvalue_err(format!(
                    "MessageBusConfig.database.type must be a string, was {other}"
                )));
            }
        }
    }

    let mut backing = KafkaMessageBusConfig::default();

    if let Some(host) = database.get("host").and_then(Value::as_str) {
        let port = database.get("port").and_then(Value::as_u64).unwrap_or(9092);
        backing.brokers = format!("{host}:{port}");
    }

    if let Some(username) = database.get("username").and_then(Value::as_str) {
        backing.sasl_username = Some(username.to_string());
    }
    if let Some(password) = database.get("password").and_then(Value::as_str) {
        backing.sasl_password = Some(password.to_string());
    }

    if database.get("ssl").and_then(Value::as_bool).unwrap_or(false) {
        backing.security_protocol = Some(if backing.sasl_username.is_some() {
            "SASL_SSL".to_string()
        } else {
            "SSL".to_string()
        });
        if backing.sasl_username.is_some() && backing.sasl_mechanism.is_none() {
            backing.sasl_mechanism = Some("PLAIN".to_string());
        }
    }

    Ok(backing)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    fn test_parse_config_maps_database_config_to_kafka_backing() {
        let config_json = json!({
            "database": {
                "type": "kafka",
                "host": "kafka.example.com",
                "port": 9093,
                "username": "trader",
                "password": "secret",
                "ssl": true,
            },
            "streams_prefix": "stream",
            "external_streams": ["control-commands"],
        });

        let (config, backing) = parse_config(config_json.to_string().as_bytes()).unwrap();

        assert_eq!(config.streams_prefix, "stream");
        assert_eq!(
            config.external_streams,
            Some(vec!["control-commands".to_string()])
        );
        assert_eq!(backing.brokers, "kafka.example.com:9093");
        assert_eq!(backing.sasl_username, Some("trader".to_string()));
        assert_eq!(backing.sasl_password, Some("secret".to_string()));
        assert_eq!(backing.security_protocol, Some("SASL_SSL".to_string()));
        assert_eq!(backing.sasl_mechanism, Some("PLAIN".to_string()));
    }

    #[rstest]
    fn test_parse_config_rejects_wrong_database_type() {
        let config_json = json!({
            "database": {
                "type": "redis",
                "host": "localhost",
            },
        });

        let result = parse_config(config_json.to_string().as_bytes());
        assert!(result.is_err());
    }

    #[rstest]
    fn test_parse_config_defaults_without_database() {
        let config_json = json!({});

        let (_config, backing) = parse_config(config_json.to_string().as_bytes()).unwrap();
        assert_eq!(backing.brokers, "127.0.0.1:9092");
    }
}
