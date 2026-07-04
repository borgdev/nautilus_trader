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

//! Provides a Kafka-backed message bus backing implementation.

pub mod msgbus;

use std::fmt::Write as _;

use nautilus_common::msgbus::MessageBusConfig;
use nautilus_core::UUID4;
use nautilus_model::identifiers::TraderId;

const KAFKA_DELIMITER: char = '.';

/// Builds the Kafka egress topic name for this node from the same naming fields the Redis
/// backing uses for its stream key (see `redis::get_stream_key`), so operators configure one
/// naming scheme regardless of which external backing is active.
///
/// Unlike the Redis backing, this name is always a single Kafka topic: `stream_per_topic` is a
/// Redis-stream concept (Redis cannot subscribe with wildcards, so it splits topics into separate
/// streams) and does not carry over to Kafka, where creating one topic per Nautilus bus topic
/// would not scale. Instead the internal Nautilus topic travels as the Kafka message key and a
/// header on every record (see `msgbus::publish_messages`), giving per-topic partition affinity
/// (and therefore per-topic ordering) within this single Kafka topic.
pub(crate) fn get_topic_key(
    trader_id: TraderId,
    instance_id: UUID4,
    config: &MessageBusConfig,
) -> String {
    let mut topic_key = String::new();

    if config.use_trader_prefix {
        topic_key.push_str("trader-");
    }

    if config.use_trader_id {
        topic_key.push_str(trader_id.as_str());
        topic_key.push(KAFKA_DELIMITER);
    }

    if config.use_instance_id {
        write!(topic_key, "{instance_id}").expect("writing to String cannot fail");
        topic_key.push(KAFKA_DELIMITER);
    }

    topic_key.push_str(&config.streams_prefix);
    topic_key
}
