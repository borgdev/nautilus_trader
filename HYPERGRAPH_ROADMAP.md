# Hypergraph control plane — status of the touchpoints in this repo

This file only exists on `borgdev/nautilus_trader`, branch `feature/hypergraph-kafka-backing` (and any
successor branches for the remaining touchpoints below). It is not part of upstream NautilusTrader and
should not be included if this branch is ever proposed back upstream — see [ROADMAP.md](ROADMAP.md) for
the project's own (unrelated) roadmap.

**Canonical, full-picture doc:** [`anant-trader/ROADMAP.md`](https://github.com/borgdev/anant-trader/blob/main/ROADMAP.md) — phase-by-phase status across both repos, architecture, resolved decisions. This file is just the subset that lives in this codebase.

## Status of the four touchpoints

| Touchpoint | Where | Status |
|---|---|---|
| Kafka message bus backing | `crates/infrastructure/src/kafka/`, `crates/infrastructure/src/python/kafka/`, `nautilus_trader/system/kernel.py` | ✅ **Done** on this branch, including Python config wiring (`config.message_bus.database.type == "kafka"` now works exactly like `"redis"` already does). Builds clean under `clippy::pedantic` for `redis,kafka` and `redis,kafka,python` feature sets. Not yet opened as a PR. |
| Causality metadata on the wire | would touch `crates/event_store/` | ⏸️ **Deferred.** Turned out to need a new sink hook in `event_store`'s writer (see below), not a `BusMessage` field — needs its own design pass. |
| GlobalRiskOverlay veto/limit table | would touch `crates/risk/src/engine/mod.rs` (`check_order()`, line ~905) | ⬜ **Not started.** |
| Control-command ingress (`ControllerCommand` variants) | would touch `crates/system/src/messages/controller.rs`, `nautilus_trader/trading/controller.py` | ⬜ **Not started.** |

## Why causality metadata got deferred

`correlation_id`/`causation_id` are populated via `HeadersExtractor` (`crates/event_store/src/capture/registry.rs`), a per-type extraction registry that lives in `crates/event_store` — a layer above `crates/infrastructure`/`crates/common`, not below. `BusTap::on_publish(topic, message: &dyn Any)` (`crates/common/src/msgbus/mod.rs`) gets no headers; the event store's own tap downcasts to registered concrete types and extracts `Headers` itself. Reaching that from the Kafka backing in `crates/infrastructure` would mean either duplicating the registry (layering violation — `infrastructure` would depend on `event_store` internals) or inverting the dependency graph.

The coherent fix is a small new sink hook inside `event_store`'s writer (e.g. a `CapturedEntrySink` trait called alongside the `redb` write) that a Kafka bridge implements to receive entries with `Headers` already populated. Not built yet.

## A finding worth knowing before touching control-command ingress

Tracing `crates/common/src/msgbus/api.rs`: every typed publish path (`publish_quote`, `publish_order_event`, `publish_position_event`, `publish_account_state`, etc.) already calls `forward_to_external_egress` unconditionally — so once the Kafka backing is configured, the entire order/position/account/market-data lifecycle ships externally with **no application code**, Python or otherwise. The one gap is `publish_any` (line ~942), which only forwards when the payload downcasts to `CustomData` — i.e. the low-level `self.msgbus.publish(topic, obj)` path (as opposed to `self.publish_data(...)`) is not captured. Relevant if control-command ingress or anything else ends up needing a wildcard subscriber for custom bus traffic.

## Toolchain note

This repo pins `rust-toolchain.toml` to `1.96.1`. If your default `rustc --version` is older (rustup's `stable` channel can lag), run:

```sh
rustup toolchain install 1.96.1 --profile default
rustup override set 1.96.1
```

from inside this checkout.
