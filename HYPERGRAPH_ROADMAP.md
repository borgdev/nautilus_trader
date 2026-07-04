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
| Causality metadata on the wire | `crates/event_store/src/capture/adapter.rs`, `crates/event_store/src/kernel.rs` | 🟡 **Mechanism done, no concrete sink yet.** `causation_id` already flows on order-event payloads today, no change needed there. `CapturedEntrySink` + `BusCaptureAdapter::with_sink()` + `EventStoreLifecycleOptions::with_captured_entry_sink()` are built and tested (see below) — but nothing implements the trait to actually publish anywhere yet. |
| GlobalRiskOverlay veto/limit table | would touch `crates/risk/src/engine/mod.rs` (`check_order()`, line ~905) | ⬜ **Not started.** |
| Control-command ingress (`ControllerCommand` variants) | would touch `crates/system/src/messages/controller.rs`, `nautilus_trader/trading/controller.py` | ⬜ **Not started.** |

## Causality metadata: what's built, and what's still missing

`correlation_id`/`causation_id` are populated via `HeadersExtractor` (`crates/event_store/src/capture/registry.rs`), a per-type extraction registry that lives in `crates/event_store` — a layer above `crates/infrastructure`/`crates/common`, not below. `BusTap::on_publish(topic, message: &dyn Any)` (`crates/common/src/msgbus/mod.rs`) gets no headers; the event store's own tap (`EventStoreBusTap::capture`, `crates/event_store/src/kernel.rs`) downcasts to registered concrete types and extracts `Headers` itself, right before handing the entry to `BusCaptureAdapter::capture_any`. Reaching that from the Kafka backing in `crates/infrastructure` would mean either duplicating the registry (layering violation) or inverting the dependency graph.

**Built:** `CapturedEntrySink` (`crates/event_store/src/capture/adapter.rs`) — a trait notified with a clone of the `EntryDraft` (headers, topic, payload_type, payload, ts_init) right after the writer's durable submit succeeds, so a slow/failing sink can't affect capture durability. `BusCaptureAdapter::with_sink()` registers one; `EventStoreLifecycleOptions::with_captured_entry_sink()` wires it through kernel boot the same way the encoder registry and backend opener are already injectable. Off by default, zero cost when unused (the entry is only cloned if a sink is actually registered). 2 new tests; full crate suite (565 tests) and clippy clean.

**Separately, verified while building `bridge/src/schema.rs`:** every order event (`OrderInitialized` through `OrderFilled`) already serializes `causation_id: Option<UUID4>` as a field on the domain struct itself (`crates/model/src/events/order/*.rs`) — so it reaches the Kafka wire for free inside the JSON payload, no `event_store` involvement needed. `correlation_id` is not on any domain struct anywhere (checked the whole `model` crate) — it's purely an `event_store`/dispatch-boundary concept, which is exactly what `CapturedEntrySink` exists to surface.

**Still missing:** a concrete `CapturedEntrySink` implementation. Nothing publishes these entries anywhere yet — the natural next step is a Kafka producer following the same pattern as `crates/infrastructure/src/kafka/msgbus.rs`, and a bridge-side consumer that correlates it against the main event stream.

## A finding worth knowing before touching control-command ingress

Tracing `crates/common/src/msgbus/api.rs`: every typed publish path (`publish_quote`, `publish_order_event`, `publish_position_event`, `publish_account_state`, etc.) already calls `forward_to_external_egress` unconditionally — so once the Kafka backing is configured, the entire order/position/account/market-data lifecycle ships externally with **no application code**, Python or otherwise. The one gap is `publish_any` (line ~942), which only forwards when the payload downcasts to `CustomData` — i.e. the low-level `self.msgbus.publish(topic, obj)` path (as opposed to `self.publish_data(...)`) is not captured. Relevant if control-command ingress or anything else ends up needing a wildcard subscriber for custom bus traffic.

## Toolchain note

This repo pins `rust-toolchain.toml` to `1.96.1`. If your default `rustc --version` is older (rustup's `stable` channel can lag), run:

```sh
rustup toolchain install 1.96.1 --profile default
rustup override set 1.96.1
```

from inside this checkout.
