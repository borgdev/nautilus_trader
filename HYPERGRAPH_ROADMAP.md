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
| Causality metadata on the wire | `crates/event_store/src/capture/adapter.rs`, `crates/event_store/src/kernel.rs`, `crates/event_store/src/kafka.rs` | ✅ **Done end to end.** `causation_id` already flows on order-event payloads (no change needed). `CapturedEntrySink` + `BusCaptureAdapter::with_sink()` + `EventStoreLifecycleOptions::with_captured_entry_sink()` + `KafkaCapturedEntrySink` (new `kafka` feature) are built and tested, and `anant-trader/bridge`'s consumer enriches vertices from it — see below. |
| GlobalRiskOverlay veto/limit table | `crates/risk/src/overlay.rs`, `crates/risk/src/engine/mod.rs` (`check_order()`) | ✅ **Done.** `RiskOverlayTable` (per-strategy veto, TTL'd, `Arc<DashMap<...>>`-backed) consulted at the start of `check_order()`. Scoped to per-strategy only — global halt/reduce already has `RiskEngine::set_trading_state()`, no need to duplicate it. |
| Control-command ingress | `crates/system/src/messages/control_command.rs` | 🟡 **Decode/decision logic done, ingress actor not built.** `ControlCommand::resolve()` is a pure, tested function from command to action(s) — see below for why the actual ingress needs a new actor, not a core change. |

## Causality metadata: what's built, and what's still missing

`correlation_id`/`causation_id` are populated via `HeadersExtractor` (`crates/event_store/src/capture/registry.rs`), a per-type extraction registry that lives in `crates/event_store` — a layer above `crates/infrastructure`/`crates/common`, not below. `BusTap::on_publish(topic, message: &dyn Any)` (`crates/common/src/msgbus/mod.rs`) gets no headers; the event store's own tap (`EventStoreBusTap::capture`, `crates/event_store/src/kernel.rs`) downcasts to registered concrete types and extracts `Headers` itself, right before handing the entry to `BusCaptureAdapter::capture_any`. Reaching that from the Kafka backing in `crates/infrastructure` would mean either duplicating the registry (layering violation) or inverting the dependency graph.

**Built:** `CapturedEntrySink` (`crates/event_store/src/capture/adapter.rs`) — a trait notified with `identity: Option<UUID4>` (the registry's identity-extractor value, e.g. `OrderFilled::event_id`) plus a clone of the `EntryDraft` (headers, topic, payload_type, payload, ts_init), right after the writer's durable submit succeeds, so a slow/failing sink can't affect capture durability. `BusCaptureAdapter::with_sink()` registers one; `EventStoreLifecycleOptions::with_captured_entry_sink()` wires it through kernel boot the same way the encoder registry and backend opener are already injectable. Off by default, zero cost when unused. The `identity` param matters: it's the same id domain payloads already carry, so a consumer doesn't need a second identity scheme.

`KafkaCapturedEntrySink` (`crates/event_store/src/kafka.rs`, new optional `kafka` feature — lives here rather than in `crates/infrastructure` since that crate doesn't depend on `event_store`, and `event_store` already depends on `nautilus-system`, so the other direction would make a backing-implementations crate depend on the full kernel stack) publishes a compact `CausalityRecord` (identity, topic, payload_type, correlation_id, causation_id, ts_init) to a Kafka topic (`hg.nautilus.causality.v1` by default). `anant-trader/bridge/src/causality.rs` consumes it and enriches the matching HGFS vertex — scoped to order events only today, since that's the only type where the registry's `identity` and the bridge's own vertex-keying scheme (`event:{event_id}`) actually line up (`AccountState` also has a registered identity, but the bridge keys `Account` vertices differently — deliberately not enriched to avoid a disconnected duplicate vertex).

**Separately, verified while building `bridge/src/schema.rs`:** every order event (`OrderInitialized` through `OrderFilled`) already serializes `causation_id: Option<UUID4>` as a field on the domain struct itself (`crates/model/src/events/order/*.rs`) — so it reaches the Kafka wire for free inside the JSON payload, no `event_store` involvement needed. `correlation_id` is not on any domain struct anywhere (checked the whole `model` crate) — it's purely an `event_store`/dispatch-boundary concept, which is exactly what the causality side-channel exists to surface.

## GlobalRiskOverlay: why it's scoped to per-strategy only

`RiskEngine` already has `set_trading_state()` (`crates/risk/src/engine/mod.rs`) — `TradingState::Active`/`Reducing`/`Halted` — and `execution_gateway` already gates every order submission on it, publishing `TradingStateChanged`. That's a real, already-working global on/off switch. `RiskOverlayTable` deliberately doesn't duplicate it — it only covers what doesn't exist yet: vetoing a *specific* strategy without halting the whole engine. `RiskEngine::risk_overlay()` returns a clone of the table (it's `Arc`-backed) for whatever ends up calling `veto_strategy`/`clear_strategy_veto` from outside the single-threaded kernel.

## Control-command ingress: why it needs a new actor, not a core change

Tracing `crates/common/src/msgbus/api.rs`: every typed publish path (`publish_quote`, `publish_order_event`, `publish_position_event`, `publish_account_state`, etc.) already calls `forward_to_external_egress` unconditionally — so once the Kafka backing is configured, the entire order/position/account/market-data lifecycle ships externally with **no application code**, Python or otherwise. The one gap is `publish_any` (line ~942), which only forwards when the payload downcasts to `CustomData` — i.e. the low-level `self.msgbus.publish(topic, obj)` path (as opposed to `self.publish_data(...)`) is not captured.

That gap matters directly for control-command ingress: tracing `Controller::send(&ControllerCommand)` (`crates/system/src/controller.rs`) shows it's a static method that looks up the `"Controller.execute"` **endpoint** on the thread-local message bus — point-to-point, not topic pub/sub. The existing Kafka ingress (MVP0) republishes inbound messages via `publish_any`/`publish_quote`/etc. (topic-based) only, with no path to an endpoint-based `send`. So getting a `ControlCommand` from Kafka to actually pausing a strategy needs something that receives commands off a topic (reachable via the existing `CustomData` ingress path) and then calls `Controller::send`/`RiskOverlayTable` itself, using `ControlCommand::resolve()`'s output. That's a new actor — the same shape as `HypergraphEventExporter` (wildcard-subscribed, no message-bus changes) — not built here.

## Toolchain note

This repo pins `rust-toolchain.toml` to `1.96.1`. If your default `rustc --version` is older (rustup's `stable` channel can lag), run:

```sh
rustup toolchain install 1.96.1 --profile default
rustup override set 1.96.1
```

from inside this checkout.
