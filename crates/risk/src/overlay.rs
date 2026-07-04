//! A synchronous, in-memory veto table [`RiskEngine::check_order`](crate::engine::RiskEngine)
//! consults on every order submission.
//!
//! This is the "`GlobalRiskOverlay`" mechanism from the hypergraph control-plane design (see
//! `anant-trader/ROADMAP.md`) — a way for control commands arriving from outside the
//! single-threaded kernel (a Kafka control-command ingress task, most likely) to veto new
//! orders for a specific strategy without the risk engine making a remote call, or blocking, on
//! the hot path.
//!
//! Scope: per-strategy only. Per-venue (`DisableVenue`) and global (`SetTradingState`) control
//! commands don't need this table at all — `RiskEngine` already has `set_trading_state()`
//! (`crates/risk/src/engine/mod.rs`), which already gates every order submission via
//! `TradingState::Halted`/`Reducing`. Building a second global on/off switch here would just
//! duplicate that.
//!
//! `RiskEngine` itself is `Rc`/`RefCell`-based and stays on one thread (see its `clock` field);
//! this table is deliberately its own `Arc`-backed type so a *clone* of it — not the engine
//! itself — can be handed to an external, multi-threaded ingress task. [`RiskEngine::risk_overlay`]
//! is how a caller gets that clone.

use std::sync::Arc;

use dashmap::DashMap;
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::StrategyId;

#[derive(Debug, Clone, Copy)]
struct Veto {
    /// The domain timestamp after which this veto no longer applies. Checking against a
    /// caller-supplied "now" keeps this table clock-agnostic.
    expires_at: UnixNanos,
}

/// Shared, thread-safe, per-strategy veto table.
///
/// Cheap to clone — every clone shares the same underlying map via `Arc`.
#[derive(Debug, Clone, Default)]
pub struct RiskOverlayTable {
    vetoes: Arc<DashMap<StrategyId, Veto>>,
}

impl RiskOverlayTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Vetoes new order submissions for `strategy_id` until `expires_at`.
    ///
    /// Callers should compute `expires_at` from the control command's own `ttl_ms` (see the
    /// control-command contract in `anant-trader/ROADMAP.md` §6.3) rather than pass an unbounded
    /// veto — every override here is meant to expire on its own even if the compensating
    /// "clear" command is lost.
    pub fn veto_strategy(&self, strategy_id: StrategyId, expires_at: UnixNanos) {
        self.vetoes.insert(strategy_id, Veto { expires_at });
    }

    /// Clears a veto before its TTL expires — the "reasoner must publish a compensating command
    /// when the condition clears" rule from the control-command design.
    pub fn clear_strategy_veto(&self, strategy_id: &StrategyId) {
        self.vetoes.remove(strategy_id);
    }

    /// Returns `true` if `strategy_id` has an unexpired veto as of `now`.
    ///
    /// Expired entries are treated as absent but not proactively swept here — this runs on the
    /// order-submission hot path and shouldn't pay eviction cost; a stale entry is harmless dead
    /// weight until the next `veto_strategy`/`clear_strategy_veto` for the same key.
    #[must_use]
    pub fn is_vetoed(&self, strategy_id: &StrategyId, now: UnixNanos) -> bool {
        self.vetoes
            .get(strategy_id)
            .is_some_and(|entry| now < entry.expires_at)
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::StrategyId;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn unvetoed_strategy_is_not_vetoed() {
        let table = RiskOverlayTable::new();
        let strategy_id = StrategyId::from("S-001");
        assert!(!table.is_vetoed(&strategy_id, UnixNanos::from(100)));
    }

    #[rstest]
    fn veto_applies_before_expiry_and_not_after() {
        let table = RiskOverlayTable::new();
        let strategy_id = StrategyId::from("S-001");
        table.veto_strategy(strategy_id, UnixNanos::from(1_000));

        assert!(table.is_vetoed(&strategy_id, UnixNanos::from(500)));
        assert!(!table.is_vetoed(&strategy_id, UnixNanos::from(1_000)));
        assert!(!table.is_vetoed(&strategy_id, UnixNanos::from(1_500)));
    }

    #[rstest]
    fn clear_strategy_veto_lifts_it_immediately() {
        let table = RiskOverlayTable::new();
        let strategy_id = StrategyId::from("S-001");
        table.veto_strategy(strategy_id, UnixNanos::from(1_000));
        assert!(table.is_vetoed(&strategy_id, UnixNanos::from(500)));

        table.clear_strategy_veto(&strategy_id);
        assert!(!table.is_vetoed(&strategy_id, UnixNanos::from(500)));
    }

    #[rstest]
    fn clone_shares_the_same_underlying_table() {
        let table = RiskOverlayTable::new();
        let handle = table.clone();
        let strategy_id = StrategyId::from("S-001");

        // Simulates an external ingress task (holding `handle`) vetoing a strategy while the
        // risk engine's own thread (holding `table`) checks it.
        handle.veto_strategy(strategy_id, UnixNanos::from(1_000));

        assert!(table.is_vetoed(&strategy_id, UnixNanos::from(500)));
    }

    #[rstest]
    fn vetoes_are_scoped_per_strategy() {
        let table = RiskOverlayTable::new();
        let vetoed = StrategyId::from("S-001");
        let other = StrategyId::from("S-002");
        table.veto_strategy(vetoed, UnixNanos::from(1_000));

        assert!(table.is_vetoed(&vetoed, UnixNanos::from(500)));
        assert!(!table.is_vetoed(&other, UnixNanos::from(500)));
    }
}
