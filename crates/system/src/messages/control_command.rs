//! The control-command contract from the hypergraph control-plane design (§6.3 in
//! `anant-trader/ROADMAP.md`) — what a reasoner or the control-plane UI publishes to ask a node
//! to do something, and how that resolves onto primitives this repo already has.
//!
//! ## Why this is a separate type from [`super::ControllerCommand`]
//!
//! [`super::ControllerCommand`] is Nautilus's own low-level lifecycle protocol (start/stop/remove
//! an actor or strategy), dispatched via [`super::controller::Controller::send`] against the
//! thread-local message bus. [`ControlCommand`] is the richer, audit-carrying envelope an
//! external reasoner sends (TTL, policy version, evidence, ...) — [`ControlCommand::resolve`]
//! turns it into one or more [`ControlAction`]s, each of which maps onto either a
//! `ControllerCommand` or the risk engine's `RiskOverlayTable` (`crates/risk/src/overlay.rs`).
//!
//! ## What is NOT built yet
//!
//! `resolve` is a pure function — no bus access, no I/O — deliberately, so it's unit-testable
//! without a live kernel. Turning its output into actual effect (calling
//! `Controller::send`/`RiskOverlayTable::veto_strategy`) needs something that actually receives
//! these commands off a topic and calls this function, then acts on the result. That's an actor,
//! not a core change — same shape as `HypergraphEventExporter` (a wildcard-subscribed `Actor`,
//! no message-bus changes) — but it isn't wired into a running node's config here. This module is
//! the decode-and-decide logic; the actor that owns a bus subscription and a `RiskOverlayTable`
//! handle is the next concrete step.
//!
//! Only `PauseStrategy`/`ResumeStrategy`/`ThrottleStrategy`/`RequireHumanApproval` resolve to an
//! action today. `DisableVenue`/`ChangeRiskLimit`/`ChangeCapitalAllocation`/`SetTradingState`/
//! `ForceHedge`/`CancelOrders`/`CancelInstrumentOrders`/`ReduceExposure`/`RejectOrderIntent` are
//! real command types in the contract (so a producer can send them without the schema changing
//! later) but resolve to [`ControlAction::Unsupported`] — deliberately not guessed at. Note
//! `SetTradingState` in particular already has a real primitive to map onto
//! (`RiskEngine::set_trading_state`), it just isn't wired here yet.

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::identifiers::StrategyId;
use serde::{Deserialize, Serialize};

/// One command from a reasoner or the control-plane UI, per the §6.3 contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlCommand {
    pub command_id: UUID4,
    pub command_type: ControlCommandType,
    /// The kind of thing `target_id` identifies, e.g. `"Strategy"`, `"Venue"`, `"Instrument"`.
    pub target_type: String,
    pub target_id: String,
    pub severity: ControlCommandSeverity,
    /// How long this command's effect should last before it self-expires, independent of
    /// whether a compensating command ever arrives.
    pub ttl_ms: u64,
    pub requires_ack: bool,
    pub reasoner_id: String,
    pub policy_version: String,
    pub graph_snapshot_id: String,
    pub explanation: String,
    /// Vertex/hyperedge ids from the hypergraph that justify this decision.
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ControlCommandType {
    PauseStrategy,
    ResumeStrategy,
    ThrottleStrategy,
    RejectOrderIntent,
    RequireHumanApproval,
    ReduceExposure,
    CancelOrders,
    CancelInstrumentOrders,
    DisableVenue,
    ChangeRiskLimit,
    ChangeCapitalAllocation,
    SetTradingState,
    ForceHedge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ControlCommandSeverity {
    Advisory,
    Warning,
    Critical,
}

/// What a [`ControlCommand`] resolves to, expressed as data rather than by performing the
/// action — see the module doc comment for why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlAction {
    /// Maps onto `Controller::send(&ControllerCommand::StopStrategy(...))`.
    StopStrategy { strategy_id: StrategyId },
    /// Maps onto `Controller::send(&ControllerCommand::StartStrategy(...))` *and*
    /// `RiskOverlayTable::clear_strategy_veto` — resuming a strategy clears any outstanding
    /// veto rather than leaving it stuck vetoed after being un-paused.
    ResumeStrategy { strategy_id: StrategyId },
    /// Maps onto `RiskOverlayTable::veto_strategy(strategy_id, expires_at)`.
    VetoStrategy {
        strategy_id: StrategyId,
        expires_at: UnixNanos,
    },
    /// This command type doesn't resolve to an existing primitive yet — see the module doc
    /// comment for which types these are and why.
    Unsupported { command_type: ControlCommandType },
}

impl ControlCommand {
    /// Resolves this command to the action(s) a caller should take, given `now` for TTL
    /// computation. Pure — no bus access, no I/O, fully unit-testable.
    #[must_use]
    pub fn resolve(&self, now: UnixNanos) -> Vec<ControlAction> {
        let strategy_id = StrategyId::from(self.target_id.as_str());

        match self.command_type {
            ControlCommandType::PauseStrategy => {
                vec![ControlAction::StopStrategy { strategy_id }]
            }
            ControlCommandType::ResumeStrategy => {
                vec![ControlAction::ResumeStrategy { strategy_id }]
            }
            ControlCommandType::ThrottleStrategy | ControlCommandType::RequireHumanApproval => {
                let expires_at =
                    UnixNanos::from(u64::from(now).saturating_add(self.ttl_ms.saturating_mul(1_000_000)));
                vec![ControlAction::VetoStrategy {
                    strategy_id,
                    expires_at,
                }]
            }
            other => vec![ControlAction::Unsupported { command_type: other }],
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn command(command_type: ControlCommandType, target_id: &str, ttl_ms: u64) -> ControlCommand {
        ControlCommand {
            command_id: UUID4::new(),
            command_type,
            target_type: "Strategy".to_string(),
            target_id: target_id.to_string(),
            severity: ControlCommandSeverity::Critical,
            ttl_ms,
            requires_ack: true,
            reasoner_id: "GlobalExposureReasoner".to_string(),
            policy_version: "risk-policy-v17".to_string(),
            graph_snapshot_id: "snapshot-8842".to_string(),
            explanation: "test".to_string(),
            evidence: vec!["position:btc-perp".to_string()],
        }
    }

    #[rstest]
    fn pause_strategy_resolves_to_stop() {
        let cmd = command(ControlCommandType::PauseStrategy, "stat_arb-001", 30_000);
        let actions = cmd.resolve(UnixNanos::from(1_000));
        assert_eq!(
            actions,
            vec![ControlAction::StopStrategy {
                strategy_id: StrategyId::from("stat_arb-001")
            }]
        );
    }

    #[rstest]
    fn resume_strategy_resolves_to_resume() {
        let cmd = command(ControlCommandType::ResumeStrategy, "stat_arb-001", 0);
        let actions = cmd.resolve(UnixNanos::from(1_000));
        assert_eq!(
            actions,
            vec![ControlAction::ResumeStrategy {
                strategy_id: StrategyId::from("stat_arb-001")
            }]
        );
    }

    #[rstest]
    fn throttle_strategy_resolves_to_veto_with_ttl_applied() {
        let cmd = command(ControlCommandType::ThrottleStrategy, "stat_arb-001", 30_000);
        let actions = cmd.resolve(UnixNanos::from(1_000));
        assert_eq!(
            actions,
            vec![ControlAction::VetoStrategy {
                strategy_id: StrategyId::from("stat_arb-001"),
                // 30_000 ms * 1_000_000 ns/ms + now(1_000)
                expires_at: UnixNanos::from(30_000_001_000),
            }]
        );
    }

    #[rstest]
    fn require_human_approval_also_resolves_to_veto() {
        let cmd = command(ControlCommandType::RequireHumanApproval, "stat_arb-001", 60_000);
        let actions = cmd.resolve(UnixNanos::from(0));
        assert_eq!(
            actions,
            vec![ControlAction::VetoStrategy {
                strategy_id: StrategyId::from("stat_arb-001"),
                expires_at: UnixNanos::from(60_000_000_000),
            }]
        );
    }

    #[rstest]
    #[case(ControlCommandType::DisableVenue)]
    #[case(ControlCommandType::ChangeRiskLimit)]
    #[case(ControlCommandType::ChangeCapitalAllocation)]
    #[case(ControlCommandType::SetTradingState)]
    #[case(ControlCommandType::ForceHedge)]
    #[case(ControlCommandType::CancelOrders)]
    #[case(ControlCommandType::CancelInstrumentOrders)]
    #[case(ControlCommandType::ReduceExposure)]
    #[case(ControlCommandType::RejectOrderIntent)]
    fn unimplemented_command_types_resolve_to_unsupported(#[case] command_type: ControlCommandType) {
        let cmd = command(command_type, "stat_arb-001", 0);
        let actions = cmd.resolve(UnixNanos::from(0));
        assert_eq!(actions, vec![ControlAction::Unsupported { command_type }]);
    }

    #[rstest]
    fn control_command_round_trips_through_json() {
        let cmd = command(ControlCommandType::PauseStrategy, "stat_arb-001", 30_000);
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("\"PauseStrategy\""));
        assert!(json.contains("\"critical\""));
        let decoded: ControlCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, cmd);
    }
}
