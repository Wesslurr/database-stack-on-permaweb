//! "Verify-Don't-Trust" consensus validator for Shared Data replicas.

use crate::authoritative_state::{AuthoritativeState, DrainedBatch};
use crate::entity::{json_safe, pack_entity};
use crate::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub enum CommitOutcome {
    Committed {
        status: u16,
        body: String,
        batch_id: String,
        tick: u64,
        state_root: String,
        drained: Vec<DrainedBatch>,
    },
    IdempotentCached {
        status: u16,
        body: String,
        batch_id: String,
        tick: u64,
        state_root: String,
    },
    Deferred {
        status: u16,
        body: String,
        batch_id: String,
        should_reject: bool,
    },
    Rejected {
        status: u16,
        body: String,
        batch_id: String,
        reason: &'static str,
    },
    BadRequest {
        status: u16,
        body: String,
    },
}

pub struct Verifier;

impl Verifier {
    pub fn verify_and_commit(
        state: &mut AuthoritativeState,
        batch_id: &str,
        tick_from: Option<u64>,
        tick_to: Option<u64>,
        claimed_root: Option<&str>,
        delta: BTreeMap<String, (f64, f64, String, Option<u64>)>, // id -> (x, y, owner, tick)
        order: Vec<String>,
        sd_signer: &str,
    ) -> CommitOutcome {
        if batch_id.is_empty() || tick_from.is_none() || tick_to.is_none() || claimed_root.is_none() {
            let body = format!(
                "{{\"status\":\"error_invalid_commit\",\"tier\":\"shared_data_replica\",\"sd_signer\":\"{}\",\"last_tick\":{}}}",
                json_safe(sd_signer), state.last_tick
            );
            return CommitOutcome::BadRequest { status: 400, body };
        }

        let tick_from = tick_from.unwrap();
        let tick_to = tick_to.unwrap();
        let claimed_root = claimed_root.unwrap();

        // 1. Idempotency Check
        if let Some((cached_tick, cached_root, cached_body)) = state.committed_batches.get(batch_id) {
            if *cached_tick == tick_to
                && cached_root == claimed_root
                && !(tick_from == 0 && state.last_tick > tick_to)
            {
                return CommitOutcome::IdempotentCached {
                    status: 200,
                    body: cached_body.clone(),
                    batch_id: batch_id.to_owned(),
                    tick: *cached_tick,
                    state_root: cached_root.clone(),
                };
            }
        }

        // 2. Batch bounds guard
        if order.len() > state.max_batch_inputs {
            let body = format!(
                "{{\"status\":\"error_batch_too_large\",\"batch_id\":\"{}\",\"inputs\":{},\"max_inputs\":{},\"sd_signer\":\"{}\",\"last_tick\":{}}}",
                json_safe(batch_id), order.len(), state.max_batch_inputs, json_safe(sd_signer), state.last_tick
            );
            return CommitOutcome::BadRequest { status: 413, body };
        }

        // Pack delta items
        let mut packed_delta = BTreeMap::new();
        for (id, (x, y, owner, etick)) in delta.iter() {
            let t = etick.unwrap_or(tick_to);
            packed_delta.insert(id.clone(), pack_entity(*x, *y, t, owner));
        }

        // 3. Out-of-order check (tick_from > LastTick)
        if tick_from > state.last_tick {
            let should_reject = (tick_from - state.last_tick) > 8;
            if !order.is_empty() && !state.pending.batches.contains_key(batch_id) {
                if state.count_replicas() + order.len() > state.max_entities {
                    let body = format!(
                        "{{\"status\":\"error_entity_limit\",\"batch_id\":\"{}\",\"sd_signer\":\"{}\",\"last_tick\":{}}}",
                        json_safe(batch_id), json_safe(sd_signer), state.last_tick
                    );
                    return CommitOutcome::Rejected { status: 409, body, batch_id: batch_id.to_owned(), reason: "error_entity_limit" };
                } else if !state.pending.insert(batch_id, tick_from, tick_to, claimed_root, packed_delta, order) {
                    return CommitOutcome::Rejected {
                        status: 409,
                        body: format!("{{\"status\":\"error_pending_full\",\"batch_id\":\"{}\"}}", json_safe(batch_id)),
                        batch_id: batch_id.to_owned(),
                        reason: "replica_behind",
                    };
                }
            }
            let body = format!(
                "{{\"status\":\"deferred\",\"batch_id\":\"{}\",\"tick_from\":{},\"last_tick\":{},\"sd_signer\":\"{}\"}}",
                json_safe(batch_id), tick_from, state.last_tick, json_safe(sd_signer)
            );
            return CommitOutcome::Deferred {
                status: 202,
                body,
                batch_id: batch_id.to_owned(),
                should_reject,
            };
        }

        // 4. Stale tick check (tick_from < LastTick)
        if tick_from < state.last_tick {
            let body = format!(
                "{{\"status\":\"error_tick_mismatch\",\"batch_id\":\"{}\",\"tick_from\":{},\"expected_tick_from\":{},\"last_tick\":{},\"state_root\":\"{}\",\"sd_signer\":\"{}\"}}",
                json_safe(batch_id), tick_from, state.last_tick, state.last_tick, state.state_root, json_safe(sd_signer)
            );
            return CommitOutcome::Rejected {
                status: 409,
                body,
                batch_id: batch_id.to_owned(),
                reason: "replica_ahead",
            };
        }

        // 5. Cryptographic root verification (tick_from == LastTick)
        let computed_root = state.shards.candidate_root(&packed_delta, tick_to as i64);
        if computed_root != claimed_root {
            let body = format!(
                "{{\"status\":\"diverged\",\"batch_id\":\"{}\",\"expected_root\":\"{}\",\"computed_root\":\"{}\",\"last_tick\":{},\"sd_signer\":\"{}\",\"count\":{}}}",
                json_safe(batch_id), json_safe(claimed_root), json_safe(&computed_root),
                state.last_tick, json_safe(sd_signer), state.count_replicas()
            );
            return CommitOutcome::Rejected {
                status: 409,
                body,
                batch_id: batch_id.to_owned(),
                reason: "root_divergence",
            };
        }

        // 6. Verification Success: Apply delta to state
        let body = state.apply_delta(batch_id, tick_from, tick_to, &packed_delta, &order, sd_signer);
        let drained = state.drain_pending(sd_signer);

        CommitOutcome::Committed {
            status: 200,
            body,
            batch_id: batch_id.to_owned(),
            tick: state.last_tick,
            state_root: state.state_root.clone(),
            drained,
        }
    }
}
