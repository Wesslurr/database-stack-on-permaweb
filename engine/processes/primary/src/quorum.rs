//! Honest 2-of-3 quorum coordinator & circuit breaker for the Primary sequencer.
use crate::compactor::{build_delta, make_batch_id, QueueEntry};
use crate::entity::{json_safe, unpack_entity};
use crate::merkle::sha256_hex;
use crate::num::int_str;
use crate::optimistic_state::{OptimisticState, TICKS_PER_BATCH};
use crate::prelude::*;

pub const ACK_DEADLINE_REQUESTS: u64 = 512;
pub const MAX_PENDING_BATCHES: usize = 64;
pub const MAX_COMMITTED_HISTORY: usize = 256;
pub const MAX_REPAIR_BATCHES: usize = 256;
pub const MAX_LAG_INPUTS: usize = 2048;

#[derive(Clone, Debug, PartialEq)]
pub struct SdTarget {
    pub url: String,
    pub process: String,
    pub role: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Topology {
    pub genesis_url: String,
    pub epoch: u64,
    pub arbit_url: Option<String>,
    pub sd_targets: Vec<SdTarget>,
}

#[derive(Clone, Debug)]
pub struct PendingBatch {
    pub batch_id: String,
    pub tick_from: i64,
    pub tick_to: i64,
    pub root: String,
    pub delta: BTreeMap<String, String>,
    pub order: Vec<String>,
    pub entries: Vec<QueueEntry>,
    pub inputs: usize,
    pub created: u64,
    pub created_ts: Option<i64>,
    pub deadline: u64,
    pub last_sent: u64,
    pub last_sent_ts: Option<i64>,
    pub done: bool,
    pub expired: bool,
    pub acked_by: Vec<String>,
    pub cast_stage: usize,
    pub next_cast: u64,
    pub suffix: String,
}

#[derive(Clone, Debug)]
pub struct RepairBatch {
    pub batch_id: String,
    pub tick_from: i64,
    pub tick_to: i64,
    pub root: String,
    pub delta: BTreeMap<String, String>,
    pub order: Vec<String>,
    pub acked: BTreeMap<String, bool>,
    pub cast_at: u64,
    pub attempts: usize,
    pub compacted: bool,
}

#[derive(Clone, Debug)]
pub struct QuorumState {
    pub pending: BTreeMap<String, PendingBatch>,
    pub pending_order: VecDeque<String>,
    pub committed: BTreeMap<String, String>, // batch_id -> json body
    pub committed_order: VecDeque<String>,
    pub repair: Vec<RepairBatch>,
    pub topology: Option<Topology>,
    pub worker_role: String,
    pub last_acked_replicas: Vec<String>,
    pub request_clock: u64,
    pub last_batch: Option<(String, i64, String)>, // batch_id, tick, root
}

impl Default for QuorumState {
    fn default() -> Self {
        Self::new()
    }
}

impl QuorumState {
    pub fn new() -> Self {
        let default_topology = Topology {
            genesis_url: "http://127.0.0.1:8733".to_string(),
            epoch: 1,
            arbit_url: Some("http://127.0.0.1:8737".to_string()),
            sd_targets: vec![
                SdTarget {
                    url: "http://127.0.0.1:8735".to_string(),
                    process: "sd1".to_string(),
                    role: "sd1".to_string(),
                },
                SdTarget {
                    url: "http://127.0.0.1:8736".to_string(),
                    process: "sd2".to_string(),
                    role: "sd2".to_string(),
                },
            ],
        };
        Self {
            pending: BTreeMap::new(),
            pending_order: VecDeque::new(),
            committed: BTreeMap::new(),
            committed_order: VecDeque::new(),
            repair: Vec::new(),
            topology: Some(default_topology),
            worker_role: "primary".to_string(),
            last_acked_replicas: vec!["sd1".to_string(), "sd2".to_string()],
            request_clock: 0,
            last_batch: None,
        }
    }

    pub fn set_worker_role(&mut self, role: &str) {
        if role.is_empty() || (self.worker_role == role && self.topology.is_some()) {
            return;
        }
        self.worker_role = role.to_string();
        let suffix = role
            .rfind('_')
            .map(|pos| &role[pos + 1..])
            .filter(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit()));
        let (sd1_proc, sd2_proc) = match suffix {
            Some(idx) => (format!("sd1_{}", idx), format!("sd2_{}", idx)),
            None => ("sd1".to_string(), "sd2".to_string()),
        };

        self.last_acked_replicas = vec![sd1_proc.clone(), sd2_proc.clone()];

        let targets = vec![
            SdTarget {
                url: "http://127.0.0.1:8735".to_string(),
                process: sd1_proc.clone(),
                role: sd1_proc,
            },
            SdTarget {
                url: "http://127.0.0.1:8736".to_string(),
                process: sd2_proc.clone(),
                role: sd2_proc,
            },
        ];

        if let Some(ref mut topo) = self.topology {
            topo.sd_targets = targets;
        } else {
            self.topology = Some(Topology {
                genesis_url: "http://127.0.0.1:8733".to_string(),
                epoch: 1,
                arbit_url: Some("http://127.0.0.1:8737".to_string()),
                sd_targets: targets,
            });
        }
    }

    pub fn replicas_json(&self) -> String {
        let items: Vec<String> = self
            .last_acked_replicas
            .iter()
            .map(|r| format!(r#""{}""#, json_safe(r)))
            .collect();
        format!("[{}]", items.join(","))
    }

    pub fn sd_roles_json(&self) -> String {
        if let Some(topo) = &self.topology {
            let items: Vec<String> = topo
                .sd_targets
                .iter()
                .map(|t| {
                    let r = if !t.role.is_empty() { &t.role } else { &t.process };
                    format!(r#""{}""#, json_safe(r))
                })
                .collect();
            format!("[{}]", items.join(","))
        } else {
            self.replicas_json()
        }
    }

    pub fn sync_head(&self, state: &mut OptimisticState) {
        if let Some(p) = self.first_pending() {
            state.recompute_head(Some(p.tick_to), Some(&p.root));
        } else {
            state.recompute_head(None, None);
        }
    }

    pub fn sync_head_with_points(&self, state: &mut OptimisticState) {
        if let Some(p) = self.first_pending() {
            state.recompute_head(Some(p.tick_to), Some(&p.root));
            state.sync_head_points(Some(&p.delta));
        } else {
            state.recompute_head(None, None);
            state.sync_head_points(None);
        }
    }

    pub fn first_pending(&self) -> Option<&PendingBatch> {
        self.pending.values().find(|p| !p.done)
    }

    pub fn first_pending_mut(&mut self) -> Option<&mut PendingBatch> {
        self.pending.values_mut().find(|p| !p.done)
    }

    pub fn pending_count(&self) -> usize {
        self.pending.values().filter(|p| !p.done).count()
    }

    pub fn remember_committed(&mut self, batch_id: String, body: String) {
        if self.committed.contains_key(&batch_id) {
            return;
        }
        self.committed.insert(batch_id.clone(), body);
        self.committed_order.push_back(batch_id);
        while self.committed_order.len() > MAX_COMMITTED_HISTORY {
            if let Some(old) = self.committed_order.pop_front() {
                self.committed.remove(&old);
            }
        }
    }

    pub fn circuit_breaker_tripped(
        &self,
        queue_len: usize,
        circuit_breaker_flag: bool,
        now_ts: Option<i64>,
    ) -> bool {
        let in_flight = match self.first_pending() {
            Some(p) => p,
            None => return false,
        };
        if !in_flight.expired {
            return false;
        }
        if circuit_breaker_flag {
            return true;
        }
        if queue_len < MAX_LAG_INPUTS {
            return false;
        }
        if let (Some(now), Some(created)) = (now_ts, in_flight.created_ts) {
            if now > 1_000_000_000_000 && created > 1_000_000_000_000 && (now - created) < 5000 {
                return false;
            }
        }
        true
    }

    pub fn expire_pending(&mut self, now_ts: Option<i64>) {
        let clock = self.request_clock;
        for p in self.pending.values_mut() {
            if p.done {
                continue;
            }
            if !p.expired {
                match (now_ts, p.created_ts) {
                    (Some(now), Some(created)) if now > 1_000_000_000_000 && created > 1_000_000_000_000 => {
                        if (now - created) >= 850 {
                            p.expired = true;
                        }
                    }
                    _ => {
                        if clock >= p.deadline {
                            p.expired = true;
                        }
                    }
                }
            }
        }
    }

    /// Builds the Commit payload for replica broadcasting.
    pub fn commit_payload(
        batch_id: &str,
        tick_from: i64,
        tick_to: i64,
        state_root: &str,
        delta: &BTreeMap<String, String>,
        order: &[String],
        epoch: u64,
    ) -> String {
        let mut items = Vec::new();
        for id in order {
            if let Some(packed) = delta.get(id) {
                if let Some(e) = unpack_entity(packed) {
                    items.push(format!(
                        r#"["{}",{:.6},{:.6},"{}"]"#,
                        json_safe(id),
                        e.x,
                        e.y,
                        json_safe(&e.owner)
                    ));
                }
            }
        }
        format!(
            r#"{{"action":"Commit","batch_id":"{}","tick_from":{},"tick_to":{},"state_root":"{}","delta":[{}],"epoch":{}}}"#,
            json_safe(batch_id),
            tick_from,
            tick_to,
            state_root,
            items.join(","),
            epoch
        )
    }

    /// Opens/closes a batch and computes the candidate state root.
    pub fn start_batch(
        &mut self,
        state: &mut OptimisticState,
        status_suffix: &str,
    ) -> (u16, String, Option<Vec<(String, String)>>) {
        if state.queue.is_empty() {
            if let Some(in_flight) = self.first_pending() {
                let body = Self::pending_body(in_flight, state, status_suffix, "retrying");
                return (202, body, None);
            }
            state.recompute_head(None, None);
            let body = format!(
                r#"{{"status":"ok","ack":"replicated","batch_id":null,"tick":{},"head_tick":{},"replicated_tick":{},"replication_lag":0,"state_root":"{}","head_root":"{}","replicated_root":"{}","active_entity_count":{},"queued":0,"noop":true,"point":{}{}}}"#,
                state.tick,
                state.tick,
                state.tick,
                state.state_root,
                state.state_root,
                state.state_root,
                state.count_entities(),
                state.player_red_json(),
                status_suffix
            );
            return (200, body, None);
        }

        if let Some(in_flight) = self.first_pending() {
            state.recompute_head(Some(in_flight.tick_to), Some(&in_flight.root));
            let lag = (state.head_tick - state.tick).max(0);
            let q_batch_id = state
                .open_batch_id
                .clone()
                .unwrap_or_else(|| in_flight.batch_id.clone());
            let body = format!(
                r#"{{"status":"queued","ack":"accepted","batch_id":"{}","pending_batch":"{}","queued":{},"tick":{},"head_tick":{},"tick_to":{},"confirmed_tick":{},"lag_ticks":{},"state_root":"{}","head_root":"{}","active_entity_count":{},"point":{}{}}}"#,
                json_safe(&q_batch_id),
                json_safe(&in_flight.batch_id),
                state.queue.len(),
                state.tick,
                state.head_tick,
                state.head_tick,
                state.tick,
                lag,
                state.state_root,
                state.head_root,
                state.count_entities(),
                state.player_red_json(),
                status_suffix
            );
            return (202, body, None);
        }

        let tick_from = state.tick;
        let batch_entries = core::mem::take(&mut state.queue);
        let tick_to = state.tick + TICKS_PER_BATCH;
        let seq_from = batch_entries[0].seq;
        let seq_to = batch_entries[batch_entries.len() - 1].seq;

        if batch_entries.len() > 1 {
            state.compacted_count += batch_entries.len() - 1;
        }

        let delta_res = build_delta(
            &batch_entries,
            tick_to,
            &state.entity_last_tick,
            &state.points,
        );
        state.out_of_order_dropped += delta_res.out_of_order_dropped;
        for (id, t) in delta_res.temp_last_tick {
            state.entity_last_tick.insert(id, t);
        }

        let digest_raw = format!(
            "{}|{}|{}|{}",
            delta_res.order.join(","),
            seq_from,
            seq_to,
            tick_from
        );
        let digest = sha256_hex(digest_raw.as_bytes());
        let batch_id = state
            .open_batch_id
            .take()
            .unwrap_or_else(|| make_batch_id(seq_from, seq_to, tick_from, &digest));

        let new_root = state.shards.candidate_root(&delta_res.delta, tick_to);

        let epoch = self.topology.as_ref().map(|t| t.epoch).unwrap_or(1);
        let mut relay_messages = Vec::new();
        if let Some(topo) = &self.topology {
            let payload = Self::commit_payload(
                &batch_id,
                tick_from,
                tick_to,
                &new_root,
                &delta_res.delta,
                &delta_res.order,
                epoch,
            );
            for target in &topo.sd_targets {
                let url = format!("{}/~node-process@1.0/{}/push", target.url, target.process);
                relay_messages.push((url, payload.clone()));
            }
        }

        let order_len = delta_res.order.len();
        let opened_ts = state.batch_opened_ts.take();
        let batch = PendingBatch {
            batch_id: batch_id.clone(),
            tick_from,
            tick_to,
            root: new_root.clone(),
            delta: delta_res.delta,
            order: delta_res.order,
            entries: batch_entries,
            inputs: (seq_to - seq_from + 1) as usize,
            created: self.request_clock,
            created_ts: opened_ts,
            deadline: self.request_clock + ACK_DEADLINE_REQUESTS,
            last_sent: self.request_clock,
            last_sent_ts: opened_ts,
            done: false,
            expired: false,
            acked_by: Vec::new(),
            cast_stage: self
                .topology
                .as_ref()
                .map(|t| t.sd_targets.len())
                .unwrap_or(1),
            next_cast: self.request_clock + 2,
            suffix: status_suffix.to_string(),
        };

        // Bind idempotency keys
        for entry in &batch.entries {
            if let Some(ref k) = entry.key {
                state.idempotency.bind_batch_id(k, &batch_id);
            }
        }

        self.pending.insert(batch_id.clone(), batch);
        self.pending_order.push_back(batch_id.clone());
        while self.pending.len() > MAX_PENDING_BATCHES {
            if let Some(old) = self.pending_order.pop_front() {
                self.pending.remove(&old);
            }
        }

        state.recompute_head(Some(tick_to), Some(&new_root));
        let lag = (state.head_tick - state.tick).max(0);
        let replicas_expected = self
            .topology
            .as_ref()
            .map(|t| t.sd_targets.len())
            .unwrap_or(0);

        let body = format!(
            r#"{{"status":"committing","ack":"accepted","batch_id":"{}","tick":{},"head_tick":{},"confirmed_tick":{},"lag_ticks":{},"tick_from":{},"tick_to":{},"state_root":"{}","head_root":"{}","target_root":"{}","delta_count":{},"compacted_total":{},"queued":0,"replicas_expected":{},"point":{}{}}}"#,
            json_safe(&batch_id),
            state.tick,
            state.head_tick,
            state.tick,
            lag,
            tick_from,
            tick_to,
            state.state_root,
            state.head_root,
            new_root,
            order_len,
            state.compacted_count,
            replicas_expected,
            state.player_red_json(),
            status_suffix
        );

        (202, body, Some(relay_messages))
    }

    /// Verifies replica Ack and commits state.
    pub fn handle_ack(
        &mut self,
        batch_id: &str,
        claimed_tick: Option<i64>,
        claimed_root: Option<&str>,
        claimed_epoch: Option<u64>,
        role: &str,
        state: &mut OptimisticState,
        status_suffix: &str,
    ) -> (u16, String, Option<Vec<(String, String)>>) {
        let p = match self.pending.get_mut(batch_id) {
            Some(p) => p,
            None => {
                let mut acked_roles: Vec<String> = Vec::new();
                for rec in &mut self.repair {
                    if rec.batch_id == batch_id {
                        rec.acked.insert(role.to_string(), true);
                        acked_roles = rec.acked.keys().cloned().collect();
                    }
                }
                if self.committed.contains_key(batch_id) {
                    if acked_roles.is_empty() {
                        acked_roles = self.last_acked_replicas.clone();
                        if !acked_roles.iter().any(|r| r == role) {
                            acked_roles.push(role.to_string());
                        }
                        acked_roles.sort();
                    }
                    self.last_acked_replicas = acked_roles.clone();
                    let acked_by_json = format!(
                        "[{}]",
                        acked_roles
                            .iter()
                            .map(|r| format!(r#""{}""#, json_safe(r)))
                            .collect::<Vec<String>>()
                            .join(",")
                    );
                    self.sync_head(state);
                    let lag = (state.head_tick - state.tick).max(0);
                    let n_ent = state.count_entities();
                    let epoch_val = self.topology.as_ref().map(|t| t.epoch).unwrap_or(1);
                    let confirmed_tick = claimed_tick.unwrap_or(state.tick);
                    let body = format!(
                        r#"{{"http_status":200,"status":"ok","ack":"replicated","epoch":{},"batch_id":"{}","tick":{},"head_tick":{},"replicated_tick":{},"replication_lag":{},"state_root":"{}","head_root":"{}","replicated_root":"{}","replicas":{},"sd_ack":true,"sd_acked_nodes":{},"sd_confirmed_tick":{},"sd_persistence":"lmdb_disk","active_entity_count":{},"queued":{},"compacted_total":{},"out_of_order_dropped":{},"audit_ring_size":{},"point":{}{}}}"#,
                        epoch_val,
                        json_safe(batch_id),
                        state.tick,
                        state.head_tick,
                        state.tick,
                        lag,
                        state.state_root,
                        state.head_root,
                        state.state_root,
                        acked_by_json,
                        acked_by_json,
                        confirmed_tick,
                        n_ent,
                        state.queue.len(),
                        state.compacted_count,
                        state.out_of_order_dropped,
                        state.audit_ring.len(),
                        state.player_red_json(),
                        status_suffix
                    );
                    if claimed_tick == Some(state.tick) {
                        self.committed.insert(batch_id.to_string(), body.clone());
                    }
                    return (200, body, None);
                }
                self.sync_head(state);
                let lag = (state.head_tick - state.tick).max(0);
                let body = format!(
                    r#"{{"status":"ignored","reason":"unknown_batch","batch_id":"{}","tick":{},"head_tick":{},"replicated_tick":{},"replication_lag":{},"state_root":"{}","head_root":"{}","replicated_root":"{}","active_entity_count":{},"compacted_total":{}{}}}"#,
                    json_safe(batch_id),
                    state.tick,
                    state.head_tick,
                    state.tick,
                    lag,
                    state.state_root,
                    state.head_root,
                    state.state_root,
                    state.count_entities(),
                    state.compacted_count,
                    status_suffix
                );
                return (200, body, None);
            }
        };

        if p.done {
            for rec in &mut self.repair {
                if rec.batch_id == batch_id {
                    rec.acked.insert(role.to_string(), true);
                }
            }
            self.sync_head(state);
            let lag = (state.head_tick - state.tick).max(0);
            let body = format!(
                r#"{{"status":"duplicate_ack","batch_id":"{}","tick":{},"head_tick":{},"replicated_tick":{},"replication_lag":{},"state_root":"{}","head_root":"{}","replicated_root":"{}","active_entity_count":{},"compacted_total":{}{}}}"#,
                json_safe(batch_id),
                state.tick,
                state.head_tick,
                state.tick,
                lag,
                state.state_root,
                state.head_root,
                state.state_root,
                state.count_entities(),
                state.compacted_count,
                status_suffix
            );
            return (200, body, None);
        }

        if claimed_root != Some(&p.root) || claimed_tick != Some(p.tick_to) {
            let body = format!(
                r#"{{"status":"ack_mismatch","batch_id":"{}","claimed_root":"{}","expected_root":"{}","claimed_tick":{},"expected_tick":{},"tick":{},"state_root":"{}"}}"#,
                json_safe(batch_id),
                json_safe(claimed_root.unwrap_or("")),
                p.root,
                claimed_tick
                    .map(|t| int_str(t as f64))
                    .unwrap_or_else(|| "null".to_string()),
                p.tick_to,
                state.tick,
                state.state_root
            );
            return (409, body, None);
        }

        if let (Some(c_epoch), Some(topo)) = (claimed_epoch, &self.topology) {
            if c_epoch != topo.epoch {
                let body = format!(
                    r#"{{"status":"epoch_mismatch","batch_id":"{}","claimed_epoch":{},"expected_epoch":{},"tick":{},"state_root":"{}"}}"#,
                    json_safe(batch_id),
                    c_epoch,
                    topo.epoch,
                    state.tick,
                    state.state_root
                );
                return (409, body, None);
            }
        }

        // Apply batch
        for (id, packed) in &p.delta {
            state.points.insert(id.clone(), packed.clone());
        }
        state.tick = p.tick_to;
        state.state_root = state.shards.commit(&p.delta, p.tick_to);

        for (id, packed) in &p.delta {
            if let Some(e) = unpack_entity(packed) {
                state.append_audit(p.tick_to, id, e.x, e.y);
            }
        }

        p.done = true;
        p.acked_by.push(role.to_string());
        state.applied_inputs += p.inputs;
        self.last_batch = Some((batch_id.to_string(), p.tick_to, p.root.clone()));

        // Add to repair batches for the other replica
        let mut acked_map = BTreeMap::new();
        acked_map.insert(role.to_string(), true);
        self.repair.push(RepairBatch {
            batch_id: p.batch_id.clone(),
            tick_from: p.tick_from,
            tick_to: p.tick_to,
            root: p.root.clone(),
            delta: p.delta.clone(),
            order: p.order.clone(),
            acked: acked_map,
            cast_at: self.request_clock,
            attempts: 0,
            compacted: false,
        });
        while self.repair.len() > MAX_REPAIR_BATCHES {
            self.repair.remove(0);
        }

        let p_inputs = p.inputs;
        let p_tick_to = p.tick_to;
        let p_suffix = p.suffix.clone();
        self.last_acked_replicas = vec![role.to_string()];
        let acked_by_json = format!(r#"["{}"]"#, json_safe(role));
        self.sync_head(state);
        let lag = (state.head_tick - state.tick).max(0);
        let n_ent = state.count_entities();
        let epoch_val = self.topology.as_ref().map(|t| t.epoch).unwrap_or(1);

        let body = format!(
            r#"{{"http_status":200,"status":"ok","ack":"replicated","epoch":{},"batch_id":"{}","tick":{},"head_tick":{},"replicated_tick":{},"replication_lag":{},"state_root":"{}","head_root":"{}","replicated_root":"{}","replicas":{},"sd_ack":true,"sd_acked_nodes":{},"sd_confirmed_tick":{},"sd_persistence":"lmdb_disk","active_entity_count":{},"queued":{},"batch_inputs":{},"compacted_total":{},"out_of_order_dropped":{},"audit_ring_size":{},"point":{}{}}}"#,
            epoch_val,
            json_safe(batch_id),
            state.tick,
            state.head_tick,
            state.tick,
            lag,
            state.state_root,
            state.head_root,
            state.state_root,
            acked_by_json,
            acked_by_json,
            p_tick_to,
            n_ent,
            state.queue.len(),
            p_inputs,
            state.compacted_count,
            state.out_of_order_dropped,
            state.audit_ring.len(),
            state.player_red_json(),
            p_suffix
        );

        self.remember_committed(batch_id.to_string(), body.clone());
        self.pending.remove(batch_id);

        // If queue has accumulated items, start next batch
        let next_relay = if !state.queue.is_empty() {
            let (_, _, relay) = self.start_batch(state, status_suffix);
            relay
        } else {
            None
        };

        (200, body, next_relay)
    }

    pub fn pending_body(
        p: &PendingBatch,
        state: &mut OptimisticState,
        status_suffix: &str,
        status: &str,
    ) -> String {
        state.recompute_head(Some(p.tick_to), Some(&p.root));
        let lag = (state.head_tick - state.tick).max(0);
        format!(
            r#"{{"status":"{}","ack":"accepted","batch_id":"{}","tick":{},"head_tick":{},"confirmed_tick":{},"lag_ticks":{},"tick_from":{},"tick_to":{},"state_root":"{}","head_root":"{}","target_root":"{}","queued":{},"compacted_total":{},"active_entity_count":{},"point":{},"retryable":true{}}}"#,
            status,
            json_safe(&p.batch_id),
            state.tick,
            state.head_tick,
            state.tick,
            lag,
            p.tick_from,
            p.tick_to,
            state.state_root,
            state.head_root,
            p.root,
            state.queue.len(),
            state.compacted_count,
            state.count_entities(),
            state.player_red_json(),
            status_suffix
        )
    }

    pub fn pump_repair(&mut self, state: &OptimisticState) -> Option<Vec<(String, String)>> {
        let topo = match &self.topology {
            Some(t) => t,
            None => return None,
        };
        if self.repair.is_empty() {
            return None;
        }

        let mut relays = Vec::new();
        let clock = self.request_clock;
        let epoch = topo.epoch;

        self.repair.retain_mut(|rec| {
            let mut missing_targets = Vec::new();
            for t in &topo.sd_targets {
                let key = if !t.role.is_empty() { &t.role } else { &t.process };
                if !rec.acked.contains_key(key) {
                    missing_targets.push(t.clone());
                }
            }
            if missing_targets.is_empty() {
                return false;
            }

            if rec.attempts >= 8 {
                let payload = format!(
                    r#"{{"action":"Sync-State","last_tick":{},"state_root":"{}","points":{}}}"#,
                    state.tick,
                    state.state_root,
                    state.encode_points()
                );
                for target in missing_targets {
                    let url = format!("{}/~node-process@1.0/{}/push", target.url, target.process);
                    relays.push((url, payload.clone()));
                    rec.acked.insert(target.role.clone(), true);
                }
                return false;
            } else if (clock.saturating_sub(rec.cast_at)) >= 6 {
                rec.cast_at = clock;
                rec.attempts += 1;
                let payload = Self::commit_payload(
                    &rec.batch_id,
                    rec.tick_from,
                    rec.tick_to,
                    &rec.root,
                    &rec.delta,
                    &rec.order,
                    epoch,
                );
                for target in missing_targets {
                    let url = format!("{}/~node-process@1.0/{}/push", target.url, target.process);
                    relays.push((url, payload.clone()));
                }
            }
            true
        });

        if relays.is_empty() {
            None
        } else {
            Some(relays)
        }
    }
}
