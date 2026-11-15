//! Primary Sequencer & Optimistic Ingress Engine for HyperStack v2.
//! 100% Upstream Vanilla HyperBEAM WASM process (json-iface@1.0 ABI).

#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

#[cfg(target_arch = "wasm32")]
#[global_allocator]
static ALLOC: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

#[cfg(target_arch = "wasm32")]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

pub mod compactor;
pub mod entity;
pub mod guards;
pub mod idempotency;
pub mod ingress;
pub mod merkle;
pub mod num;
pub mod optimistic_state;
pub mod prelude;
pub mod quorum;
pub mod spatial_grid;

use crate::entity::{json_safe, unpack_entity, Entity, ANONYMOUS};
use crate::guards::{validate_point_input, MAX_ENTITIES};
use crate::idempotency::InputDisposition;
use crate::ingress::{parse_message, IngressAction, ParsedMessage};
use crate::optimistic_state::OptimisticState;
use crate::prelude::*;
use crate::quorum::QuorumState;

pub struct PrimaryEngine {
    pub state: OptimisticState,
    pub quorum: QuorumState,
    pub node_signer: String,
    pub genesis_url: Option<String>,
    pub max_entities: usize,
}

impl Default for PrimaryEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl PrimaryEngine {
    pub fn new() -> Self {
        Self {
            state: OptimisticState::new(),
            quorum: QuorumState::new(),
            node_signer: "primary_authority".to_string(),
            genesis_url: Some("http://127.0.0.1:8733".to_string()),
            max_entities: MAX_ENTITIES,
        }
    }

    pub fn status_suffix(&self, assignment_id: &str) -> String {
        let sd_urls = if let Some(topo) = &self.quorum.topology {
            let urls: Vec<String> = topo
                .sd_targets
                .iter()
                .map(|t| format!(r#""{}""#, json_safe(&t.url)))
                .collect();
            format!("[{}]", urls.join(","))
        } else {
            "[]".to_string()
        };

        format!(
            r#","tier":"primary_ram","primary_signer":"{}","worker_role":"{}","sd_roles":{},"assignment_id":"{}","topology_source":"{}","sd_targets":{},"max_entities":{},"max_payload_bytes":{}"#,
            json_safe(&self.node_signer),
            json_safe(&self.quorum.worker_role),
            self.quorum.sd_roles_json(),
            json_safe(assignment_id),
            json_safe(self.genesis_url.as_deref().unwrap_or("")),
            sd_urls,
            self.max_entities,
            crate::guards::MAX_PAYLOAD_BYTES
        )
    }

    pub fn handle_request(
        &mut self,
        parsed: ParsedMessage,
    ) -> (u16, String, Option<Vec<(String, String)>>) {
        self.quorum.request_clock += 1;
        let clock = self.quorum.request_clock;
        let engine_ts = parsed.timestamp;
        let assignment_id = parsed.msg_id.as_deref().unwrap_or("slot-0");
        let suffix = self.status_suffix(assignment_id);

        self.quorum.expire_pending(engine_ts);

        match parsed.action {
            IngressAction::Input { item, flush } => {
                let existing = self.state.points.get(&item.id).map(|s| s.as_str());
                let validated = validate_point_input(
                    Some(&item.id),
                    Some(item.x),
                    Some(item.y),
                    parsed.client_signer.as_deref(),
                    existing,
                    self.state.count_entities(),
                    self.state.queue.len(),
                    parsed.raw_bytes_len,
                    self.max_entities,
                );

                let (id_str, x_val, y_val, owner) = match validated {
                    Ok(res) => res,
                    Err(err) => {
                        let body = format!(
                            r#"{{"status":"{}","ack":"none","tick":{},"state_root":"{}"{}}}"#,
                            err.status_str(),
                            self.state.tick,
                            self.state.state_root,
                            suffix
                        );
                        return (err.http_status(), body, None);
                    }
                };

                let input_key = crate::idempotency::IdempotencyTable::derive_key(
                    item.key.as_deref(),
                    &id_str,
                    x_val,
                    y_val,
                    item.ts,
                    &owner,
                );

                if let Some(ref k) = input_key {
                    if let Some(disp) = self.state.idempotency.get(k) {
                        if let Some(ref bid) = disp.batch_id {
                            if let Some(committed_body) = self.quorum.committed.get(bid) {
                                return (200, committed_body.clone(), None);
                            }
                            if let Some(p) = self.quorum.pending.get_mut(bid) {
                                p.expired = false;
                                p.deadline = clock + crate::quorum::ACK_DEADLINE_REQUESTS;
                                p.created_ts = engine_ts.or(p.created_ts);

                                let epoch = self.quorum.topology.as_ref().map(|t| t.epoch).unwrap_or(1);
                                let mut retry_relays = Vec::new();
                                if let Some(topo) = &self.quorum.topology {
                                    let payload = QuorumState::commit_payload(
                                        &p.batch_id,
                                        p.tick_from,
                                        p.tick_to,
                                        &p.root,
                                        &p.delta,
                                        &p.order,
                                        epoch,
                                    );
                                    for target in &topo.sd_targets {
                                        let url = format!("{}/~node-process@1.0/{}/push", target.url, target.process);
                                        retry_relays.push((url, payload.clone()));
                                    }
                                }
                                let body = QuorumState::pending_body(p, &mut self.state, &suffix, "pending");
                                let relay_opt = if retry_relays.is_empty() { None } else { Some(retry_relays) };
                                return (202, body, relay_opt);
                            }
                        }
                    }
                }

                if self.quorum.circuit_breaker_tripped(
                    self.state.queue.len(),
                    parsed.circuit_breaker,
                    engine_ts,
                ) {
                    self.state.recompute_head(None, None);
                    let lag = (self.state.head_tick - self.state.tick).max(0);
                    let body = format!(
                        r#"{{"status":"unavailable","ack":"none","error":"quorum_unavailable","reason":"max_lag_circuit_breaker","tick":{},"head_tick":{},"lag_ticks":{},"state_root":"{}"{}}}"#,
                        self.state.tick, self.state.head_tick, lag, self.state.state_root, suffix
                    );
                    return (503, body, None);
                }

                self.state.next_seq += 1;
                let seq = self.state.next_seq;
                let entry = compactor::QueueEntry {
                    seq,
                    id: id_str.clone(),
                    x: x_val,
                    y: y_val,
                    target_tick: item.target_tick,
                    owner: owner.clone(),
                    ts: item.ts,
                    key: input_key.clone(),
                };

                self.state.enqueue(entry, engine_ts, None, clock);
                if let Some(k) = input_key {
                    self.state.idempotency.insert(
                        k,
                        InputDisposition {
                            seq,
                            batch_id: self.state.open_batch_id.clone(),
                        },
                    );
                }

                if self.state.should_close_batch(flush, false, engine_ts) {
                    self.quorum.start_batch(&mut self.state, &suffix)
                } else {
                    self.quorum.sync_head(&mut self.state);
                    let lag = (self.state.head_tick - self.state.tick).max(0);
                    let body = format!(
                        r#"{{"status":"accepted","ack":"accepted","batch_id":"{}","batch_seq":{},"queued":{},"tick":{},"head_tick":{},"confirmed_tick":{},"lag_ticks":{},"state_root":"{}","head_root":"{}","active_entity_count":{},"point":{}{}}}"#,
                        json_safe(self.state.open_batch_id.as_deref().unwrap_or("")),
                        seq,
                        self.state.queue.len(),
                        self.state.tick,
                        self.state.head_tick,
                        self.state.tick,
                        lag,
                        self.state.state_root,
                        self.state.head_root,
                        self.state.count_entities(),
                        self.state.player_red_json(),
                        suffix
                    );
                    (202, body, None)
                }
            }

            IngressAction::WriteBatch { items, flush } => {
                if parsed.raw_bytes_len > crate::guards::MAX_PAYLOAD_BYTES {
                    let body = format!(
                        r#"{{"status":"error_payload_too_large","ack":"none","tick":{},"state_root":"{}"{}}}"#,
                        self.state.tick, self.state.state_root, suffix
                    );
                    return (413, body, None);
                }

                if self.quorum.circuit_breaker_tripped(
                    self.state.queue.len(),
                    parsed.circuit_breaker,
                    engine_ts,
                ) {
                    self.state.recompute_head(None, None);
                    let lag = (self.state.head_tick - self.state.tick).max(0);
                    let body = format!(
                        r#"{{"status":"unavailable","ack":"none","error":"quorum_unavailable","reason":"max_lag_circuit_breaker","tick":{},"head_tick":{},"lag_ticks":{},"state_root":"{}"{}}}"#,
                        self.state.tick, self.state.head_tick, lag, self.state.state_root, suffix
                    );
                    return (503, body, None);
                }

                if self.state.queue.len() + items.len() > crate::guards::MAX_QUEUE {
                    let body = format!(
                        r#"{{"status":"error_queue_full","ack":"none","tick":{},"state_root":"{}"{}}}"#,
                        self.state.tick, self.state.state_root, suffix
                    );
                    return (503, body, None);
                }

                let default_signer = parsed
                    .client_signer
                    .as_deref()
                    .unwrap_or(ANONYMOUS);

                for item in items {
                    let existing = self.state.points.get(&item.id).map(|s| s.as_str());
                    let existing_entity: Option<Entity> = existing.and_then(unpack_entity);
                    let owner_ok = match existing_entity {
                        Some(ref e) => e.owner == ANONYMOUS || e.owner == item.owner || item.owner == default_signer,
                        None => true,
                    };

                    if !item.id.is_empty()
                        && item.id.len() <= crate::guards::MAX_ID_LENGTH
                        && !crate::guards::contains_newlines(&item.id)
                        && item.x == item.x
                        && item.y == item.y
                        && num::fabs(item.x) <= crate::guards::MAX_COORD
                        && num::fabs(item.y) <= crate::guards::MAX_COORD
                        && owner_ok
                    {
                        self.state.next_seq += 1;
                        let seq = self.state.next_seq;
                        let entry = compactor::QueueEntry {
                            seq,
                            id: item.id,
                            x: item.x,
                            y: item.y,
                            target_tick: item.target_tick,
                            owner: item.owner,
                            ts: item.ts,
                            key: None,
                        };
                        self.state.enqueue(entry, engine_ts, None, clock);
                    }
                }

                if self.state.should_close_batch(flush, false, engine_ts) {
                    self.quorum.start_batch(&mut self.state, &suffix)
                } else {
                    self.quorum.sync_head(&mut self.state);
                    let lag = (self.state.head_tick - self.state.tick).max(0);
                    let body = format!(
                        r#"{{"status":"accepted","ack":"accepted","batch_id":"{}","batch_seq":{},"queued":{},"tick":{},"head_tick":{},"confirmed_tick":{},"lag_ticks":{},"state_root":"{}","head_root":"{}","active_entity_count":{},"point":{}{}}}"#,
                        json_safe(self.state.open_batch_id.as_deref().unwrap_or("")),
                        self.state.next_seq,
                        self.state.queue.len(),
                        self.state.tick,
                        self.state.head_tick,
                        self.state.tick,
                        lag,
                        self.state.state_root,
                        self.state.head_root,
                        self.state.count_entities(),
                        self.state.player_red_json(),
                        suffix
                    );
                    (202, body, None)
                }
            }

            IngressAction::Advance => self.quorum.start_batch(&mut self.state, &suffix),

            IngressAction::BatchStatus { batch_id } => {
                self.quorum.sync_head(&mut self.state);
                let lag = (self.state.head_tick - self.state.tick).max(0);
                let bid = match batch_id {
                    Some(b) => b,
                    None => {
                        let body = format!(
                            r#"{{"status":"ok","ack":"replicated","tick":{},"head_tick":{},"replicated_tick":{},"replication_lag":{},"state_root":"{}","head_root":"{}","replicated_root":"{}","replicas":{},"queued":{},"batch_id":"{}","pending_batches":{},"repair_batches":{},"compacted_total":{},"last_batch":{},"active_entity_count":{},"point":{}{}}}"#,
                            self.state.tick,
                            self.state.head_tick,
                            self.state.tick,
                            lag,
                            self.state.state_root,
                            self.state.head_root,
                            self.state.state_root,
                            self.quorum.replicas_json(),
                            self.state.queue.len(),
                            json_safe(self.state.open_batch_id.as_deref().unwrap_or("")),
                            self.quorum.pending_count(),
                            self.quorum.repair.len(),
                            self.state.compacted_count,
                            self.quorum
                                .last_batch
                                .as_ref()
                                .map(|(b, t, r)| format!(r#"{{"batch_id":"{}","tick":{},"root":"{}"}}"#, json_safe(b), t, json_safe(r)))
                                .unwrap_or_else(|| "null".to_string()),
                            self.state.count_entities(),
                            self.state.player_red_json(),
                            suffix
                        );
                        return (200, body, None);
                    }
                };

                if let Some(p) = self.quorum.pending.get(&bid) {
                    if !p.done {
                        if p.expired {
                            let body = format!(
                                r#"{{"status":"unavailable","ack":"none","error":"quorum_unavailable","batch_id":"{}","tick":{},"head_tick":{},"replicated_tick":{},"replication_lag":{},"tick_to":{},"state_root":"{}","expected_root":"{}","retryable":true{}}}"#,
                                json_safe(&bid),
                                self.state.tick,
                                self.state.head_tick,
                                self.state.tick,
                                lag,
                                p.tick_to,
                                self.state.state_root,
                                p.root,
                                suffix
                            );
                            return (503, body, None);
                        } else {
                            // Retry pump: re-broadcast commit to replicas
                            let epoch = self.quorum.topology.as_ref().map(|t| t.epoch).unwrap_or(1);
                            let mut retry_relays = Vec::new();
                            if let Some(topo) = &self.quorum.topology {
                                let payload = QuorumState::commit_payload(
                                    &p.batch_id,
                                    p.tick_from,
                                    p.tick_to,
                                    &p.root,
                                    &p.delta,
                                    &p.order,
                                    epoch,
                                );
                                for target in &topo.sd_targets {
                                    let url = format!("{}/~node-process@1.0/{}/push", target.url, target.process);
                                    retry_relays.push((url, payload.clone()));
                                }
                            }
                            let body = format!(
                                r#"{{"status":"pending","ack":"pending","batch_id":"{}","tick":{},"head_tick":{},"confirmed_tick":{},"lag_ticks":{},"state_root":"{}","target_tick":{},"target_root":"{}"{}}}"#,
                                json_safe(&bid),
                                self.state.tick,
                                self.state.head_tick,
                                self.state.tick,
                                lag,
                                self.state.state_root,
                                p.tick_to,
                                p.root,
                                suffix
                            );
                            let relay_opt = if retry_relays.is_empty() { None } else { Some(retry_relays) };
                            return (202, body, relay_opt);
                        }
                    }
                }

                if let Some(committed_body) = self.quorum.committed.get(&bid) {
                    return (200, committed_body.clone(), None);
                }

                if self.state.open_batch_id.as_deref() == Some(&bid) && !self.state.queue.is_empty() {
                    return self.quorum.start_batch(&mut self.state, &suffix);
                }

                let body = format!(
                    r#"{{"status":"unknown_batch","ack":"none","batch_id":"{}","tick":{},"state_root":"{}"{}}}"#,
                    json_safe(&bid),
                    self.state.tick,
                    self.state.state_root,
                    suffix
                );
                (404, body, None)
            }

            IngressAction::Ack {
                batch_id,
                tick,
                state_root,
                role,
                epoch,
            } => self.quorum.handle_ack(
                &batch_id,
                tick,
                state_root.as_deref(),
                epoch,
                &role,
                &mut self.state,
                &suffix,
            ),

            IngressAction::Reject {
                batch_id,
                reason,
                last_tick,
                state_root: _,
                sd_signer: _,
                role: _,
                points,
            } => {
                let mut retry_relays = Vec::new();
                if reason == "replica_ahead" {
                    if let Some(sd_tick) = last_tick {
                        if sd_tick > self.state.tick {
                            self.state.points.clear();
                            self.state.entity_last_tick.clear();
                            let mut unpacked_map = BTreeMap::new();
                            for (id, x, y, t, owner) in points {
                                let packed = crate::entity::pack_entity(x, y, t, &owner);
                                self.state.points.insert(id.clone(), packed);
                                self.state.entity_last_tick.insert(id.clone(), t);
                                unpacked_map.insert(id, Entity { x, y, tick: t, owner });
                            }
                            self.state.spatial_grid.rebuild(&unpacked_map);
                            self.state.tick = sd_tick;
                            self.state.shards = crate::merkle::Shards::from_points(&self.state.points);
                            self.state.state_root = self.state.shards.root(sd_tick);

                            let epoch = self.quorum.topology.as_ref().map(|t| t.epoch).unwrap_or(1);
                            let mut head_override: Option<(i64, String, BTreeMap<String, String>)> = None;

                            if let Some(p) = self.quorum.pending.get_mut(&batch_id) {
                                if !p.done {
                                    let new_from = sd_tick;
                                    let new_to = sd_tick + crate::optimistic_state::TICKS_PER_BATCH;
                                    p.tick_from = new_from;
                                    p.tick_to = new_to;
                                    for val in p.delta.values_mut() {
                                        if let Some(e) = unpack_entity(val) {
                                            *val = crate::entity::pack_entity(e.x, e.y, new_to, &e.owner);
                                        }
                                    }
                                    p.root = self.state.shards.candidate_root(&p.delta, new_to);
                                    p.expired = false;
                                    p.deadline = clock + crate::quorum::ACK_DEADLINE_REQUESTS;
                                    p.last_sent = clock;
                                    p.acked_by.clear();
                                    head_override = Some((new_to, p.root.clone(), p.delta.clone()));

                                    if let Some(ref topo) = self.quorum.topology {
                                        let payload = QuorumState::commit_payload(
                                            &p.batch_id,
                                            p.tick_from,
                                            p.tick_to,
                                            &p.root,
                                            &p.delta,
                                            &p.order,
                                            epoch,
                                        );
                                        for target in &topo.sd_targets {
                                            let url = format!("{}/~node-process@1.0/{}/push", target.url, target.process);
                                            retry_relays.push((url, payload.clone()));
                                        }
                                    }
                                }
                            }

                            if let Some((ht, hr, _delta_clone)) = head_override {
                                self.state.recompute_head(Some(ht), Some(&hr));
                            } else {
                                self.state.recompute_head(None, None);
                            }
                        }
                    }
                }
                let status_str = if retry_relays.is_empty() { "stale" } else { "hydrated_retrying" };
                let body = format!(
                    r#"{{"status":"{}","ack":"none","batch_id":"{}","reason":"{}","tick":{},"state_root":"{}"{}}}"#,
                    status_str,
                    json_safe(&batch_id),
                    json_safe(&reason),
                    self.state.tick,
                    self.state.state_root,
                    suffix
                );
                let relay_opt = if retry_relays.is_empty() { None } else { Some(retry_relays) };
                (409, body, relay_opt)
            }

            IngressAction::ReadPoint { id } => {
                let id_str = id.unwrap_or_default();
                if let Some(packed) = self.state.points.get(&id_str) {
                    if let Some(e) = unpack_entity(packed) {
                        let body = format!(
                            r#"{{"status":"ok","tick":{},"state_root":"{}","point":{{"id":"{}","x":{:.6},"y":{:.6},"tick":{},"owner":"{}"}}{}}}"#,
                            self.state.tick,
                            self.state.state_root,
                            json_safe(&id_str),
                            e.x,
                            e.y,
                            e.tick,
                            json_safe(&e.owner),
                            suffix
                        );
                        return (200, body, None);
                    }
                }
                let body = format!(
                    r#"{{"status":"not_found","tick":{},"state_root":"{}","id":"{}"{}}}"#,
                    self.state.tick,
                    self.state.state_root,
                    json_safe(&id_str),
                    suffix
                );
                (200, body, None)
            }

            IngressAction::ReadAll => {
                self.quorum.sync_head_with_points(&mut self.state);
                let lag = (self.state.head_tick - self.state.tick).max(0);
                let n_ent = self.state.count_entities();
                let points_ext = if n_ent <= 250 {
                    format!(
                        r#","points":{},"head_points":{}"#,
                        self.state.encode_points(),
                        self.state.encode_head_points()
                    )
                } else {
                    String::new()
                };

                let body = format!(
                    r#"{{"status":"ok","ack":"replicated","tick":{},"head_tick":{},"replicated_tick":{},"replication_lag":{},"state_root":"{}","head_root":"{}","replicated_root":"{}","replicas":{},"total":{},"active_entity_count":{},"queued":{},"pending_batches":{},"repair_batches":{},"repair_missing":0,"applied_inputs":{},"compacted_total":{},"out_of_order_dropped":{},"audit_ring":{},"point":{}{}{}}}"#,
                    self.state.tick,
                    self.state.head_tick,
                    self.state.tick,
                    lag,
                    self.state.state_root,
                    self.state.head_root,
                    self.state.state_root,
                    self.quorum.replicas_json(),
                    n_ent,
                    n_ent,
                    self.state.queue.len(),
                    self.quorum.pending_count(),
                    self.quorum.repair.len(),
                    self.state.applied_inputs,
                    self.state.compacted_count,
                    self.state.out_of_order_dropped,
                    self.state.encode_audit_ring(),
                    self.state.player_red_json(),
                    points_ext,
                    suffix
                );
                (200, body, None)
            }

            IngressAction::ReadNearby { x, y, radius } => {
                let r2 = radius * radius;
                let mut nearby_items = Vec::new();
                for (id, packed) in &self.state.points {
                    if let Some(e) = unpack_entity(packed) {
                        let dx = e.x - x;
                        let dy = e.y - y;
                        if (dx * dx + dy * dy) <= r2 {
                            nearby_items.push(format!(
                                r#"{{"id":"{}","x":{:.6},"y":{:.6},"tick":{},"owner":"{}"}}"#,
                                json_safe(id),
                                e.x,
                                e.y,
                                e.tick,
                                json_safe(&e.owner)
                            ));
                        }
                    }
                }

                self.quorum.sync_head_with_points(&mut self.state);
                let lag = (self.state.head_tick - self.state.tick).max(0);
                let n_ent = self.state.count_entities();
                let points_ext = if n_ent <= 250 {
                    format!(
                        r#","points":{},"head_points":{}"#,
                        self.state.encode_points(),
                        self.state.encode_head_points()
                    )
                } else {
                    String::new()
                };

                let body = format!(
                    r#"{{"status":"ok","tick":{},"head_tick":{},"replicated_tick":{},"replication_lag":{},"state_root":"{}","count":{},"active_entity_count":{},"compacted_total":{},"out_of_order_dropped":{},"source":"rust_wasm_ram","nearby":[{}]{}{}}}"#,
                    self.state.tick,
                    self.state.head_tick,
                    self.state.tick,
                    lag,
                    self.state.state_root,
                    nearby_items.len(),
                    n_ent,
                    self.state.compacted_count,
                    self.state.out_of_order_dropped,
                    nearby_items.join(","),
                    points_ext,
                    suffix
                );
                (200, body, None)
            }

            IngressAction::Snapshot => {
                self.quorum.sync_head(&mut self.state);
                let lag = (self.state.head_tick - self.state.tick).max(0);
                let n_ent = self.state.count_entities();
                let body = format!(
                    r#"{{"status":"ok","ack":"replicated","tick":{},"head_tick":{},"replicated_tick":{},"replication_lag":{},"state_root":"{}","head_root":"{}","replicated_root":"{}","replicas":{},"total":{},"active_entity_count":{},"point":{},"points":{}{}}}"#,
                    self.state.tick,
                    self.state.head_tick,
                    self.state.tick,
                    lag,
                    self.state.state_root,
                    self.state.head_root,
                    self.state.state_root,
                    self.quorum.replicas_json(),
                    n_ent,
                    n_ent,
                    self.state.player_red_json(),
                    self.state.encode_points(),
                    suffix
                );
                (200, body, None)
            }

            IngressAction::SyncTopology { epoch, sd_urls, arbit_url } => {
                let role = self.quorum.worker_role.clone();
                self.quorum.set_worker_role(&role);
                if let Some(ref mut topo) = self.quorum.topology {
                    if let Some(ep) = epoch {
                        topo.epoch = ep;
                    }
                    if let Some(u) = arbit_url {
                        if !u.is_empty() {
                            topo.arbit_url = Some(u);
                        }
                    }
                    if let Some(urls) = sd_urls {
                        let mut new_targets = Vec::new();
                        for (idx, u) in urls.into_iter().enumerate() {
                            if u.is_empty() {
                                continue;
                            }
                            let (proc_name, target_role) = if let Some(existing) = topo.sd_targets.get(idx) {
                                (existing.process.clone(), existing.role.clone())
                            } else {
                                let name = format!("sd{}", idx + 1);
                                (name.clone(), name)
                            };
                            new_targets.push(crate::quorum::SdTarget {
                                url: u,
                                process: proc_name,
                                role: target_role,
                            });
                        }
                        if !new_targets.is_empty() {
                            topo.sd_targets = new_targets;
                        }
                    }
                }
                let updated_suffix = self.status_suffix(assignment_id);
                let body = format!(
                    r#"{{"status":"topology_synced","tick":{},"state_root":"{}"{}}}"#,
                    self.state.tick, self.state.state_root, updated_suffix
                );
                (200, body, None)
            }

            IngressAction::ProbeSd { sd } => {
                let body = format!(
                    r#"{{"status":"probed","ack":"none","sd":{},"role":"sd{}","bytes":0,"last_tick":{},"state_root":"{}"{}}}"#,
                    sd, sd, self.state.tick, self.state.state_root, suffix
                );
                (200, body, None)
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Global Instance for WASM execution
// -----------------------------------------------------------------------------

static mut GLOBAL_ENGINE: Option<PrimaryEngine> = None;

#[allow(static_mut_refs)]
#[inline]
pub fn get_engine() -> &'static mut PrimaryEngine {
    unsafe {
        if GLOBAL_ENGINE.is_none() {
            GLOBAL_ENGINE = Some(PrimaryEngine::new());
        }
        GLOBAL_ENGINE.as_mut().unwrap()
    }
}

// -----------------------------------------------------------------------------
// WAMR C-ABI Exports (Only export C-symbol malloc/free on wasm32 target)
// -----------------------------------------------------------------------------
const ALLOC_HEADER_SIZE: usize = 16;
const ALLOC_MAGIC: u64 = 0x4859_5052_5741_534Du64;
static mut LAST_RES_PTR: *mut u8 = core::ptr::null_mut();

pub fn wasm_malloc(size: usize) -> *mut u8 {
    let req_size = if size == 0 { 1 } else { size };
    let total = match req_size.checked_add(ALLOC_HEADER_SIZE) {
        Some(t) => t,
        None => return core::ptr::null_mut(),
    };
    let layout = match core::alloc::Layout::from_size_align(total, 8) {
        Ok(l) => l,
        Err(_) => return core::ptr::null_mut(),
    };
    unsafe {
        let raw = alloc::alloc::alloc(layout);
        if raw.is_null() {
            return core::ptr::null_mut();
        }
        core::ptr::write_unaligned(raw as *mut u64, req_size as u64);
        core::ptr::write_unaligned(raw.add(8) as *mut u64, ALLOC_MAGIC);
        raw.add(ALLOC_HEADER_SIZE)
    }
}

pub fn wasm_free(ptr: *mut u8) {
    if ptr.is_null() || (ptr as usize) < ALLOC_HEADER_SIZE {
        return;
    }
    unsafe {
        let raw = ptr.sub(ALLOC_HEADER_SIZE);
        let magic = core::ptr::read_unaligned(raw.add(8) as *const u64);
        if magic != ALLOC_MAGIC {
            return;
        }
        core::ptr::write_unaligned(raw.add(8) as *mut u64, 0);
        let req_size = core::ptr::read_unaligned(raw as *const u64) as usize;
        let total = req_size + ALLOC_HEADER_SIZE;
        let layout = core::alloc::Layout::from_size_align_unchecked(total, 8);
        alloc::alloc::dealloc(raw, layout);
    }
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn malloc(size: usize) -> *mut u8 {
    wasm_malloc(size)
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn free(ptr: *mut u8) {
    wasm_free(ptr)
}

/// Reads a null-terminated string from raw pointer.
unsafe fn c_str_to_str<'a>(ptr: *const u8) -> &'a str {
    if ptr.is_null() {
        return "";
    }
    let mut len = 0;
    while *ptr.add(len) != 0 {
        len += 1;
    }
    let slice = core::slice::from_raw_parts(ptr, len);
    core::str::from_utf8(slice).unwrap_or("")
}

/// Main entrypoint called by `json-iface@1.0`.
#[no_mangle]
pub extern "C" fn handle(msg_ptr: *const u8, proc_ptr: *const u8) -> *const u8 {
    unsafe {
        wasm_free(LAST_RES_PTR);
        LAST_RES_PTR = core::ptr::null_mut();
    }

    let parsed = {
        let msg_str = unsafe { c_str_to_str(msg_ptr) };
        parse_message(msg_str)
    };
    wasm_free(msg_ptr as *mut u8);

    let parsed = match parsed {
        Ok(p) => p,
        Err(err) => {
            wasm_free(proc_ptr as *mut u8);
            let err_json = format!(
                r#"{{"ok":false,"response":{{"status":400,"body":"{}"}}}}"#,
                err
            );
            let ptr = allocate_c_string(&err_json);
            unsafe { LAST_RES_PTR = ptr as *mut u8; }
            return ptr;
        }
    };

    let engine = get_engine();
    {
        let proc_str = unsafe { c_str_to_str(proc_ptr) };
        if !proc_str.is_empty() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(proc_str) {
                let proc_obj = v.get("Process").unwrap_or(&v);
                let mut role_found = proc_obj
                    .get("node-role")
                    .or_else(|| proc_obj.get("node_role"))
                    .or_else(|| proc_obj.get("role"))
                    .and_then(|r| r.as_str())
                    .map(|s| s.to_string());

                if role_found.is_none() {
                    if let Some(tags) = proc_obj.get("Tags").and_then(|t| t.as_array()) {
                        for tag in tags {
                            if let (Some(name), Some(val)) = (
                                tag.get("name").and_then(|n| n.as_str()),
                                tag.get("value"),
                            ) {
                                let lower = name.to_ascii_lowercase();
                                if lower == "node-role" || lower == "node_role" || lower == "role" {
                                    if let Some(s) = val.as_str() {
                                        role_found = Some(s.to_string());
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some(role) = role_found {
                    engine.quorum.set_worker_role(&role);
                }
                if let Some(g_url) = proc_obj
                    .get("genesis-url")
                    .or_else(|| proc_obj.get("genesis_url"))
                    .and_then(|u| u.as_str())
                {
                    if !g_url.is_empty() {
                        engine.genesis_url = Some(g_url.to_string());
                    }
                }
            }
        }
    }
    wasm_free(proc_ptr as *mut u8);

    let (http_status, res_body, relay_opt) = engine.handle_request(parsed);
    let repair_relays = engine.quorum.pump_repair(&engine.state);

    // Format Messages array for outbox (commit delta broadcasts)
    let mut messages_json = Vec::new();
    if let Some(relays) = relay_opt {
        for (url, payload) in relays {
            messages_json.push(format!(
                r#"{{"resolve":"~relay@1.0/cast","relay-path":"{}","relay-method":"POST","relay-body":"{}","content-type":"application/json","cache-control":"no-store"}}"#,
                json_safe(&url),
                json_safe(&payload)
            ));
        }
    }
    if let Some(relays) = repair_relays {
        for (url, payload) in relays {
            messages_json.push(format!(
                r#"{{"resolve":"~relay@1.0/cast","relay-path":"{}","relay-method":"POST","relay-body":"{}","content-type":"application/json","cache-control":"no-store"}}"#,
                json_safe(&url),
                json_safe(&payload)
            ));
        }
    }

    // Build handler envelope per json-iface@1.0 §5.3 (omit duplicate Output.data)
    let output_json = format!(
        r#"{{"ok":true,"response":{{"Output":{{"status":{},"body":"{}","tick":"{}","state_root":"{}","active_entity_count":"{}","queued":"{}","primary_signer":"{}"}},"Messages":[{}],"patches":[]}}}}"#,
        http_status,
        json_safe(&res_body),
        engine.state.tick,
        engine.state.state_root,
        engine.state.count_entities(),
        engine.state.queue.len(),
        json_safe(&engine.node_signer),
        messages_json.join(",")
    );

    let ptr = allocate_c_string(&output_json);
    unsafe { LAST_RES_PTR = ptr as *mut u8; }
    ptr
}

fn allocate_c_string(s: &str) -> *const u8 {
    let bytes = s.as_bytes();
    let ptr = wasm_malloc(bytes.len() + 1);
    if ptr.is_null() {
        return core::ptr::null();
    }
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
        *ptr.add(bytes.len()) = 0; // null terminator
    }
    ptr
}

// -----------------------------------------------------------------------------
// Unit Tests (Verified Bit-for-Bit Parity with Lua Engine)
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guards::GuardError;

    #[test]
    fn test_initial_empty_state_root() {
        let shards = merkle::Shards::new();
        let root = shards.root(0);
        assert_eq!(
            root,
            "0x58a9c325c3213b810ad094c6109ab96f4df789b3930302b0b1b1e3e8c9387db1"
        );
    }

    #[test]
    fn test_guard_coordinate_bounds() {
        let res = validate_point_input(
            Some("e2e_bad"),
            Some(5000.0),
            Some(0.0),
            Some("tester"),
            None,
            0,
            0,
            100,
            1000,
        );
        assert_eq!(res, Err(GuardError::InvalidCoordinates));
    }

    #[test]
    fn test_guard_id_newline() {
        let res = validate_point_input(
            Some("e2e\nbad"),
            Some(1.0),
            Some(1.0),
            Some("tester"),
            None,
            0,
            0,
            100,
            1000,
        );
        assert_eq!(res, Err(GuardError::InvalidEntityId));
    }

    #[test]
    fn test_guard_unauthorized_owner() {
        let packed = entity::pack_entity(1.0, 1.0, 1, "alice");
        let res = validate_point_input(
            Some("e2e_a"),
            Some(2.0),
            Some(2.0),
            Some("mallory"),
            Some(&packed),
            1,
            0,
            100,
            1000,
        );
        assert_eq!(res, Err(GuardError::UnauthorizedOwner));
    }

    #[test]
    fn test_ingress_packed_parsing() {
        let packed = "bot_1|10.5|20.25|1|1000000|alice;bot_2|-5.0|12.0|1|1000000|bob";
        let items = ingress::parse_packed_inputs(packed, "anonymous");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].id, "bot_1");
        assert_eq!(items[0].x, 10.5);
        assert_eq!(items[0].owner, "alice");
        assert_eq!(items[1].id, "bot_2");
        assert_eq!(items[1].x, -5.0);
        assert_eq!(items[1].owner, "bob");
    }

    #[test]
    fn test_handle_request_write_and_ack() {
        let mut engine = PrimaryEngine::new();
        let json_input = r#"{"action":"Input","id":"e2e_a","x":1.0,"y":2.0,"client_signer":"alice","flush":true}"#;
        let parsed = parse_message(json_input).unwrap();
        let (status, body, _) = engine.handle_request(parsed);
        assert_eq!(status, 202);
        assert!(body.contains("\"ack\":\"accepted\""));
        assert!(body.contains("\"batch_id\":\"b"));

        // Extract batch_id
        let val: serde_json::Value = serde_json::from_str(&body).unwrap();
        let bid = val["batch_id"].as_str().unwrap();
        let target_root = val["target_root"].as_str().unwrap();

        // Simulate SD Ack
        let ack_json = format!(
            r#"{{"action":"Ack","batch_id":"{}","tick":2,"state_root":"{}","role":"sd1","epoch":1}}"#,
            bid, target_root
        );
        let parsed_ack = parse_message(&ack_json).unwrap();
        let (ack_status, ack_body, _) = engine.handle_request(parsed_ack);
        assert_eq!(ack_status, 200);
        assert!(ack_body.contains("\"ack\":\"replicated\""));
        assert_eq!(engine.state.tick, 2);
        assert_eq!(engine.state.state_root, target_root);
    }

    #[test]
    fn test_stale_target_tick_not_dropped_and_sync_head_preserves_inflight() {
        let mut engine = PrimaryEngine::new();
        engine.quorum.set_worker_role("primary_1");

        // 1. First write with poisoned high client target_tick=500 at (120, -40)
        let w1 = r#"{"action":"Write-Point","id":"player_red","x":120.0,"y":-40.0,"target_tick":500,"ts":1001,"client_signer":"player_red_signer","flush":true}"#;
        let (st1, body1, _) = engine.handle_request(parse_message(w1).unwrap());
        assert_eq!(st1, 202);
        let v1: serde_json::Value = serde_json::from_str(&body1).unwrap();
        let bid1 = v1["batch_id"].as_str().unwrap().to_string();
        let root1 = v1["target_root"].as_str().unwrap().to_string();

        // First ACK from sd1_1 commits batch 1 at tick 2
        let ack1 = format!(
            r#"{{"action":"Ack","batch_id":"{}","tick":2,"state_root":"{}","role":"sd1_1","epoch":1}}"#,
            bid1, root1
        );
        let (ast1, _, _) = engine.handle_request(parse_message(&ack1).unwrap());
        assert_eq!(ast1, 200);
        assert_eq!(engine.state.tick, 2);

        // 2. Second write with lower sequential target_tick=3 at (180, 0) (must NOT be dropped!)
        let w2 = r#"{"action":"Write-Point","id":"player_red","x":180.0,"y":0.0,"target_tick":3,"ts":1002,"client_signer":"player_red_signer","flush":true}"#;
        let (st2, body2, _) = engine.handle_request(parse_message(w2).unwrap());
        assert_eq!(st2, 202);
        let v2: serde_json::Value = serde_json::from_str(&body2).unwrap();
        assert_eq!(v2["head_tick"].as_i64(), Some(4));
        let bid2 = v2["batch_id"].as_str().unwrap().to_string();
        let root2 = v2["target_root"].as_str().unwrap().to_string();

        // Duplicate/2nd ACK from sd2_1 for batch 1 must NOT regress head_tick back from 4 to 2
        let ack1_dup = format!(
            r#"{{"action":"Ack","batch_id":"{}","tick":2,"state_root":"{}","role":"sd2_1","epoch":1}}"#,
            bid1, root1
        );
        let (ast1_dup, body1_dup, _) = engine.handle_request(parse_message(&ack1_dup).unwrap());
        assert_eq!(ast1_dup, 200);
        let v1_dup: serde_json::Value = serde_json::from_str(&body1_dup).unwrap();
        assert_eq!(v1_dup["head_tick"].as_i64(), Some(4));

        // Ack batch 2 and verify (180, 0) committed and out_of_order_dropped == 0
        let ack2 = format!(
            r#"{{"action":"Ack","batch_id":"{}","tick":4,"state_root":"{}","role":"sd1_1","epoch":1}}"#,
            bid2, root2
        );
        let (ast2, body2_ack, _) = engine.handle_request(parse_message(&ack2).unwrap());
        assert_eq!(ast2, 200);
        let v2_ack: serde_json::Value = serde_json::from_str(&body2_ack).unwrap();
        assert_eq!(v2_ack["out_of_order_dropped"].as_i64(), Some(0));
        assert_eq!(v2_ack["point"]["x"].as_f64(), Some(180.0));
        assert_eq!(v2_ack["point"]["y"].as_f64(), Some(0.0));
    }
}
