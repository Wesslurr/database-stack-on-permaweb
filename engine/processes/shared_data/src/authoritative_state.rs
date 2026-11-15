//! Authoritative State Machine for Shared Data Replicas.
//! "Verify-Don't-Trust" consensus engine for canonical HyperStack WASM replicas.

use crate::diff_ring::{DiffRing, MAX_DIFF_SLOTS};
use crate::entity::{json_safe, unpack_entity};
use crate::merkle::Shards;
use crate::pending_buffer::{PendingBuffer, MAX_PENDING_BATCHES};
use crate::prelude::*;
use crate::spatial_grid::SpatialGrid;

pub const MAX_COMMITTED_HISTORY: usize = 512;
pub const MAX_BATCH_INPUTS: usize = 256;
pub const MAX_ENTITIES: usize = 1000;

#[derive(Clone, Debug, PartialEq)]
pub struct DrainedBatch {
    pub batch_id: String,
    pub tick: u64,
    pub state_root: String,
}

#[derive(Clone, Debug)]
pub struct AuthoritativeState {
    pub replica_store: BTreeMap<String, String>, // id -> packed "x|y|tick|owner"
    pub shards: Shards,
    pub last_tick: u64,
    pub state_root: String,

    pub diff_ring: DiffRing,
    pub pending: PendingBuffer,

    pub committed_batches: BTreeMap<String, (u64, String, String)>, // batch_id -> (tick_to, state_root, cached json response)
    pub committed_order: VecDeque<String>,
    pub max_committed: usize,

    pub spatial_grid: SpatialGrid,

    pub current_epoch: u64,
    pub last_batch_sent_tick: u64,
    pub max_entities: usize,
    pub max_batch_inputs: usize,
}

impl Default for AuthoritativeState {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthoritativeState {
    pub fn new() -> Self {
        let shards = Shards::new();
        let state_root = shards.root(0);
        Self {
            replica_store: BTreeMap::new(),
            shards,
            last_tick: 0,
            state_root,
            diff_ring: DiffRing::new(MAX_DIFF_SLOTS),
            pending: PendingBuffer::new(MAX_PENDING_BATCHES),
            committed_batches: BTreeMap::new(),
            committed_order: VecDeque::with_capacity(MAX_COMMITTED_HISTORY),
            max_committed: MAX_COMMITTED_HISTORY,
            spatial_grid: SpatialGrid::new(),
            current_epoch: 1,
            last_batch_sent_tick: 0,
            max_entities: MAX_ENTITIES,
            max_batch_inputs: MAX_BATCH_INPUTS,
        }
    }

    pub fn remember_batch(&mut self, batch_id: &str, tick: u64, state_root: &str, body: &str) {
        if !self.committed_batches.contains_key(batch_id) {
            self.committed_order.push_back(batch_id.to_owned());
        }
        self.committed_batches.insert(
            batch_id.to_owned(),
            (tick, state_root.to_owned(), body.to_owned()),
        );
        while self.committed_order.len() > self.max_committed {
            if let Some(old) = self.committed_order.pop_front() {
                self.committed_batches.remove(&old);
            }
        }
    }

    pub fn count_replicas(&self) -> usize {
        self.replica_store.len()
    }

    /// Mutates the state with an already-verified delta.
    pub fn apply_delta(
        &mut self,
        batch_id: &str,
        _tick_from: u64,
        tick_to: u64,
        delta: &BTreeMap<String, String>, // id -> packed
        order: &[String],
        sd_signer: &str,
    ) -> String {
        for id in order {
            if let Some(packed) = delta.get(id) {
                self.replica_store.insert(id.clone(), packed.clone());
                if let Some(e) = unpack_entity(packed) {
                    self.spatial_grid.insert_or_update(id, e.x, e.y);
                }
            }
        }
        self.last_tick = tick_to;
        self.state_root = self.shards.commit(delta, tick_to as i64);

        let diff_seq = self.diff_ring.push(tick_to, batch_id, order.len());

        let player_json = self.player_red_json();
        let body = format!(
            "{{\"status\":\"committed\",\"tier\":\"shared_data_replica\",\"batch_id\":\"{}\",\"last_tick\":{},\"tick\":{},\"state_root\":\"{}\",\"count\":{},\"sd_signer\":\"{}\",\"diff_seq\":{},\"persistence\":\"lmdb_disk\",\"point\":{}}}",
            json_safe(batch_id), self.last_tick, self.last_tick, self.state_root, self.count_replicas(), json_safe(sd_signer), diff_seq, player_json
        );
        let sr = self.state_root.clone();
        self.remember_batch(batch_id, self.last_tick, &sr, &body);
        body
    }

    /// Drains any pending contiguous batches whose `tick_from == self.last_tick`.
    pub fn drain_pending(&mut self, sd_signer: &str) -> Vec<DrainedBatch> {
        let mut drained = Vec::new();
        let mut progress = true;
        while progress {
            progress = false;
            let mut match_key = None;
            for (batch_id, pending) in self.pending.batches.iter() {
                if pending.tick_from == self.last_tick {
                    match_key = Some(batch_id.clone());
                    break;
                }
            }
            if let Some(batch_id) = match_key {
                if let Some(pending) = self.pending.batches.remove(&batch_id) {
                    let computed_root = self.shards.candidate_root(&pending.delta, pending.tick_to as i64);
                    if computed_root == pending.claimed_root {
                        self.apply_delta(&batch_id, pending.tick_from, pending.tick_to, &pending.delta, &pending.order, sd_signer);
                        drained.push(DrainedBatch {
                            batch_id,
                            tick: self.last_tick,
                            state_root: self.state_root.clone(),
                        });
                        progress = true;
                    }
                }
            }
        }
        drained
    }

    /// Atomically replaces local state with synced points (from Primary or Arbit).
    pub fn sync_state(&mut self, tick: u64, points: BTreeMap<String, String>) {
        self.replica_store = points;
        self.last_tick = tick;
        self.shards = Shards::from_points(&self.replica_store);
        self.state_root = self.shards.root(tick as i64);
        self.pending.clear();
        self.spatial_grid.clear();
        for (id, packed) in self.replica_store.iter() {
            if let Some(e) = unpack_entity(packed) {
                self.spatial_grid.insert_or_update(id, e.x, e.y);
            }
        }
    }

    pub fn player_red_json(&self) -> String {
        if let Some(packed) = self.replica_store.get("player_red") {
            if let Some(e) = unpack_entity(packed) {
                return format!(
                    "{{\"id\":\"player_red\",\"x\":{:.6},\"y\":{:.6},\"tick\":{},\"owner\":\"{}\"}}",
                    e.x, e.y, e.tick, json_safe(&e.owner)
                );
            }
        }
        "null".to_owned()
    }

    pub fn encode_points(&self) -> String {
        let mut items = Vec::with_capacity(self.replica_store.len());
        for (id, packed) in self.replica_store.iter() {
            if let Some(e) = unpack_entity(packed) {
                items.push(format!(
                    "[\"{}\",{:.6},{:.6},{},\"{}\"]",
                    json_safe(id), e.x, e.y, e.tick, json_safe(&e.owner)
                ));
            }
        }
        format!("[{}]", items.join(","))
    }
}
