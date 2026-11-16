//! Optimistic RAM state & dual-watermark management for the Primary sequencer.
use crate::compactor::{build_delta, enqueue_or_coalesce, make_open_batch_id, QueueEntry};
use crate::entity::{json_safe, unpack_entity};
use crate::idempotency::IdempotencyTable;
use crate::merkle::Shards;
use crate::num::lua_tostring;
use crate::prelude::*;
use crate::spatial_grid::SpatialGrid;

pub const TICKS_PER_BATCH: i64 = 2;
pub const MAX_AUDIT_RING: usize = 50;
pub const BATCH_WINDOW_MS: i64 = 33;
pub const MAX_BATCH_INPUTS: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub struct AuditRecord {
    pub tick: i64,
    pub id: String,
    pub x: f64,
    pub y: f64,
}

#[derive(Clone, Debug)]
pub struct OptimisticState {
    pub points: BTreeMap<String, String>, // id -> packed
    pub tick: i64,
    pub state_root: String,
    pub shards: Shards,

    // Dual-watermark (L0 Optimistic)
    pub head_tick: i64,
    pub head_root: String,
    pub head_points: BTreeMap<String, String>,

    // Ingress queue
    pub queue: Vec<QueueEntry>,
    pub queued_inputs: usize,
    pub open_batch_id: Option<String>,
    pub batch_opened_ts: Option<i64>,
    pub next_seq: u64,

    // Counters & guards
    pub compacted_count: usize,
    pub out_of_order_dropped: usize,
    pub applied_inputs: usize,
    pub entity_last_tick: BTreeMap<String, i64>,
    pub audit_ring: VecDeque<AuditRecord>,

    // Indexes
    pub spatial_grid: SpatialGrid,
    pub idempotency: IdempotencyTable,
}

impl Default for OptimisticState {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimisticState {
    pub fn new() -> Self {
        let shards = Shards::new();
        let state_root = shards.root(0);
        Self {
            points: BTreeMap::new(),
            tick: 0,
            state_root: state_root.clone(),
            shards,

            head_tick: 0,
            head_root: state_root,
            head_points: BTreeMap::new(),

            queue: Vec::new(),
            queued_inputs: 0,
            open_batch_id: None,
            batch_opened_ts: None,
            next_seq: 0,

            compacted_count: 0,
            out_of_order_dropped: 0,
            applied_inputs: 0,
            entity_last_tick: BTreeMap::new(),
            audit_ring: VecDeque::new(),

            spatial_grid: SpatialGrid::new(),
            idempotency: IdempotencyTable::default(),
        }
    }

    pub fn count_entities(&self) -> usize {
        self.points.len()
    }

    pub fn append_audit(&mut self, tick: i64, id: &str, x: f64, y: f64) {
        if self.audit_ring.len() >= MAX_AUDIT_RING {
            self.audit_ring.pop_front();
        }
        self.audit_ring.push_back(AuditRecord {
            tick,
            id: id.to_string(),
            x,
            y,
        });
    }

    pub fn encode_audit_ring(&self) -> String {
        let mut items = Vec::new();
        for r in &self.audit_ring {
            items.push(format!(
                r#"{{"tick":{},"id":"{}","x":{},"y":{}}}"#,
                r.tick,
                json_safe(&r.id),
                lua_tostring(r.x),
                lua_tostring(r.y)
            ));
        }
        format!("[{}]", items.join(","))
    }

    pub fn encode_points_map(points: &BTreeMap<String, String>) -> String {
        let mut items = Vec::new();
        for (id, packed) in points {
            if let Some(e) = unpack_entity(packed) {
                items.push(format!(
                    r#"["{}",{:.6},{:.6},{},"{}"]"#,
                    json_safe(id),
                    e.x,
                    e.y,
                    e.tick,
                    json_safe(&e.owner)
                ));
            }
        }
        format!("[{}]", items.join(","))
    }

    pub fn encode_points(&self) -> String {
        Self::encode_points_map(&self.points)
    }

    pub fn encode_head_points(&self) -> String {
        Self::encode_points_map(&self.head_points)
    }

    pub fn player_red_json(&self) -> String {
        if let Some(packed) = self.points.get("player_red") {
            if let Some(e) = unpack_entity(packed) {
                return format!(
                    r#"{{"id":"player_red","x":{:.6},"y":{:.6},"tick":{},"owner":"{}"}}"#,
                    e.x,
                    e.y,
                    e.tick,
                    json_safe(&e.owner)
                );
            }
        }
        "null".to_string()
    }

    /// Recomputes local optimistic HeadTick and HeadRoot.
    pub fn recompute_head(&mut self, in_flight_tick_to: Option<i64>, in_flight_root: Option<&str>) {
        let base_tick = in_flight_tick_to.unwrap_or(self.tick);
        if !self.queue.is_empty() {
            self.head_tick = base_tick + TICKS_PER_BATCH;
            self.head_root = in_flight_root.unwrap_or(&self.state_root).to_string();
        } else if let Some(t_to) = in_flight_tick_to {
            self.head_tick = t_to;
            self.head_root = in_flight_root.unwrap_or(&self.state_root).to_string();
        } else {
            self.head_tick = self.tick;
            self.head_root = self.state_root.clone();
        }
    }

    /// Syncs HeadPoints for serialization when entity count <= 250.
    pub fn sync_head_points(&mut self, in_flight_delta: Option<&BTreeMap<String, String>>) {
        self.head_points = self.points.clone();
        if let Some(delta) = in_flight_delta {
            for (id, packed) in delta {
                self.head_points.insert(id.clone(), packed.clone());
            }
        }
        if !self.queue.is_empty() {
            let res = build_delta(
                &self.queue,
                self.head_tick,
                &self.entity_last_tick,
                &self.points,
            );
            for (id, packed) in res.delta {
                self.head_points.insert(id, packed);
            }
        }
    }

    /// Enqueues an entry and updates OpenBatchId if queue was empty.
    pub fn enqueue(
        &mut self,
        entry: QueueEntry,
        engine_ts: Option<i64>,
        explicit_batch_id: Option<String>,
        request_clock: u64,
    ) {
        if self.queue.is_empty() {
            self.batch_opened_ts = engine_ts.or(entry.ts);
            if let Some(bid) = explicit_batch_id {
                self.open_batch_id = Some(bid);
            } else {
                self.open_batch_id = Some(make_open_batch_id(
                    self.tick,
                    entry.seq,
                    request_clock,
                    &entry.id,
                    entry.x,
                    entry.y,
                    engine_ts.or(entry.ts).unwrap_or(0),
                ));
            }
        }
        self.queued_inputs += 1;
        enqueue_or_coalesce(&mut self.queue, entry, &mut self.compacted_count);
    }

    pub fn should_close_batch(&self, flush: bool, advance: bool, engine_ts: Option<i64>) -> bool {
        if self.queue.len() >= MAX_BATCH_INPUTS {
            return true;
        }
        if flush || advance {
            return true;
        }
        if let (Some(now), Some(opened)) = (engine_ts, self.batch_opened_ts) {
            if now >= opened && (now - opened) >= BATCH_WINDOW_MS {
                return true;
            }
        }
        false
    }
}
