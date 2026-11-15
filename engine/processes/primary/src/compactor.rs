//! Delta compaction and queue ordering for canonical WASM Primary batching.
use crate::entity::pack_entity;
use crate::merkle::sha256_hex;
use crate::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub struct QueueEntry {
    pub seq: u64,
    pub id: String,
    pub x: f64,
    pub y: f64,
    pub target_tick: Option<i64>,
    pub owner: String,
    pub ts: Option<i64>,
    pub key: Option<String>,
}

/// Orders incoming inputs deterministically by Primary arrival sequence order (`seq`)
/// so untrusted client `target_tick` or clock skew cannot reorder or drop newer writes.
pub fn sort_queue_canonical(queue: &[QueueEntry]) -> Vec<QueueEntry> {
    let mut sorted = queue.to_vec();
    sorted.sort_by(|a, b| a.seq.cmp(&b.seq));
    sorted
}

pub struct DeltaBuildResult {
    pub delta: BTreeMap<String, String>, // id -> packed
    pub order: Vec<String>,
    pub out_of_order_dropped: usize,
    pub temp_last_tick: BTreeMap<String, i64>,
}

/// Builds the delta map from the open queue using authoritative sequencer `tick_to`.
pub fn build_delta(
    queue: &[QueueEntry],
    tick_to: i64,
    _entity_last_tick: &BTreeMap<String, i64>,
    _points: &BTreeMap<String, String>,
) -> DeltaBuildResult {
    let sorted = sort_queue_canonical(queue);
    let mut delta = BTreeMap::new();
    let mut order = Vec::new();
    let mut temp_last_tick = BTreeMap::new();

    for item in &sorted {
        let id = &item.id;
        temp_last_tick.insert(id.clone(), tick_to);
        if !delta.contains_key(id) {
            order.push(id.clone());
        }
        delta.insert(
            id.clone(),
            pack_entity(item.x, item.y, tick_to, &item.owner),
        );
    }

    DeltaBuildResult {
        delta,
        order,
        out_of_order_dropped: 0,
        temp_last_tick,
    }
}

pub fn make_open_batch_id(
    tick: i64,
    seq: u64,
    request_clock: u64,
    id: &str,
    x: f64,
    y: f64,
    ts: i64,
) -> String {
    let raw = format!(
        "open|{}|{}|{}|{}|{:.6}|{:.6}|{}",
        tick, seq, request_clock, id, x, y, ts
    );
    let digest = sha256_hex(raw.as_bytes());
    let sub = if digest.len() >= 18 {
        &digest[2..18]
    } else {
        "0000000000000000"
    };
    format!("b{}-{}-{}", tick, seq, sub)
}

pub fn make_batch_id(seq_from: u64, seq_to: u64, tick_from: i64, digest: &str) -> String {
    let sub = if digest.len() >= 18 {
        &digest[2..18]
    } else {
        "0000000000000000"
    };
    format!("b{}-{}-{}-{}", tick_from, seq_from, seq_to, sub)
}

/// Enqueue or coalesce an incoming write into the open queue.
pub fn enqueue_or_coalesce(
    queue: &mut Vec<QueueEntry>,
    entry: QueueEntry,
    compacted_count: &mut usize,
) {
    for i in (0..queue.len()).rev() {
        if queue[i].id == entry.id {
            // Coalesce in-place with latest sequencer-accepted arrival
            queue[i] = entry;
            *compacted_count += 1;
            return;
        }
    }
    queue.push(entry);
}
