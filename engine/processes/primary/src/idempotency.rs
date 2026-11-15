//! Bounded idempotency table for deduplicating confirmed and pending inputs.
use crate::num::floor_i64;
use crate::prelude::*;

pub const MAX_SEEN_INPUTS: usize = 4096;

#[derive(Clone, Debug, PartialEq)]
pub struct InputDisposition {
    pub seq: u64,
    pub batch_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct IdempotencyTable {
    map: BTreeMap<String, InputDisposition>,
    order: VecDeque<String>,
    max_entries: usize,
}

impl Default for IdempotencyTable {
    fn default() -> Self {
        Self::new(MAX_SEEN_INPUTS)
    }
}

impl IdempotencyTable {
    pub fn new(max_entries: usize) -> Self {
        Self {
            map: BTreeMap::new(),
            order: VecDeque::new(),
            max_entries,
        }
    }

    /// Derives the canonical idempotency key if none was provided explicitly:
    /// `id|floor(x)|floor(y)|ts|owner`
    pub fn derive_key(
        explicit_key: Option<&str>,
        id: &str,
        x: f64,
        y: f64,
        ts: Option<i64>,
        owner: &str,
    ) -> Option<String> {
        if let Some(k) = explicit_key {
            if !k.is_empty() {
                return Some(k.to_string());
            }
        }
        ts.map(|t| format!("{}|{}|{}|{}|{}", id, floor_i64(x), floor_i64(y), t, owner))
    }

    pub fn get(&self, key: &str) -> Option<&InputDisposition> {
        self.map.get(key)
    }

    pub fn insert(&mut self, key: String, disposition: InputDisposition) {
        if self.map.contains_key(&key) {
            return;
        }
        self.map.insert(key.clone(), disposition);
        self.order.push_back(key);
        while self.order.len() > self.max_entries {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            }
        }
    }

    pub fn bind_batch_id(&mut self, key: &str, batch_id: &str) {
        if let Some(disp) = self.map.get_mut(key) {
            disp.batch_id = Some(batch_id.to_string());
        }
    }

    pub fn remove(&mut self, key: &str) {
        self.map.remove(key);
    }
}
