//! Buffer for out-of-order batches (`tick_from > LastTick`).

use crate::prelude::*;

pub const MAX_PENDING_BATCHES: usize = 64;

#[derive(Clone, Debug)]
pub struct PendingBatch {
    pub tick_from: u64,
    pub tick_to: u64,
    pub claimed_root: String,
    pub delta: BTreeMap<String, String>, // id -> packed
    pub order: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct PendingBuffer {
    pub batches: BTreeMap<String, PendingBatch>, // batch_id -> batch
    pub max_batches: usize,
}

impl PendingBuffer {
    pub fn new(max_batches: usize) -> Self {
        Self {
            batches: BTreeMap::new(),
            max_batches,
        }
    }

    pub fn insert(
        &mut self,
        batch_id: &str,
        tick_from: u64,
        tick_to: u64,
        claimed_root: &str,
        delta: BTreeMap<String, String>,
        order: Vec<String>,
    ) -> bool {
        if self.batches.len() >= self.max_batches && !self.batches.contains_key(batch_id) {
            return false;
        }
        self.batches.insert(
            batch_id.to_owned(),
            PendingBatch {
                tick_from,
                tick_to,
                claimed_root: claimed_root.to_owned(),
                delta,
                order,
            },
        );
        true
    }

    pub fn len(&self) -> usize {
        self.batches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn clear(&mut self) {
        self.batches.clear();
    }
}
