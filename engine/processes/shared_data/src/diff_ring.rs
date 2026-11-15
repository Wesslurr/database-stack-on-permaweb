//! Bounded 512-slot `DiffRing` for recent diffs and L1 pruning.

use crate::prelude::*;

pub const MAX_DIFF_SLOTS: usize = 512;

#[derive(Clone, Debug, PartialEq)]
pub struct DiffEntry {
    pub seq: u64,
    pub tick: u64,
    pub batch_id: String,
    pub inputs: usize,
}

#[derive(Clone, Debug)]
pub struct DiffRing {
    pub ring: VecDeque<DiffEntry>,
    pub diff_seq: u64,
    pub last_committed_tick: u64,
    pub last_pruned_tick: u64,
    pub pruned_diff_count: usize,
    pub last_l1_tx_id: String,
    pub max_slots: usize,
}

impl Default for DiffRing {
    fn default() -> Self {
        Self::new(MAX_DIFF_SLOTS)
    }
}

impl DiffRing {
    pub fn new(max_slots: usize) -> Self {
        Self {
            ring: VecDeque::with_capacity(max_slots),
            diff_seq: 0,
            last_committed_tick: 0,
            last_pruned_tick: 0,
            pruned_diff_count: 0,
            last_l1_tx_id: String::new(),
            max_slots,
        }
    }

    /// Appends a newly committed batch diff.
    pub fn push(&mut self, tick: u64, batch_id: &str, inputs: usize) -> u64 {
        self.diff_seq += 1;
        self.ring.push_back(DiffEntry {
            seq: self.diff_seq,
            tick,
            batch_id: batch_id.to_owned(),
            inputs,
        });
        while self.ring.len() > self.max_slots {
            self.ring.pop_front();
        }
        self.diff_seq
    }

    /// Prunes diffs up to `committed_tick` (confirmed settled on Arweave L1).
    pub fn prune(&mut self, committed_tick: u64, l1_tx_id: &str) -> usize {
        if committed_tick > self.last_committed_tick {
            self.last_committed_tick = committed_tick;
        }
        if !l1_tx_id.is_empty() {
            self.last_l1_tx_id = l1_tx_id.to_owned();
        }
        if committed_tick > self.last_pruned_tick {
            self.last_pruned_tick = committed_tick;
        }

        let mut removed = 0;
        while let Some(front) = self.ring.front() {
            if front.tick <= committed_tick {
                self.ring.pop_front();
                removed += 1;
            } else {
                break;
            }
        }
        self.pruned_diff_count += removed;
        removed
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
}
