//! Spatial grid indexing for ultra-fast O(1) `Read-Nearby` queries.

use crate::prelude::*;

pub const CELL_SIZE: f64 = 25.0;           // 25 units per cell
pub const WORLD_OFFSET: f64 = 1000.0; // shifts [-1000, 1000] to [0, 2000]
pub const GRID_DIM: usize = 80;       // 2000 / 25 = 80 cells per axis

#[derive(Clone, Debug, Default)]
pub struct SpatialGrid {
    // Maps cell_index -> Vec<entity_id>
    cells: BTreeMap<usize, Vec<String>>,
    // Reverse map: entity_id -> cell_index
    entity_cells: BTreeMap<String, usize>,
}

impl SpatialGrid {
    pub fn new() -> Self {
        Self {
            cells: BTreeMap::new(),
            entity_cells: BTreeMap::new(),
        }
    }

    #[inline]
    pub fn cell_index(x: f64, y: f64) -> Option<usize> {
        let shifted_x = x + WORLD_OFFSET;
        let shifted_y = y + WORLD_OFFSET;
        if shifted_x < 0.0 || shifted_x > 2000.0 || shifted_y < 0.0 || shifted_y > 2000.0 {
            return None;
        }
        let gx = (shifted_x / CELL_SIZE) as usize;
        let gy = (shifted_y / CELL_SIZE) as usize;
        let gx = if gx >= GRID_DIM { GRID_DIM - 1 } else { gx };
        let gy = if gy >= GRID_DIM { GRID_DIM - 1 } else { gy };
        Some(gy * GRID_DIM + gx)
    }

    pub fn insert_or_update(&mut self, id: &str, x: f64, y: f64) {
        if let Some(new_idx) = Self::cell_index(x, y) {
            if let Some(&old_idx) = self.entity_cells.get(id) {
                if old_idx == new_idx {
                    return;
                }
                // Remove from old cell
                if let Some(list) = self.cells.get_mut(&old_idx) {
                    list.retain(|eid| eid != id);
                }
            }
            self.entity_cells.insert(id.to_owned(), new_idx);
            self.cells.entry(new_idx).or_default().push(id.to_owned());
        }
    }

    pub fn remove(&mut self, id: &str) {
        if let Some(old_idx) = self.entity_cells.remove(id) {
            if let Some(list) = self.cells.get_mut(&old_idx) {
                list.retain(|eid| eid != id);
            }
        }
    }

    pub fn clear(&mut self) {
        self.cells.clear();
        self.entity_cells.clear();
    }

    pub fn query_nearby_ids(&self, x: f64, y: f64, radius: f64) -> Vec<String> {
        let r = if radius < 0.0 { 0.0 } else { radius };
        let min_x = x - r + WORLD_OFFSET;
        let max_x = x + r + WORLD_OFFSET;
        let min_y = y - r + WORLD_OFFSET;
        let max_y = y + r + WORLD_OFFSET;

        let start_gx = if min_x < 0.0 { 0 } else { (min_x / CELL_SIZE) as usize };
        let end_gx = if max_x < 0.0 { 0 } else { (max_x / CELL_SIZE) as usize };
        let start_gy = if min_y < 0.0 { 0 } else { (min_y / CELL_SIZE) as usize };
        let end_gy = if max_y < 0.0 { 0 } else { (max_y / CELL_SIZE) as usize };

        let start_gx = start_gx.min(GRID_DIM - 1);
        let end_gx = end_gx.min(GRID_DIM - 1);
        let start_gy = start_gy.min(GRID_DIM - 1);
        let end_gy = end_gy.min(GRID_DIM - 1);

        let mut out = Vec::new();
        for gy in start_gy..=end_gy {
            for gx in start_gx..=end_gx {
                let cell_idx = gy * GRID_DIM + gx;
                if let Some(list) = self.cells.get(&cell_idx) {
                    out.extend_from_slice(list);
                }
            }
        }
        out
    }
}
