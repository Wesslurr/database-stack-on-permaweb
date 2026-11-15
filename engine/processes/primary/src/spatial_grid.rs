//! Spatial grid indexing for ultra-fast O(1) `Read-Nearby` queries.
use crate::entity::Entity;
use crate::prelude::*;

pub const DEFAULT_RADIUS: f64 = 25.0;
pub const CELL_SIZE: f64 = 25.0;
pub const WORLD_OFFSET: f64 = 1000.0; // shifts [-1000, 1000] to [0, 2000]
pub const GRID_DIM: usize = 80;       // 2000 / 25 = 80 cells per axis

#[derive(Clone, Debug)]
pub struct NearbyItem {
    pub id: String,
    pub x: f64,
    pub y: f64,
    pub tick: i64,
    pub owner: String,
    pub dist_sq: f64,
}

#[derive(Clone, Debug)]
pub struct SpatialGrid {
    // Maps cell_index -> Vec<entity_id>
    cells: Vec<Vec<String>>,
}

impl Default for SpatialGrid {
    fn default() -> Self {
        Self::new()
    }
}

impl SpatialGrid {
    pub fn new() -> Self {
        Self {
            cells: vec![Vec::new(); GRID_DIM * GRID_DIM],
        }
    }

    #[inline]
    fn cell_coord(coord: f64) -> usize {
        let shifted = coord + WORLD_OFFSET;
        if shifted <= 0.0 {
            0
        } else {
            let idx = (shifted / CELL_SIZE) as usize;
            if idx >= GRID_DIM {
                GRID_DIM - 1
            } else {
                idx
            }
        }
    }

    #[inline]
    fn cell_index(x: f64, y: f64) -> usize {
        let cx = Self::cell_coord(x);
        let cy = Self::cell_coord(y);
        cy * GRID_DIM + cx
    }

    pub fn rebuild(&mut self, points: &BTreeMap<String, Entity>) {
        for cell in self.cells.iter_mut() {
            cell.clear();
        }
        for (id, e) in points {
            let idx = Self::cell_index(e.x, e.y);
            self.cells[idx].push(id.clone());
        }
    }

    pub fn query_nearby(
        &self,
        points: &BTreeMap<String, Entity>,
        qx: f64,
        qy: f64,
        radius: f64,
    ) -> Vec<NearbyItem> {
        let r2 = radius * radius;
        let mut results = Vec::new();

        let min_x = Self::cell_coord(qx - radius);
        let max_x = Self::cell_coord(qx + radius);
        let min_y = Self::cell_coord(qy - radius);
        let max_y = Self::cell_coord(qy + radius);

        for cy in min_y..=max_y {
            for cx in min_x..=max_x {
                let cell_idx = cy * GRID_DIM + cx;
                for id in &self.cells[cell_idx] {
                    if let Some(e) = points.get(id) {
                        let dx = e.x - qx;
                        let dy = e.y - qy;
                        let dist_sq = dx * dx + dy * dy;
                        if dist_sq <= r2 {
                            results.push(NearbyItem {
                                id: id.clone(),
                                x: e.x,
                                y: e.y,
                                tick: e.tick,
                                owner: e.owner.clone(),
                                dist_sq,
                            });
                        }
                    }
                }
            }
        }

        results
    }
}
