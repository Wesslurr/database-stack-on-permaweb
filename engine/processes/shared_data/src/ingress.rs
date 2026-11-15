//! Ingress parser for Shared Data requests: JSON actions, query dispatch,
//! and packed string parsing for HyperStack Shared Data WASM replicas.

use crate::entity::pack_entity;
use crate::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub enum SdAction {
    Commit {
        batch_id: String,
        tick_from: Option<u64>,
        tick_to: Option<u64>,
        state_root: Option<String>,
        delta: BTreeMap<String, (f64, f64, String, Option<u64>)>, // id -> (x, y, owner, tick)
        order: Vec<String>,
    },
    SyncState {
        last_tick: Option<u64>,
        state_root: Option<String>,
        points: BTreeMap<String, String>, // id -> packed
    },
    Hydrate,
    PruneCommittedSlots {
        committed_tick: u64,
        l1_tx_id: String,
    },
    ReadPoint {
        id: String,
    },
    ReadNearby {
        x: f64,
        y: f64,
        radius: f64,
    },
    Snapshot,
    SyncTopology {
        primary_url: Option<String>,
    },
    GetReplicaState,
}

pub struct Ingress;

impl Ingress {
    pub fn parse_delta(val: &serde_json::Value) -> (BTreeMap<String, (f64, f64, String, Option<u64>)>, Vec<String>) {
        let mut delta = BTreeMap::new();
        let mut order = Vec::new();

        let list_opt = if let Some(arr) = val.as_array() {
            Some(arr)
        } else if let Some(sub) = val.get("delta").and_then(|d| d.as_array()) {
            Some(sub)
        } else {
            None
        };

        if let Some(arr) = list_opt {
            for item in arr {
                if let Some(row) = item.as_array() {
                    if !row.is_empty() {
                        let id = row[0].as_str().unwrap_or("").to_owned();
                        let x = row.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let y = row.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let owner = row.get(3).and_then(|v| v.as_str()).unwrap_or("anonymous").to_owned();
                        let tick = row.get(4).and_then(|v| v.as_u64());
                        if !id.is_empty() {
                            if !delta.contains_key(&id) {
                                order.push(id.clone());
                            }
                            delta.insert(id, (x, y, owner, tick));
                        }
                    }
                } else if let Some(obj) = item.as_object() {
                    let id = obj.get("id").and_then(|v| v.as_str()).unwrap_or("").to_owned();
                    let x = obj.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let y = obj.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let owner = obj.get("owner").and_then(|v| v.as_str()).unwrap_or("anonymous").to_owned();
                    let tick = obj.get("tick").and_then(|v| v.as_u64());
                    if !id.is_empty() {
                        if !delta.contains_key(&id) {
                            order.push(id.clone());
                        }
                        delta.insert(id, (x, y, owner, tick));
                    }
                }
            }
        }
        (delta, order)
    }

    pub fn parse_points(val: &serde_json::Value, max_entities: usize) -> BTreeMap<String, String> {
        let mut points = BTreeMap::new();

        let list_opt = if let Some(arr) = val.as_array() {
            Some(arr)
        } else if let Some(sub) = val.get("points").and_then(|p| p.as_array()) {
            Some(sub)
        } else {
            None
        };

        if let Some(arr) = list_opt {
            for item in arr {
                if points.len() >= max_entities {
                    break;
                }
                if let Some(row) = item.as_array() {
                    if row.len() >= 4 {
                        let id = row[0].as_str().unwrap_or("");
                        let x = row.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let y = row.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let tick = row.get(3).and_then(|v| v.as_u64()).unwrap_or(0);
                        let owner = row.get(4).and_then(|v| v.as_str()).unwrap_or("anonymous");
                        if !id.is_empty() {
                            points.insert(id.to_owned(), pack_entity(x, y, tick, owner));
                        }
                    }
                } else if let Some(obj) = item.as_object() {
                    let id = obj.get("id").and_then(|v| v.as_str()).unwrap_or("");
                    let x = obj.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let y = obj.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let tick = obj.get("tick").and_then(|v| v.as_u64()).unwrap_or(0);
                    let owner = obj.get("owner").and_then(|v| v.as_str()).unwrap_or("anonymous");
                    if !id.is_empty() {
                        points.insert(id.to_owned(), pack_entity(x, y, tick, owner));
                    }
                }
            }
        }
        points
    }

    pub fn parse_request(json_str: &str, max_entities: usize) -> Result<(SdAction, Option<u64>, String), &'static str> {
        let val: serde_json::Value = serde_json::from_str(json_str).map_err(|_| "Invalid JSON")?;
        let owned_inner: Option<serde_json::Value>;
        let root = if let Some(body_obj) = val.get("body").or_else(|| val.get("Data")).or_else(|| val.get("data")) {
            if body_obj.is_object() {
                body_obj
            } else if let Some(s) = body_obj.as_str() {
                if let Ok(inner) = serde_json::from_str::<serde_json::Value>(s) {
                    if inner.is_object() {
                        owned_inner = Some(inner);
                        owned_inner.as_ref().unwrap()
                    } else {
                        &val
                    }
                } else {
                    &val
                }
            } else {
                &val
            }
        } else {
            &val
        };

        let mut tags_map: BTreeMap<String, String> = BTreeMap::new();
        if let Some(tags) = val.get("Tags").or_else(|| root.get("Tags")).and_then(|t| t.as_array()) {
            for tag in tags {
                if let (Some(name), Some(v)) = (tag.get("name").and_then(|n| n.as_str()), tag.get("value")) {
                    let val_str = if let Some(s) = v.as_str() {
                        s.to_string()
                    } else {
                        v.to_string()
                    };
                    let lower = name.to_ascii_lowercase();
                    let under = lower.replace('-', "_");
                    tags_map.insert(under, val_str.clone());
                    tags_map.insert(lower, val_str);
                }
            }
        }

        let get_str = |key: &str| -> Option<String> {
            let k_lower = key.to_ascii_lowercase();
            let k_under = k_lower.replace('-', "_");
            root.get(key)
                .or_else(|| root.get(&k_lower))
                .or_else(|| root.get(&k_under))
                .or_else(|| root.get("query").and_then(|q| q.get(key).or_else(|| q.get(&k_lower))))
                .or_else(|| val.get(key).or_else(|| val.get(&k_lower)))
                .and_then(|v| {
                    if let Some(s) = v.as_str() {
                        Some(s.to_string())
                    } else if let Some(n) = v.as_i64() {
                        Some(n.to_string())
                    } else {
                        None
                    }
                })
                .or_else(|| tags_map.get(&k_lower).cloned())
                .or_else(|| tags_map.get(&k_under).cloned())
        };

        let get_u64 = |key: &str| -> Option<u64> {
            let k_lower = key.to_ascii_lowercase();
            let k_under = k_lower.replace('-', "_");
            root.get(key)
                .or_else(|| root.get(&k_lower))
                .or_else(|| root.get(&k_under))
                .or_else(|| root.get("query").and_then(|q| q.get(key).or_else(|| q.get(&k_lower))))
                .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok())))
                .or_else(|| tags_map.get(&k_lower).or_else(|| tags_map.get(&k_under)).and_then(|s| s.parse::<u64>().ok()))
        };

        let get_f64 = |key: &str| -> Option<f64> {
            let k_lower = key.to_ascii_lowercase();
            let k_under = k_lower.replace('-', "_");
            root.get(key)
                .or_else(|| root.get(&k_lower))
                .or_else(|| root.get(&k_under))
                .or_else(|| root.get("query").and_then(|q| q.get(key).or_else(|| q.get(&k_lower))))
                .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok())))
                .or_else(|| tags_map.get(&k_lower).or_else(|| tags_map.get(&k_under)).and_then(|s| s.parse::<f64>().ok()))
        };

        let action_str = get_str("action")
            .unwrap_or_else(|| "Get-Replica-State".to_string());

        let epoch = get_u64("epoch");

        let node_role = get_str("node-role")
            .or_else(|| get_str("role"))
            .unwrap_or_default();

        let action = match action_str.as_str() {
            "Commit" => {
                let batch_id = get_str("batch_id").unwrap_or_default();
                let tick_from = get_u64("tick_from");
                let tick_to = get_u64("tick_to");
                let state_root = get_str("state_root");

                let delta_val = root.get("delta")
                    .or_else(|| root.get("query").and_then(|q| q.get("delta")))
                    .unwrap_or(&serde_json::Value::Null);

                let (delta, order) = Self::parse_delta(delta_val);

                SdAction::Commit {
                    batch_id,
                    tick_from,
                    tick_to,
                    state_root,
                    delta,
                    order,
                }
            }
            "Sync-State" => {
                let last_tick = get_u64("last_tick");
                let state_root = get_str("state_root");
                let points_val = root.get("points")
                    .or_else(|| root.get("query").and_then(|q| q.get("points")))
                    .unwrap_or(&serde_json::Value::Null);

                let points = Self::parse_points(points_val, max_entities);

                SdAction::SyncState {
                    last_tick,
                    state_root,
                    points,
                }
            }
            "Hydrate" => SdAction::Hydrate,
            "Prune-Committed-Slots" => {
                let committed_tick = get_u64("committed_tick").unwrap_or(0);
                let l1_tx_id = get_str("l1_tx_id").unwrap_or_default();

                SdAction::PruneCommittedSlots {
                    committed_tick,
                    l1_tx_id,
                }
            }
            "Read-Point" => {
                let id = get_str("id").unwrap_or_default();
                SdAction::ReadPoint { id }
            }
            "Read-Nearby" => {
                let x = get_f64("x").unwrap_or(0.0);
                let y = get_f64("y").unwrap_or(0.0);
                let radius = get_f64("radius").unwrap_or(50.0);
                SdAction::ReadNearby { x, y, radius }
            }
            "Snapshot" => SdAction::Snapshot,
            "Sync-Topology" => SdAction::SyncTopology {
                primary_url: get_str("primary_url").or_else(|| get_str("primary-url")),
            },
            _ => SdAction::GetReplicaState,
        };

        Ok((action, epoch, node_role))
    }
}
