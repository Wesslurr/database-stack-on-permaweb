//! Message ingress parsing: handles JSON payloads, packed string formats,
//! and action dispatch for HyperStack Primary WASM.
use crate::num::parse_num;
use crate::prelude::*;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct IngressPointItem {
    pub id: String,
    pub x: f64,
    pub y: f64,
    pub target_tick: Option<i64>,
    pub ts: Option<i64>,
    pub owner: String,
    pub key: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum IngressAction {
    Input {
        item: IngressPointItem,
        flush: bool,
    },
    WriteBatch {
        items: Vec<IngressPointItem>,
        flush: bool,
    },
    Advance,
    BatchStatus {
        batch_id: Option<String>,
    },
    Ack {
        batch_id: String,
        tick: Option<i64>,
        state_root: Option<String>,
        role: String,
        epoch: Option<u64>,
    },
    Reject {
        batch_id: String,
        reason: String,
        last_tick: Option<i64>,
        state_root: Option<String>,
        sd_signer: Option<String>,
        role: Option<String>,
        points: Vec<(String, f64, f64, i64, String)>,
    },
    ReadPoint {
        id: Option<String>,
    },
    ReadAll,
    ReadNearby {
        x: f64,
        y: f64,
        radius: f64,
    },
    Snapshot,
    SyncTopology {
        epoch: Option<u64>,
        sd_urls: Option<Vec<String>>,
        arbit_url: Option<String>,
    },
    ProbeSd {
        sd: usize,
    },
}

pub struct ParsedMessage {
    pub action: IngressAction,
    pub client_signer: Option<String>,
    pub msg_id: Option<String>,
    pub timestamp: Option<i64>,
    pub circuit_breaker: bool,
    pub raw_bytes_len: usize,
}

pub fn parse_packed_inputs(packed: &str, default_owner: &str) -> Vec<IngressPointItem> {
    let mut items = Vec::new();
    for entry in packed.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let parts: Vec<&str> = entry.split('|').collect();
        if parts.len() >= 6 {
            let id = parts[0].to_string();
            let x = parse_num(parts[1]).unwrap_or(0.0);
            let y = parse_num(parts[2]).unwrap_or(0.0);
            let target_tick = parts[3].parse::<i64>().ok();
            let ts = parts[4].parse::<i64>().ok();
            let owner = if parts[5].is_empty() {
                default_owner.to_string()
            } else {
                parts[5].to_string()
            };
            items.push(IngressPointItem {
                id,
                x,
                y,
                target_tick,
                ts,
                owner,
                key: None,
            });
        }
    }
    items
}

pub fn parse_message(raw_json: &str) -> Result<ParsedMessage, String> {
    let raw_bytes_len = raw_json.len();
    let root: Value = serde_json::from_str(raw_json).map_err(|e| e.to_string())?;

    // Normalize message / body / query structure
    let msg = if let Some(body) = root.get("body").or_else(|| root.get("Data")).or_else(|| root.get("data")) {
        if body.is_object() {
            body
        } else if let Some(s) = body.as_str() {
            // Body may be a stringified JSON
            if let Ok(inner) = serde_json::from_str::<Value>(s) {
                return parse_message_value(&inner, &root, raw_bytes_len);
            }
            &root
        } else {
            &root
        }
    } else {
        &root
    };

    parse_message_value(msg, &root, raw_bytes_len)
}

fn parse_message_value(
    msg: &Value,
    outer: &Value,
    raw_bytes_len: usize,
) -> Result<ParsedMessage, String> {
    let mut tags_map: BTreeMap<String, String> = BTreeMap::new();
    if let Some(tags) = outer.get("Tags").or_else(|| msg.get("Tags")).and_then(|t| t.as_array()) {
        for tag in tags {
            if let (Some(name), Some(val)) = (tag.get("name").and_then(|n| n.as_str()), tag.get("value")) {
                let val_str = if let Some(s) = val.as_str() {
                    s.to_string()
                } else {
                    val.to_string()
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
        msg.get(key)
            .or_else(|| msg.get(&k_lower))
            .or_else(|| msg.get(&k_under))
            .or_else(|| msg.get("query").and_then(|q| q.get(key).or_else(|| q.get(&k_lower))))
            .or_else(|| outer.get(key).or_else(|| outer.get(&k_lower)))
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

    let get_f64 = |key: &str| -> Option<f64> {
        let k_lower = key.to_ascii_lowercase();
        let k_under = k_lower.replace('-', "_");
        msg.get(key)
            .or_else(|| msg.get(&k_lower))
            .or_else(|| msg.get(&k_under))
            .or_else(|| msg.get("query").and_then(|q| q.get(key).or_else(|| q.get(&k_lower))))
            .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(parse_num)))
            .or_else(|| tags_map.get(&k_lower).or_else(|| tags_map.get(&k_under)).and_then(|s| parse_num(s)))
    };

    let get_i64 = |key: &str| -> Option<i64> {
        let k_lower = key.to_ascii_lowercase();
        let k_under = k_lower.replace('-', "_");
        msg.get(key)
            .or_else(|| msg.get(&k_lower))
            .or_else(|| msg.get(&k_under))
            .or_else(|| msg.get("query").and_then(|q| q.get(key).or_else(|| q.get(&k_lower))))
            .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok())))
            .or_else(|| tags_map.get(&k_lower).or_else(|| tags_map.get(&k_under)).and_then(|s| s.parse::<i64>().ok()))
    };

    let action_str = get_str("action")
        .or_else(|| get_str("Action"))
        .unwrap_or_else(|| "Read-All".to_string());

    let client_signer = get_str("client_signer")
        .or_else(|| get_str("client-signer"))
        .or_else(|| get_str("owner"))
        .or_else(|| get_str("From"));

    let msg_id = outer
        .get("id")
        .or_else(|| outer.get("Id"))
        .or_else(|| outer.get("hash-chain"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let timestamp = outer
        .get("Timestamp")
        .or_else(|| outer.get("timestamp"))
        .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok())))
        .or_else(|| get_i64("Timestamp"))
        .or_else(|| get_i64("timestamp"))
        .or_else(|| get_i64("ts"));

    let circuit_breaker = msg
        .get("circuit_breaker")
        .and_then(|v| v.as_bool())
        .or_else(|| tags_map.get("circuit_breaker").map(|s| s == "true"))
        .unwrap_or(false);

    let flush = msg
        .get("flush")
        .map(|v| v.as_bool().unwrap_or(v.as_str() == Some("true")))
        .or_else(|| tags_map.get("flush").map(|s| s == "true" || s == "1"))
        .unwrap_or(false);

    let action = match action_str.as_str() {
        "Input" | "Write-Point" => {
            let id = get_str("id").unwrap_or_default();
            let x = get_f64("x").unwrap_or(0.0);
            let y = get_f64("y").unwrap_or(0.0);
            let target_tick = get_i64("target_tick").or_else(|| get_i64("client_tick"));
            let ts = get_i64("ts").or(timestamp);
            let owner = client_signer
                .clone()
                .unwrap_or_else(|| "anonymous".to_string());
            let key = get_str("idempotency_key").or_else(|| get_str("input_id"));

            IngressAction::Input {
                item: IngressPointItem {
                    id,
                    x,
                    y,
                    target_tick,
                    ts,
                    owner,
                    key,
                },
                flush,
            }
        }

        "Write-Batch" => {
            let mut items = Vec::new();
            let default_owner = client_signer.as_deref().unwrap_or("anonymous");

            if let Some(packed_str) = get_str("packed_inputs").or_else(|| get_str("packed-inputs")) {
                items.extend(parse_packed_inputs(&packed_str, default_owner));
            } else if let Some(arr) = msg
                .get("inputs")
                .or_else(|| msg.get("query").and_then(|q| q.get("inputs")))
                .and_then(|v| v.as_array())
            {
                for val in arr {
                    if let Some(obj) = val.as_object() {
                        let id = obj
                            .get("id")
                            .and_then(|v| v.as_str().map(|s| s.to_string()))
                            .unwrap_or_default();
                        let x = obj.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let y = obj.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let target_tick = obj
                            .get("target_tick")
                            .or_else(|| obj.get("tick"))
                            .and_then(|v| v.as_i64());
                        let ts = obj.get("ts").and_then(|v| v.as_i64()).or(timestamp);
                        let owner = obj
                            .get("owner")
                            .or_else(|| obj.get("client_signer"))
                            .and_then(|v| v.as_str().map(|s| s.to_string()))
                            .unwrap_or_else(|| default_owner.to_string());
                        items.push(IngressPointItem {
                            id,
                            x,
                            y,
                            target_tick,
                            ts,
                            owner,
                            key: None,
                        });
                    }
                }
            }

            IngressAction::WriteBatch {
                items,
                flush: msg
                    .get("flush")
                    .map(|v| v.as_bool().unwrap_or(v.as_str() != Some("false")))
                    .or_else(|| tags_map.get("flush").map(|s| s != "false" && s != "0"))
                    .unwrap_or(true),
            }
        }

        "Advance" => IngressAction::Advance,

        "Batch-Status" => {
            let batch_id = get_str("batch_id").or_else(|| get_str("batch-id"));
            IngressAction::BatchStatus { batch_id }
        }

        "Ack" => {
            let batch_id = get_str("batch_id").or_else(|| get_str("batch-id")).unwrap_or_default();
            let tick = get_i64("tick");
            let state_root = get_str("state_root").or_else(|| get_str("state-root"));
            let role = get_str("role").or_else(|| get_str("sd_signer")).unwrap_or_else(|| "replica".to_string());
            let epoch = get_i64("epoch").map(|e| e as u64);

            IngressAction::Ack {
                batch_id,
                tick,
                state_root,
                role,
                epoch,
            }
        }

        "Reject" => {
            let batch_id = get_str("batch_id").or_else(|| get_str("batch-id")).unwrap_or_default();
            let reason = get_str("reason").unwrap_or_else(|| "replica_ahead".to_string());
            let last_tick = get_i64("last_tick").or_else(|| get_i64("last-tick"));
            let state_root = get_str("state_root").or_else(|| get_str("state-root"));
            let sd_signer = get_str("sd_signer").or_else(|| get_str("sd-signer"));
            let role = get_str("role");
            let mut points = Vec::new();
            if let Some(arr) = msg
                .get("points")
                .or_else(|| outer.get("points"))
                .or_else(|| msg.get("query").and_then(|q| q.get("points")))
                .and_then(|v| v.as_array())
            {
                for item in arr {
                    if let Some(p_arr) = item.as_array() {
                        if p_arr.len() >= 5 {
                            let id = p_arr[0].as_str().unwrap_or_default().to_string();
                            let x = p_arr[1].as_f64().unwrap_or(0.0);
                            let y = p_arr[2].as_f64().unwrap_or(0.0);
                            let tick = p_arr[3].as_i64().unwrap_or(0);
                            let owner = p_arr[4].as_str().unwrap_or("anonymous").to_string();
                            if !id.is_empty() {
                                points.push((id, x, y, tick, owner));
                            }
                        }
                    } else if let Some(obj) = item.as_object() {
                        let id = obj.get("id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                        let x = obj.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let y = obj.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let tick = obj.get("tick").and_then(|v| v.as_i64()).unwrap_or(0);
                        let owner = obj.get("owner").and_then(|v| v.as_str()).unwrap_or("anonymous").to_string();
                        if !id.is_empty() {
                            points.push((id, x, y, tick, owner));
                        }
                    }
                }
            }

            IngressAction::Reject {
                batch_id,
                reason,
                last_tick,
                state_root,
                sd_signer,
                role,
                points,
            }
        }

        "Read-Point" => {
            let id = get_str("id");
            IngressAction::ReadPoint { id }
        }

        "Read-Nearby" => {
            let x = get_f64("x").unwrap_or(0.0);
            let y = get_f64("y").unwrap_or(0.0);
            let radius = get_f64("radius").unwrap_or(25.0);
            IngressAction::ReadNearby { x, y, radius }
        }

        "Snapshot" => IngressAction::Snapshot,
        "Sync-Topology" => {
            let epoch = get_i64("epoch").map(|e| e as u64);
            let arbit_url = get_str("arbit_url").or_else(|| get_str("arbit-url"));
            let mut urls = Vec::new();
            let raw_sd_urls = msg
                .get("sd_urls")
                .or_else(|| msg.get("sd-urls"))
                .or_else(|| outer.get("sd_urls"))
                .or_else(|| outer.get("sd-urls"))
                .or_else(|| msg.get("query").and_then(|q| q.get("sd_urls").or_else(|| q.get("sd-urls"))));
            if let Some(arr) = raw_sd_urls.and_then(|v| v.as_array()) {
                for item in arr {
                    if let Some(u) = item.as_str() {
                        let trimmed = u.trim();
                        if !trimmed.is_empty() {
                            urls.push(trimmed.to_string());
                        }
                    }
                }
            } else if let Some(s) = raw_sd_urls
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| get_str("sd_urls"))
                .or_else(|| get_str("sd-urls"))
            {
                for part in s.split(',') {
                    let trimmed = part.trim();
                    if !trimmed.is_empty() {
                        urls.push(trimmed.to_string());
                    }
                }
            }
            let sd_urls = if urls.is_empty() { None } else { Some(urls) };
            IngressAction::SyncTopology {
                epoch,
                sd_urls,
                arbit_url,
            }
        }

        "Probe-SD" => {
            let sd = get_i64("sd").unwrap_or(1) as usize;
            IngressAction::ProbeSd { sd }
        }

        _ => IngressAction::ReadAll,
    };

    Ok(ParsedMessage {
        action,
        client_signer,
        msg_id,
        timestamp,
        circuit_breaker,
        raw_bytes_len,
    })
}
