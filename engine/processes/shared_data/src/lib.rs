//! Shared Data (SD) Replica Engine for HyperStack v2.
//! 100% Upstream Vanilla HyperBEAM WASM process (json-iface@1.0 ABI).

#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

#[cfg(target_arch = "wasm32")]
#[global_allocator]
static ALLOC: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

#[cfg(target_arch = "wasm32")]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

pub mod authoritative_state;
pub mod diff_ring;
pub mod entity;
pub mod ingress;
pub mod merkle;
pub mod num;
pub mod pending_buffer;
pub mod prelude;
pub mod spatial_grid;
pub mod verifier;

use authoritative_state::AuthoritativeState;
use entity::json_safe;
use ingress::{Ingress, SdAction};
use prelude::*;
use verifier::{CommitOutcome, Verifier};

#[derive(Clone, Debug, Default)]
pub struct SdEngine {
    pub state: AuthoritativeState,
    pub sd_signer: String,
    pub node_role: String,
    pub primary_process: String,
    pub arbit_process: String,
    pub arbit_batch_interval: u64,
    pub primary_url: String,
    pub arbit_url: String,
    pub genesis_url: String,
}

impl SdEngine {
    pub fn new() -> Self {
        Self {
            state: AuthoritativeState::new(),
            sd_signer: "sd_authority".to_owned(),
            node_role: "sd1".to_owned(),
            primary_process: "primary".to_owned(),
            arbit_process: "arbit".to_owned(),
            arbit_batch_interval: 5,
            primary_url: "http://127.0.0.1:8734".to_owned(),
            arbit_url: "http://127.0.0.1:8737".to_owned(),
            genesis_url: "http://127.0.0.1:8733".to_owned(),
        }
    }

    pub fn target_primary(&self) -> String {
        if let Some(pos) = self.node_role.rfind('_') {
            let suffix = &self.node_role[pos + 1..];
            if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
                return format!("primary_{}", suffix);
            }
        }
        self.primary_process.clone()
    }

    pub fn handle_request(&mut self, json_str: &str) -> (u16, String, Vec<String>) {
        let parsed = match Ingress::parse_request(json_str, self.state.max_entities) {
            Ok(p) => p,
            Err(e) => {
                let body = format!("{{\"status\":\"error\",\"error\":\"{}\"}}", e);
                return (400, body, Vec::new());
            }
        };

        let (action, epoch_opt, node_role) = parsed;
        if !node_role.is_empty() {
            self.node_role = node_role;
        }

        if let Some(epoch) = epoch_opt {
            if epoch < self.state.current_epoch {
                let body = format!(
                    "{{\"status\":\"error_stale_epoch\",\"tier\":\"shared_data_replica\",\"epoch\":{},\"current_epoch\":{},\"last_tick\":{}}}",
                    epoch, self.state.current_epoch, self.state.last_tick
                );
                return (409, body, Vec::new());
            } else if epoch > self.state.current_epoch {
                self.state.current_epoch = epoch;
            }
        }

        let mut outbox = Vec::new();

        match action {
            SdAction::Commit {
                batch_id,
                tick_from,
                tick_to,
                state_root,
                delta,
                order,
            } => {
                let outcome = Verifier::verify_and_commit(
                    &mut self.state,
                    &batch_id,
                    tick_from,
                    tick_to,
                    state_root.as_deref(),
                    delta,
                    order,
                    &self.sd_signer,
                );

                match outcome {
                    CommitOutcome::Committed { status, body, batch_id, tick, state_root, drained } => {
                        let target_primary = self.target_primary();
                        if drained.is_empty() {
                            let ack_body = format!(
                                r#"{{"action":"Ack","batch_id":"{}","tick":{},"state_root":"{}","sd_signer":"{}","role":"{}","epoch":{}}}"#,
                                json_safe(&batch_id), tick, json_safe(&state_root), json_safe(&self.sd_signer), json_safe(&self.node_role), self.state.current_epoch
                            );
                            let ack_msg = format!(
                                r#"{{"resolve":"~relay@1.0/cast","relay-path":"{}/~node-process@1.0/{}/push","relay-method":"POST","relay-body":"{}","content-type":"application/json","cache-control":"no-store"}}"#,
                                self.primary_url, target_primary, json_safe(&ack_body)
                            );
                            outbox.push(ack_msg);
                        } else {
                            let mut acks = Vec::with_capacity(1 + drained.len());
                            acks.push(format!(
                                r#"{{"batch_id":"{}","tick":{},"state_root":"{}","sd_signer":"{}","role":"{}","epoch":{}}}"#,
                                json_safe(&batch_id), tick, json_safe(&state_root), json_safe(&self.sd_signer), json_safe(&self.node_role), self.state.current_epoch
                            ));
                            for d in drained {
                                acks.push(format!(
                                    r#"{{"batch_id":"{}","tick":{},"state_root":"{}","sd_signer":"{}","role":"{}","epoch":{}}}"#,
                                    json_safe(&d.batch_id), d.tick, json_safe(&d.state_root), json_safe(&self.sd_signer), json_safe(&self.node_role), self.state.current_epoch
                                ));
                            }
                            let batch_ack_body = format!(r#"{{"action":"Ack-Batch","acks":[{}]}}"#, acks.join(","));
                            let batch_ack_msg = format!(
                                r#"{{"resolve":"~relay@1.0/cast","relay-path":"{}/~node-process@1.0/{}/push","relay-method":"POST","relay-body":"{}","content-type":"application/json","cache-control":"no-store"}}"#,
                                self.primary_url, target_primary, json_safe(&batch_ack_body)
                            );
                            outbox.push(batch_ack_msg);
                        }

                        // Checkpoint trigger
                        let is_sd1 = self.node_role.starts_with("sd1");
                        let count_trigger = self.arbit_batch_interval > 0
                            && ((self.state.last_tick % self.arbit_batch_interval == 0)
                                || (self.state.last_tick.saturating_sub(self.state.last_batch_sent_tick) >= self.arbit_batch_interval));
                        if is_sd1 && self.state.last_tick > 0 && count_trigger && self.state.last_tick > self.state.last_batch_sent_tick {
                            self.state.last_batch_sent_tick = self.state.last_tick;
                            let chk_body = format!(
                                r#"{{"action":"Commit-Checkpoint","last_tick":{},"point_count":{},"state_root":"{}","checkpoint_signer":"{}","sd_node":"{}","diff_seq":{},"epoch":{},"shard":"{}"}}"#,
                                self.state.last_tick, self.state.count_replicas(), json_safe(&self.state.state_root), json_safe(&self.sd_signer), json_safe(&self.node_role), self.state.diff_ring.diff_seq, self.state.current_epoch, json_safe(&self.node_role)
                            );
                            let chk_msg = format!(
                                r#"{{"resolve":"~relay@1.0/cast","relay-path":"{}/~node-process@1.0/{}/push","relay-method":"POST","relay-body":"{}","content-type":"application/json","cache-control":"no-store"}}"#,
                                self.arbit_url, self.arbit_process, json_safe(&chk_body)
                            );
                            outbox.push(chk_msg);
                        }

                        (status, body, outbox)
                    }
                    CommitOutcome::IdempotentCached { status, body, batch_id, tick, state_root } => {
                        let target_primary = self.target_primary();
                        let ack_body = format!(
                            r#"{{"action":"Ack","batch_id":"{}","tick":{},"state_root":"{}","sd_signer":"{}","role":"{}","epoch":{}}}"#,
                            json_safe(&batch_id), tick, json_safe(&state_root), json_safe(&self.sd_signer), json_safe(&self.node_role), self.state.current_epoch
                        );
                        let ack_msg = format!(
                            r#"{{"resolve":"~relay@1.0/cast","relay-path":"{}/~node-process@1.0/{}/push","relay-method":"POST","relay-body":"{}","content-type":"application/json","cache-control":"no-store"}}"#,
                            self.primary_url, target_primary, json_safe(&ack_body)
                        );
                        outbox.push(ack_msg);
                        (status, body, outbox)
                    }
                    CommitOutcome::Deferred { status, body, batch_id, should_reject } => {
                        if should_reject {
                            let target_primary = self.target_primary();
                            let rej_body = format!(
                                r#"{{"action":"Reject","batch_id":"{}","reason":"replica_behind","last_tick":{},"state_root":"{}","sd_signer":"{}","role":"{}"}}"#,
                                json_safe(&batch_id), self.state.last_tick, json_safe(&self.state.state_root), json_safe(&self.sd_signer), json_safe(&self.node_role)
                            );
                            let rej_msg = format!(
                                r#"{{"resolve":"~relay@1.0/cast","relay-path":"{}/~node-process@1.0/{}/push","relay-method":"POST","relay-body":"{}","content-type":"application/json","cache-control":"no-store"}}"#,
                                self.primary_url, target_primary, json_safe(&rej_body)
                            );
                            outbox.push(rej_msg);
                        }
                        (status, body, outbox)
                    }
                    CommitOutcome::Rejected { status, body, batch_id, reason } => {
                        let target_primary = self.target_primary();
                        let rej_body = if reason == "replica_ahead" {
                            format!(
                                r#"{{"action":"Reject","batch_id":"{}","reason":"{}","last_tick":{},"state_root":"{}","sd_signer":"{}","role":"{}","points":{}}}"#,
                                json_safe(&batch_id), reason, self.state.last_tick, json_safe(&self.state.state_root), json_safe(&self.sd_signer), json_safe(&self.node_role), self.state.encode_points()
                            )
                        } else {
                            format!(
                                r#"{{"action":"Reject","batch_id":"{}","reason":"{}","last_tick":{},"state_root":"{}","sd_signer":"{}","role":"{}"}}"#,
                                json_safe(&batch_id), reason, self.state.last_tick, json_safe(&self.state.state_root), json_safe(&self.sd_signer), json_safe(&self.node_role)
                            )
                        };
                        let rej_msg = format!(
                            r#"{{"resolve":"~relay@1.0/cast","relay-path":"{}/~node-process@1.0/{}/push","relay-method":"POST","relay-body":"{}","content-type":"application/json","cache-control":"no-store"}}"#,
                            self.primary_url, target_primary, json_safe(&rej_body)
                        );
                        outbox.push(rej_msg);
                        (status, body, outbox)
                    }
                    CommitOutcome::BadRequest { status, body } => (status, body, outbox),
                }
            }
            SdAction::SyncState { last_tick, state_root, points } => {
                if let (Some(tick), Some(claimed)) = (last_tick, state_root) {
                    let n = points.len();
                    self.state.sync_state(tick, points);
                    let root_match = if self.state.state_root == claimed { "true" } else { "false" };
                    let body = format!(
                        "{{\"status\":\"synced\",\"last_tick\":{},\"tick\":{},\"state_root\":\"{}\",\"claimed_root\":\"{}\",\"root_match\":{},\"count\":{},\"sd_signer\":\"{}\"}}",
                        self.state.last_tick, self.state.last_tick, self.state.state_root, json_safe(&claimed), root_match, n, json_safe(&self.sd_signer)
                    );
                    (200, body, outbox)
                } else {
                    let body = format!(
                        "{{\"status\":\"error_invalid_sync\",\"sd_signer\":\"{}\",\"last_tick\":{}}}",
                        json_safe(&self.sd_signer), self.state.last_tick
                    );
                    (400, body, outbox)
                }
            }
            SdAction::Hydrate => {
                let body = format!(
                    "{{\"status\":\"ok\",\"tier\":\"shared_data_replica\",\"last_tick\":{},\"tick\":{},\"state_root\":\"{}\",\"count\":{},\"points\":{},\"sd_signer\":\"{}\",\"node_role\":\"{}\"}}",
                    self.state.last_tick, self.state.last_tick, self.state.state_root, self.state.count_replicas(), self.state.encode_points(), json_safe(&self.sd_signer), json_safe(&self.node_role)
                );
                (200, body, outbox)
            }
            SdAction::PruneCommittedSlots { committed_tick, l1_tx_id } => {
                if committed_tick > self.state.last_tick || l1_tx_id.is_empty() {
                    let body = format!(
                        "{{\"status\":\"error_invalid_prune_request\",\"last_tick\":{},\"requested_tick\":{},\"sd_signer\":\"{}\"}}",
                        self.state.last_tick, committed_tick, json_safe(&self.sd_signer)
                    );
                    (409, body, outbox)
                } else {
                    let removed = self.state.diff_ring.prune(committed_tick, &l1_tx_id);
                    let body = format!(
                        "{{\"status\":\"pruned\",\"committed_tick\":{},\"last_pruned_tick\":{},\"pruned_now\":{},\"pruned_diff_count\":{},\"recent_diff_count\":{},\"last_tick\":{},\"state_root\":\"{}\",\"point\":{},\"sd_signer\":\"{}\"}}",
                        committed_tick, self.state.diff_ring.last_pruned_tick, removed, self.state.diff_ring.pruned_diff_count, self.state.diff_ring.len(), self.state.last_tick, json_safe(&self.state.state_root), self.state.player_red_json(), json_safe(&self.sd_signer)
                    );
                    (200, body, outbox)
                }
            }
            SdAction::ReadPoint { id } => {
                if let Some(packed) = self.state.replica_store.get(&id) {
                    if let Some(e) = entity::unpack_entity(packed) {
                        let body = format!(
                            "{{\"status\":\"ok\",\"tier\":\"shared_data_replica\",\"last_tick\":{},\"state_root\":\"{}\",\"point\":{{\"id\":\"{}\",\"x\":{:.6},\"y\":{:.6},\"tick\":{},\"owner\":\"{}\"}},\"sd_signer\":\"{}\"}}",
                            self.state.last_tick, self.state.state_root, json_safe(&id), e.x, e.y, e.tick, json_safe(&e.owner), json_safe(&self.sd_signer)
                        );
                        return (200, body, outbox);
                    }
                }
                let body = format!(
                    "{{\"status\":\"not_found\",\"last_tick\":{},\"state_root\":\"{}\",\"id\":\"{}\",\"sd_signer\":\"{}\"}}",
                    self.state.last_tick, self.state.state_root, json_safe(&id), json_safe(&self.sd_signer)
                );
                (200, body, outbox)
            }
            SdAction::ReadNearby { x, y, radius } => {
                let nearby_ids = self.state.spatial_grid.query_nearby_ids(x, y, radius);
                let mut points_json = Vec::new();
                for id in nearby_ids {
                    if let Some(packed) = self.state.replica_store.get(&id) {
                        if let Some(e) = entity::unpack_entity(packed) {
                            points_json.push(format!(
                                "{{\"id\":\"{}\",\"x\":{:.6},\"y\":{:.6},\"tick\":{},\"owner\":\"{}\"}}",
                                json_safe(&id), e.x, e.y, e.tick, json_safe(&e.owner)
                            ));
                        }
                    }
                }
                let body = format!(
                    "{{\"status\":\"ok\",\"tier\":\"shared_data_replica\",\"last_tick\":{},\"state_root\":\"{}\",\"count\":{},\"points\":[{}]}}",
                    self.state.last_tick, self.state.state_root, points_json.len(), points_json.join(",")
                );
                (200, body, outbox)
            }
            SdAction::Snapshot => {
                let player_json = self.state.player_red_json();
                let body = format!(
                    "{{\"status\":\"ok\",\"tier\":\"shared_data_replica\",\"persistence\":\"lmdb_disk\",\"last_tick\":{},\"state_root\":\"{}\",\"count\":{},\"points\":{},\"sd_signer\":\"{}\",\"point\":{}}}",
                    self.state.last_tick, self.state.state_root, self.state.count_replicas(), self.state.encode_points(), json_safe(&self.sd_signer), player_json
                );
                (200, body, outbox)
            }
            SdAction::SyncTopology { primary_url } => {
                if let Some(u) = primary_url {
                    if !u.is_empty() {
                        self.primary_url = u;
                    }
                }
                (200, "{\"status\":\"ok\"}".to_owned(), outbox)
            }
            SdAction::GetReplicaState => {
                let player_json = self.state.player_red_json();
                let body = format!(
                    "{{\"status\":\"ok\",\"tier\":\"shared_data_replica\",\"persistence\":\"lmdb_disk\",\"requires_sign\":true,\"sd_signer\":\"{}\",\"relayed_by_signer\":\"unverified\",\"assignment_id\":\"sd-slot-{}\",\"topology_source\":\"\",\"arbit_target\":\"{}\",\"arbit_batch_interval\":{},\"last_batch_sent_tick\":{},\"last_tick\":{},\"tick\":{},\"count\":{},\"state_root\":\"{}\",\"last_committed_tick\":{},\"last_l1_tx_id\":\"{}\",\"last_pruned_tick\":{},\"pruned_diff_count\":{},\"recent_diff_count\":{},\"max_diff_slots\":{},\"node_role\":\"{}\",\"point\":{}}}",
                    json_safe(&self.sd_signer), self.state.last_tick, self.arbit_process, self.arbit_batch_interval, self.state.last_batch_sent_tick,
                    self.state.last_tick, self.state.last_tick, self.state.count_replicas(), json_safe(&self.state.state_root), self.state.diff_ring.last_committed_tick,
                    json_safe(&self.state.diff_ring.last_l1_tx_id), self.state.diff_ring.last_pruned_tick, self.state.diff_ring.pruned_diff_count, self.state.diff_ring.len(), self.state.diff_ring.max_slots,
                    json_safe(&self.node_role), player_json
                );
                (200, body, outbox)
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Global Instance for WASM execution
// -----------------------------------------------------------------------------
static mut SD_ENGINE: Option<SdEngine> = None;

#[allow(static_mut_refs)]
fn get_engine() -> &'static mut SdEngine {
    unsafe {
        if SD_ENGINE.is_none() {
            SD_ENGINE = Some(SdEngine::new());
        }
        SD_ENGINE.as_mut().unwrap()
    }
}

// -----------------------------------------------------------------------------
// WAMR C-ABI Exports (Only export C-symbol malloc/free on wasm32 target)
// -----------------------------------------------------------------------------
const ALLOC_HEADER_SIZE: usize = 16;
const ALLOC_MAGIC: u64 = 0x4859_5052_5741_534Du64;
static mut LAST_RES_PTR: *mut u8 = core::ptr::null_mut();

#[inline]
pub fn wasm_malloc(size: usize) -> *mut u8 {
    let req_size = if size == 0 { 1 } else { size };
    let total = match req_size.checked_add(ALLOC_HEADER_SIZE) {
        Some(t) => t,
        None => return core::ptr::null_mut(),
    };
    let layout = match core::alloc::Layout::from_size_align(total, 8) {
        Ok(l) => l,
        Err(_) => return core::ptr::null_mut(),
    };
    unsafe {
        let raw = alloc::alloc::alloc(layout);
        if raw.is_null() {
            return core::ptr::null_mut();
        }
        core::ptr::write_unaligned(raw as *mut u64, req_size as u64);
        core::ptr::write_unaligned(raw.add(8) as *mut u64, ALLOC_MAGIC);
        raw.add(ALLOC_HEADER_SIZE)
    }
}

#[inline]
pub fn wasm_free(ptr: *mut u8, _size: usize) {
    if ptr.is_null() || (ptr as usize) < ALLOC_HEADER_SIZE {
        return;
    }
    unsafe {
        let raw = ptr.sub(ALLOC_HEADER_SIZE);
        let magic = core::ptr::read_unaligned(raw.add(8) as *const u64);
        if magic != ALLOC_MAGIC {
            return;
        }
        core::ptr::write_unaligned(raw.add(8) as *mut u64, 0);
        let req_size = core::ptr::read_unaligned(raw as *const u64) as usize;
        let total = req_size + ALLOC_HEADER_SIZE;
        let layout = core::alloc::Layout::from_size_align_unchecked(total, 8);
        alloc::alloc::dealloc(raw, layout);
    }
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn malloc(size: usize) -> *mut u8 {
    wasm_malloc(size)
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn free(ptr: *mut u8) {
    wasm_free(ptr, 0)
}

unsafe fn read_null_string(ptr: *const u8) -> &'static str {
    if ptr.is_null() {
        return "";
    }
    let mut len = 0;
    while *ptr.add(len) != 0 {
        len += 1;
    }
    let slice = core::slice::from_raw_parts(ptr, len);
    core::str::from_utf8(slice).unwrap_or("")
}

#[no_mangle]
pub extern "C" fn handle(msg_ptr: *const u8, proc_ptr: *const u8) -> *mut u8 {
    let engine = get_engine();
    {
        let proc_str = unsafe { read_null_string(proc_ptr) };
        if !proc_str.is_empty() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(proc_str) {
                let proc_obj = v.get("Process").unwrap_or(&v);
                let mut role_found = proc_obj
                    .get("node-role")
                    .or_else(|| proc_obj.get("role"))
                    .and_then(|r| r.as_str())
                    .map(|s| s.to_string());

                if role_found.is_none() {
                    if let Some(tags) = proc_obj.get("Tags").and_then(|t| t.as_array()) {
                        for tag in tags {
                            if let (Some(name), Some(val)) = (
                                tag.get("name").and_then(|n| n.as_str()),
                                tag.get("value"),
                            ) {
                                let lower = name.to_ascii_lowercase();
                                if lower == "node-role" || lower == "node_role" || lower == "role" {
                                    if let Some(s) = val.as_str() {
                                        role_found = Some(s.to_string());
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some(role) = role_found {
                    if !role.is_empty() {
                        engine.node_role = role;
                    }
                }
                if let Some(p_url) = proc_obj.get("primary-url").and_then(|u| u.as_str()) {
                    if !p_url.is_empty() {
                        engine.primary_url = p_url.to_owned();
                    }
                }
                if let Some(g_url) = proc_obj.get("genesis-url").and_then(|u| u.as_str()) {
                    if !g_url.is_empty() {
                        engine.genesis_url = g_url.to_owned();
                    }
                }
            }
        }
    }
    wasm_free(proc_ptr as *mut u8, 0);

    let (status, body, outbox) = {
        let msg_str = unsafe { read_null_string(msg_ptr) };
        engine.handle_request(msg_str)
    };
    wasm_free(msg_ptr as *mut u8, 0);

    let messages_json = if outbox.is_empty() {
        "[]".to_owned()
    } else {
        format!("[{}]", outbox.join(","))
    };

    let envelope = format!(
        "{{\"ok\":true,\"response\":{{\"Output\":{{\"status\":{},\"body\":\"{}\",\"last_tick\":\"{}\",\"state_root\":\"{}\",\"sd_signer\":\"{}\",\"count\":\"{}\"}},\"Messages\":{},\"patches\":[]}}}}",
        status,
        json_safe(&body),
        engine.state.last_tick,
        engine.state.state_root,
        json_safe(&engine.sd_signer),
        engine.state.count_replicas(),
        messages_json
    );

    unsafe {
        wasm_free(LAST_RES_PTR, 0);
        LAST_RES_PTR = core::ptr::null_mut();
    }

    let bytes = envelope.as_bytes();
    let total_len = bytes.len() + 1;
    let ptr = wasm_malloc(total_len);
    if !ptr.is_null() {
        unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
            *ptr.add(bytes.len()) = 0;
            LAST_RES_PTR = ptr;
        }
    }
    ptr
}

// -----------------------------------------------------------------------------
// Unit Tests (Canonical HyperStack Shared Data WASM Parity)
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::pack_entity;

    #[test]
    fn test_initial_empty_state_root() {
        let engine = SdEngine::new();
        assert_eq!(
            engine.state.state_root,
            "0x58a9c325c3213b810ad094c6109ab96f4df789b3930302b0b1b1e3e8c9387db1"
        );
        assert_eq!(engine.state.last_tick, 0);
        assert_eq!(engine.state.count_replicas(), 0);
    }

    #[test]
    fn test_commit_valid_delta_and_ack() {
        let mut engine = SdEngine::new();
        
        // Calculate candidate root for tick 1
        let mut delta = BTreeMap::new();
        delta.insert("bot_1".to_owned(), pack_entity(10.0, 20.0, 1, "alice"));
        let expected_root = engine.state.shards.candidate_root(&delta, 1);

        let req = format!(
            "{{\"action\":\"Commit\",\"batch_id\":\"b0-1-test\",\"tick_from\":0,\"tick_to\":1,\"state_root\":\"{}\",\"delta\":[[\"bot_1\",10.0,20.0,\"alice\",1]]}}",
            expected_root
        );

        let (status, body, outbox) = engine.handle_request(&req);
        assert_eq!(status, 200);
        assert!(body.contains("\"status\":\"committed\""));
        assert_eq!(engine.state.last_tick, 1);
        assert_eq!(engine.state.state_root, expected_root);
        assert_eq!(engine.state.count_replicas(), 1);
        assert_eq!(outbox.len(), 1);
        assert!(outbox[0].contains("~relay@1.0/cast"));
        assert!(outbox[0].contains("Ack"));
        assert!(outbox[0].contains("b0-1-test"));
    }

    #[test]
    fn test_reject_tick_mismatch() {
        let mut engine = SdEngine::new();
        engine.state.last_tick = 5;

        let req = "{\"action\":\"Commit\",\"batch_id\":\"b2-test\",\"tick_from\":2,\"tick_to\":3,\"state_root\":\"0x123\",\"delta\":[[\"bot_1\",10.0,20.0,\"alice\",3]]}";
        let (status, body, outbox) = engine.handle_request(req);
        assert_eq!(status, 409);
        assert!(body.contains("\"status\":\"error_tick_mismatch\""));
        assert_eq!(outbox.len(), 1);
        assert!(outbox[0].contains("~relay@1.0/cast"));
        assert!(outbox[0].contains("Reject"));
        assert!(outbox[0].contains("replica_ahead"));
    }

    #[test]
    fn test_reject_root_divergence() {
        let mut engine = SdEngine::new();

        let req = "{\"action\":\"Commit\",\"batch_id\":\"b0-bad\",\"tick_from\":0,\"tick_to\":1,\"state_root\":\"0xdeadbeef\",\"delta\":[[\"bot_1\",10.0,20.0,\"alice\",1]]}";
        let (status, body, outbox) = engine.handle_request(req);
        assert_eq!(status, 409);
        assert!(body.contains("\"status\":\"diverged\""));
        assert_eq!(outbox.len(), 1);
        assert!(outbox[0].contains("~relay@1.0/cast"));
        assert!(outbox[0].contains("Reject"));
        assert!(outbox[0].contains("root_divergence"));
    }

    #[test]
    fn test_deferred_out_of_order_batch() {
        let mut engine = SdEngine::new();
        assert_eq!(engine.state.last_tick, 0);

        let req = "{\"action\":\"Commit\",\"batch_id\":\"b2-future\",\"tick_from\":2,\"tick_to\":3,\"state_root\":\"0xabc\",\"delta\":[[\"bot_1\",10.0,20.0,\"alice\",3]]}";
        let (status, body, outbox) = engine.handle_request(req);
        assert_eq!(status, 202);
        assert!(body.contains("\"status\":\"deferred\""));
        assert_eq!(engine.state.pending.len(), 1);
        assert!(outbox.is_empty());
    }

    #[test]
    fn test_idempotency_cached_response() {
        let mut engine = SdEngine::new();
        let mut delta = BTreeMap::new();
        delta.insert("bot_1".to_owned(), pack_entity(10.0, 20.0, 1, "alice"));
        let expected_root = engine.state.shards.candidate_root(&delta, 1);

        let req = format!(
            "{{\"action\":\"Commit\",\"batch_id\":\"b0-repeat\",\"tick_from\":0,\"tick_to\":1,\"state_root\":\"{}\",\"delta\":[[\"bot_1\",10.0,20.0,\"alice\",1]]}}",
            expected_root
        );

        let (status1, body1, outbox1) = engine.handle_request(&req);
        assert_eq!(status1, 200);
        assert_eq!(outbox1.len(), 1);

        // Repeat exact same batch
        let (status2, body2, outbox2) = engine.handle_request(&req);
        assert_eq!(status2, 200);
        assert_eq!(body1, body2);
        assert_eq!(outbox2.len(), 1);
        assert!(outbox2[0].contains("~relay@1.0/cast"));
        assert!(outbox2[0].contains("Ack"));
    }

    #[test]
    fn test_read_nearby_spatial_grid() {
        let mut engine = SdEngine::new();
        let mut delta = BTreeMap::new();
        delta.insert("bot_close".to_owned(), pack_entity(10.0, 10.0, 1, "alice"));
        delta.insert("bot_far".to_owned(), pack_entity(500.0, 500.0, 1, "bob"));
        let root = engine.state.shards.candidate_root(&delta, 1);

        let req_commit = format!(
            "{{\"action\":\"Commit\",\"batch_id\":\"b0-spatial\",\"tick_from\":0,\"tick_to\":1,\"state_root\":\"{}\",\"delta\":[[\"bot_close\",10.0,10.0,\"alice\",1],[\"bot_far\",500.0,500.0,\"bob\",1]]}}",
            root
        );
        let _ = engine.handle_request(&req_commit);

        let req_nearby = "{\"action\":\"Read-Nearby\",\"x\":10.0,\"y\":10.0,\"radius\":30.0}";
        let (status, body, _) = engine.handle_request(req_nearby);
        assert_eq!(status, 200);
        assert!(body.contains("bot_close"));
        assert!(!body.contains("bot_far"));
    }

    #[test]
    fn test_packed_delta_and_drained_ack_batch() {
        let mut engine = SdEngine::new();
        let mut d1 = BTreeMap::new();
        d1.insert("bot_1".to_owned(), pack_entity(10.0, 15.0, 2, "alice"));
        let r1 = engine.state.shards.candidate_root(&d1, 2);

        let mut tmp_shards = engine.state.shards.clone();
        tmp_shards.commit(&d1, 2);
        let mut d2 = BTreeMap::new();
        d2.insert("bot_1".to_owned(), pack_entity(20.5, 30.25, 4, "alice"));
        let r2 = tmp_shards.candidate_root(&d2, 4);

        // Future batch arrives first via packed_delta -> deferred (202)
        let req2 = format!(
            "{{\"action\":\"Commit\",\"batch_id\":\"b-2-4\",\"tick_from\":2,\"tick_to\":4,\"state_root\":\"{}\",\"packed_delta\":\"bot_1|20.500000|30.250000|alice\"}}",
            r2
        );
        let (st2, b2, out2) = engine.handle_request(&req2);
        assert_eq!(st2, 202);
        assert!(b2.contains("\"status\":\"deferred\""));
        assert!(out2.is_empty());

        // Base batch arrives -> commits both and emits single Ack-Batch outbox message!
        let req1 = format!(
            "{{\"action\":\"Commit\",\"batch_id\":\"b-0-2\",\"tick_from\":0,\"tick_to\":2,\"state_root\":\"{}\",\"packed_delta\":\"bot_1|10.000000|15.000000|alice\"}}",
            r1
        );
        let (st1, b1, out1) = engine.handle_request(&req1);
        assert_eq!(st1, 200);
        assert!(b1.contains("\"status\":\"committed\""));
        assert_eq!(engine.state.last_tick, 4);
        assert_eq!(engine.state.state_root, r2);
        assert_eq!(out1.len(), 1);
        assert!(out1[0].contains("Ack-Batch"));
        assert!(out1[0].contains("b-0-2"));
        assert!(out1[0].contains("b-2-4"));
    }
}
