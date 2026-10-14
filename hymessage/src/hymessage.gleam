//// hymessage — custom communication device (fast lane) for the
//// real-time optimistic database stack.
////
//// Addressed as `hymessage@1.0`. Packaged as a Forge-format device
//// archive (generated module name `_hb_device_*`, zip body of beams),
//// registered via the operator `trusted-devices` map. No fork.
////
//// HyDB Real-Time Living Database:
//// 1. In-RAM atomic sequencer (monotonic sequence counter)
//// 2. SHA-256 hash chain
//// 3. HyDB In-Memory Slot Table Engine (Rust WASM via wasm-64@1.0)
//// 4. Incremental Merkle-XOR State Root Accumulator
//// 5. Sub-15ms optimistic response
//// 6. Snapshot Export / Restore for Pool SU & AO Process

import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/int

@external(erlang, "erlang", "system_time")
fn system_time_native() -> Int

@external(erlang, "hymessage_runtime", "submit_from_req")
fn erl_submit_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "join_from_req")
fn erl_join_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "get_state_from_req")
fn erl_get_state_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "sync_from_req")
fn erl_sync_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "reset_from_req")
fn erl_reset_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "put_from_req")
fn erl_put_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "get_from_req")
fn erl_get_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "del_from_req")
fn erl_del_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "snapshot_from_req")
fn erl_snapshot_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "restore_from_req")
fn erl_restore_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "replay_from_req")
fn erl_replay_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

/// Device metadata. HyperBEAM calls this during key resolution.
pub fn info(_base: Dynamic, _opts: Dynamic) -> Dict(String, String) {
  dict.new()
  |> dict.insert("name", "hymessage@1.0")
  |> dict.insert("phase", "hydb-living-database")
  |> dict.insert("description", "real-time optimistic living database engine")
}

/// Liveness check: GET /~hymessage@1.0/ping
pub fn ping(
  _base: Dynamic,
  _req: Dynamic,
  _opts: Dynamic,
) -> Dict(String, String) {
  dict.new()
  |> dict.insert("ok", "pong")
  |> dict.insert("device", "hymessage@1.0")
  |> dict.insert("time-native", int.to_string(system_time_native()))
}

/// Join room & obtain session token: POST/GET /~hymessage@1.0/join?player_id=1
pub fn join(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_join_from_req(base, req, opts)
}

/// Submit action & data into pipeline: POST/GET /~hymessage@1.0/submit
pub fn submit(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_submit_from_req(base, req, opts)
}

/// HyDB Put Record (O(1)): POST/GET /~hymessage@1.0/put?key=101&val=555
pub fn put(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_put_from_req(base, req, opts)
}

/// HyDB Get Record (O(1)): GET /~hymessage@1.0/get?key=101
pub fn get(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_get_from_req(base, req, opts)
}

/// HyDB Del Record (O(1)): POST/GET /~hymessage@1.0/del?key=101
pub fn del(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_del_from_req(base, req, opts)
}

/// HyDB Snapshot Export: GET /~hymessage@1.0/snapshot
pub fn snapshot(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_snapshot_from_req(base, req, opts)
}

/// HyDB Snapshot Restore: POST /~hymessage@1.0/restore
pub fn restore(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_restore_from_req(base, req, opts)
}

/// Read current in-RAM database state: GET /~hymessage@1.0/state (<15ms)
pub fn state(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_get_state_from_req(base, req, opts)
}

/// Sync sliding window events for Pool SU: GET /~hymessage@1.0/sync?since=N
pub fn sync(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_sync_from_req(base, req, opts)
}

/// Reset in-RAM database state: POST/GET /~hymessage@1.0/reset
pub fn reset(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_reset_from_req(base, req, opts)
}

/// Replay sliding window events: POST /~hymessage@1.0/replay
pub fn replay(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_replay_from_req(base, req, opts)
}
