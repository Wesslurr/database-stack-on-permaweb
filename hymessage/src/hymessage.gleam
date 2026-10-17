//// hymessage — custom communication & sequencing device (fast lane)
//// consuming the HyDB Core Contract Library.
////
//// Addressed as `hymessage@1.0`. Packaged as a Forge-format device
//// archive (generated module name `_hb_device_*`, zip body of beams),
//// registered via the operator `trusted-devices` map. No fork.

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

@external(erlang, "hymessage_runtime", "exists_from_req")
fn erl_exists_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "schema_from_req")
fn erl_schema_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "scan_from_req")
fn erl_scan_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "snapshot_from_req")
fn erl_snapshot_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "restore_from_req")
fn erl_restore_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "replay_from_req")
fn erl_replay_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "hymessage_runtime", "promote_from_req")
fn erl_promote_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

/// Device metadata. HyperBEAM calls this during key resolution.
pub fn info(_base: Dynamic, _opts: Dynamic) -> Dict(String, String) {
  dict.new()
  |> dict.insert("name", "hymessage@1.0")
  |> dict.insert("contract", "hydb.core@1.0")
  |> dict.insert("phase", "hydb-contract-universal-stack")
  |> dict.insert("description", "universal real-time optimistic database & compute engine")
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
  |> dict.insert("contract_version", "hydb.core@1.0")
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

/// HyDB Core Contract — Put Record: POST/GET /~hymessage@1.0/put
pub fn put(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_put_from_req(base, req, opts)
}

/// HyDB Core Contract — Get Record: GET /~hymessage@1.0/get
pub fn get(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_get_from_req(base, req, opts)
}

/// HyDB Core Contract — Delete Record: POST/GET /~hymessage@1.0/del
pub fn del(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_del_from_req(base, req, opts)
}

/// HyDB Core Contract — Exists Check: GET /~hymessage@1.0/exists
pub fn exists(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_exists_from_req(base, req, opts)
}

/// HyDB Contract Schema & Capability Registry: POST/GET /~hymessage@1.0/schema
pub fn schema(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_schema_from_req(base, req, opts)
}

/// HyDB Capability Extension — Prefix Scan: GET /~hymessage@1.0/scan
pub fn scan(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_scan_from_req(base, req, opts)
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

/// Promote Epoch (Fencing Token for Failover): POST/GET /~hymessage@1.0/promote?epoch=2
pub fn promote(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_promote_from_req(base, req, opts)
}
