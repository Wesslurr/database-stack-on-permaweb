//// sd — Shared Data Pool & WAL durability worker device
//// Addressed as `sd@1.0`. Packaged as a Forge-format device
//// archive (generated module name `_hb_device_*`, zip body of beams),
//// registered via the operator `trusted-devices` map.

import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/int

@external(erlang, "erlang", "system_time")
fn system_time_native() -> Int

@external(erlang, "sd_runtime", "get_status_from_req")
fn erl_get_status_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "sd_runtime", "get_wal_from_req")
fn erl_get_wal_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "sd_runtime", "get_wal_batch_from_req")
fn erl_get_wal_batch_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "sd_runtime", "get_key_from_req")
fn erl_get_key_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "sd_runtime", "get_state_from_req")
fn erl_get_state_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "sd_runtime", "get_tick_from_req")
fn erl_get_tick_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "sd_runtime", "fence_from_req")
fn erl_fence_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "sd_runtime", "restart_from_req")
fn erl_restart_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

@external(erlang, "sd_runtime", "reset_from_req")
fn erl_reset_from_req(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic

/// Device metadata. HyperBEAM calls this during key resolution.
pub fn info(_base: Dynamic, _opts: Dynamic) -> Dict(String, String) {
  dict.new()
  |> dict.insert("name", "sd@1.0")
  |> dict.insert("contract", "hydb.core@1.0")
  |> dict.insert("role", "shared_data_pool")
  |> dict.insert("description", "HyperBEAM Shared Data Pool & WAL durability worker")
}

/// Liveness check: GET /~sd@1.0/ping
pub fn ping(
  _base: Dynamic,
  _req: Dynamic,
  _opts: Dynamic,
) -> Dict(String, String) {
  dict.new()
  |> dict.insert("ok", "pong")
  |> dict.insert("device", "sd@1.0")
  |> dict.insert("contract_version", "hydb.core@1.0")
  |> dict.insert("time-native", int.to_string(system_time_native()))
}

/// Read node status: GET /~sd@1.0/status
pub fn status(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_get_status_from_req(base, req, opts)
}

/// Read recent WAL log: GET /~sd@1.0/wal
pub fn wal(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_get_wal_from_req(base, req, opts)
}

/// Query WAL batch slice: GET /~sd@1.0/wal_batch?from_seq=A&to_seq=B
pub fn wal_batch(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_get_wal_batch_from_req(base, req, opts)
}

/// Secondary key query: GET /~sd@1.0/key?key=K
pub fn key(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_get_key_from_req(base, req, opts)
}

/// Read current state root: GET /~sd@1.0/state
pub fn state(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_get_state_from_req(base, req, opts)
}

/// Read current tick / sequence: GET /~sd@1.0/tick
pub fn tick(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_get_tick_from_req(base, req, opts)
}

/// Fence epoch (failover protection): POST/GET /~sd@1.0/fence_epoch?epoch=N
pub fn fence_epoch(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_fence_from_req(base, req, opts)
}

/// Simulate node restart from disk WAL: POST/GET /~sd@1.0/simulate_restart
pub fn simulate_restart(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_restart_from_req(base, req, opts)
}

/// Reset RAM state and clear WAL: POST/GET /~sd@1.0/reset
pub fn reset(base: Dynamic, req: Dynamic, opts: Dynamic) -> Dynamic {
  erl_reset_from_req(base, req, opts)
}
