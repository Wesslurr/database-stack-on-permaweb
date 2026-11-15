# HyperStack: Real-Time State Engine

> **High-Frequency Real-Time State Engine & Distributed Replication Stack on AO Compute & Arweave Permaweb**

[![Project](https://img.shields.io/badge/Project-HyperStack-orange)](https://github.com/Wesslurr/database-stack-on-permaweb)
[![Status](https://img.shields.io/badge/Status-Final%20Milestone%20%2F%20Empirical%20Ceiling%20Verified-brightgreen)](idea.md)
[![Permaweb](https://img.shields.io/badge/Arweave-Permaweb-black?logo=arweave)](https://arweave.org/)
[![AO Compute](https://img.shields.io/badge/AO-Compute-blue)](https://ao.g8way.io/)
[![HyperBEAM](https://img.shields.io/badge/HyperBEAM-Erlang%20OTP%2027-red)](https://github.com/permaweb/hyperbeam)
[![Tests](https://img.shields.io/badge/Live%20Cluster%20Tests-100%2F100%20Passing%20%2B%2014%20Rust%20Unit-brightgreen)](tests/)
[![Latency](https://img.shields.io/badge/p50%20Latency-10.3ms%20%2Fnow%20%7C%2064.2ms%20L0%20%7C%20274.1ms%20L1-blueviolet)](#honest-empirical-performance-benchmarks)
[![Quorum](https://img.shields.io/badge/Quorum-Honest%202--of--3%20Verified%20ACK-success)](#honest-empirical-performance-benchmarks)

---

> [!IMPORTANT]
> ### 🏆 Final Project Milestone & Empirical Transparency Notice
> **HyperStack** is a completed experimental prototype of a high-frequency real-time state engine and distributed replication stack built natively on **HyperBEAM, AO Compute, Rust WebAssembly, and Arweave L1**.
>
> - **Final Result Achieved So Far:** This codebase and benchmark baseline represent the **final consolidated architecture and empirical performance ceiling** achieved on this stack without relaxing the honest 2-of-3 quorum persistence contract or faking confirmations on the client.
> - **100% Localhost Multi-Node Topology:** All 6 cluster nodes (`:1984`, `:8733`, `:8734`, `:8735`, `:8736`, `:8737`) and the interactive client server (`:8080`) run locally on a single physical host over loopback (`127.0.0.1`), measuring raw local IPC/HTTP behavior without WAN latency or packet loss.
> - **Software-Level SD Quorum Persistence Contract (Zero Guessing):** An HTTP `200` (`ack: "replicated"`) is promoted **only after Primary + authoritative Shared Data replicas (`sd1`, `sd2`) deterministically re-verify the WASM transition, commit the slot to local LMDB (`hb_store_lmdb`, `sync: false`), and match `batch_id`, `tick`, and canonical `state_root`**. HTTP `202` means L0 acceptance into volatile Primary RAM (`hb_store_volatile`) only. Quorum loss strictly returns `503 quorum_unavailable` without mutating confirmed state.
> - **Zero VIP Lanes:** Both interactive player moves (`player_red`) and automated swarm entities traverse the exact same canonical `Write-Batch` ingest, WASM execution, and 2-of-3 LMDB quorum pipeline.

---

## Hardware & Environment Test Specifications

All raw micro-benchmarks (`tools/bench_baseline.json`), live 120-bot Circle Tests, unit tests, and failure-injection suites were executed on the following sanitized consumer test machine specification (with all personal identifiers, hostnames, paths, network addresses, and hardware serials omitted):

| Parameter | Verified Test Machine & Runtime Specification |
| :--- | :--- |
| **Physical Host OS** | Microsoft Windows 10 Pro (64-bit) |
| **Processor (CPU)** | Intel(R) Core(TM) i5-6500 CPU @ 3.20 GHz — **4 Physical Cores / 4 Threads** (x86_64) |
| **Memory (RAM)** | **8 GB Physical Host RAM** (`7.9 GiB` usable; **`3.8 GiB`** allocated to WSL2 guest) |
| **Primary Disk** | `256 GB` 2.5" SATA SSD |
| **Linux Subsystem** | WSL2 (`Ubuntu 26.04 LTS`, Linux Kernel `6.18.58.1-microsoft-standard-WSL2` x86_64) |
| **Erlang / OTP Runtime** | **Erlang/OTP 27** (`erts-15.2.7.4`), 5 independent BEAM VMs (`+S 4:4 +A 4 +MBas aobf`) |
| **WASM & Tooling** | Rust `1.98.1` (`wasm32-unknown-unknown`), Python `3.14.4`, Docker `29.1.3`, tmux `3.6` |
| **Storage Engines** | `primary`: `hb_store_volatile` (pure RAM, `store_ms: 0`)<br>`sd1` & `sd2`: `hb_store_lmdb` (`sync: False`, `snapshot_slots = 20`)<br>`genesis` & `arbit`: `hb_store_lmdb` |
| **Settlement Layer (L1)** | ArLocal Docker Container (`:1984`) + Native HyperBEAM Device (`~l1-checkpoint@1.0` on `hb-arbit :8737` using `tx@1.0` + `arweave@2.9`) |
| **Transport & Client** | HTTP/1.1 Cowboy REST + keep-alive proxy (`tools/game_client/serve.py` on `:8080`) |

---

## Honest Empirical Performance Benchmarks

### 1. Raw Micro-Benchmark Latency & Cluster Footprint (`tools/bench_baseline.json`)

Measured against a fresh single-world cluster (`FRESH=1 ./tools/apply_engine.sh`, commit `3b3e48c`) using HTTP/1.1 persistent keep-alive connections on local loopback (`127.0.0.1`):

| Operation Path | Samples (`n`) | min | p50 | p90 | p99 | max | Architectural Role |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| **`/now` (RAM State Read)** | 150 | 8.69 ms | **10.33 ms** | 12.35 ms | 20.91 ms | 21.79 ms | Direct BEAM ETS live-state fast-path (`dev_process`) |
| **`write_202` (L0 Ingest Accept)** | 60 | 54.46 ms | **64.21 ms** | 89.81 ms | 107.85 ms | 108.15 ms | Volatile RAM ingress (`store_ms: 0`, ETS `ProcessJson` cache, slim `device-stack`) |
| **`write_confirm` (L1 LMDB Quorum)** | 40 | 200.28 ms | **274.05 ms** | 353.50 ms | 406.26 ms | 406.26 ms | Full 2-of-3 quorum commit chain (Primary + SD1/SD2 LMDB + `Ack`) |
| **`advance` (Batch Tick Finalize)** | 40 | 46.75 ms | **91.86 ms** | 164.12 ms | 198.81 ms | 198.81 ms | `Advance-Batch` tick increment + lazy `sync_head_points` |
| **`relay_call_sd` (Primary → SD1)** | 60 | 39.34 ms | **42.15 ms** | 45.77 ms | 48.89 ms | 51.80 ms | Parallel inter-process actor relay (`dev_push`) |
| **`cold_hydrate` (State Hydration)** | 20 | 42.34 ms | **50.70 ms** | 66.09 ms | 69.76 ms | 69.76 ms | Full-state hydration read transfer |
| **`read_push` (Primary Snapshot)** | 60 | 81.21 ms | **86.76 ms** | 99.56 ms | 111.93 ms | 224.34 ms | Full state snapshot read (`11,891 B` p50 response) |
| **`sd_direct` (Direct SD LMDB Read)** | 60 | 90.19 ms | **100.66 ms** | 126.72 ms | 142.43 ms | 242.76 ms | Raw SD LMDB node read (`11,296 B` p50 response) |

> **Baseline vs. Final Optimization Gains:** Across optimization waves (WASM `ALLOC_MAGIC` header allocator, ETS `ProcessJson` cache, `hb_store_volatile` `store_ms: 0`, lazy `sync_head_points`, parallel `dev_push`, and trimmed `device-stack`), raw latency dropped **`3.22x` on `write_202`** (`207 ms → 64.21 ms`), **`2.26x` on `write_confirm`** (`620 ms → 274.05 ms`), **`2.21x` on `advance`** (`203 ms → 91.86 ms`), and **`3.58x` on `relay_call_sd`** (`151 ms → 42.15 ms`).

### 2. Raw Serial Quorum Floor, Open-Loop Sweep & Node Footprint (`tools/bench_baseline.json`)

```text
======================= BENCHMARK CLUSTER SUMMARY ==================
  Topology Mode               : Single-World Rust WASM (`primary`, `sd1`, `sd2`) + Lua (`genesis`, `arbit`)
  flush_serial (24 writes)    : 7.81 confirmed/s across 8 writers (24/24 HTTP 200, 3.07s elapsed, p50 969.51 ms, min 526.44 ms)
  Open-Loop Sweep (no flush)  : 50 / 100 / 200 offered/s -> 26.16 / 28.20 / 23.34 accepted/s (202, queue_peak = 8)
                                Sweep p50 latency: 290.76 ms / 278.93 ms / 339.75 ms (all bursts drained cleanly)
  Computed Slot Footprint     : primary = 49,545 B  |  sd1 = 25,875 B  |  sd2 = 25,875 B
  Node RAM RSS                : primary = 200.3 MB  |  sd1 = 188.5 MB  |  sd2 = 178.6 MB  |  genesis = 130.6 MB
  LMDB Disk Size (post-bench) : sd1 = 30.0 MB | sd2 = 26.0 MB | arbit = 3.0 MB | primary/genesis = 1.0 MB
  Converged Cluster State     : tick = 404 | entities = 153 | roots_equal = true
  Canonical State Root        : 0x76d97aeb49a23545d9eed040d9333b5fa7c062f2d33c5986d3c9da5424f02ca2
====================================================================
```

* **Zero Desync Under Burst Load:** At completion of all benchmark paths (`tick 404`, `153` entities), `primary`, `sd1`, and `sd2` converged with `roots_equal: true`.

### 3. Live Circle Test Performance & Clarity (120-Bot Stress Run @ `20 ms`)

Tested interactively on the live web client (`http://127.0.0.1:8080`) with **120+ moving bots**, **`Move interval: 20 ms`**, **`Step size: 30 px`**, and **`Max in-flight writes: 2`**:

| Live Circle Test Metric | Observed Result | Engineering Explanation |
| :--- | :--- | :--- |
| **Requested Write Rate** | **`9,400.0 w/s`** | Client swarm target cadence across active bot entities (`n * 1000 / 20 ms`). |
| **Achieved (ACKed) Throughput** | **`300.0 w/s`** | `~3 batches/s × 100 ent/batch`. Browser `pumpWorker()` caps batches at `100` entities (`index.html`), while `Primary` processes 1 quorum batch at a time (`~274–333 ms` cycle). |
| **Total ACKed / Errors** | **`93,712 / 0`** | **0% error rate** across nearly 100k sustained L1-confirmed entity mutations. |
| **Coalesced (Skipped)** | **`677,536`** | Client ring-buffer (`wp.queue > 500`) drops superseded moves before sending (`Pipeline Queue: 501 queued`), protecting server `MAX_QUEUE = 4096` (`Compacted Discards: 0`). |
| **Batch Train Size** | **`100 ent/batch`** | Client UI cap (`batchItems.length < 100`); engine `Primary` itself supports up to `MAX_BATCH_INPUTS = 256` per batch (`~750 w/s` headroom at the same batch rate). |
| **Avg Write RTT / L0 RAM Ingest** | **`286 ms` avg** (`136–297 ms` L0) | End-to-end browser `Write-Batch` (`flush: true`) round-trip into Primary L0 RAM under 2/2 pipelined load. |
| **L1 Quorum ACK (UI Polling)** | **`1,694 ms` – `2,098 ms`** | Browser L1 HUD telemetry includes waiting behind `Active In-Flight: 2 / 2` in `Primary`'s serial quorum lock + `pollNow()` (`80 ms`) `/now` confirmation cadence—not raw single-batch latency (`274.05 ms` p50). |
| **Dual-Watermark & Quorum** | **`#2100 (Head: #2104)` · `Lag: 4 ticks`** | With `2` concurrent client batches and `TICKS_PER_BATCH = 2`: 1 batch is `in_flight` to `sd1+sd2 [LMDB]` (+2 ticks) and 1 batch waits in queue (+2 ticks). |

### 4. What This Final Result Proves

1. **Verifiable Multi-Node WASM State Execution Works on Consumer Hardware:** On a single 4-core/4-thread CPU (`i5-6500`) with only **`3.8 GiB` WSL2 RAM** running 5 HyperBEAM BEAM VMs + ArLocal concurrently, HyperBEAM + Rust WASM delivers **`~10.3 ms` RAM reads**, **`~64.2 ms` L0 ingest**, and **`~274.1 ms` cryptographic 2-of-3 LMDB quorum commits** with **100% state-root parity**.
2. **Sustained Overload Does Not Break Consistency or Crash Nodes:** Pushing **`9,400.0 w/s`** through the Circle Test produced **`93,712` ACKed writes with `0` errors and `0` server queue discards**, proving client ring-buffer coalescing + `Primary` batching absorbs extreme burst traffic cleanly.
3. **Clear Software-Level SD Quorum Persistence Contract:** `Primary` executes head writes in volatile RAM (`hb_store_volatile`) while `SD1 + SD2` run deterministic WASM verification and store state slots in **`hb_store_lmdb` (`sync: False`, `snapshot_slots = 20`)**. This protects against single-process/node crashes and forged ACKs (`409`) without paying physical `fsync` stalls on every slot, leaving permanent cryptographic anchoring to the native `hb-arbit` `~l1-checkpoint@1.0` (`tx@1.0` + `arweave@2.9`) → ArLocal L1 pipeline.
4. **Empirical Architectural Ceiling Is Exactly Identified:** The bottleneck is **not** Rust WASM execution (`~12–20 ms`), nor raw reads (`10.33 ms`). The ceiling (~3–3.6 batches/sec, `300.0 w/s` at `100 ent/batch`) comes from **(a)** `QuorumState::first_pending()` enforcing a strict **1-in-flight batch lock** between `Primary` and `SD`, **(b)** HyperBEAM HTTP/Erlang slot orchestration per quorum hop, and **(c)** **single-world execution (`1/1`)**. Moving past this milestone cleanly points to multi-batch `Primary ↔ SD` pipelining and horizontal multi-world sharding.

---

## Architectural Evolution: Why the Architecture Changed

During profiling and remediation, the stack eliminated runtime bottlenecks and premature 10-world sharding in favor of a **Single-World Rust WASM (`wasm-64@1.0`) + Fast-Path HyperBEAM** architecture:

```text
+--------------------------------------------------------------------------------------------------+
|                                    ARCHITECTURAL EVOLUTION                                       |
+------------------------------------------------+-------------------------------------------------+
| PREVIOUS BOTTLENECKS (Identified & Removed)    | CURRENT PRODUCTION ARCHITECTURE (Verified)      |
+------------------------------------------------+-------------------------------------------------+
| 1. Premature 10-Shard World Loop:              | 1. Consolidated Single-World Topology:          |
|    `build_configs.py` & `index.html` split state|   One authoritative Primary (`:8734`) paired    |
|    across `primary_1..10`, `sd1_1..10`,        |   with `sd1` (`:8735`), `sd2` (`:8736`),        |
|    `sd2_1..10`, fragmenting batches & swarm.   |   `genesis` (`:8733`), and `arbit` (`:8737`).   |
+------------------------------------------------+-------------------------------------------------+
| 2. WASM Memory Leaks & Duplicate Payloads:     | 2. Header-Backed WASM Allocator & Slim JSON:    |
|    No-op `wasm_free` leaked linear memory on   |    16-byte header allocator (`ALLOC_MAGIC`) frees|
|    every call, plus duplicate `data` fields &  |    inputs/outputs cleanly; `handle_ack` omits   |
|    O(N) `points` blobs in `handle_ack`.        |    O(N) arrays and adds `http_status` + `epoch`.|
+------------------------------------------------+-------------------------------------------------+
| 3. Bundler & Cache Cliff Penalties:            | 3. Targeted HyperBEAM Runtime Fast-Paths:       |
|    Dead `127.0.0.1:1` bundler uploads fired    |    Skipped dead uploads in `dev_scheduler_server`|
|    on every slot; ETS caches wiped all keys on |    bounded `4096/256` ETS eviction; `dev_process` |
|    threshold; sequential outbox + HTTP proxy.  |    `/now` ETS fast-path; parallel `dev_push` &  |
|                                                |    unsigned `dev_json_iface` + keep-alive proxy.|
+------------------------------------------------+-------------------------------------------------+
```

---

## Current Multi-Tier Architecture Diagram

```text
                  +-----------------------------------------------+
                  |              1. GENESIS PROCESS               |
                  |       (hb-genesis :8733 | AO Scheduled)       |
                  |  Constitutional discovery & epoch authority   |
                  +-----------------------------------------------+
                                          |
                                          | 1-Hop Dynamic Discovery
                                          v
+------------------------+  Batch Pipeline Trains   +-----------------------------------------------+
|      GAME CLIENT       | ======================> |              2. PRIMARY NODE (:8734)           |
| (Red Circle WASD UI)   |                         |   Single-World Rust WASM (`primary_engine`)   |
| * Dual-Watermark HUD   | <====================== |   Devices: json-iface@1.0 + wasm-64@1.0       |
| * Real-time Telemetry  |      Confirmed ACKs     |   Quorum Gate, Epoch Guard & Head/Disk State  |
| * Zero Guessing        |    (verified 200 only)  |   Storage: Volatile RAM (hb_store_volatile)   |
+------------------------+                         +-----------------------------------------------+
                                                                  |                 |
                                                     Sync Quorum  |                 | Async Hot Standby
                                                     Commit       |                 | Replication
                                                                  v                 v
                                 +------------------------------+   +------------------------------+
                                 |      3A. SHARED DATA 1       |   |      3B. SHARED DATA 2       |
                                 |  (hb-sd-1 :8735 | Rust WASM) |   |  (hb-sd-2 :8736 | Rust WASM) |
                                 |  Verified disk state root    |   |  Quorum witness & replica    |
                                 |  Storage: LMDB (hb_store_lmdb)|  |  Storage: LMDB (hb_store_lmdb)|
                                 +------------------------------+   +------------------------------+
                                                |
                                                | Async Checkpoint (every 100 ticks)
                                                | via ~relay@1.0/cast (non-blocking)
                                                v
                                 +----------------------------------------------+
                                 |               4. ARBIT PROCESS               |
                                 |         (hb-arbit :8737 | LMDB Consensus)    |
                                 |   Anti-double-spend ledger & state roots     |
                                 +----------------------------------------------+
                                                        |
                                                        | Native ~l1-checkpoint@1.0 (tx@1.0 + arweave@2.9) + GET /mine
                                                        v
                                 +----------------------------------------------+
                                 |            5. ARWEAVE L1 PERMAWEB            |
                                 |            (arlocal :1984 | Docker)          |
                                 |   Decentralized permanent settlement ledger  |
                                 +----------------------------------------------+
```

### Core Invariants

1. **Zero Hardcoded Peer URLs:** All nodes dynamically resolve peer topology from Genesis in 1 hop.
2. **Honest 2-of-3 Replication Gate:** Writes are confirmed only after Primary + one SD agree on `batch_id`, tick, and canonical state root. Quorum failure strictly returns `503` without state mutation.
3. **Canonical Sharded State Root:** 32 FNV-1a shards; commits re-hash only mutated shards, avoiding expensive $O(N)$ full-state re-hashing.
4. **Dual-Watermark Guarantee:** The client tracks both the optimistic sub-millisecond RAM Head (`Head Tick`) and the fully verified disk state (`Replicated Tick`).
5. **Authoritative Relog Recovery:** State on reconnection or browser refresh is re-hydrated directly from SD1/Primary authoritative state, preventing desynchronized client-side drift.

---

## Node Topology

The cluster operates cleanly across 6 isolated execution nodes with zero external Python signing sidecars:

| Node / Service | Port | Process Name | Role & Storage Mode |
| :--- | :--- | :--- | :--- |
| **`arlocal`** | `:1984` | Arweave L1 Gateway | Docker container running native ArLocal permaweb gateway. |
| **`hb-genesis`** | `:8733` | `genesis` (Lua `lua@5.3a`) | Constitutional bootstrap authority; advertises topology and cluster epoch on LMDB. |
| **`hb-primary`** | `:8734` | `primary` (WASM `wasm-64@1.0`) | Single-world 2D state engine; volatile RAM (`hb_store_volatile`), L0 ingress & 2-of-3 quorum coordinator. |
| **`hb-sd-1`** | `:8735` | `sd1` (WASM `wasm-64@1.0`) | Shared Data Replica 1 on LMDB disk (`hb_store_lmdb`); primary quorum confirming replica. |
| **`hb-sd-2`** | `:8736` | `sd2` (WASM `wasm-64@1.0`) | Shared Data Replica 2 on LMDB disk (`hb_store_lmdb`); quorum witness and failover replica. |
| **`hb-arbit`** | `:8737` | `arbit` (Lua `lua@5.3a` + `~l1-checkpoint@1.0`) | Consensus gatekeeper, native Arweave Format 2 TX signer/settler (`tx@1.0` + `arweave@2.9`), and slot pruning coordinator on LMDB. |

---

## Rust WASM Execution & HyperBEAM Runtime Fast-Paths

Instead of executing high-frequency game mutations inside an interpreted Lua VM or maintaining an unmaintained custom Erlang NIF device, HyperStack executes deterministic state transitions inside compiled **Rust WebAssembly** modules (`wasm32-unknown-unknown`) on HyperBEAM's native `~json-iface@1.0` + `~wasm-64@1.0` stack (`primary_engine.wasm` & `sd_engine.wasm`), backed by targeted BEAM runtime optimizations.

### 1. Rust WASM Process Architecture ([`engine/processes/`](engine/processes/))

```text
+----------------------------------------------------------------------------------------------------+
|                         RUST WASM ENGINE ARCHITECTURE (wasm-64@1.0 + json-iface@1.0)               |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|  1. HEADER-BACKED WASM ALLOCATOR (`ALLOC_MAGIC` 16-Byte Header)                                    |
|     * `wasm_malloc` / `wasm_free` manage linear WASM memory safely across `handle()` calls.        |
|     * Frees `proc_ptr`, `msg_ptr`, and prior `LAST_RES_PTR` on every transition—zero memory leak.  |
|                                                                                                    |
|  2. SLIM ACK & JSON OUTPUT CONTRACT                                                                |
|     * Omits redundant wrapper `data` envelopes; returns canonical JSON state directly.             |
|     * `handle_ack` returns `{status, batch_id, tick, state_root, http_status:200, epoch}` without  |
|       copying O(N) `points` or `head_points` arrays on every replica confirmation.                 |
|                                                                                                    |
|  3. DYNAMIC GENESIS TOPOLOGY SYNCHRONIZATION (`SyncTopology`)                                      |
|     * Primary (`primary/src/ingress.rs`) & SD (`shared_data/src/ingress.rs`) accept live           |
|       `sd_urls`, `arbit_url`, and `primary_url` topology updates without hardcoded cluster URLs.   |
|                                                                                                    |
|  4. HONEST QUORUM, EPOCH GUARD & OUTBOX REPLAY (`primary/src/quorum.rs`)                           |
|     * Requires matching `batch_id`, integer `tick`, canonical 32-byte `state_root`, and `epoch`.   |
|     * Rejects forged ACKs with `409 ack_mismatch` / `409 epoch_mismatch`; returns `503` on loss.   |
|                                                                                                    |
+----------------------------------------------------------------------------------------------------+
```

### 2. Primary Action Contract (`POST /push` via `~json-iface@1.0`)

| Action / Path | Role & Description |
| :--- | :--- |
| `Write-Point` / `Write-Batch` | Ingests 2D point updates into Primary L0 RAM with client-signer ownership & bounds checks. |
| `Advance-Batch` | Closes pending batch, advances tick, computes canonical root, and dispatches fan-out to SD1/SD2. |
| `Ack` | Verifies replica `batch_id`, `tick`, `state_root`, and `epoch`; promotes L1 confirmation on 2-of-3 quorum. |
| `Sync-Topology` | Dynamically configures `sd_targets`, `arbit_url`, or replica `primary_url` after Genesis discovery. |
| `Read-Point` / `Read-All` / `Snapshot` | Reads entity state directly from Primary or Replica canonical tables. |
| `GET /~process@1.0/now/...` | Fast RAM read of current live process state without allocating a new scheduler slot. |

### 3. Targeted HyperBEAM Runtime Optimizations ([`HyperBEAM/src/`](HyperBEAM/src/))

- **Live `/now` ETS Fast-Path ([`dev_process.erl`](HyperBEAM/src/preloaded/process/dev_process.erl)):** Serves unsigned read-only `/now` queries directly from per-process ETS live state (`dev_process_live_state`) without waiting in scheduler slot resolution.
- **Unsigned JSON Interface Fast-Path ([`dev_json_iface.erl`](HyperBEAM/src/preloaded/codec/dev_json_iface.erl)):** Encodes unsigned `prep_call/3` JSON payloads directly via `hb_json:encode` with safe fallback to the general path, plus ETS `ProcessJson` caching.
- **Parallel Push Fan-Out ([`dev_push.erl`](HyperBEAM/src/preloaded/process/dev_push.erl)):** Dispatches multi-target outbox relays concurrently with `spawn_monitor` while preserving deterministic key ordering.
- **Zero Dead Bundler Uploads ([`dev_scheduler_server.erl`](HyperBEAM/src/preloaded/process/dev_scheduler_server.erl)):** Guards `upload/2` spawns so local non-authority runs skip invalid `127.0.0.1:1` bundler targets.
- **Bounded ETS Eviction & Volatile Fast-Write ([`dev_scheduler_cache.erl`](HyperBEAM/src/preloaded/process/dev_scheduler_cache.erl), [`lib_process_cache.erl`](HyperBEAM/src/preloaded/process/lib_process_cache.erl), [`lib_process.erl`](HyperBEAM/src/preloaded/process/lib_process.erl)):** Replaces full-table `ets:delete_all_objects` latency cliffs with sliding `(Slot - 4096)` / `256`-entry incremental eviction, and avoids redundant store overhead on volatile nodes.

---

## Quick Start Guide

### Prerequisites
- Linux or WSL2 (Ubuntu 22.04+ / 26.04 LTS)
- Erlang/OTP 26+ (tested on OTP 27 `erts-15.2.7.4`) and Rebar3
- Rust toolchain with `wasm32-unknown-unknown` target (`rustup target add wasm32-unknown-unknown`)
- Docker (for ArLocal)
- Python 3.10+ (standard library for tools/tests/UI proxy)
- `tmux` and `psmisc` (`fuser`)

### 1. Clone Repository & Setup
```bash
git clone --recurse-submodules https://github.com/Wesslurr/database-stack-on-permaweb.git
cd database-stack-on-permaweb
```

### 2. Launch Multi-Node Cluster
Launch ArLocal in Docker and all 5 HyperBEAM nodes in independent tmux sessions:
```bash
./tools/cluster_up.sh
```
All nodes will initialize and run automated self-health checks.

### 3. Launch Interactive Game Client & Live Telemetry
Start the dedicated HTTP client server:
```bash
python3 tools/game_client/serve.py
```
Open your browser at **`http://127.0.0.1:8080/`**.

- Move the red circle using **W / A / S / D** or click inside the arena.
- Run the **Swarm Simulation** (+35 Bots, +100 Bots, or **🚀 120 Bots Turbo**).
- Observe real-time Round-Trip Time (RTT), Dual-Watermark ticks, and the **Backstage Multi-Tier Inspector** displaying live RAM, CPU, and LMDB disk growth.

### 4. Hard Reset All Circle Test & Cluster Data (Back to Tick #0)
To completely wipe all LMDB stores (`genesis`, `primary`, `sd1`, `sd2`, `arbit`), ArLocal chain state, and runtime logs so the Circle Test starts cleanly from Tick `#0` with `0` bots:
```bash
python3 tools/reset_hard.py --yes --with-ui
```
(Omit `--with-ui` to skip launching `serve.py`, or pass `--no-start` to stop and wipe without restarting nodes. Preserves node `wallet.json` files by default.)

### 5. Stop Cluster
Gracefully shut down all tmux node sessions and stop ArLocal:
```bash
./tools/cluster_down.sh
```

---

## Automated Verification & Test Suite

The test suite runs against the **live local cluster** and enforces strict contract invariants:

### 1. Contract E2E Test (30/30 Checks Passing)
Verifies ACK contracts, 202 vs 200 semantics, idempotent deduplication, ownership guards, and Merkle root convergence across all nodes:
```bash
python3 tests/test_e2e_2d_points.py
```

### 2. Cluster Interconnect & Identity Test (34/34 Checks Passing)
Verifies ArLocal mining, distinct cryptographic node wallets, and direct HTTP round-trip connectivity between all nodes:
```bash
./tests/test_e2e_connectivity.sh
```

### 3. Destructive Failure-Injection Test (36/36 Checks Passing)
Simulates node crashes via tmux: quorum failure `503`, forged ACKs `409`, SD1 failover to SD2, and delta-log rehydration:
```bash
python3 tests/test_failure_modes.py --yes
```

### 4. Mutation Test (Enforces Rigorous Test Guards)
Deliberately corrupts the Primary's ACK root/tick verification and ensures the test suite catches the mutation and goes red:
```bash
./tests/mutation_ack.sh
```

---

## Repository Structure

```text
├── engine/
│   ├── config/              # Single-world node manifests (genesis, primary, sd1, sd2, arbit)
│   ├── data/                # Wallets, LMDB disk environments (data.mdb), and node logs
│   └── processes/           # Process implementations (Rust WASM + Lua authority scripts)
│       ├── primary/         # Primary WASM engine (`src/{lib,ingress,quorum,optimistic_state,...}.rs` -> `primary_engine.wasm`)
│       ├── shared_data/     # Shared Data WASM replica (`src/{lib,ingress,verifier,...}.rs` -> `sd_engine.wasm`)
│       ├── genesis/         # Genesis constitutional authority Lua process
│       └── arbit/           # Arbit consensus & finality Lua process
├── HyperBEAM/               # HyperBEAM Erlang runtime with preloaded runtime fast-paths & `~l1-checkpoint@1.0`
├── tests/
│   ├── _harness.py          # Live-cluster test harness and contract invariants
│   ├── test_e2e_2d_points.py# 30-check ACK contract E2E test suite
│   ├── test_e2e_connectivity.sh # 34-check cluster interconnect and wallet suite
│   ├── test_failure_modes.py# 36-check destructive failure-injection suite
│   └── mutation_ack.sh      # Mutation test enforcing Primary ACK root/tick verification
├── tools/
│   ├── apply_engine.sh      # Compile WASM + HyperBEAM, rebuild single-world configs, restart cluster (`FRESH=1` wipes stores)
│   ├── bench_engine.py      # 8-path latency & throughput baseline benchmark -> tools/bench_baseline.json
│   ├── build_configs.py     # Deterministic single-world configuration builder
│   ├── client.py            # HyperStack Python client with 2ms polling & `/now` confirmation fast-path
│   ├── cluster_up.sh        # Automated cluster launcher (Docker ArLocal + 5 HyperBEAM Tmux nodes)
│   ├── cluster_down.sh      # Cluster teardown script
│   ├── reset_hard.py        # Full hard data reset tool (wipes LMDB/ArLocal/logs back to Tick #0)
│   ├── simulate_gameplay.py # Automated gameplay movement simulator
│   ├── test_profile_detailed.py # Detailed GET `/now` and POST `/push` latency profiler
│   ├── test_relog_flow.py   # Client relog and state rehydration test
│   └── game_client/
│       ├── index.html       # Single-world interactive WASD client & Swarm simulator
│       └── serve.py         # HTTP proxy with persistent keep-alive pool & `/metrics` telemetry
├── start.sh                 # All-in-one WASM builder, HyperBEAM launcher, and Web UI starter
├── idea.md                  # Comprehensive architectural specification and design blueprint
└── README.md                # Project documentation, benchmarks, and architecture guide
```

---

## License

This project is open-source under the Apache-2.0 License.
