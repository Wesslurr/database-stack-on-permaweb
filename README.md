# HyperStack: Real-Time State Engine

> **High-Frequency Real-Time State Engine & Distributed Replication Stack on Unmodified HyperBEAM, AO Compute & Arweave Permaweb**

[![Project](https://img.shields.io/badge/Project-HyperStack-orange)](https://github.com/Wesslurr/database-stack-on-permaweb)
[![HyperBEAM](https://img.shields.io/badge/HyperBEAM-Unmodified%20Upstream%20(Forge%20Devices)-red)](https://github.com/permaweb/hyperbeam)
[![Permaweb](https://img.shields.io/badge/Arweave-Permaweb%20L1%20(Format%202)-black?logo=arweave)](https://arweave.org/)
[![AO Compute](https://img.shields.io/badge/AO-Compute%20WASM%20%2B%20Lua-blue)](https://ao.g8way.io/)
[![Tests](https://img.shields.io/badge/Verified%20Tests-30%2F30%20E2E%20%2B%2036%2F36%20Failure%20%2B%20L1-brightgreen)](tests/)
[![Latency](https://img.shields.io/badge/p50%20Latency-10.49ms%20%2Fnow%20%7C%2032.64ms%20L0%20%7C%2086.21ms%20L1-blueviolet)](#honest-empirical-performance-benchmarks)
[![Quorum](https://img.shields.io/badge/Quorum-Honest%202--of--3%20LMDB%20Verified%20ACK-success)](#honest-empirical-performance-benchmarks)

---

> [!IMPORTANT]
> ### Architectural Pivot, Distributed Portability & Empirical Honesty Notice
> **HyperStack** runs **100% on unmodified upstream [`HyperBEAM/`](HyperBEAM)** (`nothing to commit, working tree clean`), isolating every execution fast-path, persistent WASM actor, outbox relay pipeline, and Arweave L1 settlement bridge inside **3 packaged external HyperBEAM Forge devices** under [`engine/devices/`](engine/devices/).
>
> - **Why Move Out of `HyperBEAM/src`:** Earlier milestones (`3b3e48c` / `b77a166`) patched 11 internal core/preloaded files inside `HyperBEAM/src` (`dev_stack`, `dev_json_iface`, `dev_process`, `lib_process`, `dev_scheduler`, `hb_beamr_io`, plus volatile storage) to reach `64.21 ms` L0 `write_202` p50. Moving those paths into external Forge devices broke portability zero times, while initially raising `write_202` p50 to `203.88 ms` (`735.41 ms` L1) when upstream's uncached 4-device stack ran on every slot.
> - **Persistent Actor + WAL + Pipelined Outbox Fast Path (`hs-wasm-output@1.0` + `hs-push@1.0`):** Adding persistent warm actor execution (`HSCKPT2` checkpoint + `.wal` frame persistence on cold boot), exact `hb_maps:get` ingress coalescing, `/now` LMDB slot tracking, and pipelined `~relay@1.0/call` outbox lanes cut idle/low-contention latency to **`32.64 ms` L0 `write_202` p50** and **`86.21 ms` L1 quorum `write_confirm` p50** (`10.49 ms` `/now` p50) — without touching `HyperBEAM/`.
> - **Honest Throughput Scaling Caveat (35+ Entities Under Heavy Load):** Single-batch / low-contention latency is very fast (`32.6 ms` L0 / `86.2 ms` L1), **but latency is not constant under heavy multi-entity saturation**. Under sustained **35-entity concurrent `Write-Batch` saturation** (4 parallel writers saturating the single-world actor + 2-of-3 LMDB quorum), L1 confirmation rises to **`661.25 ms` p50 / `746.19 ms` p90** — and **~750–800+ ms end-to-end in the live Circle Test UI** once browser proxy & `/now` polling intervals are included.
> - **Software-Level 2-of-3 LMDB Quorum Persistence Contract:** An HTTP `200` (`ack: "replicated"`) is returned **only after Primary + authoritative Shared Data replicas (`sd1`, `sd2`) deterministically verify the WASM transition, persist to `hb_store_lmdb`, and match `batch_id`, `tick`, epoch, and canonical `state_root`**. Quorum loss returns `503` without mutating confirmed state, and forged `Write-Batch` entity owners are rejected with `403 error_unauthorized_owner`.
> - **100% Localhost Multi-Node Topology:** All 6 nodes (`:1984`, `:8733`–`:8737`) and the interactive web proxy (`:8080`) run locally over loopback (`127.0.0.1`) without WAN latency or packet loss.

---

## Hardware & Environment Test Specifications

All current micro-benchmarks (`BENCH_N=60`), E2E checks, Arweave L1 settlement tests, and Circle Test stress runs were executed on the following consumer test machine:

| Parameter | Verified Test Machine & Runtime Specification |
| :--- | :--- |
| **Physical Host OS** | Microsoft Windows 10 Pro (64-bit) |
| **Processor (CPU)** | Intel(R) Core(TM) i5-6500 CPU @ 3.20 GHz — **4 Physical Cores / 4 Threads** (x86_64) |
| **Memory (RAM)** | **8 GB Physical Host RAM** (`7.9 GiB` usable; **`3.8 GiB`** allocated to WSL2 guest) |
| **Primary Disk** | `256 GB` 2.5" SATA SSD |
| **Linux Subsystem** | WSL2 (`Ubuntu 26.04 LTS`, Linux Kernel `6.18.58.1-microsoft-standard-WSL2` x86_64) |
| **Erlang / OTP Runtime** | **Erlang/OTP 27** (`erts-15.2.7.4`), 5 independent BEAM VMs (`+S 4:4 +sbwt none +sbwtdcpu none +sbwtdio none +A 8 +MBas aobf`, auto-clamped `2..4` schedulers by `nproc`) |
| **WASM & Tooling** | Rust (`wasm32-unknown-unknown`), Python 3, Docker, tmux |
| **HyperBEAM Runtime** | **Unmodified upstream `HyperBEAM` submodule** (`acb90b14`), loading 3 external Forge devices |
| **Storage Engines** | All 5 nodes use `hb_store_lmdb` (`capacity: 4 GiB`, `sync: False`, `read-ahead: False`, `flush-every: 0`) + `cache-control: ["no-cache", "no-store"]`. WASM nodes mount read-only `hb_store_fs` (`engine/data/wasm_store`); `hb-arbit` mounts `hb_store_gateway` for live Forge artifact fetches from `hb-genesis`. |
| **Settlement Layer (L1)** | ArLocal Docker Container (`:1984`) + hot-loaded Forge device `~l1-checkpoint@1.0` on `hb-arbit` (`:8737`, signing Format 2 TX via `tx@1.0` + `arweave@2.9`) |
| **Transport & Client** | HTTP/1.1 Cowboy REST + keep-alive proxy (`tools/game_client/serve.py` on `:8080`) |

---

## Custom HyperBEAM Forge Devices (`engine/devices/`)

Instead of modifying [`HyperBEAM/src`](HyperBEAM/src), all HyperStack-specific behavior lives in three clean Erlang devices packaged with HyperBEAM's native Forge (`rebar3 device package` / `rebar3 device verify`) and loaded by unmodified HyperBEAM nodes:

| Custom Device | Source & Delivery Model | Execution Role & Problems Solved |
| :--- | :--- | :--- |
| **`hs-wasm-output@1.0`** | [`dev_hs_wasm_output.erl`](engine/devices/hs_wasm_output/src/dev_hs_wasm_output.erl)<br>Pre-packaged via [`prepare_wasm_store.escript`](tools/prepare_wasm_store.escript) into `wasm_store` + registered in node `trusted-devices` (`primary`, `sd1`, `sd2`) | **Unified WASM `execution-device`**. Collapses upstream `stack@1.0` + `multipass@1.0` + `json-iface@1.0` + `wasm-64@1.0` into a single pass while keeping core HyperBEAM intact:<br>- **`fast_opts/1`**: sets `hashpath => ignore` and `resolve-mode => raw` on hot slots.<br>- **Process JSON Cache**: caches serialized `Process` metadata in ETS (`hs_wasm_proc_cache`) + `hb_private` instead of re-hashing `ProcDef` every slot.<br>- **Fast Message Encoding & Direct Call**: `fast_msg_json/6` (with `slow_msg_json/6` fallback) writes inputs directly and invokes `hb_beamr:call(Inst, "handle", ...)`.<br>- **16 KiB Chunked Memory Reads (`read_c_string/2`)**: uses `hb_beamr_io:size/1` + `hb_beamr_io:read/3` (`16,384 B` chunks), replacing upstream's uncached 8-byte IPC read loop.<br>- **Warm Instance Pool (`hs_wasm_pool`)**: reuses persistent `hb_beamr` ports across slots (`1` active port per WASM node instead of spawning/leaking C ports). Delegates `init`/cold `normalize` via `hb_ao:resolve` to `wasm-64@1.0` and calls `hb_beamr:serialize/1` on `snapshot/3`. |
| **`hs-push@1.0`** | [`dev_hs_push.erl`](engine/devices/hs_push/src/dev_hs_push.erl)<br>Pre-packaged into `wasm_store` + registered in node `trusted-devices` (`primary`, `sd1`, `sd2`) | **Outbox Relay & Pool-Release `push-device`**. Wraps unmodified upstream `push@1.0` to handle Rust WASM outbox entries that specify `resolve` routes without a fixed `target` process:<br>- Executes `~relay@1.0/cast` replication/ACK broadcasts asynchronously (`async_cast`) so Primary/SD pushes do not stall waiting on downstream cast responses.<br>- Executes multi-target synchronous `resolve` outbox messages in parallel while stripping upstream `404` stubs for resolved keys.<br>- Always invokes `release_wasm_caller/0` to return the active WASM instance back to `hs_wasm_pool` immediately after `push/3`. |
| **`l1-checkpoint@1.0`** | [`dev_l1_checkpoint.erl`](engine/devices/l1_checkpoint/src/dev_l1_checkpoint.erl)<br>**Zero-restart live Forge deployment** published to `hb-genesis` by [`deploy_forge_device.py`](tools/deploy_forge_device.py) & hot-loaded by `hb-arbit` over `/~<SpecID>` | **Native Arweave Format 2 L1 Settlement Device** running on `hb-arbit` (`:8737`, gated by `l1-checkpoint-enabled: true` + `trusted-device-signers`):<br>- Dynamically discovers `l1_url` from `hb-genesis` (zero hardcoded L1 endpoints).<br>- Auto-mints/funds the Arbit node wallet on ArLocal (`localhost` testing) and fetches `tx_anchor` via `arweave@2.9`.<br>- Constructs and cryptographically signs Arweave Format 2 checkpoint transactions (`App-Name: HyperStack`, `Type: Checkpoint`, `Official-Tick`, `State-Root`, `Point-Count`, `Epoch`, `Shard`) using `tx@1.0` with the node's own JWK wallet.<br>- Submits TX via `arweave@2.9` and triggers `/mine` on ArLocal so L1 checkpoints finalize immediately. |

---

## Architecture Problems, Distributed Trade-Offs & Remaining Ceiling

Moving customizations out of [`HyperBEAM/src`](HyperBEAM/src) solved distributed maintainability while restoring sub-35 ms L0 accepts on single-world bursts, **but real architectural trade-offs and scaling limits remain**:

1. **Fast Idle/Low-Contention Floor (`32.64 ms` L0 / `86.21 ms` L1) vs. Saturated Multi-Entity Latency (`~660–800+ ms` for 35 Entities):**
   - Consolidating `stack@1.0 + multipass@1.0 + json-iface@1.0 + wasm-64@1.0` + persistent actor pooling + `HSCKPT2`/WAL frames + pipelined `~relay@1.0/call` lanes inside **`hs-wasm-output@1.0`** + **`hs-push@1.0`** cut isolated L0 accept (`write_202`) from `203.88 ms` → **`32.64 ms p50`** and L1 quorum confirmation (`write_confirm`) from `735.41 ms` → **`86.21 ms p50`**.
   - **Why latency climbs to ~800 ms at 35+ entities under heavy throughput:** `primary` runs as a **single authoritative sequential WASM actor** coordinating deterministic 2-of-3 LMDB quorum replication (`Primary` ↔ `SD1`/`SD2`) over pipelined HTTP calls. When 35 active entities hammer the cluster simultaneously at high cadence (`Write-Batch` bursts from Circle Test Swarm), request serialization, queueing behind active quorum commits, multi-entity JSON encoding/compaction, and replication round-trips push end-to-end L1 confirmation from the `~86 ms` idle floor up into **`661.25 ms` p50 / `746.19 ms` p90** (~**800+ ms** observed inside the Circle Test browser HUD with `/proxy/` and `/now` polling).
2. **Memory Stability on Constrained Hosts (`3.8 GiB` WSL2 VM + 5 BEAM VMs):**
   - Switching all 5 nodes from `hb_store_volatile` to **`hb_store_lmdb` (`capacity: 4 GiB`, `sync: False`, `read-ahead: False`, `flush-every: 0`)** plus `HSZ1`/`HSCKPT2` checkpoint compression + `.wal` frame persistence kept BEAM RSS stable (~**`163.9 MB` RSS** on `hb-primary`, **`143.0 MB`** on `hb-sd-1`, **`142.4 MB`** on `hb-sd-2`, **`13.0 MB`** LMDB disk on `primary` post-bench, `1` active `hb_beamr` C-port per WASM node) with zero crashes (`0` `CRASH REPORT`, `0` corrupt entity IDs).
3. **Single-World Quorum Gate & Honest Backpressure:**
   - `QuorumState` inside `primary` tracks ordered pending quorum commits with `pump_repair_with_ts` auto-retrying stalled batches without falsifying `p.expired` (`503` remains honest when quorum is genuinely lost). Breaking far beyond single-world saturated throughput without raising tail latency requires true independent multi-world sharding.

---

## Honest Empirical Performance Benchmarks

### 1. Raw Micro-Benchmarks (`BENCH_N=60`, Option A Forge Devices on Unmodified `HyperBEAM`)

Measured on a fresh cluster (`BENCH_N=60 python3 -u tools/bench_engine.py`, keep-alive HTTP/1.1 on `127.0.0.1`, saved in [`tools/bench_baseline.json`](tools/bench_baseline.json)):

| Operation Path | Samples (`n`) | min | **p50 (Current)** | p90 | p99 | max | Pre-Option-A Forge Stack (p50) | Patched-Core Historical `3b3e48c` (p50) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **`/now` (RAM State Read)** | 60 | 8.76 ms | **10.49 ms** | 12.16 ms | 14.31 ms | 14.56 ms | 9.13 ms | 10.33 ms |
| **`write_202` (L0 Ingest Accept)** | 60 | 29.43 ms | **32.64 ms** | 36.98 ms | 43.49 ms | 51.52 ms | 203.88 ms (**-84.0%**) | 64.21 ms |
| **`write_confirm` (L1 LMDB Quorum)** | 40 | 70.29 ms | **86.21 ms** | 108.90 ms | 118.40 ms | 118.40 ms | 735.41 ms (**-88.3%**) | 274.05 ms |
| **`advance` (Batch Tick Finalize)** | 40 | 27.39 ms | **42.83 ms** | 57.78 ms | 74.78 ms | 74.78 ms | 212.42 ms (**-79.8%**) | 91.86 ms |
| **`relay_call_sd` (Primary → SD1)** | 60 | 22.77 ms | **24.26 ms** | 27.24 ms | 30.24 ms | 35.70 ms | 175.01 ms (**-86.1%**) | 42.15 ms |
| **`cold_hydrate` (State Hydration)** | 20 | 23.13 ms | **24.88 ms** | 29.00 ms | 29.67 ms | 29.67 ms | 171.93 ms (**-85.5%**) | 50.70 ms |
| **`read_push` (Primary Snapshot)** | 60 | 81.92 ms | **86.94 ms** | 91.61 ms | 97.95 ms | 100.12 ms | 231.24 ms (**-62.4%**) | 86.76 ms |
| **`sd_direct` (Direct SD LMDB Read)** | 60 | 77.90 ms | **81.81 ms** | 86.36 ms | 93.63 ms | 95.85 ms | 220.82 ms (**-62.9%**) | 100.66 ms |

### 2. Serial Quorum Floor, Open-Loop Sweep & Node Footprint (`BENCH_N=60`)

```text
======================= BENCHMARK CLUSTER SUMMARY ==================
  Topology Mode               : Unmodified HyperBEAM + Forge Devices (`hs-wasm-output@1.0`, `hs-push@1.0`, `l1-checkpoint@1.0`)
  flush_serial (24 writes)    : 24.56 confirmed/s across 8 writers (24/24 HTTP 200, 0.98s elapsed, p50 236.77 ms, p90 344.92 ms)
  Open-Loop Sweep (8 writers) : 50 / 100 / 200 offered/s -> 49.34 / 61.88 / 59.58 accepted/s (202, queue_peak = 8)
                                Confirmed throughput: 46.46 / 57.94 / 55.05 confirmed/s (confirm_ratio = 1.0, drain = 0.38–0.51s)
                                Sweep L0 p50 latency: 103.73 / 128.21 / 135.78 ms (max 221.55–238.46 ms)
  Computed Slot Footprint     : primary = 8,283 B
  BEAM Process Memory (RSS)   : primary = 163.9 MB | sd1 = 143.0 MB | sd2 = 142.4 MB | genesis = 112.0 MB
  LMDB Disk Size (post-bench) : primary = 13.0 MB | sd1 = 5.0 MB | sd2 = 5.0 MB | arbit = 15.0 MB | genesis = 1.0 MB
  Converged Cluster State     : tick = 1028 | entities = 200 | roots_equal = true
  Canonical State Root        : 0x4f3e4fc4876e332706a9f7deb6a5fb44f2fd836b55674754b0952d5ad7ea35aa
====================================================================
```

### 3. Heavy Multi-Entity Throughput Reality (**35 Concurrent Entities** / Circle Test Swarm)

While single-entity and low-contention writes execute in **`32.64 ms` L0** / **`86.21 ms` L1**, pushing **sustained high throughput across 35 active entities** increases queueing and replication work per tick:

| Load Regime | Offered Workload | HTTP Accept / `Write-Batch` RTT | L1 Quorum Confirm (`Batch-Status` `200`) | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Isolated / Single-Writer** (`BENCH_N=60`) | 1 entity per write | **`32.64 ms` p50** (`36.98 ms` p90) | **`86.21 ms` p50** (`108.90 ms` p90) | Fast baseline floor with warm WASM actor + idle queue. |
| **35-Entity Saturated `Write-Batch` Probe** | 35 entities × 4 concurrent HTTP workers (6.32s burst, `83` batch requests, `38` `202` + `45` `200`, `drained = true`) | **`299.86 ms` p50** (`334.54 ms` p90, `373.03 ms` max) | **`661.25 ms` p50** (`746.19 ms` p90, `min 342.75 ms`) | In the live **Circle Test UI (`:8080`)**, adding browser `/proxy/` overhead + `/now` polling intervals pushes displayed end-to-end latency under full **35-entity swarm throughput into the ~750–800+ ms range**. |

*(This trade-off is expected and honest: all 35 entities still execute deterministically inside one canonical WASM world and settle through real 2-of-3 LMDB quorum with `0` errors and `roots_equal = true`.)*

---

## Current Multi-Tier Distributed Architecture

```text
                  +-----------------------------------------------+
                  |              1. GENESIS PROCESS               |
                  |   (hb-genesis :8733 | Lua + Forge Artifacts)  |
                  |  Topology discovery, epoch authority & GraphQL |
                  +-----------------------------------------------+
                             |                         |
            1-Hop Discovery  |                         | Live Forge Hot-Load
                             v                         | (/~<SpecID> via hb_store_gateway)
+------------------------+   Batches    +-----------------------------------------------+
|      GAME CLIENT       | ===========> |              2. PRIMARY NODE (:8734)           |
| (Red Circle WASD UI)   |              |   Single-World Rust WASM (`primary_engine`)   |
| * Dual-Watermark HUD   | <=========== |   Devices: `hs-wasm-output@1.0` + `hs-push@1.0` |
| * Ring-Buffer Coalesce |  Confirmed   |   Quorum Gate, Epoch Guard & Compaction Queue   |
| * Real-time Telemetry  |  200 ACK     |   Storage: LMDB (`hb_store_lmdb`, sync: false)  |
+------------------------+              +-----------------------------------------------+
                                                       |                 |
                                          Sync Quorum  | (~relay cast)   | Standby
                                          Commit       |                 | Replication
                                                       v                 v
                                      +------------------------------+   +------------------------------+
                                      |      3A. SHARED DATA 1       |   |      3B. SHARED DATA 2       |
                                      |  (hb-sd-1 :8735 | Rust WASM) |   |  (hb-sd-2 :8736 | Rust WASM) |
                                      |  `hs-wasm-output`+`hs-push`  |   |  `hs-wasm-output`+`hs-push`  |
                                      |  Storage: LMDB (snapshot=20) |   |  Storage: LMDB (snapshot=20) |
                                      +------------------------------+   +------------------------------+
                                                     |
                                                     | Async Checkpoint (every 100 ticks)
                                                     | via `~relay@1.0/cast` (via `hs-push@1.0`)
                                                     v
                                      +----------------------------------------------+
                                      |               4. ARBIT PROCESS               |
                                      |    (hb-arbit :8737 | Lua + Hot Forge Dev)    |
                                      |   Hot-loaded: `~l1-checkpoint@1.0`           |
                                      +----------------------------------------------+
                                                     |
                                                     | Format 2 TX (`tx@1.0` + `arweave@2.9`) + GET /mine
                                                     v
                                      +----------------------------------------------+
                                      |            5. ARWEAVE L1 PERMAWEB            |
                                      |            (arlocal :1984 | Docker)          |
                                      |   Decentralized permanent settlement ledger  |
                                      +----------------------------------------------+
```

### Core Invariants

1. **Unmodified HyperBEAM Substrate:** No local edits to [`HyperBEAM/`](HyperBEAM); all custom behaviors run as verifiable Forge devices (`hs-wasm-output@1.0`, `hs-push@1.0`, `l1-checkpoint@1.0`).
2. **Zero Hardcoded Peer URLs:** Nodes resolve peer and L1 gateway URLs dynamically from Genesis (`hb-genesis :8733`).
3. **Honest 2-of-3 Replication Gate:** Confirmed `200` ACK requires Primary + at least one SD replica (`sd1` or `sd2`) to verify execution, commit to LMDB, and match `batch_id`, `tick`, epoch, and canonical `state_root`. Quorum failure returns `503` without state mutation.
4. **Canonical Sharded State Root:** 32 FNV-1a shards; commits re-hash only mutated shards, avoiding full-state $O(N)$ re-hashing.
5. **Dual-Watermark & Relog Recovery:** Client tracks optimistic RAM Head (`Head Tick`) vs. replicated LMDB disk (`Replicated Tick`), rehydrating directly from Primary/SD on reconnect.

---

## Node Topology

| Node / Service | Port | Process & Execution Device | Role & Storage Configuration |
| :--- | :--- | :--- | :--- |
| **`arlocal`** | `:1984` | Arweave L1 Gateway | Docker container running local Arweave L1 + GraphQL indexer. |
| **`hb-genesis`** | `:8733` | `genesis` (`lua@5.3a` + `push@1.0`) | Bootstrap topology/epoch authority & GraphQL host for Forge `SpecID`/`ImplID` artifacts (`hb_store_lmdb`). |
| **`hb-primary`** | `:8734` | `primary` (`hs-wasm-output@1.0` + `hs-push@1.0`) | Single-world ingress, L0 batching, compaction & 2-of-3 quorum coordinator (`hb_store_lmdb` + read-only `wasm_store`). |
| **`hb-sd-1`** | `:8735` | `sd1` (`hs-wasm-output@1.0` + `hs-push@1.0`) | Shared Data Replica 1 on `hb_store_lmdb` (`process-snapshot-slots: 20`); quorum verifier. |
| **`hb-sd-2`** | `:8736` | `sd2` (`hs-wasm-output@1.0` + `hs-push@1.0`) | Shared Data Replica 2 on `hb_store_lmdb` (`process-snapshot-slots: 20`); quorum witness & failover. |
| **`hb-arbit`** | `:8737` | `arbit` (`lua@5.3a` + hot-loaded `~l1-checkpoint@1.0`) | Consensus checkpoint coordinator, native Arweave Format 2 TX signer/settler, and shard pruning trigger (`hb_store_lmdb` + `hb_store_gateway`). |

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

### 2. Package Devices, Build WASM & Launch Cluster
`tools/cluster_up.sh` compiles unmodified HyperBEAM + Rust WASM processes, packages external Forge devices (`tools/build_configs.py`), launches ArLocal + 5 HyperBEAM nodes in tmux, and hot-loads `l1-checkpoint@1.0` onto `hb-arbit`:
```bash
./tools/cluster_up.sh
```

### 3. Launch Interactive Circle Test Client & Live Telemetry
```bash
python3 tools/game_client/serve.py
```
Open **`http://127.0.0.1:8080/`**:
- Move the red circle with **W / A / S / D** or click inside the arena.
- Start the **Swarm Simulation** and inspect real-time L0/L1 RTT, `Active In-Flight`, `Coalesced`, `Compacted Discards`, and Dual-Watermark ticks.

### 4. Hard Reset Cluster Data (Back to Tick #0)
```bash
python3 tools/reset_hard.py --yes --with-ui
```

### 5. Stop Cluster
```bash
./tools/cluster_down.sh
```

---

## Automated Verification & Test Suite

Run against the **live local cluster**:

1. **Contract E2E Test (30/30 Checks Passing):** Verifies 202 vs 200 ACK semantics, idempotency, ownership forgery rejection (`403`), and state-root convergence across `primary`, `sd1`, and `sd2`:
   ```bash
   python3 tests/test_e2e_2d_points.py
   ```
2. **Arweave L1 Settlement & Pruning Test (ALL PASS):** Pushes live writes past checkpoint threshold (`+100` ticks) and verifies Format 2 L1 TX mining, GraphQL tags on ArLocal `:1984`, and shard pruning cascade:
   ```bash
   python3 tools/test_arweave_l1_settlement.py
   ```
3. **8-Path Micro-Benchmark & Throughput Sweep:**
   ```bash
   BENCH_N=60 python3 tools/bench_engine.py
   ```
4. **Cluster Interconnect, Failure-Injection & Mutation Suites:**
   ```bash
   ./tests/test_e2e_connectivity.sh
   python3 tests/test_failure_modes.py --yes
   ./tests/mutation_ack.sh
   ```

---

## Repository Structure

```text
├── engine/
│   ├── config/                     # Generated node configs (genesis, primary, sd1, sd2, arbit)
│   ├── data/                       # Wallets, LMDB stores (lmdb/), read-only wasm_store/
│   ├── devices/                    # External HyperBEAM Forge devices (0 edits to HyperBEAM/)
│   │   ├── hs_wasm_output/         # `hs-wasm-output@1.0`: fast WASM execution-device + pool
│   │   ├── hs_push/                # `hs-push@1.0`: outbox resolve/cast relay + pool release
│   │   └── l1_checkpoint/          # `l1-checkpoint@1.0`: hot-loaded Arweave Format 2 L1 settler
│   └── processes/                  # Process logic (Rust WASM + Lua authority scripts)
│       ├── primary/                # `primary_engine.wasm` (batch queue, compactor, quorum)
│       ├── shared_data/            # `sd_engine.wasm` (deterministic replica verifier)
│       ├── genesis/                # Constitutional topology & epoch authority (`genesis_process.lua`)
│       └── arbit/                  # Consensus checkpoint & pruning coordinator (`arbit_process.lua`)
├── HyperBEAM/                      # Unmodified upstream HyperBEAM Erlang runtime (submodule)
├── tests/                          # Contract E2E, connectivity, failure-injection & mutation tests
├── tools/
│   ├── build_configs.py            # Generates node configs, trusted-devices & wasm_store images
│   ├── prepare_wasm_store.escript  # Packages WASM binaries + Forge devices into hb_store_fs
│   ├── deploy_forge_device.py      # Publishes l1-checkpoint@1.0 to hb-genesis & hot-loads hb-arbit
│   ├── publish_forge_device.escript# Signs & publishes Forge Spec/Impl ANS-104 messages
│   ├── bench_engine.py             # 8-path latency & open-loop/serial throughput benchmark
│   ├── test_arweave_l1_settlement.py # Live L1 Format 2 checkpoint + GraphQL + pruning E2E test
│   ├── reset_hard.py               # Wipes LMDB/ArLocal/logs cleanly back to Tick #0
│   ├── cluster_up.sh / cluster_down.sh
│   └── game_client/                # Interactive WASD Circle Test (`index.html`) + proxy (`serve.py`)
├── start.sh                        # All-in-one builder, cluster launcher & UI starter
└── README.md
```

---

## License

This project is open-source under the Apache-2.0 License.
