# HyperStack: Real-Time State Engine

> **High-Frequency Real-Time State Engine & Distributed Replication Stack on Unmodified HyperBEAM, AO Compute & Arweave Permaweb**

[![Project](https://img.shields.io/badge/Project-HyperStack-orange)](https://github.com/Wesslurr/database-stack-on-permaweb)
[![HyperBEAM](https://img.shields.io/badge/HyperBEAM-Unmodified%20Upstream%20(Forge%20Devices)-red)](https://github.com/permaweb/hyperbeam)
[![Permaweb](https://img.shields.io/badge/Arweave-Permaweb%20L1%20(Format%202)-black?logo=arweave)](https://arweave.org/)
[![AO Compute](https://img.shields.io/badge/AO-Compute%20WASM%20%2B%20Lua-blue)](https://ao.g8way.io/)
[![Tests](https://img.shields.io/badge/Verified%20Tests-30%2F30%20E2E%20%2B%20L1%20Settlement-brightgreen)](tests/)
[![Latency](https://img.shields.io/badge/p50%20Latency-10.98ms%20%2Fnow%20%7C%20154.81ms%20L0%20%7C%20551.75ms%20L1-blueviolet)](#honest-empirical-performance-benchmarks)
[![Quorum](https://img.shields.io/badge/Quorum-Honest%202--of--3%20LMDB%20Verified%20ACK-success)](#honest-empirical-performance-benchmarks)

---

> [!IMPORTANT]
> ### Architectural Pivot, Distributed Portability & Empirical Honesty Notice
> **HyperStack** now runs **100% on unmodified upstream [`HyperBEAM/`](HyperBEAM)** (`nothing to commit, working tree clean`), isolating every execution fast-path, outbox relay hook, and Arweave L1 settlement bridge inside **3 packaged external HyperBEAM Forge devices** under [`engine/devices/`](engine/devices/).
>
> - **Why Move Out of `HyperBEAM/src`:** Earlier milestones (`3b3e48c` / `b77a166`) patched 11 internal core/preloaded files inside `HyperBEAM/src` (`dev_stack`, `dev_json_iface`, `dev_process`, `lib_process`, `dev_scheduler`, `hb_beamr_io`, plus volatile storage) to reach `64.21 ms` L0 `write_202` p50. However, core patches tied the engine to a forked HyperBEAM binary and broke portability across standard distributed nodes.
> - **Honest Latency Trade-Off:** Moving optimizations into external Forge devices on unmodified `HyperBEAM` initially raised `write_202` p50 to `203.88 ms` (`735.41 ms` L1) because upstream's default 4-device stack (`stack@1.0` + `multipass@1.0` + `json-iface@1.0` + `wasm-64@1.0`) ran uncached on every slot. Consolidating those execution stages into **`hs-wasm-output@1.0`** recovered **24–33%** across write/quorum/relay paths (**`154.81 ms` L0 `write_202`**, **`551.75 ms` L1 `write_confirm`**, **450.0 w/s** live Circle Test throughput with **0 errors** across **112,279 ACKed writes**) without touching `HyperBEAM/`.
> - **Software-Level 2-of-3 LMDB Quorum Persistence Contract:** An HTTP `200` (`ack: "replicated"`) is returned **only after Primary + authoritative Shared Data replicas (`sd1`, `sd2`) deterministically verify the WASM transition, persist to `hb_store_lmdb`, and match `batch_id`, `tick`, epoch, and canonical `state_root`**. Quorum loss returns `503` without mutating confirmed state.
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
| **Erlang / OTP Runtime** | **Erlang/OTP 27** (`erts-15.2.7.4`), 5 independent BEAM VMs (`+S 4:4 +A 4 +MBas aobf`) |
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

Moving customizations out of [`HyperBEAM/src`](HyperBEAM/src) solved distributed maintainability, **but exposed real trade-offs and infrastructure limits** that explain the exact shape of our benchmarks:

1. **Forked Fast Core (`~64.2 ms`) vs. Portable Unmodified HyperBEAM (`~154.8 ms`):**
   - Upstream `HyperBEAM` executes `dev_process`, `lib_process`, `lib_process_cache`, `dev_scheduler`, and `dev_scheduler_server` on every slot. Without modifying those core Erlang modules, slot scheduling, commitment normalization, and LMDB cache coordination remain outside external device control, setting a current floor of **`~154.8 ms` L0 (`118.9 ms` min)** and **`~551.8 ms` L1 quorum confirm**.
   - Even so, consolidating `stack@1.0 + multipass@1.0 + json-iface@1.0 + wasm-64@1.0` inside **`hs-wasm-output@1.0`** cut **~49 ms (-24.1%)** off L0 accept and **~184 ms (-25.0%)** off L1 confirmation relative to the unoptimized multi-device Forge stack (`203.88 ms` / `735.41 ms`).
2. **Memory Stability on Constrained Hosts (`3.8 GiB` WSL2 VM + 5 BEAM VMs):**
   - Previously, `primary` used `hb_store_volatile` (pure BEAM RAM). When WASM snapshots (`~1.31 MB` each) accumulated across hundreds of slots inside BEAM's binary heap, Erlang hit heavy memory pressure.
   - Switching all 5 nodes to **`hb_store_lmdb` (`capacity: 4 GiB`, `sync: False`, `read-ahead: False`, `flush-every: 0`)** plus `cache-control: ["no-cache", "no-store"]` offloaded large binary snapshots to OS-managed mmap storage. BEAM memory stabilized at **`58.6 MB` total (`169.0 MB` RSS)** on `hb-primary` with **`1` active `hb_beamr` C-port** (down from `2` ports). Under heavy burst benchmarks on a `3.8 GiB` WSL2 guest, BEAM's built-in 80% `system_memory_high_watermark` alarm still logs once per node without crashes (`0` `CRASH REPORT`, `0` timeouts, `0` errors).
3. **Why Distributed Throughput Plateaus Around ~4.5 Batches/s (`450.0 w/s`):**
   - `QuorumState` inside `primary` serializes pending quorum commits (**1 active quorum batch lock** at a time between `Primary` and `SD1/SD2`) while HyperBEAM's single-process slot pipeline executes `Advance-Batch` → `~hs-push@1.0` → SD HTTP relay → LMDB commit → `Ack`.
   - Under `Max in-flight writes: 4`, incoming writes fill `Active In-Flight` + `Pipeline Queue` (`501 queued` cap in `index.html`), causing aggressive **client coalescing (`305,358` skipped)** and **server queue/batch compaction (`113,387` compacted)** rather than overloading the cluster. Breaking past ~450–750 w/s would require multi-batch `Primary ↔ SD` pipelining or true independent world sharding.

---

## Honest Empirical Performance Benchmarks

### 1. Raw Micro-Benchmarks (`BENCH_N=60`, Option A Forge Devices on Unmodified `HyperBEAM`)

Measured on a fresh cluster (`test_e2e_2d_points` + `test_arweave_l1_settlement` + `BENCH_N=60 python3 tools/bench_engine.py`, keep-alive HTTP/1.1 on `127.0.0.1`):

| Operation Path | Samples (`n`) | min | **p50 (Current Option A)** | p90 | p99 | max | Pre-Option-A Forge Stack (p50) | Patched-Core Baseline `3b3e48c` (p50) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **`/now` (RAM State Read)** | 60 | 8.58 ms | **10.98 ms** | 14.07 ms | 18.13 ms | 27.31 ms | 9.13 ms | 10.33 ms |
| **`write_202` (L0 Ingest Accept)** | 60 | 118.87 ms | **154.81 ms** | 184.68 ms | 221.86 ms | 227.56 ms | 203.88 ms (**-24.1%**) | 64.21 ms |
| **`write_confirm` (L1 LMDB Quorum)** | 40 | 462.92 ms | **551.75 ms** | 619.65 ms | 709.26 ms | 709.26 ms | 735.41 ms (**-25.0%**) | 274.05 ms |
| **`advance` (Batch Tick Finalize)** | 40 | 130.64 ms | **256.51 ms** | 341.07 ms | 444.72 ms | 444.72 ms | 212.42 ms (+20.8%) | 91.86 ms |
| **`relay_call_sd` (Primary → SD1)** | 60 | 101.51 ms | **116.32 ms** | 139.60 ms | 145.91 ms | 156.37 ms | 175.01 ms (**-33.5%**) | 42.15 ms |
| **`cold_hydrate` (State Hydration)** | 20 | 104.13 ms | **116.47 ms** | 127.16 ms | 145.33 ms | 145.33 ms | 171.93 ms (**-32.3%**) | 50.70 ms |
| **`read_push` (Primary Snapshot)** | 60 | 160.19 ms | **175.64 ms** | 204.05 ms | 220.10 ms | 220.81 ms | 231.24 ms (**-24.0%**) | 86.76 ms |
| **`sd_direct` (Direct SD LMDB Read)** | 60 | 144.89 ms | **177.57 ms** | 208.02 ms | 224.79 ms | 1941.69 ms | 220.82 ms (**-19.6%**) | 100.66 ms |

*(Note: `tools/bench_baseline.json` in git preserves the `3b3e48c` historical patched-core reference for regression tracking; the "Current Option A" column reflects the clean `BENCH_N=60` run with unmodified `HyperBEAM` + `hs-wasm-output@1.0` + `hs-push@1.0`.)*

### 2. Serial Quorum Floor, Open-Loop Sweep & Node Footprint (`BENCH_N=60` Option A)

```text
======================= BENCHMARK CLUSTER SUMMARY (OPTION A) =======
  Topology Mode               : Unmodified HyperBEAM + Forge Devices (`hs-wasm-output@1.0`, `hs-push@1.0`, `l1-checkpoint@1.0`)
  flush_serial (24 writes)    : 3.95 confirmed/s across 8 writers (24/24 HTTP 200, 6.08s elapsed, p50 1808.14 ms, min 1042.46 ms)
                                [+29.5% vs 3.05/s pre-Option-A Forge stack]
  Open-Loop Sweep (no flush)  : 50 / 100 / 200 offered/s -> 11.80 / 11.98 / 12.13 accepted/s (202, queue_peak = 8)
                                Sweep p50 latency: 646.77 / 637.50 / 635.61 ms (all drained cleanly in 0.84–0.87s)
                                Confirmed throughput under open-loop bursts: 1.06 / 0.80 / 1.08 confirmed/s
  Computed Slot Footprint     : primary = 51,004 B  |  sd1 = 24,552 B  |  sd2 = 24,552 B
  Active WASM Ports & BEAM Mem: primary = 58.6 MB (bin 2.8 MB, 169,000 KiB RSS, 1 hb_beamr port)
                                sd1     = 57.5 MB (bin 2.3 MB, 135,084 KiB RSS, 1 hb_beamr port)
                                sd2     = 57.5 MB (bin 2.3 MB, 133,232 KiB RSS, 1 hb_beamr port)
                                genesis = 65.3 MB (113,272 KiB RSS) | arbit = 59.1 MB (123,360 KiB RSS)
  LMDB Disk Size (post-bench) : primary = 1,606 MB | sd1 = 428 MB | sd2 = 341 MB | arbit = 11 MB | genesis = 1 MB
  Converged Cluster State     : tick = 504 | entities = 174 | roots_equal = true
  Canonical State Root        : 0xd32a80525c3cb303bb56b099927c7b567fca4e11d753e1cde60406b99fa04a22
====================================================================
```

### 3. Live Interactive Circle Test Stress Run (Sustained Multi-Bot Run @ `20 ms`)

Directly observed telemetry from the live web UI (`http://127.0.0.1:8080/`) running **Random per-bot timing**, **`Move interval: 20 ms`**, **`Step size: 5 px`**, and **`Max in-flight writes: 4`**:

| Live Circle Test Metric | Observed Value | Honest Engineering Explanation |
| :--- | :--- | :--- |
| **Requested Write Rate** | **`1,750.0 w/s`** | Swarm target generation cadence (`20 ms` move interval across active entities). |
| **Achieved (ACKed) Throughput** | **`450.0 w/s`** | `~4.5 batches/s × 100 ent/batch` sustained end-to-end L1 confirmation throughput. |
| **Total ACKed / Errors** | **`112,279 / 0`** | **0% error rate** across >112k confirmed 2D entity state transitions. |
| **Avg Write RTT (Client)** | **`453 ms`** | Average client-measured HTTP round-trip across batched requests (`Active In-Flight: 2 / 2`). |
| **L0 RAM Ingest / L1 Quorum ACK** | **`535 ms` L0 / `1,601 ms` L1** *(HUD: `404 ms L0 / 1,651 ms L1`)* | Time under full pipelined load (`Active In-Flight: 2 / 2`) for batch L0 acceptance (`404–535 ms`) and full browser `/now` confirmation after SD quorum (`1,601–1,651 ms`). |
| **Coalesced (Skipped)** | **`305,358`** | Client ring-buffer cap (`wp.queue.length > 500`, `Pipeline Queue: 501 queued`) overwrites older superseded bot moves before HTTP dispatch. |
| **Compacted Discards** | **`113,387`** | Server-side WASM compaction (`compacted_count` in `compactor.rs` / `quorum.rs`): same-entity updates superseded in Primary's open queue and batch delta compaction before commit. |
| **Batch Train Size & Engine** | **`100 ent/batch` · `1 / 1 (Single-World WASM)`** | Single authoritative Rust WASM world executing deterministic 100-entity trains via `hs-wasm-output@1.0`. |
| **Dual-Watermark & Quorum** | **`#712 (Head: #716)` · `Lag: 2–4 ticks`** | Head RAM (`#716`) leads confirmed disk (`#712`) by 2–4 ticks while queued/in-flight batches settle cleanly to **`SD1+SD2 [LMDB Quorum]`** (`Verified Disk State Root: 0xecb21b263907d653…`, player position `(50, 30)` in sync with Head RAM). |

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
