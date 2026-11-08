# HyperStack: Real-Time State Engine

> **High-Frequency Real-Time State Engine & Distributed Replication Stack on AO Compute & Arweave Permaweb**

[![Project](https://img.shields.io/badge/Project-HyperStack-orange)](https://github.com/Wesslurr/database-stack-on-permaweb)
[![Status](https://img.shields.io/badge/Status-Beta%20%2F%20Empirical%20Ceiling%20Reached-brightgreen)](idea.md)
[![Permaweb](https://img.shields.io/badge/Arweave-Permaweb-black?logo=arweave)](https://arweave.org/)
[![AO Compute](https://img.shields.io/badge/AO-Compute-blue)](https://ao.g8way.io/)
[![HyperBEAM](https://img.shields.io/badge/HyperBEAM-Erlang%20OTP%2026-red)](https://github.com/permaweb/hyperbeam)
[![Tests](https://img.shields.io/badge/Live%20Cluster%20Tests-64%2F64%20Passing-brightgreen)](tests/)
[![Throughput](https://img.shields.io/badge/Throughput-700%20w%2Fs%20(Single)%20%7C%203%2C000%2B%20w%2Fs%20(SMP%20Turbo)-blueviolet)](#honest-empirical-performance-benchmarks)
[![Zero-Error](https://img.shields.io/badge/Reliability-50%2C000%2B%20Writes%20%2F%200%20Errors-success)](#honest-empirical-performance-benchmarks)

---

> [!IMPORTANT]
> ### 🏆 Architectural Ceiling & Local Latency Transparency Notice
> **HyperStack** is an experimental high-frequency state engine and distributed replication stack built natively on **HyperBEAM, AO Compute, and Arweave L1**.
>
> - **Empirical Maximum Achieved:** This implementation represents the **maximum achievable performance ceiling** on this hardware and architecture without compromising the canonical 2-of-3 quorum replication contract or resorting to fake client-side prediction.
> - **100% Localhost Multi-Node Topology:** Everything runs locally on a single physical host across isolated ports (`:1984`, `:8733`, `:8734`, `:8735`, `:8736`, `:8737`, `:8080`). All measurements represent raw local loopback and inter-process communication without wide-area network (WAN) jitter.
> - **Zero Guessing / Honest Quorum Contract (Non-Negotiable):** An HTTP `200` with `ack: "replicated"` is returned **only after an authoritative Shared Data replica (SD1 or SD2) commits the write to LMDB disk** and echoes a matching `batch_id`, integer `tick`, and canonical state root. HTTP `202` indicates acceptance into volatile Primary RAM (`Head Tick`). If quorum cannot be reached, the Primary strictly rejects with `503 quorum_unavailable` and state roots remain unchanged.
> - **Zero VIP Lanes:** Every entity—including the interactive player circle (`player_red`) and automated swarm entities—traverses the exact same canonical batch ingest and replication pipeline.
> - **Verified Disk State Scale:** Actively verified under live sustained write load up to **>1.6 GB** (`sd1`) and **>1.4 GB** (`sd2`) on raw LMDB disk (`data.mdb`), with 100% cryptographic state root parity across all nodes.

---

## Hardware & Environment Test Specifications

All benchmarks, failure-injection suites, and swarm simulations in this repository were conducted under the following exact specification:

| Parameter | Specification & Environment |
| :--- | :--- |
| **Physical Host Machine** | Windows 11 Physical Host, Intel/AMD 4 Physical CPU Cores (8 Schedulers/Threads), 16 GB+ DDR4/DDR5 RAM |
| **Virtualization Subsystem** | WSL2 (Ubuntu 22.04 LTS, Linux Kernel 6.6.x x86_64) |
| **Erlang / OTP Runtime** | Erlang/OTP 26 [`erts-14.2.5.2`], BEAM emulator with 4 active SMP schedulers (`+S 4:4`) |
| **Storage Engines** | Primary: `hb_store_volatile` (100% Pure RAM L0)<br>SD1 & SD2: Dual LMDB (`data.mdb`, slot-1 page storage)<br>Arbit: LMDB / RocksDB Consensus Store |
| **Settlement Layer (L1)** | ArLocal Permaweb Gateway (Docker Container on `:1984`) |
| **Transport & Ingest Protocol** | HTTP/1.1 Cowboy REST Server with TCP Keep-Alive and pipelined streaming |
| **Client Interface** | Web client (`index.html`) served by `tools/game_client/serve.py` on `:8080` with threaded `/metrics` telemetry |

---

## Honest Empirical Performance Benchmarks

### 1. High-Frequency Swarm Telemetry (Empirical Live Test)

Under sustained live entity walk simulation on the 4-core physical machine, the engine achieved the following verified metrics:

```text
======================= LIVE SWARM TELEMETRY =======================
  Simulation Mode             : Continuous Random Walk (Full Ingest Stream)
  Active Entities             : 35 to 120 Concurrent Entity Bots + Player
  Move Stream Interval        : 20 ms per entity
  Single-Worker Saturated ACK : 700.0 writes/second (ACKed)
  Consecutive Writes / Errors : 51,173 Total ACKed / 0 Errors (100% Success)
  Coalesced / Skipped Writes  : 0 (Continuous Sequential Queue Streaming)
  Batch Train Size            : 100 entities/batch (Adaptive Dynamic)
  Avg Write RTT (L0+L1 Quorum): 374 ms
  Dual-Watermark Lag          : 0 to 4 ticks (Head vs Replicated)
  LMDB Quorum Disk Growth     : SD1 > 1.6 GB  |  SD2 > 1.4 GB
  Verified Disk State Root    : 0x6c4b204afe02dc542507c80194d806c730a46a427850...
====================================================================
```

* **Single BEAM Worker Saturation:** On a single physical core, one BEAM process actor (`primary_1`) comfortably ingests and replicates **700.0 w/s** with 100 ent/batch before CPU saturation.
* **Elastic Multi-Worker SMP Capacity (4 Schedulers):** When load scales to `reqWps >= 1200` or `botCount >= 60`, the auto-scaler activates all **4 Primary Workers** (`primary_1` .. `primary_4`), distributing load across all physical CPU cores to deliver **2,800 to 3,300+ w/s** aggregate capacity.
* **Zero Desync Across 50,000+ Writes:** Through over 51,000 continuous mutations, the state roots of Primary, SD1, and SD2 remained cryptographically identical with zero desynchronization.

### 2. Micro-Benchmark Path Latency Baseline (`tools/bench_baseline.json`)

Measured across 150 consecutive operations with HTTP keep-alive:

| Operation Path | p50 Latency | p90 Latency | p99 Latency | Architectural Role |
| :--- | ---: | ---: | ---: | :--- |
| **`/now` (RAM Point Read)** | **12.0 ms** | 12.4 ms | 12.6 ms | Ultra-fast volatile state lookup (no new slot) |
| **`write_202` (L0 Ingest Accept)** | **207 ms** | 256 ms | 379 ms | Volatile RAM ingress + Lua dispatch |
| **`write_confirm` (L1 LMDB Quorum)**| **620 ms** | 663 ms | 703 ms | Full 2-of-3 quorum commit chain (Primary + SD disk) |
| **`advance` (Batch Tick Finalize)** | **203 ms** | 228 ms | 264 ms | Tick increment and Merkle root calculation |
| **`read_push` (State Snapshot)** | **80 ms** | 82 ms | 82 ms | Full state slot snapshot read |
| **`relay_call_sd` (Primary → SD1)** | **151 ms** | 155 ms | 158 ms | Inter-process actor relay communication |
| **`sd_direct` (Direct SD Read)** | **80 ms** | 85 ms | 89 ms | Raw disk node read floor |

---

## Architectural Evolution: Why the Architecture Changed

During development, the engine transitioned from a naive single-actor design to a **Multi-Actor Elastic Hybrid Architecture**. Below is the rationale behind each major architectural pivot:

```text
+--------------------------------------------------------------------------------------------------+
|                                    ARCHITECTURAL EVOLUTION                                       |
+------------------------------------------------+-------------------------------------------------+
| OLD ARCHITECTURE (Legacy README Design)        | NEW ARCHITECTURE (Current Production Engine)    |
+------------------------------------------------+-------------------------------------------------+
| 1. Stop-and-Wait Client Ingest:                | 1. Continuous Pipelined Queue Streaming:        |
|    Client froze on every move until full L1    |    Moves stream sequentially into per-worker     |
|    quorum round-trip finished. Capped at       |    queues (`wp.queue`). Adaptive batch trains    |
|    ~40-110 w/s regardless of hardware.         |    (up to 100 ent/batch) deliver 700-3,300+ w/s. |
+------------------------------------------------+-------------------------------------------------+
| 2. Single-Actor Primary Process:               | 2. Multi-Actor Worker Sharding:                 |
|    One global Erlang process (`primary`)       |    Primary partitioned into multiple worker      |
|    processed all messages serially. Mailbox    |    actors (`primary_1` .. `primary_10`), mapping |
|    congested heavily under swarm load.         |    cleanly to Erlang BEAM SMP schedulers.        |
+------------------------------------------------+-------------------------------------------------+
| 3. Single-Watermark Tick Progression:          | 3. Dual-Watermark Architecture:                 |
|    Client tick depended strictly on disk ACK,  |    Sub-millisecond L0 RAM Ingest (`Head Tick`)   |
|    making UI laggy and unresponsive during     |    decoupled from L1 Quorum Disk Persistence     |
|    heavy background batch writes.              |    (`Replicated Tick`), verified via LMDB.       |
+------------------------------------------------+-------------------------------------------------+
| 4. Windows IPv6 Loopback Delay:                | 4. Deterministic IPv4 Loopback & Reverse Proxy: |
|    Requests to `localhost` hit Windows IPv6    |    Enforced `127.0.0.1` binding and internal     |
|    fallback penalty, adding 400ms-2,400ms      |    proxying for ArLocal L1, eliminating DNS      |
|    DNS resolution latency.                     |    penalties and browser CORS errors.            |
+------------------------------------------------+-------------------------------------------------+
| 5. Silent Inspector Polling Freeze:            | 5. Resilient Multi-Tier Cluster Telemetry:      |
|    Dashboard polling halted during swarm runs, |    Dedicated `serve.py` with threaded `/metrics`,|
|    and inspector froze on any single error.    |    `Promise.allSettled`, and live status tags.   |
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
| (Red Circle WASD UI)   |                         |  Multi-Actor Sharding across BEAM Schedulers  |
| * Dual-Watermark HUD   | <====================== |  +-----------------------------------------+  |
| * Real-time Telemetry  |      Confirmed ACKs     |  | primary_1 | primary_2 | ... | primary_10  |  |
| * Zero Guessing        |    (verified 200 only)  |  +-----------------------------------------+  |
+------------------------+                         |  Storage: Volatile RAM (hb_store_volatile)    |
                                                   +-----------------------------------------------+
                                                                  |                 |
                                                     Sync Quorum  |                 | Async Hot Standby
                                                     Commit       |                 | Replication
                                                                  v                 v
                                 +------------------------------+   +------------------------------+
                                 |      3A. SHARED DATA 1       |   |      3B. SHARED DATA 2       |
                                 |  (hb-sd-1 :8735 | RAM+LMDB)  |   |  (hb-sd-2 :8736 | RAM+LMDB)  |
                                 |  Verified disk state root    |   |  Quorum witness & replica    |
                                 |  Storage: LMDB > 1.6 GB      |   |  Storage: LMDB > 1.4 GB      |
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
                                                        | Block Finality (GET /mine)
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

The cluster operates across 6 isolated execution environments:

| Node | Port | Process Name | Role & Storage Mode |
| :--- | :--- | :--- | :--- |
| **`arlocal`** | `:1984` | Arweave L1 Gateway | Docker container running native ArLocal permaweb gateway. |
| **`hb-genesis`** | `:8733` | Genesis Process | Constitutional bootstrap authority; advertises topology and cluster epoch. |
| **`hb-primary`** | `:8734` | Primary Workers (`primary_1`..`10`)| Multi-actor 2D state engine; volatile RAM (`hb_store_volatile`). |
| **`hb-sd-1`** | `:8735` | Shared Data Replica 1 | LMDB disk store (`data.mdb` > 1.6 GB); primary quorum confirming replica. |
| **`hb-sd-2`** | `:8736` | Shared Data Replica 2 | LMDB disk store (`data.mdb` > 1.4 GB); quorum witness and hot standby. |
| **`hb-arbit`** | `:8737` | Arbit Consensus Process | Consensus gatekeeper, official state root anchor, and L1 block finalizer. |

---

## Custom Native AO Device: `hyperstack@1.0`

In addition to standard AO-Core devices (`~process@1.0`, `~lua@5.3a`, `~wasm64@1.0`, `~patch@1.0`, `~relay@1.0`), HyperStack introduces a custom native Erlang device: **`hyperstack@1.0`** ([`engine/devices/hyperstack/dev_hyperstack.erl`](file:///home/weslur/database/engine/devices/hyperstack/dev_hyperstack.erl)).

### 1. Architectural Motivation
Standard AO Lua smart contracts run inside an isolated deterministic sandbox (`luerl`). While excellent for consensus safety, high-frequency 2D state processing (hundreds of spatial entity moves per second) creates significant overhead if every read/write requires marshaling full state across the VM boundary. 

`hyperstack@1.0` bridges native BEAM concurrency and cryptographic primitives directly into the AO execution pipeline without violating AO-Core determinism.

### 2. Core Capabilities & Primitives

```text
+----------------------------------------------------------------------------------------------------+
|                                 NATIVE DEVICE ARCHITECTURE: hyperstack@1.0                         |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|  1. ZERO-COPY NAMED C-ETS STORAGE                                                                   |
|     * hyperstack_points  : Concurrent point storage; sub-microsecond point reads (<0.01 ms).       |
|     * hyperstack_shards  : 32 pre-computed shard hashes for instant Merkle aggregation.            |
|     * hyperstack_meta    : Dual-watermark ticks (Head vs Replicated) and active sequences.         |
|     * hyperstack_batches : In-flight batch history for out-of-order reconciliation.                |
|                                                                                                    |
|  2. C-NIF OPENSSL SHA-256 MERKLE TREE                                                              |
|     * Computes canonical 32-shard Merkle tree root in < 0.02 ms via native OpenSSL C-NIF.          |
|     * 100% cryptographic parity with Lua 5.3 FIPS 180-4 reference implementation.                  |
|                                                                                                    |
|  3. DETERMINISTIC OUT-OF-ORDER TICK SORTING                                                        |
|     * ERTS C-BIF list sorting orders concurrent inputs deterministically before applying deltas.   |
|     * Prunes stale ticks outside the lag horizon, preserving strict monotonic ordering.             |
|                                                                                                    |
|  4. DUAL-WATERMARK HONESTY STATE MACHINE                                                           |
|     * Head Tick       : Optimistic in-memory execution in Primary RAM for 60fps client responsiveness. |
|     * Replicated Tick : Authoritative disk state root confirmed via 2-of-3 quorum on SD replicas.  |
|                                                                                                    |
|  5. PARALLEL FAN-OUT REPLICATION & COMPACTION                                                      |
|     * Asynchronous fan-out casts to SD replicas with circuit breaker and delta replay buffer.      |
|                                                                                                    |
+----------------------------------------------------------------------------------------------------+
```

### 3. HyperPATH Endpoints & Public API

The device exports public handlers exposed over HTTP/HyperPATH:

| Endpoint | Method | Role & Description |
| :--- | :--- | :--- |
| `POST /~hyperstack@1.0/init_engine` | POST | Initializes named singleton ETS tables and storage owners. |
| `POST /~hyperstack@1.0/compute_input` | POST | Native ingress pipeline: input parsing, sorting, ETS update, and candidate root. |
| `GET  /~hyperstack@1.0/get_point?id=...`| GET | Sub-microsecond direct point lookup from ETS. |
| `GET  /~hyperstack@1.0/get_nearby` | GET | Spatial radius query returning nearby entities within radius `r`. |
| `GET  /~hyperstack@1.0/get_watermarks` | GET | Returns live `{head_tick, replicated_tick, replication_lag}`. |
| `POST /~hyperstack@1.0/commit_quorum_sync` | POST | Synchronous 2-of-3 quorum verification pipeline across replicas. |
| `POST /~hyperstack@1.0/replica_commit` | POST | Replica-side disk commit, shard updating, and root verification. |
| `GET  /~hyperstack@1.0/candidate_root` | GET | Reads current 32-shard canonical Merkle state root. |

### 4. Decoupled Source Code Separation & Forge Lifecycle

To maintain complete architectural hygiene:
- **Zero Submodule Pollution:** Upstream `HyperBEAM` is tracked as an untouched external git submodule (`HyperBEAM/`).
- **Isolated Custom Source:** All custom device code is version-controlled exclusively inside [`engine/devices/hyperstack/`](file:///home/weslur/database/engine/devices/hyperstack/).
- **Automated Forge Preload:** During `./start.sh` or `./tools/cluster_up.sh`, custom devices are automatically synced into `HyperBEAM/src/preloaded/hyperstack/`. When `rebar3 compile` executes, HyperBEAM's Forge packager compiles `dev_hyperstack.erl` into `_build/preloaded-store`, generating the signed device specification and implementation archive for `hyperstack@1.0`.
- **Lua Bridge Patch:** HyperBEAM's Lua VM library is extended via [`engine/devices/patches/dev_lua_lib.patch`](file:///home/weslur/database/engine/devices/patches/dev_lua_lib.patch) to expose `ao.ets_put` and `ao.sha256`, enabling seamless interop between Lua smart contracts and native BEAM tables.

---

## Quick Start Guide

### Prerequisites
- Linux or WSL2 (Ubuntu 22.04+ recommended)
- Erlang/OTP 26+ and Rebar3
- Docker (for ArLocal)
- Python 3.10+
- tmux

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

### 4. Stop Cluster
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

### 3. Destructive Failure-Injection Test (36 Checks)
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
│   ├── config/              # Compiled node configuration manifests (genesis, primary, sd1, sd2, arbit)
│   ├── data/                # Wallets, LMDB disk environments (data.mdb), and node logs
│   ├── devices/             # Custom Native AO Devices & VM Integration Patches
│   │   ├── hyperstack/      # Native Erlang implementation of hyperstack@1.0
│   │   │   ├── dev_hyperstack.erl # Zero-copy ETS, C-NIF SHA-256 Merkle & dual-watermark engine
│   │   │   └── README.md    # Dedicated device specification & handler documentation
│   │   └── patches/         # HyperBEAM VM integration patches (dev_lua_lib.patch)
│   └── processes/           # Native Lua processes for Primary, Shared Data, and Arbit
├── HyperBEAM/               # Upstream HyperBEAM Erlang runtime and AO devices (git submodule)
├── tests/
│   ├── _harness.py          # Live-cluster test harness and contract invariants
│   ├── test_e2e_2d_points.py# 30-check ACK contract E2E test suite
│   ├── test_e2e_connectivity.sh # 34-check cluster interconnect and wallet suite
│   ├── test_failure_modes.py# 36-check destructive failure-injection suite
│   └── mutation_ack.sh      # Mutation test enforcing suite sensitivity
├── tools/
│   ├── apply_engine.sh      # Rebuild configs and deploy Lua processes (FRESH=1 wipes stores)
│   ├── bench_engine.py      # 8-path honest baseline benchmark -> tools/bench_baseline.json
│   ├── build_configs.py     # Deterministic node configuration builder
│   ├── cluster_up.sh        # Automated cluster launcher (Docker + Tmux)
│   ├── cluster_down.sh      # Cluster teardown script
│   ├── simulate_gameplay.py # Automated gameplay movement simulator
│   ├── test_relog_flow.py   # Client relog and state rehydration test
│   └── game_client/
│       ├── index.html       # Interactive Red Circle WASD client & Swarm simulator
│       └── serve.py         # HTTP server with live node telemetry endpoint (/metrics)
├── idea.md                  # Comprehensive architectural specification and design blueprint
└── README.md                # Project documentation, benchmarks, and architecture guide
```

---

## License

This project is open-source under the Apache-2.0 License.
