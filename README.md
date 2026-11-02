# HyperStack: Real-Time State Engine

> **High-Frequency Real-Time State Engine & Distributed Replication Stack on AO Compute & Arweave Permaweb**

[![Project](https://img.shields.io/badge/Project-HyperStack-orange)](https://github.com/Wesslurr/database-stack-on-permaweb)
[![Status](https://img.shields.io/badge/Status-Active%20Construction%20%2F%20Beta-amber)](idea.md)
[![Permaweb](https://img.shields.io/badge/Arweave-Permaweb-black?logo=arweave)](https://arweave.org/)
[![AO Compute](https://img.shields.io/badge/AO-Compute-blue)](https://ao.g8way.io/)
[![HyperBEAM](https://img.shields.io/badge/HyperBEAM-Erlang%20OTP%2026-red)](https://github.com/permaweb/hyperbeam)
[![Tests](https://img.shields.io/badge/E2E%20Tests-89%2F89%20Passing-brightgreen)](tests/)

> [!IMPORTANT]
> ### 🚧 Project Status & Local Latency Transparency Notice
> **HyperStack** is an experimental multi-tier state engine and distributed replication stack built directly on **HyperBEAM, AO Compute, and Arweave L1**.
>
> - **100% Localhost Multi-Node Architecture:** Everything in this repository currently runs **completely locally** on a single host machine across isolated localhost ports (`:1984`, `:8733`, `:8734`, `:8735`, `:8736`, `:8737`, `:8080`). There is **zero wide-area network (WAN) or public internet overhead** involved in these tests—all measurements represent raw local loopback and inter-process communication.
> - **Under Active Construction:** This codebase represents an architectural foundation currently under active construction and beta testing. While the multi-tier pipeline, dual-replica LMDB disk persistence, micro-epoch RSA sealing, and Arweave L1 settlement are fully functional (verified by 89/89 automated assertions), the engine stack is being actively refined.
> - **Explicit Local Database Latency Target (60 Ticks/Second / ~16.6 ms):** For a high-frequency real-time engine or 60 Hz simulation loop, 1 second comprises 60 discrete ticks (1000 ms / 60 = **16.66 ms per tick**). Therefore, the ultimate engineering target for local database round-trip write + dual-replica quorum confirmation is **~16 ms (or under 20 ms local latency)**.
> - **Current Local Benchmark Reality (~80–120 ms):** **At present, the engine has not yet achieved this local 16–20 ms target.** Even on pure localhost loopback, observed round-trip latency (RTT) under strict synchronous dual-replica write-through replication (Primary RAM &rarr; SD1 RAM+LMDB + SD2 RAM+LMDB quorum) hovers around **~80–120 ms**. This shows that internal process dispatch, HTTP/1.1 message parsing, and multi-tier synchronous waiting still exceed the 60 Hz tick budget and require further transport decoupling.
> - **Interactive Visual Latency Proving Ground (The Red Circle Test):** To expose this local latency transparently without artificial figures or client-side prediction, the repo provides an interactive WASD web client (`http://localhost:8080/`). The Red Circle moves strictly after authoritative backend confirmation, visually displaying true local RTT in real-time.

**HyperStack** is designed for high-frequency interactive applications (such as real-time multiplayer games, streaming IoT coordinates, and low-latency order books) operating natively on the **AO Compute & HyperBEAM** architecture with decentralized settlement on **Arweave L1**.

---

## Architecture Overview

Traditional blockchain smart contracts are bounded by global block intervals (seconds to minutes) and per-transaction signature delays. This engine solves this challenge via a **Multi-Tier Frequency Damper**:

```text
                  +-----------------------------------------------+
                  |              1. GENESIS PROCESS               |
                  |       (hb-genesis :8733 | AO Scheduled)       |
                  |  Constitutional discovery & epoch authority   |
                  +-----------------------------------------------+
                                          |
                                          | 1-Hop Dynamic Discovery
                                          v
+------------------------+      Move (X, Y)       +-------------------------------+
|      GAME CLIENT       | ---------------------> |      2. PRIMARY PROCESS       |
| (Red Circle WASD UI)   | <--------------------- | (hb-primary :8734 | Pure RAM) |
| * Zero client guess    |      Confirmed ACK     +-------------------------------+
| * Instant relog read   |      (within RTT)             |                 |
+------------------------+                               | Parallel ACK    | Parallel ACK
                                                         | Gate            | Gate
                                                         v                 v
                                +------------------------------+   +------------------------------+
                                |      3A. SHARED DATA 1       |   |      3B. SHARED DATA 2       |
                                |  (hb-sd-1 :8735 | RAM+LMDB)  |   |  (hb-sd-2 :8736 | RAM+LMDB)  |
                                |  Rolling diffs & LMDB backup |   |  Quorum witness & hot replica|
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

1. **Zero Hardcoded Peer URLs:** All worker nodes dynamically resolve peer topology from Genesis in 1 hop. Zero hardcoded ports or IP addresses exist inside any worker process Lua code.
2. **Strict Replication Gate:** Client moves are never confirmed solely on volatile memory; feedback is returned only after **SD1 and SD2 acknowledge state mutation in RAM and snapshot persistence on disk** (`raw_tick >= pt.tick`).
3. **Micro-Epoch RSA-4096 Sealing:** Moves are processed via ultra-fast in-memory quorum, and sealed into authoritative cryptographic roots every 10 ticks.
4. **Cascade-of-Relief Downstream Batching:** SD1 asynchronously forwards summarized diff batches to Arbit every 100 ticks (`arbit_batch_interval: 100`). Arbit finalizes blocks on Arweave L1 and prunes upstream SD diff buffers.
5. **Authoritative Relog State Recovery:** On client refresh or reconnection, player coordinates are restored authoritatively from Primary RAM and SD replicas via `Read-Point` queries—eliminating stale client-side caches or guess-work.

---

## Node Topology

The local cluster operates across 6 isolated execution environments:

| Node | Port | Process | Role & Storage Mode |
| :--- | :--- | :--- | :--- |
| **`arlocal`** | `:1984` | Arweave L1 Gateway | Docker container running native ArLocal permaweb gateway. |
| **`hb-genesis`** | `:8733` | Genesis Process | Constitutional bootstrap authority; advertises topology and cluster epoch. |
| **`hb-primary`** | `:8734` | Primary Process | High-frequency 2D state engine; 100% volatile RAM (`hb_store_volatile`). |
| **`hb-sd-1`** | `:8735` | Shared Data Replica 1 | Dual RAM latest + LMDB disk snapshot persistence (`hb_store_lmdb`). |
| **`hb-sd-2`** | `:8736` | Shared Data Replica 2 | Quorum witness and redundant backup replica (`hb_store_lmdb`). |
| **`hb-arbit`** | `:8737` | Arbit Consensus Process | Consensus gatekeeper, official state root anchor, and L1 block finalizer. |

---

## Quick Start Guide

### Prerequisites
- Linux or WSL2 (Ubuntu 22.04+ recommended)
- Erlang/OTP 26+ and Rebar3
- Docker (for ArLocal)
- Python 3.10+
- tmux

### 1. Clone with Submodule
```bash
git clone --recurse-submodules https://github.com/Wesslurr/database-stack-on-permaweb.git
cd database-stack-on-permaweb
```
*(If cloned normally without submodules, `./tools/cluster_up.sh` automatically fetches and sets up the `HyperBEAM` submodule on first run).*

### 2. Start Cluster
Launch ArLocal in Docker and all 5 HyperBEAM nodes in independent tmux sessions:
```bash
./tools/cluster_up.sh
```
All nodes will initialize and perform automated self-health checks.

### 3. Launch Interactive Game Client
Start the game client server with live OS and Docker container telemetry:
```bash
python3 tools/game_client/serve.py
```
Open your browser at **`http://localhost:8080/`**.

- Move the red circle using **W / A / S / D** or **Arrow Keys** (or click inside the arena).
- Observe real-time Round-Trip Time (RTT), confirmed tick numbers, and micro-epoch indicators.
- Inspect the **Backstage Multi-Tier Inspector** on the right column to monitor live RAM (RSS), CPU, LMDB disk sizes, and Arweave L1 block height progression.

### 4. Stop Cluster
Gracefully shut down all tmux node sessions and stop ArLocal:
```bash
./tools/cluster_down.sh
```

---

## Automated Verification & Test Suite

The codebase includes an extensive suite of automated tests verifying the entire 4-tier lifecycle:

### Multi-Tier Coordinate E2E Test (55 Assertions)
Verifies constitutional discovery, Primary RAM writes and reads, SD1 & SD2 dual replication, Arbit consensus state roots, and Arweave L1 block mining:
```bash
python3 tests/test_e2e_2d_points.py
```

### Cluster Interconnect & Identity Test (34 Assertions)
Verifies ArLocal health and mining, distinct cryptographic node wallets, and direct HTTP round-trip connectivity between all nodes:
```bash
./tests/test_e2e_connectivity.sh
```

### Real-Time Gameplay Simulation
Simulates a 40-step WASD path, testing directional movement, micro-epoch sealing, and replica consistency:
```bash
python3 tools/simulate_gameplay.py
```

### Relog & State Recovery Test
Simulates client disconnections and verifies authoritative coordinate recovery via `Read-Point`:
```bash
python3 tools/test_relog_flow.py
```

---

## Repository Structure

```text
├── engine/
│   ├── config/              # Compiled node configuration manifests (genesis, primary, sd1, sd2, arbit)
│   ├── data/                # Node runtime wallets, LMDB disk environments, and logs
│   └── processes/           # Native Lua processes for Primary, Shared Data, and Arbit
├── HyperBEAM/               # Upstream HyperBEAM Erlang runtime and AO devices
├── tests/
│   ├── test_e2e_2d_points.py       # 55-assertion multi-tier E2E test
│   └── test_e2e_connectivity.sh    # 34-assertion cluster health and interconnect test
├── tools/
│   ├── build_configs.py     # Deterministic node config builder
│   ├── client.py            # Headless Python client for the state engine
│   ├── cluster_up.sh        # Cluster launcher (Docker + Tmux)
│   ├── cluster_down.sh      # Cluster teardown
│   ├── simulate_gameplay.py # Automated WASD gameplay simulator
│   ├── test_relog_flow.py   # Client relog and state recovery test
│   └── game_client/
│       ├── index.html       # Interactive Red Circle WASD web client & Backstage Inspector
│       └── serve.py         # HTTP server with live node telemetry endpoint (/metrics)
├── idea.md                  # Comprehensive architectural specification and design blueprint
└── README.md                # Project documentation and quick start guide
```

---

## License

This project is open-source under the Apache-2.0 License.
