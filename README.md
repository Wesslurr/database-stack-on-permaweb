# HyperStack: Real-Time State Engine

> **High-Frequency Real-Time State Engine & Distributed Replication Stack on AO Compute & Arweave Permaweb**

[![Project](https://img.shields.io/badge/Project-HyperStack-orange)](https://github.com/Wesslurr/database-stack-on-permaweb)
[![Status](https://img.shields.io/badge/Status-Active%20Construction%20%2F%20Beta-amber)](idea.md)
[![Permaweb](https://img.shields.io/badge/Arweave-Permaweb-black?logo=arweave)](https://arweave.org/)
[![AO Compute](https://img.shields.io/badge/AO-Compute-blue)](https://ao.g8way.io/)
[![HyperBEAM](https://img.shields.io/badge/HyperBEAM-Erlang%20OTP%2026-red)](https://github.com/permaweb/hyperbeam)
[![Tests](https://img.shields.io/badge/Live%20Cluster%20Tests-66%2F66%20Passing-brightgreen)](tests/)

> [!IMPORTANT]
> ### 🚧 Project Status & Local Latency Transparency Notice
> **HyperStack** is an experimental multi-tier state engine and distributed replication stack built directly on **HyperBEAM, AO Compute, and Arweave L1**.
>
> - **100% Localhost Multi-Node Architecture:** Everything in this repository currently runs **completely locally** on a single host machine across isolated localhost ports (`:1984`, `:8733`, `:8734`, `:8735`, `:8736`, `:8737`, `:8080`). There is **zero wide-area network (WAN) or public internet overhead** involved in these tests—all measurements represent raw local loopback and inter-process communication.
> - **Under Active Construction:** This codebase represents an architectural foundation currently under active construction and beta testing. The multi-tier pipeline, dual-replica LMDB disk persistence, quorum ACK verification and Arweave L1 settlement are exercised by a live-cluster suite (30 contract checks + 36 failure-injection checks), not by self-reported assertions.
> - **ACK contract (non-negotiable):** an HTTP `200` with `ack: "replicated"` is returned **only after a real SD response was verified** (echoed `batch_id`, tick and canonical root). A `202` means only *accepted into Primary RAM*. If the quorum cannot be reached the Primary returns `503 quorum_unavailable` and `Tick` / `StateRoot` stay unchanged.
> - **Measured local latency (loopback, keep-alive, 2026-11-06):** L0 accept (`202`) **p50 207 ms / p90 256 ms / p99 379 ms**; L1 confirmed (`200`) **p50 620 ms / p90 663 ms / p99 703 ms**; `/now` read **p50 12 ms**; `/push` snapshot read **p50 80 ms**; one `relay/call` to an SD **p50 151 ms**. These are the honest numbers from `tools/bench_baseline.json`; nothing is predicted or faked.
> - **Why the 60 Hz tick target is not met:** the simulation tick budget (`16.6 ms`) is a pure-RAM quantity, while an honest replicated ACK must pay at least one network round trip **plus** the Lua/relay cost inside HyperBEAM. Each `relay/cast` inside `luerl` costs tens of milliseconds (~70 ms in the measured path), so the L1 commit chain (write → SD apply+ack → Primary verification) is bounded by process dispatch, not by the tick loop. The ROADMAP SLO table is revised from measurement, not from hope.
> - **Slot size target not met:** the Primary's computed slot is **~521 KB** with 140 entities (target `≤ 8 KB`). Sharding removed the O(N) full-state re-hash from the commit path, but the slot still carries the process state. This is tracked as an open gap, not hidden.
> - **Interactive Visual Latency Proving Ground (The Red Circle Test):** To expose this local latency transparently without artificial figures or client-side prediction, the repo provides an interactive WASD web client (`http://localhost:8080/`). The Red Circle moves strictly after the verified backend confirmation, and the UI shows separate **accepted** and **confirmed** badges.

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
| * Instant relog read   |   (verified 200 only)         |                 |
+------------------------+                               | Sync commit     | Async commit
                                                         | (quorum)        | (hot backup)
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
2. **Honest Replication Gate (2-of-3):** A write is confirmed only after **Primary + one SD** agree on `batch_id`, tick and canonical root. The confirming SD is chosen at cast time (SD1 normally, SD2 when SD1 is down); the other replica receives the same delta asynchronously as a hot standby and is healed from the delta log if it was offline. Quorum failure returns `503` and never mutates Primary state.
3. **Canonical sharded state root:** the root is `sha256("HS2|tick=<n>|shards" + per-shard digests)` over 32 FNV-1a shards; a commit re-hashes only the shards it touched, so the commit path no longer re-hashes the whole state.
4. **Cascade-of-Relief Downstream Batching:** SD1 asynchronously forwards summarized diff batches to Arbit every 100 ticks (`arbit_batch_interval: 100`). Arbit finalizes blocks on Arweave L1 and prunes upstream SD diff buffers.
5. **Authoritative Relog State Recovery:** On client refresh or reconnection, player coordinates are restored authoritatively from Primary RAM and SD replicas via `Read-Point` queries—eliminating stale client-side caches or guess-work. A Primary that restarted empty hydrates from the replica that is ahead and refuses to overwrite it with stale state.

---

## Node Topology

The local cluster operates across 6 isolated execution environments:

| Node | Port | Process | Role & Storage Mode |
| :--- | :--- | :--- | :--- |
| **`arlocal`** | `:1984` | Arweave L1 Gateway | Docker container running native ArLocal permaweb gateway. |
| **`hb-genesis`** | `:8733` | Genesis Process | Constitutional bootstrap authority; advertises topology and cluster epoch. |
| **`hb-primary`** | `:8734` | Primary Process | High-frequency 2D state engine; 100% volatile RAM (`hb_store_volatile`). |
| **`hb-sd-1`** | `:8735` | Shared Data Replica 1 | LMDB disk store; normally the replica that confirms the 2-of-3 quorum. |
| **`hb-sd-2`** | `:8736` | Shared Data Replica 2 | LMDB disk store; async hot standby, and the confirming replica when SD1 is down. |
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

The suite talks to a **live local cluster** and fails loudly when a node is missing; it never fakes replication or silently skips. See `tests/_harness.py`.

### Contract E2E test (30 checks)
Verifies the ACK contract end to end: `202 accepted` never claims replication, `200 replicated` is verified, idempotent retry keeps one `batch_id`, ownership forgery is rejected with `403`, input guards fire, unknown batches are `404`, and all three nodes converge on the same canonical root.
```bash
python3 tests/test_e2e_2d_points.py
```

### Failure-injection test (36 checks, destructive)
Kills and restarts replicas with tmux: quorum failure → `503` with unchanged tick/root, forged Acks → `409`, retry of the same `batch_id` → exactly one apply, failover SD1 → SD2, rejoining replica catches up from the delta log, out-of-order commits are buffered and stale ones rejected.
```bash
python3 tests/test_failure_modes.py --yes
```

### Cluster Interconnect & Identity Test
Verifies ArLocal health and mining, distinct cryptographic node wallets, and direct HTTP round-trip connectivity between all nodes:
```bash
./tests/test_e2e_connectivity.sh
```

### Baseline benchmark (honest numbers)
Measures 8 paths with keep-alive connections and writes `tools/bench_baseline.json`:
```bash
python3 tools/bench_engine.py
```

### Mutation test (does the suite actually guard anything?)
Breaks the Primary's ACK root/tick verification on purpose, rebuilds the cluster, and **requires the failure-injection suite to fail**. The original file is restored and the cluster rebuilt in a trap, whatever happens.
```bash
./tests/mutation_ack.sh
```

### Gameplay & relog helpers
```bash
python3 tools/simulate_gameplay.py
python3 tools/test_relog_flow.py
```

---

## Measured Baseline (loopback, 2026-11-06)

From `tools/bench_baseline.json` (`BENCH_N=150`, keep-alive, single local host):

| Path | p50 | p90 | p99 | Notes |
| :--- | ---: | ---: | ---: | :--- |
| `/now` read | 12.0 ms | 12.4 ms | 12.6 ms | no new slot |
| `write_202` (L0 accept) | 207 ms | 256 ms | 379 ms | one `relay/cast` + Lua compute |
| `write_confirm` (L1 replicated) | 620 ms | 663 ms | 703 ms | write + SD apply/Ack + Primary verification |
| `advance` (batch close) | 203 ms | 228 ms | 264 ms | |
| `read_push` (snapshot) | 80 ms | 82 ms | 82 ms | creates a slot |
| `relay_call_sd` (Primary → SD1) | 151 ms | 155 ms | 158 ms | measured through `Probe-SD` |
| `sd_direct` (direct SD1 call) | 80 ms | 85 ms | 89 ms | network/process floor |
| `cold_hydrate` (full state from SD1) | 153 ms | 159 ms | 163 ms | recovery proxy |

Cluster at measurement time: 140 entities, Primary RSS **420 MB**, SD RSS ~177 MB, LMDB ~27 MB per SD, computed slot **521 KB** (Primary) / 334 KB (SD), roots equal on all three nodes.

The L1 number is the honest cost of the contract: `202` + one SD round trip + verification. Cutting it requires transport/device changes (or an L2-style batched settlement), not a smaller number in the README.

---

## Repository Structure

```text
├── engine/
│   ├── config/              # Compiled node configuration manifests (genesis, primary, sd1, sd2, arbit)
│   ├── data/                # Node runtime wallets, LMDB disk environments, and logs
│   └── processes/           # Native Lua processes for Primary, Shared Data, and Arbit
├── HyperBEAM/               # Upstream HyperBEAM Erlang runtime and AO devices
├── tests/
│   ├── _harness.py          # live-cluster harness + contract invariants
│   ├── test_e2e_2d_points.py# 30-check ACK contract E2E test
│   ├── test_failure_modes.py# 36-check failure injection (--yes)
│   └── mutation_ack.sh      # breaks ACK verification; suite must go red
├── tools/
│   ├── apply_engine.sh      # Rebuild configs/embed Lua and restart nodes (FRESH=1 wipes stores)
│   ├── bench_engine.py      # 8-path honest baseline -> tools/bench_baseline.json
│   ├── build_configs.py     # Deterministic node config builder
│   ├── client.py            # Headless Python client for the honest ACK contract
│   ├── cluster_up.sh        # Cluster launcher (Docker + Tmux)
│   ├── cluster_down.sh      # Cluster teardown
│   ├── simulate_gameplay.py # Automated WASD gameplay simulator
│   ├── test_relog_flow.py   # Client relog and state recovery test
│   └── game_client/
│       ├── index.html       # Interactive WASD client (accepted vs confirmed badges)
│       └── serve.py         # HTTP server with live node telemetry endpoint (/metrics)
├── idea.md                  # Comprehensive architectural specification and design blueprint
└── README.md                # Project documentation and quick start guide
```

---

## License

This project is open-source under the Apache-2.0 License.
