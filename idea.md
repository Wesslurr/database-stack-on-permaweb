# HyperStack: Architectural Blueprint & Technical Specification
## High-Frequency Real-Time State Engine & Distributed Replication Stack on AO / Permaweb

---

## 1. Executive Summary & PoC Objectives

This document serves as the **core architectural blueprint** for **HyperStack**, a high-frequency real-time state engine (targeting **60 ticks/second**) operating **100% natively on top of the AO Compute, HyperBEAM, and Arweave (Permaweb)** ecosystem in a local cluster environment.

> ### ⚠️ Project Status & Local Latency Reality
> - **100% Localhost Architecture:** All components currently run **100% locally on a single machine** (localhost loopback via WSL/Linux), meaning all measured latencies are free from internet/WAN network delays.
> - **Active Construction & Architectural Foundation:** HyperStack is an architectural foundation under active construction and beta testing.
> - **Explicit Local Database Latency Target (60 Ticks/Second / ~16.6 ms):** The ultimate target for 60 Hz interactive applications is **60 ticks per second**, which equals **~16.6 ms per tick (or under 20 ms local round-trip write + quorum replication)**.
> - **Current Local Latency Gap:** Despite running entirely locally, current round-trip write latency under strict dual-replica quorum (Primary RAM + SD1 RAM+LMDB + SD2 RAM+LMDB) measures **~80–120 ms**. **The engine has not yet met the 16–20 ms (60 tick/s) target.** Bridging this gap through dedicated binary channels and asynchronous batch decoupling is the primary focus of the next development phase.
> - **Interactive Visual Proving Ground:** The **Red Circle WASD Game Client** (`http://localhost:8080/`) exposes this exact local latency live without client-side prediction or mocked numbers.

The primary objective is to **build, prove, and battle-test the concrete Engine Stack** under real-world gameplay workloads prior to extracting generic library or SDK abstractions.

The architecture operates as a **Multi-Tier Frequency Damper**:
- **60 Hz (every ~16.6 ms):** Input streams are absorbed entirely in volatile RAM by the **Primary Process** via direct node-to-node dispatch (`dev_push`).
- **Synchronous Write-Through Gate:** Client feedback is only confirmed once both **Shared Data Replicas (SD1 & SD2)** acknowledge state mutation in RAM and snapshot persistence to LMDB disk (`raw_tick >= pt.tick`).
- **Micro-Epoch RSA Sealing (every 10 ticks):** Eliminates per-move cryptographic bottleneck by grouping moves into micro-epochs sealed with RSA-4096 state roots every 10 ticks.
- **Downstream Asynchronous Batching (every 100 ticks):** SD1 asynchronously dispatches summarized diff checkpoints to the **Arbit Process** via `~relay@1.0/cast`, decoupling high-frequency moves from consensus latency.
- **Permanent L1 Settlement:** Arbit anchors official state roots and triggers decentralized block finality on **Arweave L1 (`arlocal :1984`)**.

---

## 2. Two Communication Paradigms: Direct Node-to-Node vs Scheduled AO Message Box

A foundational architectural requirement is understanding the distinct properties, trade-offs, and appropriate roles of the two communication pathways available in HyperBEAM and AO:

| Dimension | 1. Scheduled AO Message Box (`Inbox / Outbox` via SU & Arweave) | 2. Direct Node-to-Node (Native HyperBEAM HTTP Dispatch) |
| :--- | :--- | :--- |
| **Mechanism** | Messages are wrapped and signed (`ans104@1.0`), dispatched to the Scheduler (`scheduler@1.0` / `~process@1.0/push`), assigned absolute slot numbers, and recorded to Arweave. | Requests are dispatched directly from client or peer HyperBEAM nodes over persistent HTTP (`relay@1.0` / `dev_push`) straight to process memory. |
| **Key Advantage (+)** | **Anti-Double-Spend & Crash-Resilient:** Global deterministic transaction ordering. Even if the recipient node is temporarily down, messages persist in the scheduler queue and execute upon restart. | **Sub-50ms Web2-Grade Latency:** Sub-millisecond to low-millisecond compute loops, zero global scheduler queue delay, and zero per-tick transaction overhead. Capable of sustaining high-frequency game ticks. |
| **Trade-Off (-)** | **Higher Latency:** Constrained by scheduler indexing and per-message cryptographic signing (~50–200ms per hop). Unsuitable for per-frame 60 Hz feedback loops. | **Transient Endpoint:** Pure memory state can be vulnerable if a physical node crashes prior to replication or disk commit. |
| **Enforced Usage** | • Constitutional bootstrap & topology updates in **Genesis Process** (`:8733`).<br>• Checkpoint commitments & anti-double-spend adjudication in **Arbit Process** (`:8737`).<br>• Failover coordination and epoch promotion. | • Real-time input stream & low-latency coordinate writes between **Client $\leftrightarrow$ Primary Process** (`:8734`).<br>• Parallel replication & Quorum ACKs between **Primary $\leftrightarrow$ SD1 (`:8735`) / SD2 (`:8736`)**. |

> **Architectural Synergy:** The transient nature of Direct Node-to-Node execution is fully mitigated by **dynamic topology resolution from Genesis** combined with **synchronous replication across two independent physical SD nodes with dual RAM + LMDB disk persistence**.

---

## 3. Independent Topology: One Process = One Dedicated HyperBEAM Node

All components run locally on authentic Permaweb infrastructure with zero mocked behaviors:
- **Docker** hosts **ArLocal (`:1984`)** acting as the local Arweave L1 Permaweb gateway.
- **Tmux** hosts 5 independent **HyperBEAM Worker Nodes**, ensuring process isolation (failure of one node never halts sibling nodes).

| Physical Node (Tmux / Docker) | Local Port | Executed Process | HyperBEAM Storage & Driver Configuration |
| :--- | :--- | :--- | :--- |
| **`arlocal`** *(Docker)* | `:1984` | **Arweave L1 Ledger** | Permanent storage for Lua/WASM bytecode and ANS-104 transaction bundles. |
| **`hb-genesis`** *(Tmux)* | `:8733` | **1. Genesis Process** | **Constitutional Authority** (`process@1.0` + `scheduler@1.0` + `lua@5.3a` + `patch@1.0`). Single source of truth for cluster topology. |
| **`hb-primary`** *(Tmux)* | `:8734` | **2. Primary Process** | **High-Frequency State Engine** (`hb_store_volatile` pure RAM, `process_workers: true`, micro-epoch RSA signing). |
| **`hb-sd-1`** *(Tmux)* | `:8735` | **3A. Shared Data Replica 1** | **Hot Backup Replica** (`hb_store_lmdb`, RAM latest + LMDB disk snapshot slot 1, diff buffer, batch dispatcher). |
| **`hb-sd-2`** *(Tmux)* | `:8736` | **3B. Shared Data Replica 2** | **Quorum Witness Replica** (`hb_store_lmdb`, independent dual replica for high availability and split-brain defense). |
| **`hb-arbit`** *(Tmux)* | `:8737` | **4. Arbit Process** | **Consensus Gatekeeper** (`process@1.0` + `scheduler@1.0` + `hb_store_lmdb` + `relay@1.0`). Validates batch checkpoints and triggers L1 settlement. |

---

## 4. End-to-End Execution Flow & Component Responsibilities

```text
                  +----------------------------------------------+
                  |              1. GENESIS PROCESS              |
                  |       (Node: hb-genesis | Port :8733)        |
                  | * Stores Epoch, Cluster Topology & Wallets   |
                  | * Zero hardcoded URLs in any worker node     |
                  +----------------------------------------------+
                                         |
                                         | Dynamic 1-Hop Discovery
                                         v
+-----------------------+     Move (X, Y)      +--------------------------------+
|      GAME CLIENT      | -------------------> |       2. PRIMARY PROCESS       |
| (Red Circle WASD UI)  | <------------------- |  (hb-primary :8734 | Pure RAM) |
| * Discover via Genesis|      Confirmed ACK   +--------------------------------+
| * Zero client guess   |      (within RTT)           |                 |
+-----------------------+                             | Write-Through   | Write-Through
                                                      | Parallel Gate   | Parallel Gate
                                                      v                 v
                               +-----------------------------+   +-----------------------------+
                               |     3A. SHARED DATA 1       |   |     3B. SHARED DATA 2       |
                               | (hb-sd-1 :8735 | RAM+LMDB)  |   | (hb-sd-2 :8736 | RAM+LMDB)  |
                               | * Confirms slot & persistence|   | * Confirms slot & persistence|
                               | * Quorum ACK -> Primary     |   | * Quorum ACK -> Primary     |
                               +-----------------------------+   +-----------------------------+
                                              |
                                              | Async Batch Checkpoint (Every 100 Ticks)
                                              | via ~relay@1.0/cast (No client block)
                                              v
                               +-----------------------------------------------+
                               |               4. ARBIT PROCESS                |
                               |         (hb-arbit :8737 | LMDB Consensus)     |
                               | * Validates batch sequence & state root       |
                               | * Triggers L1 Block Settlement on ArLocal     |
                               | * Prunes upstream diff buffers via Cascade    |
                               +-----------------------------------------------+
                                                      |
                                                      | Block Finality (GET /mine)
                                                      v
                               +-----------------------------------------------+
                               |            5. ARWEAVE L1 PERMAWEB             |
                               |             (arlocal :1984 | Docker)          |
                               | * Permanent decentralized ledger settlement   |
                               +-----------------------------------------------+
```

### Detailed Component Specifications

#### 1. Genesis Process (`hb-genesis :8733`)
- **Communication Pathway:** Native AO Process (`process@1.0` + `scheduler@1.0`).
- **Core Responsibilities:**
  1. Acts as the constitutional bootstrap authority for the cluster.
  2. Holds active `Epoch`, verified public keys / addresses of worker nodes, and dynamically advertises peer endpoints (`primary_url`, `sd1_url`, `sd2_url`, `arbit_url`, `l1_url`).
  3. Strict zero-hardcoding guarantee: If cluster topology changes, worker nodes re-sync dynamically from Genesis without code redeployment.

#### 2. Primary Process (`hb-primary :8734`)
- **Communication Pathway:** Direct Persistent HTTP (`dev_push` / `~node-process@1.0/primary/push`).
- **Storage Profile:** 100% Volatile RAM (`hb_store_volatile`). No disk I/O on critical write path.
- **Core Responsibilities:**
  1. Absorbs real-time player input coordinates with minimum latency.
  2. Maintains dense entity state map (`Points`) and bounded audit ring in memory.
  3. **Micro-Epoch RSA Sealing:** Organizes sequential ticks into 10-tick micro-epochs. Moves within an epoch confirm via ultra-fast in-memory quorum; tick multiples of 10 seal an authoritative cryptographic root.
  4. **Strict Write-Through Gate:** Dispatches parallel state writes to SD1 and SD2 via `ao.resolve`, awaiting dual RAM+Disk ACKs before confirming moves to client.
  5. **Authoritative Relog Recovery:** Handles `Read-Point` queries to return verified player coordinates upon page reload or reconnection.

#### 3. Shared Data Processes (`hb-sd-1 :8735` & `hb-sd-2 :8736`)
- **Communication Pathway:** Hybrid — Receives direct synchronous writes from Primary; dispatches async checkpoints to Arbit.
- **Storage Profile:** Dual RAM latest state + LMDB disk persistence (`hb_store_lmdb`).
- **Core Responsibilities:**
  1. Validates incoming tick sequence, ensuring monotonic progression and epoch matching.
  2. Commits coordinates to memory and updates LMDB snapshot state on disk (`raw_tick >= pt.tick`).
  3. Returns signed ACK payload to Primary with confirmed tick numbers and replica identity.
  4. Maintains rolling diff buffer (`RecentDiffs`) for client lag compensation and downstream consensus.
  5. Every 100 ticks (`arbit_batch_interval: 100`), SD1 dispatches an asynchronous batch checkpoint via `~relay@1.0/cast` to Arbit without blocking player move latency.

#### 4. Arbit Process (`hb-arbit :8737`)
- **Communication Pathway:** Scheduled AO Process (`process@1.0` + `scheduler@1.0`).
- **Storage Profile:** LMDB Disk (`hb_store_lmdb`).
- **Core Responsibilities:**
  1. Enforces anti-double-spend consensus and anti-replay validation on batch checkpoints.
  2. Advances `OfficialTick`, increments `CommitmentSeq`, and computes official `OfficialStateRoot`.
  3. Executes block settlement against Arweave L1 (`arlocal :1984/mine`).
  4. Dispatches Cascade-of-Relief prune notifications upstream to SD1 and SD2, releasing older diff slots up to the finalized tick.

#### 5. Arweave L1 Permaweb (`arlocal :1984`)
- **Core Responsibilities:** Decentralized immutable storage layer recording module bytecode, block headers, and finalized state commitments.

---

## 5. Verification & Testing Matrix

The implementation is verified end-to-end against rigorous automated test suites:

1. **Constitutional Discovery Test:** Zero hardcoded IP or port references in any Lua process script; all peers resolve dynamically from Genesis.
2. **Multi-Tier E2E Coordinate Test ([`tests/test_e2e_2d_points.py`](tests/test_e2e_2d_points.py)):** 55/55 assertions verifying Genesis resolution, Primary RAM updates, SD1/SD2 dual replication, Arbit consensus commitment, and Arweave L1 block height progression.
3. **Cluster Health & Interconnect Test ([`tests/test_e2e_connectivity.sh`](tests/test_e2e_connectivity.sh)):** 34/34 assertions verifying ArLocal mining, node Arweave addresses, isolated wallet storage, and direct node-to-node HTTP round-trips.
4. **Interactive Red Circle WASD Client ([`tools/game_client/`](tools/game_client/)):** Live web-based verification of player movement, instant relog position restoration, micro-epoch indicator, and 4-tier backstage telemetry.
