# @hydb/ao-processes — AO Stack Control Plane & Arbitrator Data Plane

This package contains the on-chain consensus, cluster topology constitution, fraud verification, and autonomous committer logic for HyDB running on the AO Computer and Arweave.

## Architecture: Dual Independent AO Process Model

HyDB partitions on-chain responsibilities between two independent AO processes bound cryptographically via canonical hashes:

```
+----------------------------------------------------------------------------------+
|                           HYDB AO CLUSTER ARCHITECTURE                           |
+----------------------------------------------------------------------------------+
|                                                                                  |
|   1. AO Control Plane: Genesis Process (ao_process/genesis.lua)                  |
|      - Root of Trust & Topology Constitution (StackId, GenesisHash, ConfigEpoch) |
|      - Cluster Node & Component Registry (AuthorizedNodes, NodesRegistry)        |
|      - Internal AO Process Pointer (ArbitratorProcessId)                         |
|      - Observability & Cluster Health (GetStackStatus, ReportHeartbeat)          |
|      - Lazy / Lightweight: zero fast-path bottleneck                             |
|                                                                                  |
|   2. AO Data Plane: Arbitrator Process (ao_process/arbitrator.lua)               |
|      - Pure Data Plane Consensus & Cryptographic Settlement                      |
|      - 64-bit Merkle-XOR Accumulator & 256-bit SHA-256 State Commitments         |
|      - Fraud & Equivocation Proof Verifier                                       |
|      - Fencing & Constitutional Guardrails                                       |
|      - Bound to Stack via Genesis-Process-Id & Genesis-Hash tags                 |
|                                                                                  |
+----------------------------------------------------------------------------------+
```

## Directory Contents

- **`genesis.lua`**: The AO Genesis Process smart contract. Manages stack topology, authorized nodes, heartbeats, and binds internal stack processes (`ArbitratorProcessId`).
- **`arbitrator.lua`**: The production AO Arbitrator smart contract. Handles batch checkpoint commits (`CommitCheckpoint`), policy configurations (`SetPolicy`), epoch promotions (`PromoteEpoch`), and fraud proofs.
- **`committer.mjs`**: Core Arweave & AO transaction builder and autonomous committer supporting `@permaweb/aoconnect` (public AO network), local ArLocal, durable cursor persistence, and dynamic stack discovery via `AO_GENESIS_ID`. Note: In production HyperBEAM deployments, committing is handled natively inside BEAM via `sd_committer.erl`.
- **`real_ao_evaluator.mjs`**: Deterministic offline AO execution sandbox utilizing `@permaweb/ao-loader` and `process.wasm`.
- **`deploy_ao_process.mjs`**: CLI utility for deploying individual processes to the AO network.

## Spawning a Genesis Stack

You can spawn and bind a complete dual-process stack using the spawner script:

```bash
# Dry-run compilation and verification
npm run stack:spawn -- --dry-run

# Live deployment to AO network
npm run stack:spawn -- --network testnet --stack-id hydb-alpha --wallet secrets/wallet.json
```

## Running the Committer CLI / Service

The committer automatically discovers the Arbitrator process from `AO_PROCESS_ID` or through the Genesis process via `AO_GENESIS_ID`:

```bash
# Continuous background mode (polls SU endpoint and commits checkpoints)
node committer.mjs --interval 3000 --su-url http://127.0.0.1:8735/~scheduler@1.0

# Single execution commit cycle
node committer.mjs --once
```

## Dynamic Resolution Invariant

When `AO_GENESIS_ID` or `process_genesis_id.txt` is provided, `getOrCreateProcessId()` queries `genesis.lua` (`GetGenesisConfig`) to dynamically resolve `ArbitratorProcessId`. This decouples cluster components while ensuring tamper-evident alignment with the stack constitution.
