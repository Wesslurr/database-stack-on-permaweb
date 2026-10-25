# Reproducible Setup, Build & Test Harness Guide

> **Normative Document:** End-to-end instructions for setting up a clean clone, compiling all artifact layers (HyperBEAM, SD Worker, HyDB WASM, AO Loader), and executing isolated test suites.

---

## 1. System Requirements & Toolchains

The stack runs on Linux (native Ubuntu 22.04+ or WSL2 on Windows):

| Dependency | Required Version | Verification Command | Purpose |
| :--- | :--- | :--- | :--- |
| **Node.js** | `>= 20.0.0` (LTS 22 recommended) | `node -v` | Test runners, SDK, ArLocal, AO Loader. |
| **Erlang/OTP** | `27.0` | `erl -eval 'io:format("~s~n", [erlang:system_info(otp_release)]), halt().' -noshell` | BEAM runtime for Hot Node and SD Worker. |
| **Rust & Cargo** | `>= 1.75.0` with `wasm32-unknown-unknown` | `rustc --version && rustup target list \| grep installed` | Compiling `hydb/src/lib.rs` to bare-metal WebAssembly. |
| **Gleam** | `>= 1.0.0` | `gleam --version` | Building typed devices `hymessage` and `sd`. |
| **Lua** | `5.3` or `5.4` | `lua -v` | Conformance runner and standalone Lua evaluation. |

---

## 2. Clean Clone Setup Procedure

Execute the following in a clean clone of the repository:

```bash
# 1. Install Node.js dependencies
npm install

# 2. Generate ephemeral developer/test wallets (if secrets/ is absent)
node scripts/gen-wallet.mjs
node scripts/gen-wallet-su.mjs

# 3. Setup build environment
source scripts/env.sh
```

---

## 3. Building All Artifact Layers

### 3.1 HyDB Silicon WASM Engine (`hydb.wasm`)
Compiles `hydb/src/lib.rs` to bare-metal WebAssembly without heap (`no_std`):
```bash
bash hydb/build.sh
```
*Artifact Output:* `hydb/out/hydb.wasm` and `hydb/out/wasm_id.txt`.

### 3.2 Hot Node Hymessage Device (`_hb_device_hymessage_1_0_h0.beam`)
Builds Gleam source, renames module into device identifier, packages into `device.zip`, and updates trusted device hash in `config/hymessage.json`:
```bash
bash hymessage/deploy.sh
```
*Artifact Output:* `hymessage/out/device.zip` and `hymessage/out/device_id.txt`.

### 3.3 SD Worker Device (`_hb_device_sd_1_0_sd0.beam`)
Builds Gleam SD pool device, packages into `device.zip`, and updates `config/sd.json`:
```bash
bash sd/deploy.sh
```
*Artifact Output:* `sd/out/device.zip` and `sd/out/device_id.txt`.

### 3.4 AO Loader & Process WASM
The Arbitrator and Genesis processes run on the AO Computer and use the official `@permaweb/ao-loader` package installed via npm alongside the official AO `process.wasm` located in `ao_process/process.wasm`.

---

## 4. Spawning the AO Genesis Stack

HyDB uses an autonomous dual-process stack model:
1. **AO Genesis Process** (`ao_process/genesis.lua`): Topology anchor, cluster nodes registry, and internal process binding.
2. **AO Arbitrator Process** (`ao_process/arbitrator.lua`): Data plane consensus, 64-bit Merkle-XOR and 256-bit SHA-256 commitments.

```bash
# Dry-run stack compilation and hash verification
npm run stack:spawn -- --dry-run

# Live deployment to AO testnet
npm run stack:spawn -- --network testnet --stack-id hydb-alpha --wallet secrets/wallet.json
```

---

## 5. Test Suite Execution Catalog

Every test suite is runnable via dedicated npm commands:

```bash
# 1. Cross-Language Conformance (JS, Erlang, Rust, Lua - 87 Tests)
npm run test:conformance

# 2. Phase 01: Security & Cluster Endpoint Hardening (16 Tests)
npm run test:security

# 3. Phase 02: WAL Recovery & Data Integrity (10 Tests)
npm run test:recovery

# 4. Phase 03: Canonical Hash & Fixed Quorum Verification
npm run test:quorum

# 5. Phase 04: Write Idempotency & Provisional Status Semantics
npm run test:idempotency

# 6. Phase 05: Multi-Process Chaos & Fault Injection
npm run test:chaos

# 7. Phase 06: Evidence-Based AO Settlement & Durable Cursor (81 Tests)
npm run test:committer

# 8. Phase 07: Semantic & Documentation Release Gate Audit (62 Tests)
npm run test:release-gate

# 9. Phase 08: Developer SDK & Canonical Manifest (39 Tests)
npm run test:sdk

# 10. Phase 09: AO Genesis Block & Master Contract Verification (29 Tests)
npm run test:genesis

# 11. Phase 10: SDK Spawner & Stack Genesis Integration (68 Tests)
npm run test:spawner

# 12. Pre-Sequence Gating & Core Contract Rules (54 Tests)
npm run test:contract

# 13. Physical Durability, fsync, and Auth (39 Tests)
npm run test:durability

# 14. AO Arbitrator State Machine & Policy Circuit (109 Tests)
npm run test:ao

# 15. Business Logic Scenarios (Financial Ledger & Gaming - 16 Tests)
npm run test:business

# 16. Master Fault-Tolerance & Security Verification (14 End-to-End Phases)
npm run test:integration

# 17. Run All Verified Suites Sequentially
npm run test:all
```

---

## 6. Clean Environment Guarantee

To ensure tests never depend on stale WAL or hidden state:
1. `secrets/` is generated automatically via `npm run wallet` if missing.
2. `sd/wal.log` and `hymessage/hot_wal.log` are wiped or reset cleanly at the beginning of integration suites.
3. All file paths use environment variable overrides (`DB_DIR`, `WALLET_PATH`, `HYDB_WAL_PATH`) defaulting to repository-relative paths.
