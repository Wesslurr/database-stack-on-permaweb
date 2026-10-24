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
The Arbitrator uses the official `@permaweb/ao-loader` package installed via npm and the official AO `process.wasm` located in `ao_process/process.wasm`.

---

## 4. Test Suite Execution Catalog

Every test suite is runnable via dedicated npm commands:

```bash
# 1. Cross-Language Conformance (JS, Erlang, Rust, Lua - 63 Tests)
npm run test:conformance

# 2. Pre-Sequence Gating & Schema Rules (Tahap 3 & 4 - 72 Tests)
npm run test:contract

# 3. Physical Durability, WAL fsync, and Auth (Tahap 5 - 39 Tests)
npm run test:durability

# 4. AO Arbitrator State Machine & Policy Circuit (Tahap 6 & 7 - 109 Tests)
npm run test:ao

# 5. Developer SDK & Canonical Manifest (Tahap 8 - 39 Tests)
npm run test:sdk

# 6. Business Logic Scenarios (Financial Ledger & Gaming)
npm run test:business

# 7. Master Fault-Tolerance & Security Verification (14 Phases)
npm run test:integration

# 8. Run All Verified Suites Sequentially
npm run test:all
```

---

## 5. Clean Environment Guarantee

To ensure tests never depend on stale WAL or hidden state:
1. `secrets/` is generated automatically via `npm run wallet` if missing.
2. `sd/wal.log` is wiped or reset cleanly at the beginning of integration suites.
3. All file paths use environment variable overrides (`DB_DIR`, `WALLET_PATH`, `HYDB_WAL_PATH`) defaulting to repository-relative paths.
