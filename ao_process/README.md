# @hydb/ao-arbitrator — AO Verification Authority & Committer Daemon

This package contains the on-chain consensus, fraud verification, and autonomous committer logic for HyDB on the AO Computer and Arweave.

## Contents

- `arbitrator.lua`: The production Lua smart contract that executes inside the AO process. Handles `CommitCheckpoint`, `SetPolicy`, `PromoteEpoch`, and fraud proofs.
- `committer_daemon.mjs`: Standalone background service that continuously monitors Storage Unit sequences and commits signed checkpoint batches to AO.
- `committer.mjs`: Core Arweave & AO transaction builder supporting both `@permaweb/aoconnect` (for live public AO network) and local ArLocal.
- `real_ao_evaluator.mjs`: High-precision offline AO execution sandbox utilizing `@permaweb/ao-loader`.

## Running the Committer Daemon

```bash
# Continuous background daemon
node committer_daemon.mjs --interval 3000 --su-url http://127.0.0.1:8735/~scheduler@1.0

# Or execute a single commit cycle
node committer_daemon.mjs --once
```
