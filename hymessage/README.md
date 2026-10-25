# hymessage — Custom Communication Device (Gleam / BEAM)

`hymessage` is the native custom communication device (`hymessage@1.0`) powering the optimistic real-time sequencer and consensus coordinator for the HyDB + HyperBEAM stack.

## Roles & Responsibilities

- **Protocol Ingress**: Implements core HTTP API routes (`/ping`, `/join`, `/put`, `/get`, `/del`, `/snapshot`, `/restore`, `/state`, `/sync`, `/reset`, `/promote_epoch`, `/request_status`).
- **Atomic Sequencer**: Allocates strictly monotonic sequence numbers (`seq`) in RAM via concurrent ETS tables with sub-millisecond overhead.
- **Cryptographic Hash Chain**: Computes deterministic SHA-256 event hash chains (`prev_hash + seq + action + data`).
- **Session Authentication & Fencing**: Evaluates cluster secrets, 128-bit session tokens, and enforces epoch fencing (`hot_epoch.meta`) to prevent split-brain zombies.
- **Sliding Window Buffer**: Retains recent mutation events in memory for fast-path Storage Unit pool synchronization.
- **Bridge to HyDB (WASM)**: Feeds sequenced mutations directly into the in-process `hydb.wasm` engine sharing the node's linear memory.

## Packaging & Deployment

Packaged as a Forge Device Archive (`device.zip`) containing the compiled BEAM module `_hb_device_hymessage_1_0_h0.beam`, registered via operator `trusted-devices` without requiring HyperBEAM engine forks:

```bash
cd hymessage
bash deploy.sh
```
