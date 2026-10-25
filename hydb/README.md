# HyDB — In-Memory Real-Time Database Engine (Rust WASM)

HyDB is a bare-metal in-memory database engine executing directly within HyperBEAM's process address space via `wasm-64@1.0`.

## Core Characteristics

- **In-Process Linear RAM**: WASM linear memory is mapped directly inside the HyperBEAM host process address space. Zero network sockets, zero IPC, with pure CPU memory bus read/write latencies (~100–300 nanoseconds).
- **Zero Heap Allocation**: Employs a contiguous 1024-entry Dense Slot Table with 64-byte cache-line alignment. Completely zero garbage collection pauses and zero memory fragmentation.
- **Incremental State Root ($O(1)$)**: Merkle-XOR Accumulator dynamically computes and updates the 64-bit State Root Hash in $O(1)$ constant time without requiring re-hashing of the entire database.
- **Deterministic Snapshots**: Capable of exporting active state to a compact binary format (`HYDB` magic header) and restoring snapshots into a clean instance in a single `memcpy` operation.
- **Boundary & Guardrail Invariants**: Instruction-level enforcement of numeric limits (`min_value`, `max_value`) directly returning error code `-422` on schema violation, preserving state integrity without host context switches.

## Exported Operations

- `db_put(key: i64, val: i64, seq: i64) -> i64`: Inserts or updates a slot and returns the updated 64-bit State Root.
- `db_get(key: i64) -> i64`: Retrieves value for key in $O(1)$ time.
- `db_del(key: i64, seq: i64) -> i64`: Marks key as tombstone and updates the State Root.
- `db_root_hash() -> i64`: Returns current 64-bit State Root hash.
- `db_count() -> i64`: Returns total active (non-tombstone) slot count.
- `db_reset()`: Clears all slots and resets State Root to zero.
- `db_snapshot_export() -> i64`: Serializes active state to binary export buffer.
- `db_snapshot_import(bytes: i64) -> i64`: Deserializes and restores state with root verification.

## Building and Verification

```bash
# Build bare-metal WebAssembly release binary
cd hydb
bash build.sh

# Target output: hydb/out/hydb.wasm
```
