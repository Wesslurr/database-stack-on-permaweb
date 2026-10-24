# HyDB — In-Memory Living Database Engine (Rust WASM)

HyDB adalah engine database in-memory *real-time* yang berjalan langsung di dalam memori RAM HyperBEAM via `wasm-64@1.0`.

## Karakteristik Inti

- **In-Process RAM**: Linear memory WASM dialokasikan langsung di address space proses HyperBEAM. Nol socket, nol IPC, latensi akses murni CPU memory bus (~100–300 nanodetik).
- **Zero Heap Allocation**: Menggunakan 1024-entry Dense Slot Table kontigu dengan alinyemen cache-line 64-byte. Nol garbage collection, nol memory fragmentation.
- **Incremental State Root ($O(1)$)**: Merkle-XOR Accumulator menghitung pembaruan State Root Hash secara instan tanpa perlu re-hashing seluruh database.
- **Snapshot Deterministik**: Mampu mengekspor state aktif menjadi binary compact (HYDB format) dan me-restore snapshot ke instance baru dalam satu kali `memcpy`.

## Operasi yang Diekspor

- `db_put(key: i64, val: i64, seq: i64) -> i64` (Menghasilkan State Root baru)
- `db_get(key: i64) -> i64` (Membaca nilai key dalam $O(1)$)
- `db_del(key: i64, seq: i64) -> i64` (Menghapus key & memperbarui State Root)
- `db_root_hash() -> i64` (State Root saat ini)
- `db_count() -> i64` (Total record aktif)
- `db_snapshot_export() -> i64` (Serialisasi state ke buffer biner)
- `db_snapshot_import(bytes: i64) -> i64` (Restore state & verifikasi State Root)

## Build & Test

```bash
cd hydb
bash build.sh
```
