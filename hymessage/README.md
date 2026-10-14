# hymessage — Custom Communication Device (Gleam / BEAM)

`hymessage` adalah custom communication device (`hymessage@1.0`) untuk stack database real-time optimistic AO + HyperBEAM.

## Peran & Tanggung Jawab

- **Pintu Masuk Protokol**: Menangani rute request AO-Core (`/ping`, `/join`, `/put`, `/get`, `/del`, `/snapshot`, `/restore`, `/state`, `/sync`, `/reset`).
- **Atomic Sequencer**: Mengalokasikan sequence number monotonik (`seq`) secara aman dan instan di RAM via ETS.
- **Cryptographic Hash Chain**: Menghitung rantai hash SHA-256 (`prev_hash + seq + action + data`) deterministik.
- **Session Auth**: Manajemen token sesi 128-bit untuk verifikasi klien mikrodetik.
- **Sliding Window Buffer**: Menyimpan 100 mutasi event terakhir di RAM untuk kebutuhan failover/sinkronisasi Pool SU.
- **Bridge ke HyDB (WASM)**: Meneruskan data mutasi ke engine `hydb` (WASM) yang hidup di RAM node HyperBEAM yang sama.

## Packaging

Dipaketkan sebagai Forge Device Archive (`device.zip`) yang berisi generated BEAM module `_hb_device_hymessage_1_0_h0.beam`, didaftarkan via `trusted-devices` operator tanpa fork HyperBEAM.
