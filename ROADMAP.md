# ROADMAP — HyperStack Engine

> **Prinsip utama dokumen:** Feedback ke client hanya berlabel *confirmed* jika state
> benar-benar sudah direplikasi (quorum). Ack tidak boleh berbohong — pernah, tidak boleh lagi.
>
> Terakhir diperbarui: 2026-11-06
> Basis bukti: pengukuran live cluster localhost (Fase 0 di bawah), verifikasi sumber
> di `HyperBEAM/src`, dan audit `engine/processes/` + `engine/config/`.

---

## 1. Masalah yang harus diselesaikan

### 1.1 Temuan audit (terbukti, bukan teori)

| # | Temuan | Bukti | Lokasi |
|---|--------|-------|--------|
| T1 | Read lewat `/push` mencemari store | 1 slot baru/185 KB per detik; p50 **93 ms** ketimbang `/now` **16,3 ms** | `tools/game_client/index.html`, `dev_push.erl` |
| T2 | `sd_ack: true` palsu dan **tautologis** | `ack_nodes` diisi di luar `pcall`; `confirmed_sd_tick` di-set tanpa membaca respons; `is_quorum` tidak pernah bisa `false` | `primary_process.lua` |
| T3 | Klaim "atomic rollback" = dead code | Tidak ada jalur eksekusi yang bisa mencapai `error_sd_backup_incomplete` | `primary_process.lua` |
| T4 | State root tidak kanonik | Hash dihitung dari `pairs()` (urutan tidak deterministik) + string format | `sd_process.lua` |
| T5 | Divergensi replika nyata | Primary 36 entitas, SD1 67, SD2 59; root `0x852d…` vs `0xc727…` | live cluster |
| T6 | Hydration cold-start rusak | Regex tidak match karena `"owner"` muncul setelah `"tick"` → 0 poin termuat saat restart | `primary_process.lua` ↔ `sd_process.lua` |
| T7 | Slot menyimpan full state | `computed_slot_size` ~185 KB, tumbuh O(jumlah entitas) | `dev_process.erl:404–433` |
| T8 | Memory tidak berbatas | Primary ~596 MB (binaries ~437 MB, ETS ~90 MB); cache SD1 453 MB / SD2 433 MB, tanpa GC | live cluster, `lib_process_cache`, `hb_store_volatile` |
| T9 | `process_snapshot_slots` bukan memory bound | Primary sudah 1000, slot tetap 185 KB; knob mengatur frekuensi snapshot saja | `primary.json` |
| T10 | Guard payload bisa dilewati | `message_too_large()` hanya cek body string; body table lolos; `http_max_body` primary 65 KB vs SD 1 MB | `primary_process.lua`, config |
| T11 | Ownership spoofable | `client_signer` adalah string di body yang dikirim client sendiri | `primary_process.lua` |
| T12 | Tombstone/GC tidak ada | Tidak ada mekanisme hapus entitas di Primary maupun SD | semua proses |
| T13 | Tes tidak menjaga apapun | Tes meng-assert `sd_ack` yang dilaporkan sendiri; tidak ada failure injection | `tests/test_failure_modes.py` |

### 1.2 Kesalahan konseptual yang harus dikoreksi

**"16,6 ms" dipakai untuk dua hal berbeda:**

- 16,6 ms adalah budget **simulasi (tick)** — murni hitungan di RAM.
- 16,6 ms **bukan** budget **commit jaringan** — jaminan "ack = sudah direplikasi"
  secara matematis membutuhkan setidaknya 1 RTT replikasi.

Selama target ini tidak dipisah, angka apapun akan terasa gagal. Roadmap ini
memisahkannya, dan tetap menjaga: **ack ke client hanya dikirim setelah replikasi.**

### 1.3 Status implementasi (2026-11-06)

| Fase | Status | Bukti / sisa pekerjaan |
|------|--------|------------------------|
| F0 baseline | selesai (kecuali soak & cache size) | `tools/bench_baseline.json`, README diperbarui |
| F1 ingress/batching | fungsional, target latency tidak tercapai | 202 = 198 ms p50 (target 5 ms) |
| F2 quorum jujur | selesai & diuji | 503 + state utuh, failover SD2, dedup, tanpa `sd_ack` palsu |
| F3 delta + shard | root kanonik selesai; slot belum kecil | 32 shard incremental; slot 516 KB (target 8 KB) |
| F4 recovery | selesai (kecuali tombstone/soak) | hydrate 156 ms, root identik |
| F5 verify-don't-trust | sebagian | payload guard + owner-spoof 403; signature per-item & `force-signed` SD belum |
| F6 tes jujur | sebagian | 30/30 + 35/35 live checks; mutation test terbukti merah; soak belum |

Tes live terakhir: `python3 tests/test_e2e_2d_points.py` → **30/30**;
`python3 tests/test_failure_modes.py --yes` → **35/35**.

---

## 2. Target SLO (definition of done)

> Angka target di kolom 2 adalah **hipotesis awal**. Kolom 3 adalah hasil ukur
> nyata (`tools/bench_baseline.json`, 2026-11-06, loopback, keep-alive, 140 entitas).
> Yang tidak boleh direvisi adalah kontrak kejujuran (§3), bukan angkanya.

| Metrik | Target awal (hipotesis) | Terukur 2026-11-06 | Status |
|--------|-------------------------|--------------------|--------|
| Sim tick | 60 Hz (2 tick/batch) | 2 tick/batch diterapkan | tick loop bukan bottleneck |
| Batch commit | 30 Hz (window ~33 ms) | `advance` p50 **206 ms** (~4,9 batch/s) | belum tercapai |
| ACK L0 (202) | ≤ 5 ms p50 | **198 ms p50 / 252 p90 / 313 p99** | belum; 1 `relay/cast` + compute Lua |
| ACK L1 (200) | ≤ 35 ms p50, ≤ 70 ms p99 | **580 ms p50 / 712 p90 / 779 p99** | belum; floor kontrak (202 + 1 RTT SD + verifikasi) |
| Throughput | 1.000 input/s, 30 batch/s | belum diukur (bench masih sekuensial) | belum |
| Ukuran slot | ≤ 8 KB, konstan | **516 KB** primary, 334 KB SD | belum; slot masih membawa state O(N) |
| RAM/node | ≤ 250 MB, flat 1 jam | **419 MB** primary, ~174 MB SD | belum; soak belum dijalankan |
| Disk growth | ≤ 5 MB/menit @ 30 Hz | ~27 MB LMDB per SD setelah bench | belum; soak belum dijalankan |
| Divergensi state root | 0, root identik 3 node | **0 setelah konvergensi** (repair-driven, bukan instan) | tercapai |
| Recovery | ≤ 2 s, root identik | `cold_hydrate` **156 ms p50**; root identik | tercapai |
| Kejujuran ack | `503` saat quorum gagal | **503 + `Tick`/`StateRoot` tidak berubah** (verified) | tercapai |

Catatan penting: "Divergensi 0" berarti **konvergen** setelah hot standby menerima
delta, bukan bahwa ketiga node identik pada milidetik yang sama. Replika yang mati
di-heal dari delta log (`RepairBatches`) begitu ada trafik; tidak ada timer di
sandbox Lua, jadi healing bersifat traffic-driven dan diuji sebagai invariant
"eventually equal".

---

## 3. Kontrak ACK (tidak bisa dinegosiasi)

### 3.1 Level replikasi

| Level | Nama | Arti eksak | Dipakai untuk |
|-------|------|-----------|---------------|
| L0 | `accepted` | Input masuk antrean Primary RAM | Respons `202` instan |
| L1 | `replicated` | Δstate ditulis di proses SD + LMDB (`sync: false`), root SD == root Primary | **ACK default ke client** |
| L2 | `durable` | fsync LMDB pada 2 node | Checkpoint periodik saja |
| L3 | `settled` | Tercatat di Arweave L1 via Arbit | Settlement |

### 3.2 Kontrak HTTP

| Kode | Arti | Kondisi | Client boleh |
|------|------|---------|--------------|
| `202` | `accepted` | Masuk antrean Primary | Tampilkan badge *pending*; **prediction opsional** |
| `200` | `replicated` | `sd_ok && root_sd == root_primary && batch_id cocok` | Badge *confirmed* + tick + root |
| `503` | **tidak** direplikasi | Quorum gagal / timeout | Badge *confirmed* tidak boleh tampil; tampilkan error + `batch_id` |
| `409` | `conflict` | Root SD ≠ root Primary / stale epoch | Re-sync dari Primary |

**Invariant (tidak boleh dilanggar dalam kode manapun):**

1. `200` hanya dikirim setelah respons nyata dari salah satu SD diverifikasi
   (root equality + `batch_id` echo + tick).
2. `ack_nodes` hanya diisi di dalam jalur sukses.
3. `confirmed_sd_tick` dibaca dari respons SD, bukan dari nilai kandidat.
4. Quorum gagal → `503`; `Points`, `Tick`, `PrevStateRoot` **tidak boleh berubah**.
5. Retry batch dengan `batch_id` sama → persis satu apply (idempotency, §5 Fase 1).
6. Semua badge di UI: `accepted` ≠ `confirmed`. Dua label berbeda.

---

## 4. Protokol commit (hot path)

```
Client ──POST /input──────────────► Primary
                                    │ 1. append queue (batch_id, seq)
        ◄── 202 {batch_id,seq} ─────┤    L0, ≤5 ms, TIDAK diklaim replicated
                                    │
                                    │ 2. window 33 ms / 64 input → batch tutup
                                    │ 3. simulasi 2 tick → Δ + root   (≤8 KB)
                                    │ 4. relay/call POST /commit → SD1 (sinkron)
                                    │      SD1: dedup → apply Δ → LMDB
                                    │          → root → {ok,tick,root,batch_id}
                                    │ 5. verifikasi root_sd == root_lokal
                                    │ 6. quorum = Primary + SD1 ✓
                                    │ 7. commit ke primary RAM (baru sekarang)
        ◄── 200 {tick_to,root, ─────┤                                  (L1)
              replicas:[primary,sd1]}
                                    │
                                    └─ SD1 ──cast──► SD2 /commit (async, hot backup)
```

### 4.1 Keputusan desain protokol

| Keputusan | Pilihan | Alasan |
|-----------|---------|--------|
| Quorum | **2-of-3**: Primary + 1 SD | `ao.resolve` Lua **sekuensial** (`hb_ao:resolve_many/2` = rantai langkah, bukan fan-out). 2 SD sinkron = +1 RTT tanpa tambahan jaminan |
| SD2 | hot backup async via `cast` | divergensi SD2 dideteksi lewat root di checkpoint |
| Failover | SD1 timeout (>3× p50) → SD2 dalam batch yang sama | tanpa failover, SD mati = semua write gagal |
| Batch | `Advance` di dalam satu `compute` | 1 batch = 1 slot; tick = loop Lua murni tanpa hash/store |
| Ticks/batch | 2 | 30 batch/s × 2 = 60 tick/s |
| Window | 33 ms ATAU 64 input, mana dulu | batas atas latency |
| Dedup | `dedup@1.0`, subject = `batch_id` | retry tidak pernah double-apply (`dev_dedup.erl`) |
| Verifikasi hot path | root equality (tanpa kripto) | murah; menangkap divergensi → risiko nyata |
| Verifikasi setlement | `force-signed: true` SD (hashpath) | hashpath menandatangani hasil; `maybe_sign/3` menolak jika body berubah |
| Durabilitas per-input | L1 (`sync: false`) | L2 per input menghancurkan latency |
| Tick | eksplisit dari batch, **bukan** `os.clock()` | `requires-determinism` mem-sandbox `os.time/os.clock/os.date/math.random` |

### 4.2 Catatan: apakah 35 ms realistis?

Faktor belum terukur: biaya 1 `relay/call` ke SD dengan body ~4 KB.
Gate Fase 0 (§5) mewajibkan angka ini sebelum SLO dikunci. Jika ternyata
`relay/call` SD ~50 ms, target p50 direvisi: 16,5 + 50 + 3 ≈ 70 ms, *atau*
window dipangkas. Kontrak protokol di §4 tetap — hanya angka yang berubah.

---

## 5. Fase-fase pekerjaan

> Setiap fase memiliki **gate** — daftar centang yang harus hijau sebelum fase berikutnya.
> Kalau gate gagal, berhenti; jangan loncat fase.

### Fase 0 — Ukur & kunci baseline (1–2 hari)

**Tujuan:** berhenti menebak angka.
**File:** `tools/bench_engine.py` (baru), `README.md`.

- [x] Buat `tools/bench_engine.py` mengukur 8 jalur: `/now`, tulis `/push`,
      baca `/push`, batch (`Advance`), `relay/call` 1 SD, SD langsung,
      `force-signed` (lewat config), cold start/hydrate.
- [x] Output `tools/bench_baseline.json`: p50/p90/p99, `computed_slot_size`,
      RSS, LMDB size, status code, ukuran respons.
- [ ] Soak 10 menit → kurva memori per node. **(belum dijalankan)**
- [x] README: ganti klaim "89/89 tests" dan "16 ms target" dengan angka baseline.

**Gate F0:**

- [x] `bench_baseline.json` tersimpan di repo
- [x] Biaya `relay/call` 1 SD terukur (`relay_call_sd` p50 **155 ms**) — menentukan SLO bagian 2
- [x] README berisi angka hasil nyata, bukan target
- [ ] Cache size per node tercatat di JSON **(belum; baru RSS + LMDB + slot)**

---

### Fase 1 — Ingress & batching (3 hari)

**Tujuan:** input cepat masuk; slot jadi batch, bukan per write.
**File:** `primary_process.lua`, `tools/game_client/index.html`, `tools/client.py`.

- [x] `action: "Input"` → append queue, balas `202 {batch_id, seq}` (`ack:"accepted"`)
- [x] Tutup batch: window 33 ms / 64 input, mana yang lebih dulu (`BatchWindowMs`, `MaxBatchInputs`)
- [x] `action: "Advance"` loop 2 tick deterministik per batch
- [x] Cap ticks/batch = 6 (cegah spiral of death; `compute` memblokir proses)
- [x] Idempotency: retry dengan input sama memakai `batch_id` yang sama
      (**implementasi in-process `SeenInputs`, bukan device `dedup@1.0`**)
- [x] UI: polling status via `/now` (slot-free); tabel entitas di-seed sekali, lalu
      di-update dari ACK tulis (bukan polling `/push`)
- [x] UI: badge terpisah `accepted` (202) vs `confirmed` (200), plus `503`

**Gate F1:**

- [ ] `202` p50 ≤ 5 ms — **gagal: terukur 198 ms p50** (biaya 1 cast + compute Lua)
- [ ] 1 batch = **1 slot** — belum diverifikasi ulang di run terakhir
- [x] Replay batch yang sama → state identik (dedup terbukti; E2E check [5])
- [x] Polling `/now` menghasilkan **0 slot** baru (`now` p50 12 ms, tanpa slot)
- [ ] 500 input/s tanpa antrean menumpuk — belum diukur

---

### Fase 2 — Commit jujur + quorum nyata (4 hari)

**Tujuan:** ACK = `replicated`. Membersihkan T1–T3.
**File:** `primary_process.lua`, `sd_process.lua`.

- [x] SD: endpoint `Commit` — dedup → verifikasi root → apply Δ → LMDB → balas
- [x] Primary memakai respons SD nyata (`Ack` asynchronous: `batch_id`, `tick`, `state_root`)
      — bukan `relay/call` sinkron, karena `relay/call` ke peer mati mematikan proses
- [x] `is_quorum = sd_ok AND root_sd == root_primary AND batch_id cocok AND tick_sd >= tick_target`
- [x] `ack_nodes`/`replicas` hanya diisi di jalur sukses (dari `role` pada Ack)
- [x] `confirmed_sd_tick` dibaca dari respons SD, bukan nilai kandidat
- [x] Hapus `sd_ack: true` hardcoded; `sd_ack` hanya ada di body terverifikasi
- [x] Implementasikan `503` yang benar-benar meninggalkan state utuh
- [x] Failover: SD1 mati → batch yang sama dikonfirmasi SD2 (cast ke semua target)
- [x] SD2 tetap menerima duplikat batch async; tidak ikut menahan ack

**Gate F2 (membuktikan dengan mematikan hal):**

- [x] SD1+SD2 mati → `503`, state Primary **tidak berubah**, root tidak bergerak
- [x] SD1 mati sendiri → tetap `200` lewat SD2 (failover), `replicas:["sd2"]`
      *(gate awal "SD1 mati → 503" direvisi: kontraknya 2-of-3, jadi SD1 mati
      belum gagal quorum; yang harus 503 adalah saat tidak ada SD hidup)*
- [ ] SD1 delay 200 ms → failover ke SD2 → tetap `200` (butuh delay injection, belum ada)
- [x] Batch duplikat → persis satu apply (retry memakai `batch_id` yang sama)
- [ ] 10 menit beban → `root_primary == root_sd1` (belum; hanya uji fungsional)
- [x] Tidak ada satu jalur pun yang bisa menyebut `sd_ack:true` tanpa verifikasi

---

### Fase 3 — Bentuk state: delta + shard (4 hari)

**Tujuan:** slot ≤ 8 KB konstan. Membersihkan T4, T7. Prasyarat revisi SLO.
**File:** `primary_process.lua`, `sd_process.lua`, `tools/build_configs.py`.

- [ ] State proses = `{tick, root, last_k_diffs, shard_ptrs}` — bukan semua entitas
      **(belum: state masih penuh di slot; hanya *root* yang di-shard)**
- [ ] Entitas disimpan sebagai blob content-addressed per shard **(belum)**
- [x] Commit mengirim Δ (bukan full state); snapshot penuh hanya untuk hydration
- [x] Root kanonik: sorted keys + fixed-point (`int_str`) — mengatasi T4 + determinisme
      (skema `HS2`: 32 shard FNV-1a, `sha256("HS2|tick=<n>|shards\n" + digest)`)
- [x] Hanya shard yang tersentuh delta yang di-hash ulang (bukan seluruh state)
- [ ] `max-ttl` / `max-ttl-ms` di `hb_store_volatile` **(belum dipakai)**
- [ ] Retensi eksplisit untuk `lib_process_cache` (masih tanpa GC)

**Gate F3:**

- [ ] Slot 100 vs 10.000 entitas → ukuran sama — **gagal: 516 KB @140 entitas**
- [ ] Disk growth ≤ 5 MB/menit @ 30 Hz — belum diukur (soak)
- [ ] RAM flat 1 jam — belum diukur (soak)
- [x] Root SD1 == SD2 == Primary, deterministik (sorted ids per shard + `int_str`)
- [x] Revisi tabel SLO bagian 2 dengan angka terukur

---

### Fase 4 — Recovery & retention (3 hari)

**Tujuan:** crash tidak boleh menghilangkan state. Membersihkan T6, T8, T12.
**File:** `primary_process.lua`, `sd_process.lua`, `arbit_process.lua`, `build_configs.py`.

- [x] Perbaiki parser hydration (pakai `pack_entity`/`unpack_entity`, bukan regex rapuh)
- [x] Pulihkan `owner` dari replika
- [x] Restart Primary → root identik dengan sebelum restart (hydrate dari SD yang ahead)
- [x] Restore entitas hasil parsing & `ActiveEntityCount` benar
- [ ] Checkpoint → Arbit tiap 1–5 detik; `sync: true` hanya di checkpoint (L2)
      **(checkpoint lama masih per `arbit_batch_interval` tick, bukan timer)**
- [ ] Tombstone untuk entitas yang dihapus **(belum ada)**
- [x] Prune diff setelah commit diterima (`max_diff_slots` di SD; diuji tidak menumbuhkan ghost entity)

**Gate F4:**

- [x] Primary kosong + restart → hydrate dari SD, root identik, **156 ms p50** (`cold_hydrate`)
- [x] SD tidak lagi menambah entitas "ghost" (root primary == sd1 == sd2 setelah converge)
- [ ] Soak 1 jam → RAM flat ≤ 250 MB — **gagal: 419 MB primary pada bench singkat**

---

### Fase 5 — Verify-don't-trust (4 hari)

**Tujuan:** kuat terhadap pihak yang tidak dipercaya. Membersihkan T11, T10.
**File:** `build_configs.py`, config SD, `sd_process.lua`, `primary_process.lua`.

**Catatan penting — prinsip desain:**
- **Hot path** cukup root equality (tanpa kripto) — menangkap divergensi, risiko nyata utama.
- **Settlement / open cluster** butuh signature. Jangan dimasukkan ke hot path.

**Task:**

- [ ] `force-signed: true` di SD → hashpath menandatangani hasil
      **(belum: baru `genesis` dan `arbit` yang `force-signed: true`)**
- [ ] Auth per-item dari signer kriptografis, **bukan** `client_signer` string
      **(sebagian: owner spoof ditolak `403` dengan membandingkan owner tersimpan,
      tetapi belum ada verifikasi signature per-input)**
- [x] Guard payload: body **table** ikut dicek (T10)
- [x] `http_max_body` disamakan semua node (65 KB)
- [ ] `MaxIdLength`, `MaxEntities` disinkronkan ke Genesis
      **(MaxEntities dari Genesis; `MaxIdLength` masih default 128 di Lua)**
- [x] Epoch masuk perhitungan quorum set: `Ack` membawa `epoch`, dan Primary
      menolak (`409 epoch_mismatch`) Ack dari epoch yang berbeda

**Gate F5:**

- [x] Forge `client_signer` → ditolak `403`, owner pt tidak rusak (E2E check [7])
- [x] Body table > `MaxPayloadBytes` → ditolak
- [ ] `state_root` di respons Arbit terverifikasi dari signature SD
- [ ] Node asing tidak bisa menulis dengan quorum palsu

---

### Fase 6 — Tes jujur (3 hari)

**Tujuan:** tes dipakai untuk menjaga invariant, bukan untuk lulus.
**File:** `tests/test_failure_modes.py`, `tests/test_e2e_2d_points.py`.

- [x] Kill SD1 mid-batch → failover ke SD2; kill **kedua** SD → `503`, state utuh
      *(gate awal "kill SD1 → 503" direvisi mengikuti quorum 2-of-3)*
- [ ] SD1 delay/blocked → failover benar (butuh delay injection)
- [x] Batch duplikat → satu apply
- [x] Out-of-order tick → dibuffer (`202 deferred`), bukan dibuang diam-diam
- [x] Forge `client_signer` → ditolak
- [x] Root equality 3 node setelah beban (konvergen, diverifikasi test)
- [ ] Replay 1.000 batch → root sama (determinisme) — belum
- [ ] Soak 1 jam → memori & disk bounded — belum
- [x] Cold start → state 100% pulih (root identik)
- [x] **Mutation test:** rusak verifikasi ACK secara sengaja → suite failure-injection
      harus **merah** (`tests/mutation_ack.sh`; terbukti merah pada 2026-11-06)

**Gate F6:**

- [ ] Semua tes di atas ada & hijau (masih kurang: delay injection, replay 1.000 batch, soak 1 jam)
- [x] Mutation test terintegrasi: satu invariant rusak → suite merah, tidak bisa dilangkahi

---

## 6. Peta file yang dirombak

| File/aset | Perubahan inti |
|-----------|----------------|
| `engine/processes/primary/primary_process.lua` | queue + window, `Advance` 2-tick, quorum nyata, `202/200/503`, canonical root, delta state |
| `engine/processes/shared_data/sd_process.lua` | endpoint `Commit`, dedup, root ack, apply Δ, tombstone |
| `engine/processes/arbit/arbit_process.lua` | checkpoint L2 + prune diff |
| `engine/config/*.json` + `tools/build_configs.py` | `force-signed`, `max-ttl`, `http_max_body`, snapshot-slot, sync |
| `tools/game_client/index.html` | polling via `/now`, badge `accepted` vs `confirmed` |
| `tools/bench_engine.py` (baru) | baseline & regression SLO |
| `tests/*` | failure injection + mutation test |
| `README.md` | SLO hasil terukur, bukan klaim |

---

## 7. Keputusan arsitektur (ADR ringkas)

| # | Keputusan | Alternatif yang ditolak | Alasan |
|---|-----------|------------------------|--------|
| A1 | Tick = loop di dalam 1 message | tick = 1 slot (21–32 ms/slot) | 60 slot/s mustahil |
| A2 | Replikasi **input**, bukan state | replikasi state | O(N) per commit |
| A3 | Commit 30 Hz + prediksi client opsional | 60 Hz / 1 Hz | latency vs agresi |
| A4 | State = head kecil + shard | semua inline | slot ≤ 8 KB |
| A5 | Root kanonik sorted + fixed-point | root `pairs()` | determinisme |
| A6 | Verifikasi hot path = root eq; settlement = sign | sign di hot path | murah; kripto hanya wajib saat open cluster |
| A7 | Idempotency via `dedup@1.0` | retry tanpa dedup | double-apply |
| A8 | Quorum = Primary + 1 SD | 2 SD sinkron | `ao.resolve` sekuensial, 1 RTT cukup 2-of-3 |
| A9 | Sharding per region untuk scale | satu proses untuk semua | `process@1.0` adalah aktor tunggal |

---

## 8. Anti-goal (dilarang dibangun dulu)

- Bukan menulis device Erlang baru di `HyperBEAM/` — semua cukup di Lua + config.
- Bukan mengganti `process@1.0`, `scheduler@1.0`, `push@1.0`.
- Bukan mengejar 60 commit/detik (target 30).
- Bukan WAN/multi-host dulu — loopback dulu, baru jaringan.
- Bukan menambah node ke-4/5 sebelum 3 node terbukti benar (root identik).
- Bukan signature di hot path sebelum Fase 5 (dilakukan tepat waktu, bukan sekarang).

---

## 9. Risiko & trade-off yang harus diputuskan saat ini

| # | Trade-off | Pilihan yang dipakai | Konsekuensi jika diubah |
|---|-----------|----------------------|------------------------|
| R1 | Client menunggu `200` (jujur, ~35 ms) atau bergerak di `202` (~5 ms)? | gerak di `202`; badge/label berbeda | Dua label tetap berbeda + teks sesuai |
| R2 | Durabilitas per-input L1 atau L2 | L1 | fsync tiap batch = +5–15 ms latency |
| R3 | Quorum 2-of-3 atau 3-of-3 | 2-of-3 | 3-of-3 butuh fan-out paralel → kode Erlang baru |

---

## 10. Urutan kerja & alasan

```
Fase 0 (1-2 hari) baseline           → semua angka terukur
Fase 1 (3 hari)   batching + 202     → input cepat masuk
Fase 2 (4 hari)   commit jujur + q.  → ACK tidak bohong
Fase 3 (4 hari)   delta state        → ACK bisa ≤35 ms
Fase 4 (3 hari)   recovery+retention → kuat setelah crash
Fase 5 (4 hari)   signatures + auth  → kuat terhadap pihak lain
Fase 6 (3 hari)   tes jujur          → terbukti, bukan diklaim
```

**Kenapa quorum bukan yang pertama:** quorum di atas state 185 KB hanya akan
memperlambat tanpa memperbaiki apa pun — Anda mendapat kebenaran yang lebih lambat
pada data yang masih salah bentuk.

**Kenapa Fase 3 tidak boleh loncat:** dengan body 185 KB, 30 batch/s =
5,5 MB/s. Satu-satunya cara mencapai SLO adalah memperkecil body **dahulu**.

---

## 11. Langkah pertama (hari ini)

1. **Ukur** — jalankan `tools/bench_write_latency.py` + `bench_keepalive.py`
   (dua tools sudah ada), tambahkan langkah untuk mengukur biaya `relay/call` SD
   dengan body 4 KB. Simpan hasil sebagai `bench_baseline.json`.
2. **Hapus kebohongan** — ubah `is_quorum` agar bergantung pada respons nyata
   dan tambahkan jalur `503`. Perubahan kecil, dampak besar.
3. **Buktikan** — tambahkan satu tes: matikan SD1 → harus `503`.
   Jika tes ini merah sekarang, Anda sudah punya bukti bahwa masalahnya nyata.

---

## 12. Lampiran: fakta HyperBEAM yang jadi dasar desain

| Fakta | Sumber di `HyperBEAM/src` |
|-------|---------------------------|
| `relay@1.0/cast` = async, `call` = sinkron | `preloaded/util/dev_relay.erl` |
| Lua `ao.resolve` list = **sekuensial**, bukan paralel | `core/resolver/hb_ao.erl` `resolve_many/2` |
| Proses = 1 slot penuh per message & `computed_slot_size` menyimpan full state | `preloaded/process/dev_process.erl:404–433` |
| `/push` menambah slot & men-plakkan ke `maybe_trigger_push` | `dev_process.erl:544+`, `dev_push.erl` |
| `dedup@1.0` deduplikasi per sujet tanpa penulisnya sendiri | `preloaded/util/dev_dedup.erl` |
| `force-signed` = menandatangani hashpath; menolak tred if body changed | `core/http/hb_http_server.erl:1236–1253`, `hb_hashpath.erl:756` |
| `requires-determinism` mem-sandbox `math.random/os.time/os.clock/os.date` | `preloaded/process/dev_process.erl:180–185`, `vm/dev_lua.erl:284` |
| `process_snapshot_slots` = frekuensi snapshot, bukan memory bound | `dev_process.erl:655–664` |
| Cache per-slot tanpa retensi; setiap slot disimpan penuh | `preloaded/process/lib_process_cache.erl` |
| ETS store punya `max-ttl` tapi tidak dikonfigurasi | `core/store/hb_store_volatile.erl` |
| `cron@1.0` mendukung interval sub-detik (test: `200-milliseconds`) | `preloaded/node/dev_cron.erl` |

---

*Disusun dari sesi audit 2026-11-03. Semua angka "sekarang" berasal dari
pengukuran live, bukan estimasi. Plan ini boleh direvisi, tapi kontrak ACK di §3
harus tetap — itu adalah inti dari engine yang dibangun.*
