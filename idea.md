# Proposal & Kompas Arsitektur Teknis End-to-End
## High-Frequency Real-Time State Engine & Distributed Replication Stack on AO / Permaweb

---

## 1. Executive Summary & Tujuan PoC

Dokumen ini adalah **kompas utama pembangunan Proof-of-Concept (PoC)** untuk mesin state real-time berfrekuensi tinggi (**target 60 tick/detik**) yang berjalan **100% di atas ekosistem asli AO Compute, HyperBEAM, dan Arweave (Permaweb)** di lingkungan lokal.

Fokus utama tahap ini adalah **membangun dan membuktikan Engine Stack secara maksimal dan konkret terlebih dahulu** (bukan membuat abstraksi *library* generik di awal). Setelah engine terbukti kencang, tahan banting, dan akurat pada beban penggunaan nyata, barulah di tahap berikutnya stack ini diekstrak menjadi *library/SDK* yang mudah dikustomisasi.

Inti arsitektur ini bekerja sebagai **Peredam Frekuensi Bertingkat (*3-Tier Frequency Damper*)**:
- **60 Hz (`tiap 16.6 ms`)** diserap habis di memori RAM oleh **Primary Process** via komunikasi langsung (*Direct Node-to-Node*).
- **2 Hz (`tiap 500 ms`)** direplikasi secara asinkron ke **Shared Data (SD) Process** sebagai *hot backup* anti-kehilangan state.
- **0.1–0.2 Hz (`tiap 5–10 detik` / event penting)** dikonsensuskan lewat kotak pesan resmi AO (*Inbox/Outbox SU*) ke **Arbit Process** dan diabadikan ke **Arweave L1**.

---

## 2. Dua Paradigma Komunikasi: Kapan Pakai *Inbox/Outbox (SU)* vs *Direct Node-to-Node*

Kunci utama agar tidak salah desain adalah memahami sifat fisik, kelebihan, dan kelemahan dari dua jalur komunikasi yang berbeda di ekosistem HyperBEAM/AO:

| Dimensi | 1. AO Message Box (`Inbox / Outbox` via SU & Arweave) | 2. Direct Node-to-Node (HTTP Native HyperBEAM Langsung) |
| :--- | :--- | :--- |
| **Cara Kerja** | Pesan dibungkus & ditandatangani (`ans104@1.0`), dikirim ke Scheduler (`scheduler@1.0` / `~process@1.0/push`), diberi nomor slot mutlak, dan dicatat di Arweave. | Request dikirim langsung dari Klien ke Node atau antar-Node HyperBEAM lewat koneksi HTTP persistent (`httpsig@1.0` / `relay@1.0`) langsung ke proses di RAM. |
| **Kelebihan Utama (+)** | **Anti-Double-Spend & Tahan Mati:** Urutan transaksi mutlak (deterministik global). **Walaupun node proses tujuan sedang mati, pesan tetap masuk antrean SU/Arweave** dan pasti dieksekusi saat node hidup kembali. | **Kecepatan Setara Server Web2:** Latensi sub-milidetik hingga satuan milidetik, nol antrean SU global, dan nol biaya transaksi per tick. Sanggup melayani **60 tick/detik** tanpa lag. |
| **Kelemahan (-)** | **Lambat:** Mengikuti ritme indexing SU dan penyimpanan Arweave (puluhan–ratusan milidetik). Mustahil dipakai untuk loop 60 tick/detik. | **Rentan Mati seperti Web2:** Bersifat *live-endpoint*; **jika node fisik tujuan mati, komunikasi langsung terputus saat itu juga** dan state transien di RAM yang belum di-snapshot bisa hilang. |
| **Kapan WAJIB Dipakai?** | • Bootstrap & update konfigurasi di **Genesis Process**.<br>• Pengiriman checkpoint resmi & mutasi bernilai tinggi (anti-double-spend) dari SD ke **Arbit Process**.<br>• Koordinasi promosi failover / pergantian `Epoch`. | • Stream input real-time (60 Hz) & pembacaan state cepat antara **Klien $\leftrightarrow$ Primary Process**.<br>• Pengiriman snapshot tiap 0.5 detik & balasan ACK cepat antara **Primary Process $\leftrightarrow$ Shared Data Process**. |

> **Rumus Sinergi Keduanya:** Kelemahan *Direct Node-to-Node* ("bisa mati kalau node mati") ditutup oleh **koordinasi bootstrap dari Genesis Process** (yang hidup di atas *Message Box SU* yang kebal mati) ditambah **replikasi 0.5 detik ke Shared Data Process** di node fisik berbeda!

---

## 3. Topologi Independen: Satu Proses = Satu Node HyperBEAM Mandiri

Seluruh komponen berjalan **100% di mesin lokal** menggunakan ekosistem Permaweb asli без simulasi palsu:
- **Docker** digunakan khusus untuk menyalakan **ArLocal (`:1984`)** sebagai node gateway Arweave L1 lokal.
- **Tmux** digunakan untuk menyalakan dan memisahkan setiap **Node Pekerja HyperBEAM** sehingga tiap proses benar-benar hidup di node independen (jika 1 node dimatikan paksa, proses lain tidak ikut mati).

| Node Fisik (Tmux / Docker) | Port Lokal | Proses yang Dijalankan | Mode Eksekusi HyperBEAM |
| :--- | :--- | :--- | :--- |
| **`arlocal`** *(Docker)* | `:1984` | **Arweave L1 Node** | Penyimpanan permanen biner modul WASM/Lua & bundel transaksi ANS-104. |
| **`hb-genesis`** *(Tmux)* | `:8733` | **1. Genesis Process** | **Scheduled AO Process** (`process@1.0` + `scheduler@1.0` + `lua@5.3a` + `patch@1.0`). |
| **`hb-primary`** *(Tmux)* | `:8734` | **2. Primary Process** | **Direct Persistent Process** (`lua@5.3a` Gate + `wasm-64@1.0` Rust 60Hz Engine + `patch@1.0`). |
| **`hb-sd-1`** *(Tmux)* | `:8735` | **3A. Shared Data Process 1** | **Direct Replica Process** (`lua@5.3a` + `hb_store_rocksdb` + `cron@1.0` + `ans104@1.0`). |
| **`hb-sd-2`** *(Tmux)* | `:8736` | **3B. Shared Data Process 2** | **Direct Replica Process** (Replika independen ke-2 untuk kuorum & ketersediaan tinggi). |
| **`hb-arbit`** *(Tmux)* | `:8737` | **4. Arbit Process** | **Scheduled AO Process** (`process@1.0` + `scheduler@1.0` + `lua@5.3a` + `patch@1.0` + `ans104@1.0`). |

*(Catatan penting isolasi: **Arbit Process** berjalan di node `hb-arbit` miliknya sendiri, terpisah dari `hb-primary`. Jika `hb-primary` meledak/mati, `hb-sd` tetap bisa mengirim checkpoint terakhir ke `hb-arbit` tanpa gangguan!)*

---

## 4. Alur End-to-End (E2E Flow) & Pemilihan Tugas Tiap Komponen

```
                  ┌──────────────────────────────────────────────┐
                  │              1. GENESIS PROCESS              │
                  │       (Node: hb-genesis | Via Inbox/SU)      │
                  │ • Simpan ModuleTxID, Epoch, & Otorisasi Node │
                  │ • Bootstrap konfigurasi Direct Node-to-Node  │
                  │ • Direktori Active Endpoint untuk Klien/Node │
                  └──────┬────────────────┬───────────────┬──────┘
       (1. Discovery &   │                │ (Bootstrap    │ (Otorisasi Epoch
        Endpoint Lookup) │                │  Peer Direct) │  & Signer Resmi)
                         ▼                ▼               ▼
[ Klien / Players ] ══► ┌──────────────────────────┐      │
  (Direct HTTP          │    2. PRIMARY PROCESS    │      │
   Keep-Alive, 60 Hz)   │    (Node: hb-primary)    │      │
                        │ • Lua: Session/Auth Gate │      │
                        │ • Rust WASM (60 Tick/s): │      │
                        │   - Fixed-Slot State RAM │      │
                        │   - Ring Buffer 1-2 dtk  │      │
                        └────────────┬─────────────┘      │
                                     │                    │
              (Direct Node-to-Node:  │  ▲ (Signed HTTP    │
               Async Snapshot 0.5s   │  │  ACK Feedback)  │
               / Critical Flush)     ▼  │                 │
                        ┌──────────────────────────┐      │
                        │  3. SHARED DATA PROCESS  │      │
                        │ (Node: hb-sd-1, hb-sd-2) │      │
                        │ • Validasi Epoch & Root  │      │
                        │ • Rolling Store RocksDB  │      │
                        │ • Bungkus Batch ANS-104  │      │
                        └────────────┬─────────────┘      │
                                     │                    │
              (Inbox / Outbox via SU:│                    │
               ANS-104 Checkpoint    │                    │
               tiap 5-10 detik)      ▼                    │
                        ┌──────────────────────────┐ ◄────┘
                        │     4. ARBIT PROCESS     │
                        │     (Node: hb-arbit)     │
                        │ • Anti-Double-Spend Gate │
                        │ • Verifikasi Bukti SD    │
                        │ • Kunci Official State   │
                        └────────────┬─────────────┘
                                     │
              (Scheduled Assignment  │
               & L1 DataItem Commit) ▼
                        ┌──────────────────────────┐
                        │   5. ARWEAVE L1 (ArLocal)│
                        │   (Docker :1984 Storage) │
                        └──────────────────────────┘
```

### Rincian Tugas Spesifik & Batas Tanggung Jawab Tiap Komponen

#### 1. Genesis Process (`hb-genesis` — *Bootstrap & Control Plane*)
* **Jalur Komunikasi:** **Inbox/Outbox (SU)** untuk seluruh perubahan konfigurasi/registrasi; ekspos baca cepat via `/now` (`patch@1.0`).
* **Tugas Utama:**
  1. Menyimpan konstitusi awal stack: `WasmModuleTxID` (yang sudah di-upload ke ArLocal), `ArbitProcessId`, nomor `Epoch` aktif, dan daftar alamat kriptografis (wallet JWK) operator node yang sah (`Primary` & `SD`).
  2. Menjadi **pusat bootstrap komunikasi Direct Node-to-Node**: saat `hb-primary` dan `hb-sd` menyala, mereka membaca konfigurasi rekan (*peer endpoints*) dari Genesis dan mendaftarkan URL aktif + heartbeat mereka ke Genesis.
  3. Melayani **Node Discovery** bagi Klien: klien cukup bertanya ke `GenesisProcessId` untuk mendapatkan URL `hb-primary` yang sedang aktif pada `Epoch` terbaru.
  4. Mengatur **Failover Epoch**: bila `hb-primary` mati, Genesis menaikkan `Epoch + 1` dan mengarahkan topology ke node pengganti yang telah memulihkan snapshot terakhir dari SD.

#### 2. Primary Process (`hb-primary` — *The 60Hz Real-Time Engine*)
* **Jalur Komunikasi:** **Direct Node-to-Node HTTP Keep-Alive** (`httpsig@1.0` / `relay@1.0`).
* **Tugas Utama (Pembagian Ketat Lua vs Rust WASM agar Tidak *Double-Parsing*):**
  1. **Lua Gate (`lua@5.3a`):** Menangani kontrol berfrekuensi rendah—validasi otorisasi sesi pemain, sinkronisasi konfigurasi `Epoch` dari Genesis, mengatur timer snapshot 0.5 detik, dan menangani pemulihan state awal saat boot.
  2. **Rust WASM Core (`wasm-64@1.0`):** Menjalankan loop state **60 tick/detik** murni di linear memory RAM:
     - **Dense Fixed-Slot Array ($O(1)$):** Menyimpan state terkini seluruh entitas aktif tanpa alokasi heap liar.
     - **Ring Buffer (60–120 slot = 1–2 detik histori):** Menyimpan delta pergerakan/input terbaru untuk interpolasi klien & kompensasi lag; otomatis menimpa slot lama (*zero memory leak, konstan $O(1)$*).
     - **Snapshot Exporter:** Mengekspor ringkasan state kompak + `state_root` untuk dikirim ke SD.
  3. **Dua Mode Pengiriman ke SD Process:**
     - *Transient Tick State* (mis. koordinat gerak): dikirim **asinkron setiap 0.5 detik (30 tick)** via `relay@1.0` tanpa memblokir loop 60 Hz.
     - *Critical Event* (mis. transaksi item/skor akhir): memicu **immediate sync flush** ke SD Process saat itu juga sebelum memberi konfirmasi selesai ke klien.

#### 3. Shared Data (SD) Process (`hb-sd-1`, `hb-sd-2` — *Hot Replica & ANS-104 Bundler*)
* **Jalur Komunikasi:** **Hibrida** — menerima snapshot dari Primary lewat **Direct Node-to-Node**, dan mengirim batch penyelesaian ke Arbit Process lewat **Inbox/Outbox (SU + ANS-104)**.
* **Tugas Utama:**
  1. **Validasi & Penyimpanan Cepat:** Memverifikasi tanda tangan `hb-primary`, memastikan `Epoch` cocok dengan Genesis (menolak *zombie primary* dari epoch lama), memastikan `tick` berurutan, lalu menyimpan *rolling snapshot* terbaru ke **`hb_store_rocksdb`** bawaan HyperBEAM.
  2. **Signed ACK ke Primary:** Mengirim balik ACK bertanda tangan ke Primary Process sebagai bukti state sampai tick $T$ sudah aman di node fisik terpisah.
  3. **Sumber Pemulihan Instan (*Failover Source*):** Menyediakan snapshot terakhir ($\le 0.5$ detik) jika Primary mati dan node baru mengambil alih di `Epoch + 1`.
  4. **Pembungkus & Pengirim Checkpoint ANS-104 ke Arbit:** Setiap interval penyelesaian (mis. **5–10 detik** atau akhir sesi), node SD merangkum *Summary Checkpoint + Bukti Tanda Tangan Kuorum SD*, menandatanganinya menjadi bundel **ANS-104** menggunakan wallet JWK node SD (`ans104@1.0`), lalu mem-push-nya ke Inbox **Arbit Process**. *(Node SD cadangan mengecek tip `/now` Arbit lebih dulu agar tidak mengirim duplikat jika SD utama sehat).*

#### 4. Arbit Process (`hb-arbit` — *Consensus Gatekeeper & Anti-Double-Spend Judge*)
* **Jalur Komunikasi:** **Murni Inbox/Outbox (SU)** (`process@1.0` + `scheduler@1.0` + `ans104@1.0`).
* **Tugas Utama:**
  1. Berdiri sebagai gerbang konsensus tunggal yang **tahan mati & bebas double-spend** (karena setiap pesan masuk diurutkan secara mutlak oleh SU dan tersimpan meskipun node sempat restart).
  2. Memverifikasi keabsahan bukti kuorum SD berdasarkan daftar operator sah dan `Epoch` dari Genesis, memvalidasi urutan `tick` & komitmen state (anti-replay, anti-spam, anti-cheat satu node), serta mencatat idempotensi batch.
  3. Mengunci **Official Permanent State** yang diproyeksikan via `/now` (`patch@1.0`) sekaligus ter-anchor permanen bersama assignment SU ke **Arweave L1**.

#### 5. Arweave L1 (`arlocal :1984` — *Permanent Permaweb Settlement*)
* **Tugas Utama:** Menyimpan abadi biner modul Rust WASM & skrip Lua proses sejak Fase Spawn, serta menyimpan seluruh bundel transaksi ANS-104 dan riwayat settlement Arbit Process.

---

## 5. Urutan Pembuktian PoC (Roadmap Pembangunan dari Nol)

Agar pembangunan tetap tajam, bersih, dan nol *over-engineering*, kita mengeksekusinya dalam 4 langkah konkret:

1. **Tahap 1 — Infrastruktur Node Pekerja Lokal (Docker + Tmux):**
   Menyiapkan skrip bersih untuk menyalakan `ArLocal (:1984)` via Docker dan 5 node HyperBEAM independen (`hb-genesis`, `hb-primary`, `hb-sd-1`, `hb-sd-2`, `hb-arbit`) via Tmux di atas engine asli HyperBEAM.
2. **Tahap 2 — Bootstrap Konstitusi & Message Box Flow (`Genesis` & `Arbit`):**
   Menghidupkan **Genesis Process** dan **Arbit Process** menggunakan mekanisme native AO (`process@1.0` + `scheduler@1.0` + `ans104@1.0`), lengkap dengan registrasi konfigurasi node direct dan verifikasi anti-double-spend.
3. **Tahap 3 — Mesin Real-Time 60 Hz & Direct Node-to-Node Flow (`Primary` & `SD`):**
   Membangun modul **Rust WASM** (Fixed-Slot State + Ring Buffer 1–2 detik + Snapshot Exporter) + **Lua Gate** di `hb-primary`, menghubungkan lemparan snapshot 0.5 detik via HTTP direct ke `hb-sd-1`/`hb-sd-2` (`hb_store_rocksdb`), hingga tembus batch ANS-104 ke `hb-arbit` & `ArLocal`.
4. **Tahap 4 — Simulasi Tugas Penggunaan Nyata (60 Tick/s + Uji Mati Node):**
   Menjalankan skenario beban nyata 60 tick/detik (simulasi pergerakan entitas + event transaksi penting + mematikan paksa node di tengah jalan) untuk mengukur latensi, memori konstan Ring Buffer, ketahanan Inbox/Outbox Arbit, dan kecepatan pemulihan dari SD. *(Baru setelah Tahap 4 lulus penuh, kita memikirkan ekstraksi menjadi library/SDK umum).*
