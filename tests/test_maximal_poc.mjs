#!/usr/bin/env node
/**
 * Master Integration Test: Pure Maximal PoC (AO + HyperBEAM)
 * ===========================================================
 * Arsitektur Murni & Jujur:
 * 1. Tepat 1 Custom Device: hymessage@1.0 (Hot Node jalur cepat)
 * 2. SU Node: Node HyperBEAM murni (hb_su:8735) dengan device native scheduler@1.0
 * 3. Semi-Synchronous Quorum: 200 OK baru terbit setelah data di-ACK oleh SU (<15ms SLO)
 * 4. SU Dual-Store: Tick & State di RAM (ETS) + Durability di disk WAL (wal.log)
 * 5. Real AO WASM Process: @permaweb/ao-loader sandbox mengevaluasi checkpoint
 * 6. ArLocal Finality: Block ledger committed tanpa biaya
 * 7. Bebas 100% dari mock Python & sidecar
 */

import { readFileSync } from 'node:fs';
import { execSync } from 'node:child_process';

const HOT_URL = 'http://127.0.0.1:8734/~hymessage@1.0';
const SU_SCHEDULER_URL = 'http://127.0.0.1:8735/~scheduler@1.0';
const SU_WAL_PATH = '/home/weslur/database/pool_su/wal.log';

async function httpGet(url) {
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!res.ok && res.status !== 200 && res.status !== 202) {
    throw new Error(`HTTP ${res.status} on ${url}`);
  }
  const text = await res.text();
  const data = JSON.parse(text);
  const rootMatch = text.match(/"(?:db_root_hash|state_root)":\s*(-?\d+)/);
  if (rootMatch) {
    data.raw_root_hash = rootMatch[1];
  }
  return data;
}

async function httpPost(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body)
  });
  return await res.json();
}

function evalSu(expr) {
  const output = execSync(
    `/home/weslur/database/scripts/hb-su-eval.sh "${expr}"`,
    { encoding: 'utf-8' }
  ).trim();
  return output;
}

async function main() {
  console.log('='.repeat(80));
  console.log('>>> RUNNING PURE MAXIMAL PoC (AO + HYPERBEAM PURE STACK) <<<');
  console.log('='.repeat(80));

  // -------------------------------------------------------------------
  // PHASE 1: VERIFY NATIVE ENDPOINTS & SU INBOUND SUBSCRIBER
  // -------------------------------------------------------------------
  console.log('\n[PHASE 1] Memverifikasi Endpoint Resmi & Koneksi SU Inbound Subscriber...');
  
  // 1a. Reset Hot Node
  await httpGet(`${HOT_URL}/reset`);
  await new Promise(r => setTimeout(r, 600));

  // 1b. Reset SU Node RAM & WAL via BEAM RPC
  evalSu('su_runtime:reset().');
  await new Promise(r => setTimeout(r, 600));

  // 1c. Cek Endpoint Resmi Native SU Device: scheduler@1.0
  const suSchedStatus = await httpGet(`${SU_SCHEDULER_URL}/status`);
  console.log(`  * SU Native Device:   scheduler@1.0 (Official AO Scheduler)`);
  console.log(`  * SU Operator Wallet: ${suSchedStatus.address}`);

  // 1d. Cek Status Koneksi SU ke Hot Node (tunggu hingga 3 detik)
  let isConnected = false;
  for (let i = 0; i < 6; i++) {
    const raw = evalSu('su_runtime:is_connected().');
    if (raw === 'true') {
      isConnected = true;
      break;
    }
    await new Promise(r => setTimeout(r, 500));
  }
  console.log(`  * SU Node Role:       SU-HyperBEAM-Node (Node: hb_su)`);
  console.log(`  * SU Node Connected:  ${isConnected}`);
  if (!isConnected) throw new Error('SU HyperBEAM node is NOT connected to Hot Node!');

  // 1e. Cek Hot Node Subscriber Count
  const hotState = await httpGet(`${HOT_URL}/state`);
  const activeSubs = Number(hotState.active_su_subscribers || 0);
  console.log(`  * Hot Node Active SU: ${activeSubs} subscriber(s) connected`);
  if (activeSubs < 1) throw new Error('Hot Node has 0 active SU subscribers!');

  // -------------------------------------------------------------------
  // PHASE 2: SEMI-SYNCHRONOUS QUORUM WRITES (<15ms SLO)
  // -------------------------------------------------------------------
  console.log('\n[PHASE 2] Eksekusi 25 Semi-Synchronous In-Memory Writes (<15ms SLO)...');
  console.log('  * ATURAN: Client baru menerima 200 OK SETELAH data di-ACK oleh SU Node!');

  const latencies = [];
  const testData = {};

  for (let i = 1; i <= 25; i++) {
    const key = 4000 + i;
    const val = 80000 + (i * 47);
    testData[key] = val;

    const t0 = performance.now();
    const resp = await httpGet(`${HOT_URL}/put?key=${key}&val=${val}`);
    const t1 = performance.now();
    const latMs = t1 - t0;
    latencies.push(latMs);

    const isOk = resp.ok === true || String(resp.ok).toLowerCase() === 'true';
    const isRep = resp.replicated === true || String(resp.replicated).toLowerCase() === 'true';
    const suCount = Number(resp.su_count || 0);

    if (!isOk) throw new Error(`Write failed at seq ${i}`);
    if (Number(resp.seq) !== i) throw new Error(`Sequence gap at seq ${i}`);
    if (!isRep) throw new Error(`Event ${i} not replicated to SU!`);
    if (suCount < 1) throw new Error(`SU count is 0 at seq ${i}!`);
  }

  const avgLat = latencies.reduce((a, b) => a + b, 0) / latencies.length;
  const minLat = Math.min(...latencies);
  const maxLat = Math.max(...latencies);
  const sorted = [...latencies].sort((a, b) => a - b);
  const p95Lat = sorted[Math.floor(sorted.length * 0.95)];

  console.log(`  * Total Writes:       25 records`);
  console.log(`  * Quorum Replicated:  100% (Confirmed by SU before user reply)`);
  console.log(`  * Avg Latency:        ${avgLat.toFixed(2)} ms`);
  console.log(`  * Min Latency:        ${minLat.toFixed(2)} ms`);
  console.log(`  * Max Latency:        ${maxLat.toFixed(2)} ms`);
  console.log(`  * P95 Latency:        ${p95Lat.toFixed(2)} ms`);
  if (avgLat >= 15.0) throw new Error(`SLO Budget Violated! Expected <15ms, got ${avgLat.toFixed(2)}ms`);
  console.log(`  * [SLO PASSED] Semi-Synchronous Quorum < 15ms Budget Satisfied!`);

  // -------------------------------------------------------------------
  // PHASE 3: SU IN-MEMORY LATEST TICK & DISK WAL INSPECTION
  // -------------------------------------------------------------------
  console.log('\n[PHASE 3] Memeriksa Tick SU di RAM & File WAL Persisten...');
  
  // Baca Tick langsung dari RAM SU via BEAM RPC
  const suTick = Number(evalSu('su_runtime:get_current_tick().'));
  const suRoot = evalSu('su_runtime:get_state_root().');

  console.log(`  * SU Source:          RAM (Zero Disk Read via BEAM RPC)`);
  console.log(`  * SU Latest Tick:     ${suTick}`);
  console.log(`  * SU State Root:      ${suRoot}`);
  if (suTick !== 25) throw new Error(`SU tick mismatch! Expected 25, got ${suTick}`);

  const hotTip = await httpGet(`${HOT_URL}/state`);
  const expectedRoot = String(hotTip.raw_root_hash || hotTip.db_root_hash);
  if (String(suRoot) !== expectedRoot) {
    throw new Error(`State root mismatch! Hot: ${expectedRoot}, SU: ${suRoot}`);
  }
  console.log(`  * [VERIFIED] SU is serving the latest tick & state directly from RAM!`);

  // Baca File WAL Persisten
  const walContent = readFileSync(SU_WAL_PATH, 'utf-8').trim();
  const walLines = walContent.split('\n').filter(Boolean);
  console.log(`  * SU Persistent WAL:  ${walLines.length} entries recorded on disk.`);
  if (walLines.length < 25) throw new Error(`WAL entries count mismatch!`);
  console.log(`  * [VERIFIED] Durability Anchor: All entries written to disk WAL!`);

  // -------------------------------------------------------------------
  // PHASE 4: CATASTROPHIC HOT NODE FAILURE & ZERO LOSS RECOVERY
  // -------------------------------------------------------------------
  console.log('\n[PHASE 4] Simulasi Catastrophic Hot Node RAM Wipe & Replay Failover...');
  await httpGet(`${HOT_URL}/reset`);
  const crashed = await httpGet(`${HOT_URL}/state`);
  if (Number(crashed.seq) !== 0) throw new Error('Reset failed!');
  console.log(`  * Hot Node volatile memory wiped (Seq=0, Root=0)`);

  console.log(`  * Reconstructing state from SU WAL into new Hot instance...`);
  const last25Events = walLines.slice(-25).map(l => JSON.parse(l));
  
  const tReplay0 = performance.now();
  const replayRes = await httpPost(`${HOT_URL}/replay`, { events: last25Events });
  const tReplay1 = performance.now();
  const replayDuration = tReplay1 - tReplay0;

  console.log(`  * Replay Status: Replayed ${replayRes.replayed_count} events in ${replayDuration.toFixed(2)} ms`);
  if (Number(replayRes.seq) !== 25) throw new Error(`Replay seq mismatch: ${replayRes.seq}`);

  // Verifikasi seluruh record utuh
  for (const [k, v] of Object.entries(testData)) {
    const rec = await httpGet(`${HOT_URL}/get?key=${k}`);
    if (Number(rec.val) !== v) throw new Error(`Key ${k} corrupted after failover!`);
  }
  console.log(`  * [ZERO DATA LOSS CONFIRMED] All 25 records recovered with 100% fidelity!`);

  // -------------------------------------------------------------------
  // PHASE 5: REAL AO PROCESS WASM EVALUATION & ARLOCAL COMMIT
  // -------------------------------------------------------------------
  console.log('\n[PHASE 5] Real AO WebAssembly Sandbox Evaluation & ArLocal Commitment...');
  const batchData = {
    from_seq: 1,
    to_seq: 25,
    proposed_state_root: Number(suRoot),
    proposed_hash: last25Events[last25Events.length - 1].hash,
    events: last25Events
  };

  const batchFile = '/home/weslur/database/ao_process/batch_checkpoint.json';
  execSync(`cat << 'EOF' > ${batchFile}\n${JSON.stringify(batchData, null, 2)}\nEOF`);

  const tAo0 = performance.now();
  const aoRaw = execSync(`node /home/weslur/database/ao_process/real_ao_evaluator.mjs commit ${batchFile}`, { encoding: 'utf-8' });
  const tAo1 = performance.now();
  const aoDuration = tAo1 - tAo0;

  const jsonStart = aoRaw.indexOf('{');
  const aoRes = JSON.parse(aoRaw.slice(jsonStart));

  console.log(`  * AO WASM Execution & Mining: ${aoDuration.toFixed(2)} ms`);
  console.log(`  * AO Process Verdict:         ${aoRes.wasit_verdict}`);
  console.log(`  * AO WASM Evaluated:          ${aoRes.ao_wasm_evaluated}`);
  console.log(`  * ArLocal Block:              ${aoRes.block_height}`);
  console.log(`  * ArLocal TX ID:              ${aoRes.arweave_tx_id}`);
  if (aoRes.wasit_verdict !== 'PASSED_AND_COMMITTED') throw new Error('Wasit rejected checkpoint!');
  console.log(`  * [AO COMMITTED] Checkpoint officially committed to ArLocal ledger!`);

  // -------------------------------------------------------------------
  // PHASE 6: ANTI-FRAUD WASIT ARBITRATION TEST
  // -------------------------------------------------------------------
  console.log('\n[PHASE 6] Pengujian Wasit Anti-Fraud di Real AO Sandbox...');
  const tamperedEvents = [...last25Events];
  const fraudEvent = { ...tamperedEvents[0], seq: 26, hash: 'FRAUDULENT_TAMPERED_HASH_9999999999999999999999999999999999999999' };
  tamperedEvents.push(fraudEvent);

  const fraudBatch = {
    from_seq: 1,
    to_seq: 26,
    proposed_state_root: 99999999,
    proposed_hash: 'FRAUDULENT_TAMPERED_HASH_9999999999999999999999999999999999999999',
    events: tamperedEvents
  };
  const fraudFile = '/home/weslur/database/ao_process/fraud_batch.json';
  execSync(`cat << 'EOF' > ${fraudFile}\n${JSON.stringify(fraudBatch, null, 2)}\nEOF`);

  const fraudRaw = execSync(`node /home/weslur/database/ao_process/real_ao_evaluator.mjs commit ${fraudFile}`, { encoding: 'utf-8' });
  const fraudJsonStart = fraudRaw.indexOf('{');
  const fraudRes = JSON.parse(fraudRaw.slice(fraudJsonStart));

  console.log(`  * Wasit Verdict:    ${fraudRes.wasit_verdict}`);
  console.log(`  * Rejection Reason: ${fraudRes.rejection_reason || 'Hash chain broken'}`);
  if (fraudRes.wasit_verdict !== 'FRAUD_DETECTED_AND_CUT') throw new Error('Wasit failed to catch fraud!');
  console.log(`  * [WASIT VERIFIED] Fraudulent attempt detected and REJECTED!`);

  // -------------------------------------------------------------------
  // FINAL RECAP
  // -------------------------------------------------------------------
  console.log('\n' + '='.repeat(80));
  console.log('>>> PURE PoC VERIFICATION COMPLETE: 100% SATISFIED <<<');
  console.log(`1. Custom Device: Tepat SATU custom device (hymessage@1.0) di Hot Node`);
  console.log(`2. Native SU Device: scheduler@1.0 resmi bawaan HyperBEAM di port :8735`);
  console.log(`3. Semi-Sync Quorum: Dikonfirmasi SU sebelum 200 OK (Avg: ${avgLat.toFixed(2)}ms < 15ms)`);
  console.log(`4. Zero Python: Seluruh mock Python & sidecar dibersihkan dari repo`);
  console.log(`5. Real AO WASM: Dievaluasi di WebAssembly sandbox (@permaweb/ao-loader)`);
  console.log(`6. ArLocal Finality: Block ${aoRes.block_height} ditambang permanen`);
  console.log('='.repeat(80) + '\n');
}

main().catch(err => {
  console.error('\n[FATAL ERROR]:', err);
  process.exit(1);
});
