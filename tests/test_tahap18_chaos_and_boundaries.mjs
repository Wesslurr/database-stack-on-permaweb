// tests/test_tahap18_chaos_and_boundaries.mjs
// Phase 6 Audit: Resilience & Fail-Closed Boundaries before the game benchmark.
//
// Continues from the committed state left by Tahap 17 (no reset), so it also proves
// continuity of the Hot sequence, SD WALs, committer cursor and Arbitrator tip.
//
// Topology: hb-genesis :8733 (Genesis + Arbitrator AO processes), hb-hot :8734,
//           hb-sd-1 :8735 (committer owner), hb-sd-2 :8745, hb-sd-3 :8755, ArLocal :1984.
// Quorum target: majority of configured membership (3) = 2.
//
// Scenarios:
//  0. Baseline: all 5 nodes healthy, write is durable with su_count = 3.
//  1. One SD down (sd-3): majority write still durable (su_count = 2);
//     quorum=all write does NOT claim durability.
//  2. Two SDs down (sd-2, sd-3): majority write fails closed (ERR_DURABILITY_TIMEOUT,
//     replicated=false, never "durable").
//  3. Self-repair: restart sd-2 and sd-3; they catch up from Hot over TCP; all 3 WAL
//     tips/state roots converge; new write durable with su_count = 3.
//     Committer then commits the whole range (incl. events written during the outage).
//  4. Genesis outage: stop hb-genesis.
//     - Data plane (Hot -> SD quorum) keeps accepting durable writes.
//     - SD-1 keeps its last valid Genesis pin (genesis_id / arbitrator_id).
//     - A config change (bootstrap to a different Genesis ID) is rejected and does NOT
//       overwrite the pinned config.
//     - Committer refuses to report "committed" and does not advance the cursor.
//     Restart Genesis: commit succeeds, cursor == SD tip == Arbitrator OfficialSeq,
//     authentic re-bootstrap returns healthy.

import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import assert from 'node:assert';
import * as arbundles from 'arbundles';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const ROOT_DIR = join(__dirname, '..');

const HB_GENESIS_URL = 'http://127.0.0.1:8733';
const HB_HOT_URL = 'http://127.0.0.1:8734';
const HB_SD1_URL = 'http://127.0.0.1:8735';
const HB_SD2_URL = 'http://127.0.0.1:8745';
const HB_SD3_URL = 'http://127.0.0.1:8755';

const NODE_PORTS = { genesis: 8733, node: 8734, node_sd_1: 8735, node_sd_2: 8745, node_sd_3: 8755 };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------
async function fetchJson(url) {
  const resp = await fetch(url, { headers: { 'Accept': 'application/json', 'Connection': 'close' } });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`Request to ${url} failed with HTTP ${resp.status}: ${text}`);
  }
  return await resp.json();
}

// Never throws on HTTP status; returns { httpStatus, body } (body may be text).
async function fetchAny(url, timeoutMs = 180000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { headers: { 'Accept': 'application/json', 'Connection': 'close' }, signal: ctrl.signal });
    const text = await resp.text();
    let body;
    try { body = JSON.parse(text); } catch { body = text; }
    return { httpStatus: resp.status, body };
  } finally {
    clearTimeout(t);
  }
}

async function isUp(port) {
  try {
    const r = await fetch(`http://127.0.0.1:${port}/~meta@1.0/info`, {
      headers: { 'Accept': 'application/json' }, signal: AbortSignal.timeout(3000)
    });
    return r.ok;
  } catch { return false; }
}

const isTrue = (v) => v === true || v === 'true';

// ---------------------------------------------------------------------------
// Cluster control (real process kill / restart via repo scripts)
// ---------------------------------------------------------------------------
function stopNodes(...names) {
  execFileSync('bash', [join(ROOT_DIR, 'scripts/stop-cluster.sh'), ...names], { stdio: 'inherit' });
}
function startNodes(...names) {
  execFileSync('bash', [join(ROOT_DIR, 'scripts/start-cluster.sh'), ...names], { stdio: 'inherit' });
}
async function waitDown(name, timeoutMs = 30000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (!(await isUp(NODE_PORTS[name]))) return;
    await sleep(500);
  }
  throw new Error(`${name} still up after ${timeoutMs}ms`);
}
async function waitUp(name, timeoutMs = 60000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (await isUp(NODE_PORTS[name])) return;
    await sleep(1000);
  }
  throw new Error(`${name} not up after ${timeoutMs}ms`);
}

// ---------------------------------------------------------------------------
// Data-plane helpers
// ---------------------------------------------------------------------------
async function put(key, val, quorum = 'majority') {
  const url = `${HB_HOT_URL}/~hymessage@1.0/put?namespace=default&key=${key}&val=${val}&quorum=${quorum}&require_durability=true`;
  const t0 = performance.now();
  const r = await fetchAny(url, 30000);
  return { ...r, latencyMs: performance.now() - t0 };
}
const tick = (url) => fetchJson(`${url}/~sd@1.0/tick`);
const sdStatus = () => fetchJson(`${HB_SD1_URL}/~sd@1.0/status`);
function readCursor() {
  return JSON.parse(readFileSync(join(ROOT_DIR, 'data/node_sd_1/cursor.json'), 'utf8'));
}
function walLineCount(node) {
  return readFileSync(join(ROOT_DIR, `data/${node}/wal.log`), 'utf8').trim().split('\n').filter(Boolean).length;
}

async function waitConverged(expectedTick, timeoutMs = 90000) {
  const t0 = Date.now();
  let last;
  while (Date.now() - t0 < timeoutMs) {
    try {
      last = await Promise.all([tick(HB_SD1_URL), tick(HB_SD2_URL), tick(HB_SD3_URL)]);
      if (last.every(t => Number(t.latest_tick) === expectedTick)) return last;
    } catch {}
    await sleep(1000);
  }
  throw new Error(`SD replicas did not converge to tick ${expectedTick}: ${JSON.stringify(last?.map(t => t.latest_tick))}`);
}

async function commitNow(maxAttempts = 3) {
  let last;
  for (let i = 0; i < maxAttempts; i++) {
    last = await fetchAny(`${HB_SD1_URL}/~sd@1.0/status?action=commit_now`, 200000);
    if (last.body && isTrue(last.body.ok) && last.body.result === 'committed') return last;
    console.log(`    commit attempt ${i + 1} -> ${JSON.stringify(last.body)}`);
    await sleep(5000);
  }
  return last;
}

// ---------------------------------------------------------------------------
// Arbitrator read path (real HyperBEAM process@1.0 on hb-genesis)
// ---------------------------------------------------------------------------
async function postAns104(url, dataItem) {
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/ans104', 'Accept': 'application/json', 'Connection': 'close' },
    body: dataItem.getRaw()
  });
  if (!resp.ok) throw new Error(`POST ${url} failed with HTTP ${resp.status}: ${await resp.text().catch(() => '')}`);
  return await resp.json();
}

async function scheduleMessage(signer, procId, action, data) {
  const createData = arbundles.createData || arbundles.default?.createData;
  const tags = [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Type', value: 'Message' },
    { name: 'Target', value: procId },
    { name: 'Action', value: action },
    { name: 'Timestamp', value: Date.now().toString() }
  ];
  const item = createData(data, signer, { tags });
  await item.sign(signer);
  const res = await postAns104(`${HB_GENESIS_URL}/${procId}~process@1.0/schedule`, item);
  return { slot: res.slot };
}

async function getOfficialState(signer, arbitratorId) {
  const { slot } = await scheduleMessage(signer, arbitratorId, 'GetOfficialState', '');
  for (let attempt = 0; attempt < 120; attempt++) {
    try {
      const now = await fetchJson(`${HB_GENESIS_URL}/${arbitratorId}~process@1.0/now`);
      if (parseInt(now['at-slot'], 10) >= slot && now['results+link']) {
        const results = await fetchJson(`${HB_GENESIS_URL}/${now['results+link']}`);
        if (results['outbox+link']) {
          const outbox = await fetchJson(`${HB_GENESIS_URL}/${results['outbox+link']}`);
          for (const [k, v] of Object.entries(outbox)) {
            if (!k.endsWith('+link')) continue;
            const msg = await fetchJson(`${HB_GENESIS_URL}/${v}`);
            if (msg.Action === 'OfficialStateResponse') return msg;
          }
        }
      }
    } catch {}
    await sleep(1000);
  }
  throw new Error('Timeout reading Arbitrator OfficialState');
}

// ---------------------------------------------------------------------------
async function main() {
  console.log('========================================================================');
  console.log('TAHAP 18: RESILIENCE & FAIL-CLOSED BOUNDARY AUDIT');
  console.log('========================================================================\n');

  // Pre-check cluster availability
  const isClusterAlive = await isUp(NODE_PORTS.genesis).catch(() => false);
  if (!isClusterAlive) {
    console.log('  ⚠️ [Live Cluster Inactive] HyperBEAM cluster is not running.');
    console.log('  ⚠️ Skipping live cluster chaos tests (run npm run cluster:start for live testing).\n');
    console.log('========================================================================');
    console.log(' TAHAP 18 VERIFICATION SUMMARY: SKIPPED (Cluster Offline)');
    console.log('========================================================================');
    return;
  }

  const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
  const signerSu = new ArweaveSigner(JSON.parse(readFileSync(join(ROOT_DIR, 'secrets/wallet_su.json'), 'utf8')));

  const genesisId = readFileSync(join(ROOT_DIR, 'ao_process/process_genesis_id.txt'), 'utf8').trim();
  const genesisHash = readFileSync(join(ROOT_DIR, 'ao_process/genesis_hash.txt'), 'utf8').trim();
  const arbitratorId = readFileSync(join(ROOT_DIR, 'ao_process/process_id.txt'), 'utf8').trim();
  console.log('  Genesis ID   :', genesisId);
  console.log('  Arbitrator ID:', arbitratorId);

  // -------------------------------------------------------------------------
  // SCENARIO 0: Baseline
  // -------------------------------------------------------------------------
  console.log('\n[Scenario 0] Baseline: all nodes healthy, durable write with su_count = 3');
  for (const n of Object.keys(NODE_PORTS)) assert(await isUp(NODE_PORTS[n]), `${n} is up`);
  const startTick = Number((await tick(HB_SD1_URL)).latest_tick);
  const startCursor = readCursor();
  console.log(`  Start: SD tick = ${startTick}, committed cursor = ${startCursor.last_committed_seq}`);

  let r = await put(3001, 10);
  assert.strictEqual(r.body.write_status, 'durable', `baseline durable: ${JSON.stringify(r.body)}`);
  assert.strictEqual(Number(r.body.su_count), 3, 'baseline su_count = 3');
  let expectedSeq = startTick + 1;
  assert.strictEqual(Number(r.body.seq), expectedSeq, 'baseline seq continues from Tahap 17');
  console.log(`  ✅ seq=${r.body.seq} durable su_count=3 (${r.latencyMs.toFixed(1)}ms)`);

  // -------------------------------------------------------------------------
  // SCENARIO 1: One SD down -> quorum (2/3) still met
  // -------------------------------------------------------------------------
  console.log('\n[Scenario 1] Kill hb-sd-3 -> majority quorum (2 of 3) must still be durable');
  stopNodes('node_sd_3');
  await waitDown('node_sd_3');
  await sleep(1000);

  r = await put(3002, 20);
  console.log(`  majority put -> HTTP ${r.httpStatus} ${JSON.stringify(r.body)}`);
  expectedSeq++;
  assert.strictEqual(r.body.write_status, 'durable', 'one-SD-down majority write is durable');
  assert(isTrue(r.body.replicated), 'replicated=true');
  assert.strictEqual(Number(r.body.su_count), 2, 'su_count = 2 with one SD down');
  assert.strictEqual(Number(r.body.seq), expectedSeq);
  console.log('  ✅ Majority write durable with su_count = 2');

  r = await put(3003, 30, 'all');
  console.log(`  quorum=all put -> HTTP ${r.httpStatus} ${JSON.stringify(r.body)}`);
  expectedSeq++;
  assert.notStrictEqual(r.body.write_status, 'durable', 'quorum=all must not claim durable with one SD down');
  assert(!isTrue(r.body.replicated), 'quorum=all replicated=false');
  assert.strictEqual(r.body.error_code, 'ERR_DURABILITY_TIMEOUT');
  console.log('  ✅ quorum=all fails closed (not durable) while one SD is down');

  // -------------------------------------------------------------------------
  // SCENARIO 2: Two SDs down -> below quorum, must fail closed
  // -------------------------------------------------------------------------
  console.log('\n[Scenario 2] Kill hb-sd-2 as well -> below quorum, must NOT claim durability');
  stopNodes('node_sd_2');
  await waitDown('node_sd_2');
  await sleep(1000);

  r = await put(3004, 40);
  console.log(`  majority put -> HTTP ${r.httpStatus} ${JSON.stringify(r.body)}`);
  expectedSeq++;
  assert.notStrictEqual(r.body.write_status, 'durable', 'below-quorum write must not be durable');
  assert(!isTrue(r.body.replicated), 'replicated=false below quorum');
  assert.strictEqual(r.body.error_code, 'ERR_DURABILITY_TIMEOUT', 'fail-closed error code');
  assert(Number(r.body.su_count) < 2, 'su_count below quorum');
  console.log(`  ✅ Fail-closed: ERR_DURABILITY_TIMEOUT, su_count = ${r.body.su_count}, HTTP ${r.httpStatus}`);

  // -------------------------------------------------------------------------
  // SCENARIO 3: Self-repair
  // -------------------------------------------------------------------------
  console.log('\n[Scenario 3] Restart hb-sd-2 and hb-sd-3 -> TCP catch-up and convergence');
  startNodes('node_sd_2', 'node_sd_3');
  await waitUp('node_sd_2');
  await waitUp('node_sd_3');

  const caughtUp = await waitConverged(expectedSeq);
  console.log(`  ✅ All 3 SDs caught up to tick ${expectedSeq}`);
  assert.strictEqual(caughtUp[0].latest_hash, caughtUp[1].latest_hash);
  assert.strictEqual(caughtUp[1].latest_hash, caughtUp[2].latest_hash);
  assert.strictEqual(String(caughtUp[0].state_root_str), String(caughtUp[1].state_root_str));
  assert.strictEqual(String(caughtUp[1].state_root_str), String(caughtUp[2].state_root_str));

  r = await put(3005, 50);
  expectedSeq++;
  assert.strictEqual(r.body.write_status, 'durable', `post-repair durable: ${JSON.stringify(r.body)}`);
  assert.strictEqual(Number(r.body.su_count), 3, 'su_count back to 3 after repair');
  assert.strictEqual(Number(r.body.seq), expectedSeq);
  console.log(`  ✅ Post-repair write seq=${r.body.seq} durable su_count=3`);

  const repaired = await waitConverged(expectedSeq);
  assert(repaired.every(t => t.latest_hash === repaired[0].latest_hash), 'tip hash identical on all SDs');
  assert(repaired.every(t => String(t.state_root_str) === String(repaired[0].state_root_str)), 'state root identical on all SDs');
  const walCounts = ['node_sd_1', 'node_sd_2', 'node_sd_3'].map(walLineCount);
  assert(walCounts.every(c => c === expectedSeq), `WAL line counts equal tip: ${walCounts}`);
  console.log(`  ✅ WALs converged: lines=${walCounts.join('/')}, tip=${repaired[0].latest_hash.slice(0, 16)}…`);

  console.log('  Committing recovered range through in-BEAM committer -> Arbitrator...');
  let c = await commitNow();
  assert(c.body && c.body.result === 'committed', `commit after repair: ${JSON.stringify(c.body)}`);
  let cursor = readCursor();
  assert.strictEqual(Number(cursor.last_committed_seq), expectedSeq, 'cursor at repaired tip');
  assert.strictEqual(cursor.last_committed_hash, repaired[0].latest_hash, 'cursor hash = SD tip');
  console.log(`  ✅ Committed through seq ${expectedSeq} (includes events written during the outage)`);

  // -------------------------------------------------------------------------
  // SCENARIO 4: Genesis outage
  // -------------------------------------------------------------------------
  console.log('\n[Scenario 4] Kill hb-genesis -> data plane continues, config changes fail closed');
  const pinnedBefore = await sdStatus();
  assert.strictEqual(pinnedBefore.genesis_id, genesisId, 'SD-1 pinned to authoritative Genesis');
  assert.strictEqual(pinnedBefore.arbitrator_id, arbitratorId, 'SD-1 pinned to authoritative Arbitrator');
  const cursorBefore = readCursor();

  stopNodes('genesis');
  await waitDown('genesis');

  r = await put(3006, 60);
  expectedSeq++;
  assert.strictEqual(r.body.write_status, 'durable', `write durable without Genesis: ${JSON.stringify(r.body)}`);
  assert.strictEqual(Number(r.body.su_count), 3);
  console.log(`  ✅ Data plane alive without Genesis: seq=${r.body.seq} durable su_count=3`);

  const forgedGenesis = 'FORGEDGENESISxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
  const cfg = await fetchAny(`${HB_SD1_URL}/~sd@1.0/status?action=bootstrap_genesis&genesis_id=${forgedGenesis}&genesis_hash=${'00'.repeat(32)}`, 120000);
  console.log(`  config change attempt -> ${JSON.stringify(cfg.body)}`);
  assert(!isTrue(cfg.body.ok), 'config change rejected while Genesis unreachable');
  const pinnedDuring = await sdStatus();
  assert.strictEqual(pinnedDuring.genesis_id, genesisId, 'last valid Genesis ID retained (not overwritten)');
  assert.strictEqual(pinnedDuring.arbitrator_id, arbitratorId, 'last valid Arbitrator ID retained');
  console.log('  ✅ Config change rejected; last valid Genesis/Arbitrator pin retained');

  const cDown = await fetchAny(`${HB_SD1_URL}/~sd@1.0/status?action=commit_now`, 200000);
  console.log(`  commit attempt without Genesis -> ${JSON.stringify(cDown.body)}`);
  assert(cDown.body.result !== 'committed', 'committer must not report committed without Arbitrator');
  const cursorDuring = readCursor();
  assert.strictEqual(Number(cursorDuring.last_committed_seq), Number(cursorBefore.last_committed_seq), 'cursor not advanced');
  console.log('  ✅ Committer fails closed: no commit, cursor unchanged at', cursorDuring.last_committed_seq);

  console.log('  Restarting hb-genesis...');
  startNodes('genesis');
  await waitUp('genesis', 120000);

  c = await commitNow(5);
  assert(c.body && c.body.result === 'committed', `commit after Genesis restart: ${JSON.stringify(c.body)}`);
  const finalTicks = await waitConverged(expectedSeq);
  cursor = readCursor();
  assert.strictEqual(Number(cursor.last_committed_seq), expectedSeq, 'cursor at final tip');
  assert.strictEqual(cursor.last_committed_hash, finalTicks[0].latest_hash, 'cursor hash = SD tip');
  console.log(`  ✅ Commit resumed after Genesis restart: cursor seq = ${cursor.last_committed_seq}`);

  const official = await getOfficialState(signerSu, arbitratorId);
  console.log('  Arbitrator Official State:', JSON.stringify({ seq: official.OfficialSeq, hash: official.OfficialHash, root: official.OfficialStateRoot }));
  assert.strictEqual(Number(official.OfficialSeq), expectedSeq, 'Arbitrator OfficialSeq = final tip');
  assert.strictEqual(official.OfficialHash, finalTicks[0].latest_hash, 'Arbitrator hash = SD tip');
  assert.strictEqual(String(official.OfficialStateRoot), String(finalTicks[0].state_root_str), 'Arbitrator root = SD root');

  const reboot = await fetchAny(`${HB_SD1_URL}/~sd@1.0/status?action=bootstrap_genesis&genesis_id=${genesisId}&genesis_hash=${genesisHash}`, 200000);
  assert(isTrue(reboot.body.ok), `authentic re-bootstrap ok: ${JSON.stringify(reboot.body)}`);
  const finalStatus = await sdStatus();
  assert.strictEqual(finalStatus.committer_health, 'healthy', 'committer healthy after Genesis recovery');
  assert.strictEqual(finalStatus.genesis_id, genesisId);
  assert.strictEqual(finalStatus.arbitrator_id, arbitratorId);
  console.log('  ✅ Authentic re-bootstrap healthy; Arbitrator tip matches SD tip');

  console.log('\n========================================================================');
  console.log('🎉 TAHAP 18: RESILIENCE & FAIL-CLOSED BOUNDARIES PASSED 100%');
  console.log(`   Final tip: seq ${expectedSeq}, hash ${finalTicks[0].latest_hash}`);
  console.log('========================================================================\n');
}

main().catch(err => {
  console.error('\n❌ TAHAP 18 AUDIT FAILED:', err);
  process.exit(1);
});
