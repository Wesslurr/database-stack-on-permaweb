// tests/test_tahap15_arbitrator_native_committer.mjs
// Phase 3 Audit: In-BEAM Committer to Native HyperBEAM Arbitrator AO Process Integration
//
// Invariants Tested:
// 1. Arbitrator AO Process spawned on hb-genesis (:8733) via native WAMR/AOS runtime.
// 2. SU Committer node authorized in Arbitrator contract state.
// 3. hb-sd-1 in-BEAM Committer configured to target native Arbitrator on hb-genesis.
// 4. Valid checkpoint batch committed by in-BEAM committer -> COMMITTED at OfficialSeq 3.
// 5. Cursor persisted to data/node_sd_1/cursor.json with exact hash and state root.
// 6. Idempotent Retry: Replaying batch against advanced tip returns DUPLICATE_BATCH_IDEMPOTENT without cursor corruption.
// 7. Cluster Restart Reconciliation: Committer cold-boots, reconciles with Arbitrator, reaches HEALTHY state.

import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import * as crypto from 'node:crypto';
import assert from 'node:assert';
import { execSync } from 'node:child_process';
import * as arbundles from 'arbundles';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const ROOT_DIR = join(__dirname, '..');

const HB_GENESIS_URL = 'http://127.0.0.1:8733';
const HB_SD1_URL = 'http://127.0.0.1:8735';
const MASK64 = 0xffffffffffffffffn;

function canonicalHash64(keyInt, valInt, verInt) {
  const prime = 0x100000001b3n;
  const ukey = BigInt(keyInt) & MASK64;
  const uval = BigInt(valInt) & MASK64;
  const uver = BigInt(verInt) & MASK64;
  let h = 0xcbf29ce484222325n;
  h = ((h ^ ukey) * prime) & MASK64;
  h = ((h ^ uval) * prime) & MASK64;
  h = ((h ^ uver) * prime) & MASK64;
  h = (h ^ (h >> 30n)) & MASK64;
  h = (h * 0xbf58476d1ce4e5b9n) & MASK64;
  h = (h ^ (h >> 27n)) & MASK64;
  h = (h * 0x94d049bb133111ebn) & MASK64;
  h = (h ^ (h >> 31n)) & MASK64;
  return BigInt.asIntN(64, h);
}

function buildValidEvents(startSeq, count, prevHashInit, prevRootInit) {
  const events = [];
  let prevHash = prevHashInit;
  let root = BigInt(prevRootInit);
  for (let i = 0; i < count; i++) {
    const seq = startSeq + i;
    const key = 100 + seq;
    const val = seq * 1000;
    const ver = 1;
    const payload = `${prevHash}${seq}20${key}${val}`;
    const hash = crypto.createHash('sha256').update(payload).digest('hex').toUpperCase();
    const recHash = canonicalHash64(key, val, ver);
    root = BigInt.asIntN(64, (root ^ recHash) & MASK64);
    events.push({
      contract_version: 'hydb.core@1.0',
      epoch: 1,
      seq,
      namespace: 'default',
      action: 'put',
      key,
      val,
      val_type: 'int64',
      canonical_val: String(val),
      ver,
      prev_hash: prevHash,
      hash,
      state_root: root.toString(),
      state_root_str: root.toString()
    });
    prevHash = hash;
  }
  return events;
}

async function fetchJson(url) {
  const resp = await fetch(url, {
    headers: {
      'Accept': 'application/json',
      'Connection': 'close'
    }
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`GET ${url} failed with HTTP ${resp.status}: ${text}`);
  }
  return await resp.json();
}

async function postAns104(url, dataItem) {
  const rawBytes = dataItem.getRaw();
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json',
      'Connection': 'close'
    },
    body: rawBytes
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`POST ${url} failed with HTTP ${resp.status}: ${text}`);
  }
  return await resp.json();
}

async function spawnProcess(signer, scheduler, wasmImage) {
  const createData = arbundles.createData || arbundles.default?.createData;
  const spawnTags = [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Variant', value: 'ao.TN.1' },
    { name: 'Type', value: 'Process' },
    { name: 'Device', value: 'process@1.0' },
    { name: 'Scheduler-Device', value: 'scheduler@1.0' },
    { name: 'Execution-Device', value: 'stack@1.0' },
    { name: 'Scheduler', value: scheduler },
    { name: 'Image', value: wasmImage },
    { name: 'Device-Stack', value: '"wasi@1.0", "json-iface@1.0", "wasm-64@1.0", "multipass@1.0"' },
    { name: 'Output-Prefix', value: 'wasm' },
    { name: 'Patch-From', value: '/results/outbox' },
    { name: 'Passes', value: '2' },
    { name: 'Stack-Keys', value: '"init", "compute", "snapshot", "normalize"' },
    { name: 'Ao-Types', value: 'device-stack="list", passes="integer", stack-keys="list"' }
  ];

  const spawnItem = createData('', signer, { tags: spawnTags });
  await spawnItem.sign(signer);

  const res = await postAns104(`${HB_GENESIS_URL}/~push@1.0/push`, spawnItem);
  assert(res.status === 200, 'Process spawn assignment returned status 200');
  return spawnItem.id;
}

async function scheduleMessage(signer, procId, action, data, extraTags = []) {
  const createData = arbundles.createData || arbundles.default?.createData;
  const tags = [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Type', value: 'Message' },
    { name: 'Target', value: procId },
    { name: 'Action', value: action },
    ...extraTags
  ];
  const item = createData(data, signer, { tags });
  await item.sign(signer);

  const schedRes = await postAns104(`${HB_GENESIS_URL}/${procId}~process@1.0/schedule`, item);
  assert(schedRes.status === 200, `Message scheduled at slot ${schedRes.slot}`);
  return { itemId: item.id, slot: schedRes.slot };
}

async function computeAndGetLatestResult(procId) {
  const now = await fetchJson(`${HB_GENESIS_URL}/${procId}~process@1.0/now`);
  assert(now.status === 200, 'Now query returned status 200');
  if (!now['results+link']) {
    return { now, results: null, outbox: [] };
  }
  const results = await fetchJson(`${HB_GENESIS_URL}/${now['results+link']}`);
  let outbox = [];
  if (results['outbox+link']) {
    const outboxMap = await fetchJson(`${HB_GENESIS_URL}/${results['outbox+link']}`);
    for (const [k, v] of Object.entries(outboxMap)) {
      if (k.endsWith('+link') || (k !== 'status' && !isNaN(Number(k)))) {
        const msg = await fetchJson(`${HB_GENESIS_URL}/${v}`);
        outbox.push(msg);
      }
    }
  }
  return { now, results, outbox };
}

async function main() {
  console.log('========================================================================');
  console.log('TAHAP 15: IN-BEAM COMMITTER TO NATIVE ARBITRATOR AO PROCESS AUDIT');
  console.log('========================================================================\n');

  // Load SU & Genesis Wallets
  const info = await fetchJson(`${HB_GENESIS_URL}/~meta@1.0/info`);
  const scheduler = info.address || 'gw0iqMNNGgXcAj_yejTV0H_152Hwz941gxhnzvB_PY0';
  const suWalletPath = join(ROOT_DIR, 'secrets/wallet_su.json');
  const suJwk = JSON.parse(readFileSync(suWalletPath, 'utf8'));
  const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
  const signerSu = new ArweaveSigner(suJwk);
  const suAddress = readFileSync(join(ROOT_DIR, 'secrets/address_su.txt'), 'utf8').trim();

  const wasmImage = 'FSq0wtCYfPUVRHprQEB_4hAvQBpMCf_HYnJLn5FuqNI';

  // Step 1: Spawn Arbitrator AO Process on hb-genesis (:8733)
  console.log('[Step 1] Spawning Arbitrator AO Process on hb-genesis (:8733)...');
  const arbitratorId = await spawnProcess(signerSu, scheduler, wasmImage);
  console.log('  ✅ Arbitrator Process spawned with ID:', arbitratorId);

  // Step 2: Load arbitrator.lua into process
  console.log('\n[Step 2] Loading ao_process/arbitrator.lua into Arbitrator...');
  const arbitratorLua = readFileSync(join(ROOT_DIR, 'ao_process/arbitrator.lua'), 'utf8');
  await scheduleMessage(signerSu, arbitratorId, 'Eval', arbitratorLua);
  const evalRes = await computeAndGetLatestResult(arbitratorId);
  console.log('  ✅ Arbitrator Lua contracts initialized in WAMR');

  // Step 3: Authorize SU Committer node on Arbitrator
  console.log('\n[Step 3] Authorizing SU Committer node in Arbitrator contract state...');
  await scheduleMessage(signerSu, arbitratorId, 'Eval', `AuthorizedNodes["${suAddress}"] = true; return "AUTHORIZED"`);
  await computeAndGetLatestResult(arbitratorId);
  console.log('  ✅ SU Committer authorized:', suAddress);

  // Step 4: Verify Initial Arbitrator State
  console.log('\n[Step 4] Verifying Arbitrator initial official state (seq 0)...');
  await scheduleMessage(signerSu, arbitratorId, 'GetOfficialState', '');
  const initStateRes = await computeAndGetLatestResult(arbitratorId);
  const initMsg = initStateRes.outbox.find(m => m.Action === 'OfficialStateResponse');
  assert(initMsg, 'Arbitrator returned OfficialStateResponse');
  assert.strictEqual(String(initMsg.OfficialSeq), '0', 'Initial OfficialSeq must be 0');
  console.log('  ✅ Arbitrator initial tip: OfficialSeq =', initMsg.OfficialSeq);

  // Write new Arbitrator ID to ao_process/process_id.txt so cold starts use it
  writeFileSync(join(ROOT_DIR, 'ao_process/process_id.txt'), arbitratorId);

  // Step 5: Configure hb-sd-1 Committer with Arbitrator ID
  console.log('\n[Step 5] Configuring hb-sd-1 in-BEAM committer with Arbitrator ID...');
  const setArbRes = await fetchJson(`${HB_SD1_URL}/~sd@1.0/status?action=set_arbitrator&arbitrator_id=${arbitratorId}`);
  assert(setArbRes.ok === true || setArbRes.ok === 'true', 'set_arbitrator returned ok = true');
  assert.strictEqual(setArbRes.arbitrator_id, arbitratorId, 'Arbitrator ID updated on hb-sd-1 committer');
  console.log('  ✅ hb-sd-1 in-BEAM Committer updated with Arbitrator ID');

  // Step 6: Write valid events (seq 1..3) directly into hb-sd-1's isolated WAL
  console.log('\n[Step 6] Appending valid events (seq 1..3) into hb-sd-1 WAL...');
  const walPath = join(ROOT_DIR, 'data/node_sd_1/wal.log');
  const events1 = buildValidEvents(1, 3, '0000000000000000000000000000000000000000000000000000000000000000', '0');
  const lines1 = events1.map(e => JSON.stringify(e)).join('\n') + '\n';
  writeFileSync(walPath, lines1);

  // Trigger simulate_restart on hb-sd-1 to recover WAL into RAM store
  const adminToken = process.env.HYDB_ADMIN_TOKEN || 'admin_token_for_local_mesh_test_only_32_bytes_long';
  const recRes = await fetchJson(`${HB_SD1_URL}/~sd@1.0/simulate_restart?admin_token=${adminToken}`);
  assert(recRes.ok === true || recRes.ok === 'true', 'simulate_restart returned ok = true');
  assert.strictEqual(Number(recRes.recovered_entries), 3, 'Recovered 3 entries from WAL');
  assert.strictEqual(Number(recRes.tick), 3, 'Tick advanced to 3 in SD RAM');
  console.log('  ✅ WAL entries recovered into SD RAM: tick = 3, root =', recRes.state_root_str);

  // Step 7: Trigger In-BEAM Committer Commit Cycle
  console.log('\n[Step 7] Triggering native in-BEAM committer commit cycle...');
  const commitRes = await fetchJson(`${HB_SD1_URL}/~sd@1.0/status?action=commit_now`);
  console.log('  Committer result:', JSON.stringify(commitRes));
  assert(commitRes.ok === true || commitRes.ok === 'true', 'Commit cycle succeeded');
  assert.strictEqual(commitRes.result, 'committed', 'Commit cycle returned "committed"');
  console.log('  ✅ In-BEAM Committer successfully committed checkpoint batch to Arbitrator on hb-genesis!');

  // Step 8: Verify Cursor Persistence and Arbitrator Tip
  console.log('\n[Step 8] Verifying cursor persistence and Arbitrator on-chain tip...');
  const cursorPath = join(ROOT_DIR, 'data/node_sd_1/cursor.json');
  assert(existsSync(cursorPath), 'data/node_sd_1/cursor.json exists');
  const cursorData = JSON.parse(readFileSync(cursorPath, 'utf8'));
  assert.strictEqual(Number(cursorData.last_committed_seq), 3, 'Cursor last_committed_seq is 3');
  assert.strictEqual(cursorData.status, 'COMMITTED', 'Cursor status is COMMITTED');
  console.log('  ✅ Disk Cursor verified: seq =', cursorData.last_committed_seq, 'hash =', cursorData.last_committed_hash);

  // Verify Arbitrator on-chain tip on hb-genesis
  await scheduleMessage(signerSu, arbitratorId, 'GetOfficialState', '');
  const tipRes = await computeAndGetLatestResult(arbitratorId);
  const tipMsg = tipRes.outbox.find(m => m.Action === 'OfficialStateResponse');
  assert(tipMsg, 'Arbitrator returned OfficialStateResponse');
  assert.strictEqual(String(tipMsg.OfficialSeq), '3', 'Arbitrator official sequence advanced to 3');
  assert.strictEqual(tipMsg.OfficialHash, cursorData.last_committed_hash, 'Arbitrator OfficialHash matches committer cursor');
  console.log('  ✅ Arbitrator on-chain tip confirmed: OfficialSeq = 3, OfficialHash matches!');

  // Step 9: Test Idempotent Retry of Duplicate Batch
  console.log('\n[Step 9] Testing Idempotent Retry of duplicate batch...');
  // Reset cursor locally to sequence 0 without touching Arbitrator or WAL
  const resetCursor = {
    ...cursorData,
    last_committed_seq: 0,
    last_committed_hash: '0000000000000000000000000000000000000000000000000000000000000000',
    last_state_root: '0'
  };
  writeFileSync(cursorPath, JSON.stringify(resetCursor, null, 2));
  await fetchJson(`${HB_SD1_URL}/~sd@1.0/status?action=reload_cursor`);

  // Trigger commit_now: Committer re-proposes seq 1..3 to Arbitrator (which is already at seq 3)
  const dupCommitRes = await fetchJson(`${HB_SD1_URL}/~sd@1.0/status?action=commit_now`);
  console.log('  Duplicate retry result:', JSON.stringify(dupCommitRes));
  assert(dupCommitRes.ok === true || dupCommitRes.ok === 'true', 'Duplicate commit returned ok = true');
  assert.strictEqual(dupCommitRes.result, 'duplicate', 'Committer recognized DUPLICATE_BATCH_IDEMPOTENT verdict');

  // Verify cursor correctly updated back to sequence 3 with canonical hash and root
  const postDupCursor = JSON.parse(readFileSync(cursorPath, 'utf8'));
  assert.strictEqual(Number(postDupCursor.last_committed_seq), 3, 'Cursor safely updated back to sequence 3');
  assert.strictEqual(postDupCursor.last_committed_hash, cursorData.last_committed_hash, 'Cursor hash intact');
  console.log('  ✅ Idempotent duplicate handled cleanly: Cursor preserved at tip sequence 3!');

  // Step 10: Cold-Start Restart & Sequence Reconciliation Audit
  console.log('\n[Step 10] Testing cluster restart and cold-start reconciliation...');
  console.log('  Stopping cluster...');
  execSync('/home/weslur/database/scripts/stop-cluster.sh', { stdio: 'inherit' });
  console.log('  Restarting cluster...');
  execSync('/home/weslur/database/scripts/start-cluster.sh', { stdio: 'inherit' });

  // Await hb-sd-1 and hb-genesis readiness
  console.log('  Verifying node health post-restart...');
  const sd1Status = await fetchJson(`${HB_SD1_URL}/~sd@1.0/status`);
  assert(sd1Status.ok === true || sd1Status.ok === 'true', 'hb-sd-1 responds ok = true post restart');
  
  // Verify Committer reconciled with Arbitrator on-chain state
  const committerInfo = await fetchJson(`${HB_SD1_URL}/${sd1Status['committer+link']}`);
  console.log('  Committer status post-restart:', JSON.stringify(committerInfo));
  assert.strictEqual(Number(committerInfo.last_committed_seq), 3, 'Committer retained last_committed_seq = 3');
  assert.strictEqual(committerInfo.last_committed_hash, cursorData.last_committed_hash, 'Committer retained last_committed_hash');
  assert.strictEqual(committerInfo.health, 'healthy', 'Committer reconciled to HEALTHY state');
  console.log('  ✅ Cold-start reconciliation verified: Committer is HEALTHY at sequence 3!');

  console.log('\n========================================================================');
  console.log('🎉 TAHAP 15 IN-BEAM COMMITTER TO ARBITRATOR AO AUDIT COMPLETED 100%');
  console.log('========================================================================');
}

main().catch(err => {
  console.error('\n❌ TAHAP 15 AUDIT FAILED:', err);
  process.exit(1);
});
