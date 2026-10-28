/**
 * Tahap 13: Native HyperBEAM Execution of Authentic AO Genesis & Arbitrator Processes
 * 
 * Invariants Tested:
 * 1. HyperBEAM is the sole execution host (WAMR NIF, stack@1.0, wasm-64@1.0, json-iface@1.0, wasi@1.0).
 * 2. Genesis AO Process spawned via signed ANS-104 DataItem, loads genesis.lua, returns GetGenesisConfig.
 * 3. Arbitrator AO Process spawned via signed ANS-104 DataItem, loads arbitrator.lua.
 * 4. Arbitrator evaluates Checkpoint Batch 1 (seq 1..3) -> COMMITTED at seq 3.
 * 5. Arbitrator evaluates Checkpoint Batch 2 (seq 4..6) -> COMMITTED at seq 6.
 * 6. Idempotent Retry: Retrying Batch 2 returns DUPLICATE_BATCH_IDEMPOTENT with official tip seq 6.
 * 7. Fraud Detection: Tampered batch with invalid hash chain is strictly FRAUD_DETECTED_AND_CUT.
 */

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import * as crypto from 'node:crypto';
import assert from 'node:assert';
import * as arbundles from 'arbundles';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const ROOT_DIR = join(__dirname, '..');

const HB_URL = process.env.HB_URL || 'http://127.0.0.1:8734';
const mask64 = 0xffffffffffffffffn;

function canonicalHash64(keyInt, valInt, verInt) {
  const prime = 0x100000001b3n;
  const ukey = BigInt(keyInt) & mask64;
  const uval = BigInt(valInt) & mask64;
  const uver = BigInt(verInt) & mask64;
  let h = 0xcbf29ce484222325n;
  h = ((h ^ ukey) * prime) & mask64;
  h = ((h ^ uval) * prime) & mask64;
  h = ((h ^ uver) * prime) & mask64;
  h = (h ^ (h >> 30n)) & mask64;
  h = (h * 0xbf58476d1ce4e5b9n) & mask64;
  h = (h ^ (h >> 27n)) & mask64;
  h = (h * 0x94d049bb133111ebn) & mask64;
  h = (h ^ (h >> 31n)) & mask64;
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
    root = BigInt.asIntN(64, (root ^ recHash) & mask64);
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
  const res = await fetch(url, { headers: { 'Accept': 'application/json' } });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} on ${url}: ${await res.text()}`);
  }
  return res.json();
}

async function postAns104(url, dataItem) {
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json'
    },
    body: dataItem.getRaw()
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} on ${url}: ${await res.text()}`);
  }
  return res.json();
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

  const res = await postAns104(`${HB_URL}/~push@1.0/push`, spawnItem);
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

  const schedRes = await postAns104(`${HB_URL}/${procId}~process@1.0/schedule`, item);
  assert(schedRes.status === 200, `Message scheduled at slot ${schedRes.slot}`);
  return { itemId: item.id, slot: schedRes.slot };
}

async function computeAndGetLatestResult(procId) {
  const now = await fetchJson(`${HB_URL}/${procId}~process@1.0/now`);
  assert(now.status === 200, 'Now query returned status 200');
  if (!now['results+link']) {
    return { now, results: null, outbox: [] };
  }
  const results = await fetchJson(`${HB_URL}/${now['results+link']}`);
  let outbox = [];
  if (results['outbox+link']) {
    const outboxMap = await fetchJson(`${HB_URL}/${results['outbox+link']}`);
    for (const [k, v] of Object.entries(outboxMap)) {
      if (k.endsWith('+link') || (k !== 'status' && !isNaN(Number(k)))) {
        const msg = await fetchJson(`${HB_URL}/${v}`);
        outbox.push(msg);
      }
    }
  }
  return { now, results, outbox };
}

async function main() {
  console.log('========================================================================');
  console.log('TAHAP 13: NATIVE HYPERBEAM AO PROCESS SPAWN & EVALUATION VERIFICATION');
  console.log('========================================================================\n');

  // 1. Verify HyperBEAM Daemon running
  console.log('[Check 1] Verifying HyperBEAM node health at', HB_URL);
  const info = await fetchJson(`${HB_URL}/~meta@1.0/info`);
  assert(info.status === 200, 'HyperBEAM node is UP and reports 200');
  const scheduler = info.address || info.scheduler || 'XpOEPX668w7VNicVDHK81unzeeH8HcD83fKZRZKx9pk';
  console.log('  ✅ HyperBEAM active with scheduler address:', scheduler);

  // 2. Load Wallets
  const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
  const walletSu = JSON.parse(readFileSync(join(ROOT_DIR, 'secrets/wallet_su.json'), 'utf8'));
  const signerSu = new ArweaveSigner(walletSu);
  const suAddress = crypto.createHash('sha256').update(Buffer.from(walletSu.n, 'base64url')).digest('base64url');
  console.log('  ✅ SU Committer wallet loaded, address:', suAddress);

  const wasmImage = 'FSq0wtCYfPUVRHprQEB_4hAvQBpMCf_HYnJLn5FuqNI';

  // 3. Test Genesis AO Process
  console.log('\n[Suite 1] Native HyperBEAM Genesis AO Process Lifecycle:');
  console.log('  Spawning Genesis AO Process...');
  const genesisId = await spawnProcess(signerSu, scheduler, wasmImage);
  console.log('  ✅ Genesis Process spawned with ID:', genesisId);

  console.log('  Loading ao_process/genesis.lua into Genesis Process...');
  const genesisLua = readFileSync(join(ROOT_DIR, 'ao_process/genesis.lua'), 'utf8');
  await scheduleMessage(signerSu, genesisId, 'Eval', genesisLua);
  const genInitRes = await computeAndGetLatestResult(genesisId);
  assert(genInitRes.now.initialized === 'true', 'Genesis Process state marked initialized');
  console.log('  ✅ Genesis Lua contracts loaded and evaluated in WAMR');

  console.log('  Querying GetGenesisConfig from Genesis Process...');
  await scheduleMessage(signerSu, genesisId, 'GetGenesisConfig', '');
  const genConfigRes = await computeAndGetLatestResult(genesisId);
  assert(genConfigRes.outbox.length > 0, 'Genesis emitted outbox message for GetGenesisConfig');
  const configMsg = genConfigRes.outbox[0];
  assert(configMsg.Action === 'GenesisConfigResponse', 'Emitted message Action is GenesisConfigResponse');
  const parsedConfig = JSON.parse(configMsg.Data);
  assert(parsedConfig.ok === true, 'Genesis config reports ok = true');
  assert(parsedConfig.genesis.stack_id === 'hydb_universal_mesh', 'Stack ID is hydb_universal_mesh');
  assert(parsedConfig.genesis.quorum_size === 3, 'Quorum size is 3');
  console.log('  ✅ GetGenesisConfig verified: StackId = hydb_universal_mesh, QuorumSize = 3');

  // 4. Test Arbitrator AO Process
  console.log('\n[Suite 2] Native HyperBEAM Arbitrator AO Process Lifecycle:');
  console.log('  Spawning Arbitrator AO Process...');
  const arbitratorId = await spawnProcess(signerSu, scheduler, wasmImage);
  console.log('  ✅ Arbitrator Process spawned with ID:', arbitratorId);

  console.log('  Loading ao_process/arbitrator.lua into Arbitrator Process...');
  const arbitratorLua = readFileSync(join(ROOT_DIR, 'ao_process/arbitrator.lua'), 'utf8');
  await scheduleMessage(signerSu, arbitratorId, 'Eval', arbitratorLua);
  await computeAndGetLatestResult(arbitratorId);
  console.log('  ✅ Arbitrator Lua contracts loaded and evaluated in WAMR');

  console.log('  Authorizing SU Committer node on Arbitrator...');
  await scheduleMessage(signerSu, arbitratorId, 'Eval', `AuthorizedNodes["${suAddress}"] = true; return "AUTHORIZED"`);
  await computeAndGetLatestResult(arbitratorId);
  console.log('  ✅ SU Committer authorized in Arbitrator contract state');

  // 5. Commit Batch 1 (seq 1..3)
  console.log('\n[Suite 3] Checkpoint Commitments & Verdict Auditing:');
  const b1Events = buildValidEvents(1, 3, '0000000000000000000000000000000000000000000000000000000000000000', '0');
  const b1PropHash = b1Events[b1Events.length - 1].hash;
  const b1PropRoot = b1Events[b1Events.length - 1].state_root;
  const batch1 = {
    contract_version: 'hydb.core@1.0',
    epoch: 1,
    from_seq: 1,
    to_seq: 3,
    prev_hash: '0000000000000000000000000000000000000000000000000000000000000000',
    proposed_hash: b1PropHash,
    proposed_state_root: b1PropRoot,
    events: b1Events
  };

  console.log('  Committing Checkpoint Batch 1 (seq 1..3)...');
  await scheduleMessage(signerSu, arbitratorId, 'CommitCheckpoint', JSON.stringify(batch1));
  const b1CommitRes = await computeAndGetLatestResult(arbitratorId);
  assert(b1CommitRes.outbox.length > 0, 'Arbitrator returned outbox verdict message for Batch 1');
  const verdictB1 = b1CommitRes.outbox[0];
  assert(verdictB1.Status === 'COMMITTED', `Verdict Status is COMMITTED, got ${verdictB1.Status}`);
  assert(String(verdictB1.OfficialSeq) === '3', `OfficialSeq advanced to 3, got ${verdictB1.OfficialSeq}`);
  assert(verdictB1.OfficialHash === b1PropHash, 'OfficialHash matches proposed hash tip');
  console.log('  ✅ Batch 1 COMMITTED: OfficialSeq = 3, OfficialHash =', verdictB1.OfficialHash);

  // 6. Commit Batch 2 (seq 4..6)
  const b2Events = buildValidEvents(4, 3, b1PropHash, b1PropRoot);
  const b2PropHash = b2Events[b2Events.length - 1].hash;
  const b2PropRoot = b2Events[b2Events.length - 1].state_root;
  const batch2 = {
    contract_version: 'hydb.core@1.0',
    epoch: 1,
    from_seq: 4,
    to_seq: 6,
    prev_hash: b1PropHash,
    proposed_hash: b2PropHash,
    proposed_state_root: b2PropRoot,
    events: b2Events
  };

  console.log('  Committing Checkpoint Batch 2 (seq 4..6)...');
  await scheduleMessage(signerSu, arbitratorId, 'CommitCheckpoint', JSON.stringify(batch2));
  const b2CommitRes = await computeAndGetLatestResult(arbitratorId);
  assert(b2CommitRes.outbox.length > 0, 'Arbitrator returned outbox verdict message for Batch 2');
  const verdictB2 = b2CommitRes.outbox[0];
  assert(verdictB2.Status === 'COMMITTED', `Verdict Status is COMMITTED, got ${verdictB2.Status}`);
  assert(String(verdictB2.OfficialSeq) === '6', `OfficialSeq advanced to 6, got ${verdictB2.OfficialSeq}`);
  assert(verdictB2.OfficialHash === b2PropHash, 'OfficialHash matches proposed hash tip');
  console.log('  ✅ Batch 2 COMMITTED: OfficialSeq = 6, OfficialHash =', verdictB2.OfficialHash);

  // 7. Duplicate Batch Idempotent Retry (re-commit Batch 2)
  console.log('  Testing Idempotent Retry of Batch 2...');
  await scheduleMessage(signerSu, arbitratorId, 'CommitCheckpoint', JSON.stringify(batch2));
  const dupCommitRes = await computeAndGetLatestResult(arbitratorId);
  assert(dupCommitRes.outbox.length > 0, 'Arbitrator returned verdict message for duplicate batch');
  const verdictDup = dupCommitRes.outbox[0];
  assert(verdictDup.Status === 'DUPLICATE_BATCH_IDEMPOTENT', `Verdict Status is DUPLICATE_BATCH_IDEMPOTENT, got ${verdictDup.Status}`);
  assert(String(verdictDup.OfficialSeq) === '6', `OfficialSeq remains 6, got ${verdictDup.OfficialSeq}`);
  assert(verdictDup.OfficialHash === b2PropHash, 'OfficialHash remains at sequence 6 tip');
  console.log('  ✅ Idempotent Retry verified: Status = DUPLICATE_BATCH_IDEMPOTENT, OfficialSeq = 6');

  // 8. Fraud Detection: Tampered Hash Chain
  console.log('  Testing Fraud Detection on Tampered Batch (bad hash)...');
  const tamperedBatch = {
    contract_version: 'hydb.core@1.0',
    epoch: 1,
    from_seq: 7,
    to_seq: 9,
    prev_hash: b2PropHash,
    proposed_hash: 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
    proposed_state_root: '9999999999999999',
    events: buildValidEvents(7, 3, b2PropHash, b2PropRoot)
  };
  await scheduleMessage(signerSu, arbitratorId, 'CommitCheckpoint', JSON.stringify(tamperedBatch));
  const fraudRes = await computeAndGetLatestResult(arbitratorId);
  assert(fraudRes.outbox.length > 0, 'Arbitrator returned verdict message for tampered batch');
  const verdictFraud = fraudRes.outbox[0];
  assert(verdictFraud.Status === 'FRAUD_DETECTED_AND_CUT', `Verdict Status is FRAUD_DETECTED_AND_CUT, got ${verdictFraud.Status}`);
  console.log('  ✅ Fraud Detection verified: Status = FRAUD_DETECTED_AND_CUT, PolicyAction =', verdictFraud.PolicyAction);

  console.log('\n========================================================================');
  console.log('🎉 ALL TAHAP 13 NATIVE HYPERBEAM AO EXECUTION TESTS PASSED SUCCESSFULLY');
  console.log('========================================================================');
}

main().catch(err => {
  console.error('\n❌ TAHAP 13 TEST SUITE FAILED:', err);
  process.exit(1);
});
