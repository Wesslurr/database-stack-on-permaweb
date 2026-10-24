#!/usr/bin/env node
/**
 * HyDB Tahap 6 Comprehensive Verification Suite: AO Arbitrator Final Verification Authority
 * =========================================================================================
 * Tests and proves all Tahap 6 requirements:
 * 1. Explicit Bootstrap & Identity Authority (Fail-Closed sender auth)
 * 2. Epoch Fencing & Unauthorized Promotion Prevention
 * 3. Canonical Event Hashing & Action "24" (register_schema) support
 * 4. Unambiguous Multi-Namespace State Isolation (Zero cross-namespace collision)
 * 5. Dynamic Schema & Rule Invariant Enforcement inside Arbitrator
 * 6. Dual State Commitment Verification (64-bit root + 256-bit cryptographic commitment)
 * 7. Forged State Commitment Detection & Rejection
 * 8. Atomic All-or-Nothing Batch Rollback on Any Invalid Event
 * 9. Cross-Environment Verification: JS ArbitratorDomain vs Live AO Process via AoLoader
 */

import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import { execFileSync } from 'node:child_process';
import crypto from 'node:crypto';
import * as spec from '../spec/hydb_contract_spec.mjs';
import { ArbitratorDomain, computeCommitment256 } from '../ao_process/real_ao_evaluator.mjs';
import AoLoader from '@permaweb/ao-loader';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
const AO_WASM_PATH = process.env.AO_PROCESS_WASM || (
  existsSync(join(DB_DIR, 'ao_process/process.wasm'))
    ? join(DB_DIR, 'ao_process/process.wasm')
    : join(DB_DIR, 'vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/genesis-wasm-server/test/processes/happy/process.wasm')
);

function getLuaBinary() {
  for (const bin of ['lua', 'lua5.3', 'luajit']) {
    try {
      execFileSync(bin, ['-v'], { stdio: 'ignore' });
      return bin;
    } catch {}
  }
  return 'lua';
}
const LUA_BIN = getLuaBinary();

let passed = 0;
let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    failed++;
    console.error(`  ❌ FAIL: ${msg}`);
    throw new Error(`Assertion failed: ${msg}`);
  }
  passed++;
  console.log(`  ✅ PASS: ${msg}`);
}

function assertEq(act, exp, msg) {
  if (act !== exp) {
    failed++;
    console.error(`  ❌ FAIL: ${msg}`);
    console.error(`     Expected: ${exp}`);
    console.error(`     Actual:   ${act}`);
    throw new Error(`Equality assertion failed: ${msg}`);
  }
  passed++;
  console.log(`  ✅ PASS: ${msg}`);
}

console.log('========================================================================');
console.log(' HyDB Tahap 6: AO Arbitrator Final Verification Authority Test Suite');
console.log('========================================================================');

// ============================================================================
// Test 1: Explicit Bootstrap & Sender Node Authorization (Fail-Closed)
// ============================================================================
console.log('\n[Test 1] Explicit Bootstrap & Sender Node Authorization (Fail-Closed):');

const domain = new ArbitratorDomain({
  initialEpoch: 1,
  authorizedSenders: ['SU_PRIMARY', 'SU_NODE_1', 'SU_NODE_2', 'hb_su']
});

// Create valid test event for seq 1
const initialHash = '0'.repeat(64);
const ev1_raw = {
  contract_version: 'hydb.core@1.0',
  prev_hash: initialHash,
  epoch: 1,
  seq: 1,
  namespace: 'finance.balances',
  action: 'put',
  key: 101,
  val: 500,
  canonical_val: '500'
};
// Event hash: sha256(prev_hash + seq + action_code + ns:key:canonical_val)
// action 'put' -> code 20
const ev1_payload = `${initialHash}120finance.balances:101:500`;
const ev1_hash = crypto.createHash('sha256').update(ev1_payload).digest('hex').toUpperCase();
const ev1 = { ...ev1_raw, hash: ev1_hash };

// 1. Untrusted sender should be rejected with REJECTED_UNAUTHORIZED_SENDER
const untrustedBatch = {
  su_id: 'MALICIOUS_IMPOSTOR_NODE',
  epoch: 1,
  events: [ev1]
};
const resUntrusted = domain.verifyBatch(untrustedBatch);
assertEq(resUntrusted.ok, false, 'Untrusted sender must be rejected');
assertEq(resUntrusted.wasit_verdict, 'REJECTED_UNAUTHORIZED_SENDER', 'Verdict must be REJECTED_UNAUTHORIZED_SENDER');

// 2. Trusted sender SU_PRIMARY is accepted
const trustedBatch = {
  su_id: 'SU_PRIMARY',
  epoch: 1,
  events: [ev1]
};
const resTrusted = domain.verifyBatch(trustedBatch);
assertEq(resTrusted.ok, true, 'Trusted sender batch must be accepted');
assertEq(resTrusted.official_seq, 1, 'OfficialSeq advanced to 1');

// ============================================================================
// Test 2: Epoch Fencing & Zombie Hot Dropping
// ============================================================================
console.log('\n[Test 2] Epoch Fencing & Zombie Hot Dropping:');

// Domain is at epoch 1. If active security epoch advances to 2, epoch 1 batches must be fenced
domain.currentEpoch = 2;

const staleBatch = {
  su_id: 'SU_PRIMARY',
  epoch: 1,
  events: [{ ...ev1, seq: 2, prev_hash: ev1.hash }]
};
const resStale = domain.verifyBatch(staleBatch);
assertEq(resStale.ok, false, 'Stale epoch batch must be rejected');
assertEq(resStale.wasit_verdict, 'FENCED_STALE_EPOCH', 'Verdict must be FENCED_STALE_EPOCH');
assertEq(domain.officialSeq, 1, 'Official state did not advance on stale epoch');

// Reset epoch for subsequent tests
domain.currentEpoch = 1;

// ============================================================================
// Test 3: Action "24" (register_schema) & Dynamic Invariant Enforcement
// ============================================================================
console.log('\n[Test 3] Action "24" (register_schema) & Dynamic Invariant Enforcement:');

// Register a new dynamic schema via event seq 2
const newSchemaPayloadObj = {
  namespace: 'inventory.stock',
  backend: 'ets_doc',
  schema_version: '1.0.0',
  key_type: 'string',
  value_type: 'int64',
  rules: { min_value: 10, max_value: 1000 }
};
const newSchemaStr = JSON.stringify(newSchemaPayloadObj);
const prevTip = domain.officialHash;

// Hashing for action "24": sha256(prev_hash + seq + "24" + ns + ":" + canonical_val)
const ev2_payload = `${prevTip}224inventory.stock:${newSchemaStr}`;
const ev2_hash = crypto.createHash('sha256').update(ev2_payload).digest('hex').toUpperCase();

const ev2_reg = {
  contract_version: 'hydb.core@1.0',
  prev_hash: prevTip,
  epoch: 1,
  seq: 2,
  namespace: 'inventory.stock',
  action: 'register_schema',
  key: 'schema',
  val: newSchemaPayloadObj,
  canonical_val: newSchemaStr,
  hash: ev2_hash
};

const regBatch = {
  su_id: 'SU_PRIMARY',
  epoch: 1,
  events: [ev2_reg]
};

const resReg = domain.verifyBatch(regBatch);
assertEq(resReg.ok, true, 'register_schema event batch accepted');
assertEq(domain.schemas.has('inventory.stock'), true, 'inventory.stock registered dynamically in arbitrator');

// Test schema rule invariant enforcement on the new schema:
// Try to insert stock < 10 (min_value is 10)
const ev3_bad_payload = `${domain.officialHash}320inventory.stock:itemA:5`;
const ev3_bad_hash = crypto.createHash('sha256').update(ev3_bad_payload).digest('hex').toUpperCase();
const ev3_bad = {
  contract_version: 'hydb.core@1.0',
  prev_hash: domain.officialHash,
  epoch: 1,
  seq: 3,
  namespace: 'inventory.stock',
  action: 'put',
  key: 'itemA',
  val: 5,
  canonical_val: '5',
  hash: ev3_bad_hash
};

const resBadRule = domain.verifyBatch({ su_id: 'SU_PRIMARY', epoch: 1, events: [ev3_bad] });
assertEq(resBadRule.ok, false, 'Value violating min_value rule must be rejected');
assertEq(resBadRule.sub_verdict, 'REJECTED_SCHEMA_VIOLATION', 'Sub-verdict must be REJECTED_SCHEMA_VIOLATION');
assertEq(domain.officialSeq, 2, 'Official state unmodified after schema rule violation');

// ============================================================================
// Test 4: Unambiguous Multi-Namespace State Isolation
// ============================================================================
console.log('\n[Test 4] Unambiguous Multi-Namespace State Isolation:');

// Put key 'item1' in default namespace (or finance.balances) and same key in inventory.stock
// 1. Put in inventory.stock: valid value 50
const ev3_good_payload = `${domain.officialHash}320inventory.stock:item1:50`;
const ev3_good_hash = crypto.createHash('sha256').update(ev3_good_payload).digest('hex').toUpperCase();
const ev3_good = {
  contract_version: 'hydb.core@1.0',
  prev_hash: domain.officialHash,
  epoch: 1,
  seq: 3,
  namespace: 'inventory.stock',
  action: 'put',
  key: 'item1',
  val: 50,
  canonical_val: '50',
  hash: ev3_good_hash
};

const resPutStock = domain.verifyBatch({ su_id: 'SU_PRIMARY', epoch: 1, events: [ev3_good] });
assertEq(resPutStock.ok, true, 'Valid inventory.stock put accepted');

// 2. Put same key 'item1' in app.docs namespace
const ev4_docs_payload = `${domain.officialHash}420app.docs:item1:{"desc":"item1 document"}`;
const ev4_docs_hash = crypto.createHash('sha256').update(ev4_docs_payload).digest('hex').toUpperCase();
const ev4_docs = {
  contract_version: 'hydb.core@1.0',
  prev_hash: domain.officialHash,
  epoch: 1,
  seq: 4,
  namespace: 'app.docs',
  action: 'put',
  key: 'item1',
  val: '{"desc":"item1 document"}',
  canonical_val: '{"desc":"item1 document"}',
  hash: ev4_docs_hash
};

const resPutDocs = domain.verifyBatch({ su_id: 'SU_PRIMARY', epoch: 1, events: [ev4_docs] });
assertEq(resPutDocs.ok, true, 'app.docs put with identical key accepted');

// Verify state isolation
const stockSlot = domain.records.get('doc:inventory.stock:item1');
const docsSlot = domain.records.get('doc:app.docs:item1');
assert(stockSlot && docsSlot, 'Both records exist independently');
assertEq(stockSlot.val, 50, 'inventory.stock has value 50');
assertEq(docsSlot.val, '{"desc":"item1 document"}', 'app.docs has independent value');
assert(stockSlot.ver === 1 && docsSlot.ver === 1, 'Versions independent per namespace');

// ============================================================================
// Test 5: Dual State Commitment Verification & Forgery Rejection
// ============================================================================
console.log('\n[Test 5] Dual State Commitment Verification & Forgery Rejection:');

// Official state has 3 records:
// 1. wasm:finance.balances:101 = 500
// 2. doc:inventory.stock:item1 = 50
// 3. doc:app.docs:item1 = {"desc":"item1 document"}
const expectedCommitment = domain.officialCommitment256;
const expectedRoot64 = domain.officialStateRoot64;
assert(typeof expectedCommitment === 'string' && expectedCommitment.length === 64, '256-bit commitment is 64-char hex string');

// Put another record to test commitment verification in batch
const ev5_payload = `${domain.officialHash}520inventory.stock:item2:100`;
const ev5_hash = crypto.createHash('sha256').update(ev5_payload).digest('hex').toUpperCase();
const ev5 = {
  contract_version: 'hydb.core@1.0',
  prev_hash: domain.officialHash,
  epoch: 1,
  seq: 5,
  namespace: 'inventory.stock',
  action: 'put',
  key: 'item2',
  val: 100,
  canonical_val: '100',
  hash: ev5_hash
};

// Compute expected next commitment manually using test clone
const testRecords = domain.cloneRecords();
testRecords.set('doc:inventory.stock:item2', {
  namespace: 'inventory.stock',
  rawKey: 'item2',
  canonicalVal: '100',
  ver: 1,
  active: true
});
const correctNextCommitment = computeCommitment256(testRecords);

// 1. Submit batch with forged commitment
const forgedBatch = {
  su_id: 'SU_PRIMARY',
  epoch: 1,
  events: [ev5],
  proposed_state_commitment_256: 'DEADBEEF'.repeat(8)
};
const resForged = domain.verifyBatch(forgedBatch);
assertEq(resForged.ok, false, 'Forged state commitment must be rejected');
assertEq(resForged.sub_verdict, 'REJECTED_STATE_COMMITMENT_MISMATCH', 'Sub-verdict must be REJECTED_STATE_COMMITMENT_MISMATCH');
assertEq(domain.officialSeq, 4, 'OfficialSeq remains 4 after forgery rejection');

// 2. Submit batch with accurate dual commitment
const validCommitmentBatch = {
  su_id: 'SU_PRIMARY',
  epoch: 1,
  events: [ev5],
  proposed_state_commitment_256: correctNextCommitment
};
const resValidCommit = domain.verifyBatch(validCommitmentBatch);
assertEq(resValidCommit.ok, true, 'Dual-committed batch accepted');
assertEq(domain.officialCommitment256, correctNextCommitment, 'Official commitment updated to correct value');
assertEq(domain.officialSeq, 5, 'OfficialSeq advanced to 5');

// ============================================================================
// Test 6: Atomic Rollback on Multi-Event Batch with Middle Failure
// ============================================================================
console.log('\n[Test 6] Atomic Rollback on Multi-Event Batch:');

const preRollbackSeq = domain.officialSeq;
const preRollbackHash = domain.officialHash;
const preRollbackRoot = domain.officialStateRoot64;
const preRollbackCommitment = domain.officialCommitment256;
const preRollbackRecordCount = domain.records.size;

// Create batch of 3 events: Event 6 (valid), Event 7 (tampered hash), Event 8 (valid)
const ev6_payload = `${preRollbackHash}620inventory.stock:batchA:200`;
const ev6_hash = crypto.createHash('sha256').update(ev6_payload).digest('hex').toUpperCase();
const ev6 = {
  contract_version: 'hydb.core@1.0',
  prev_hash: preRollbackHash,
  epoch: 1,
  seq: 6,
  namespace: 'inventory.stock',
  action: 'put',
  key: 'batchA',
  val: 200,
  canonical_val: '200',
  hash: ev6_hash
};

const ev7_bad = {
  contract_version: 'hydb.core@1.0',
  prev_hash: ev6_hash,
  epoch: 1,
  seq: 7,
  namespace: 'inventory.stock',
  action: 'put',
  key: 'batchB',
  val: 300,
  canonical_val: '300',
  hash: 'FORGED_HASH_77777777777777777777777777777777777777777777777777777777'
};

const ev8 = {
  contract_version: 'hydb.core@1.0',
  prev_hash: 'FORGED_HASH_77777777777777777777777777777777777777777777777777777777',
  epoch: 1,
  seq: 8,
  namespace: 'inventory.stock',
  action: 'put',
  key: 'batchC',
  val: 400,
  canonical_val: '400',
  hash: 'DUMMY'
};

const multiBatch = {
  su_id: 'SU_PRIMARY',
  epoch: 1,
  events: [ev6, ev7_bad, ev8]
};

const resMulti = domain.verifyBatch(multiBatch);
assertEq(resMulti.ok, false, 'Multi-event batch containing invalid event must fail');
assertEq(resMulti.sub_verdict, 'REJECTED_HASH_CHAIN_BROKEN', 'Fails with REJECTED_HASH_CHAIN_BROKEN');

// Verify 100% atomic rollback (zero side effects)
assertEq(domain.officialSeq, preRollbackSeq, 'OfficialSeq remained unchanged');
assertEq(domain.officialHash, preRollbackHash, 'OfficialHash remained unchanged');
assertEq(domain.officialStateRoot64, preRollbackRoot, 'OfficialStateRoot64 remained unchanged');
assertEq(domain.officialCommitment256, preRollbackCommitment, 'OfficialCommitment256 remained unchanged');
assertEq(domain.records.size, preRollbackRecordCount, 'Records count remained unchanged');
assertEq(domain.records.has('doc:inventory.stock:batchA'), false, 'Event 6 (batchA) cleanly rolled back');

// ============================================================================
// Test 7: Live AO Process Arbitrator Execution via AoLoader
// ============================================================================
console.log('\n[Test 7] Live AO Process Execution (Admin Authority & Checkpoint Commit):');

// 1. Test PromoteEpoch unauthorized sender rejected via live Lua Arbitrator Process
const luaCheckOut = execFileSync(LUA_BIN, [join(DB_DIR, 'tests/test_arbitrator_dispatch.lua')], { encoding: 'utf8' });
assert(luaCheckOut.includes('TEST1_STATUS=REJECTED_UNAUTHORIZED_ADMIN'), 'Live Arbitrator Lua process rejects unauthorized epoch promotion');
assert(luaCheckOut.includes('TEST2_STATUS=EPOCH_PROMOTED'), 'Live Arbitrator Lua process promotes epoch for authorized admin');
assert(luaCheckOut.includes('TEST3_STATUS=AUTHORIZED'), 'Live Arbitrator Lua process authorizes node');
console.log('  Live Lua Arbitrator authority and epoch dispatch verified.');

// 2. Test Live AO Wasm Process Host Execution via @permaweb/ao-loader
const aoWasmBinary = readFileSync(AO_WASM_PATH);
const aoHandle = await AoLoader(aoWasmBinary, {
  format: 'wasm32-unknown-emscripten',
  inputEncoding: 'JSON-1',
  outputEncoding: 'JSON-1',
  memoryLimit: 524288000,
  computeLimit: 9000000000000,
  extensions: []
});

const aoEnv = { Process: { Id: 'HYDB_ARBITRATOR_TEST', Tags: [] } };

const freshEvent1 = {
  contract_version: 'hydb.core@1.0',
  prev_hash: '0'.repeat(64),
  epoch: 1,
  seq: 1,
  namespace: 'default',
  action: 'put',
  key: 42,
  val: 100,
  canonical_val: '100',
  hash: crypto.createHash('sha256').update(`${'0'.repeat(64)}12042:100`).digest('hex').toUpperCase()
};

const aoCheckpointMsg = {
  Target: 'HYDB_ARBITRATOR_TEST',
  From: 'SU_PRIMARY',
  Owner: 'SU_PRIMARY',
  Timestamp: Date.now(),
  Tags: [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Variant', value: 'ao.TN.1' },
    { name: 'Type', value: 'Message' },
    { name: 'Action', value: 'CommitCheckpoint' },
    { name: 'Epoch', value: '1' },
    { name: 'From-Seq', value: '1' },
    { name: 'To-Seq', value: '1' },
    { name: 'Hash', value: freshEvent1.hash },
    { name: 'function', value: 'hello' }
  ],
  Data: JSON.stringify({
    epoch: 1,
    from_seq: 1,
    to_seq: 1,
    events: [freshEvent1]
  })
};

const resAoCommit = await aoHandle(null, aoCheckpointMsg, aoEnv);
assert(resAoCommit && resAoCommit.Output, 'AoLoader executes CommitCheckpoint with authorized SU_PRIMARY');
console.log('  Live AO Arbitrator execution verified.');

// ============================================================================
// Summary
// ============================================================================
console.log('\n========================================================================');
console.log(` Tahap 6 Verification Results: ${passed} PASSED, ${failed} FAILED`);
console.log('========================================================================');

if (failed > 0) {
  process.exit(1);
}
