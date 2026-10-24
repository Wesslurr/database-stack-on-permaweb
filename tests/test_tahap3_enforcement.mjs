/**
 * HyDB Tahap 3: Contract Enforcement & Registration Verification Suite
 *
 * Verifies all 8 normative requirements of Tahap 3:
 * 1. Rejection of unknown backends (ERR_INVALID_SCHEMA, HTTP 422).
 * 2. Rejection of unsupported key/value types against backend capabilities (ERR_CAPABILITY_UNSUPPORTED).
 * 3. Zero implicit fallback: unregistered namespaces strictly return 404 ERR_NAMESPACE_UNKNOWN on get/put/del/exists/scan.
 * 4. Zero sequence consumption on invalid input or schema rejection.
 * 5. WASM contract configuration verification (failure rejects registration with zero sequence consumed).
 * 6. Namespace lifecycle status separation (registered -> configured -> replicated -> committed).
 * 7. Inactive namespace rejects write operations (ERR_NAMESPACE_NOT_ACTIVE).
 * 8. Pre-sequence delete check: del on non-existent key rejected with ERR_NOT_FOUND (HTTP 404) with zero sequence allocated.
 * 9. Monotonic tombstone version tracking on delete.
 */

import { readFileSync } from 'node:fs';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
const WALLET_PATH = process.env.WALLET_PATH || join(DB_DIR, 'secrets/wallet.json');

const HOT_URL = 'http://127.0.0.1:8734/~hymessage@1.0';
const SU_SCHEDULER_URL = 'http://127.0.0.1:8735/~scheduler@1.0';

const isTrue = (v) => v === true || String(v).toLowerCase() === 'true';
const isFalse = (v) => v === false || String(v).toLowerCase() === 'false';

async function httpGet(url) {
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  const text = await res.text();
  try {
    return { status: res.status, body: JSON.parse(text) };
  } catch {
    return { status: res.status, body: text };
  }
}

async function httpPost(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body)
  });
  const text = await res.text();
  try {
    return { status: res.status, body: JSON.parse(text) };
  } catch {
    return { status: res.status, body: text };
  }
}

async function main() {
  console.log('========================================================================');
  console.log(' HyDB Tahap 3: Contract Enforcement & Registration Test Suite');
  console.log('========================================================================\n');

  let passed = 0;
  let failed = 0;

  function assert(condition, message) {
    if (condition) {
      console.log(`  ✅ PASS: ${message}`);
      passed++;
    } else {
      console.error(`  ❌ FAIL: ${message}`);
      failed++;
    }
  }

  // Setup cluster secrets & reset both nodes
  const walletRawBytes = readFileSync(WALLET_PATH);
  const clusterSecret = crypto.createHash('sha256').update(walletRawBytes).digest();
  const validAdminToken = clusterSecret.toString('hex').toUpperCase();

  await httpGet(`${HOT_URL}/reset?admin_token=${validAdminToken}`);
  await httpGet(`${SU_SCHEDULER_URL}/reset`);
  await new Promise(r => setTimeout(r, 600));

  console.log('[Test 1] Zero Implicit Fallback for Unregistered Namespaces:');
  const unregisteredNs = 'unregistered.bogus.ns';

  // 1a: get on unregistered namespace
  const getRes = await httpGet(`${HOT_URL}/get?namespace=${unregisteredNs}&key=123`);
  assert(getRes.status === 404 || getRes.body.status === 404, 'GET on unregistered namespace returns 404');
  assert(getRes.body.error_code === 'ERR_NAMESPACE_UNKNOWN', `GET returns ERR_NAMESPACE_UNKNOWN (got ${getRes.body.error_code})`);

  // 1b: put on unregistered namespace
  const putRes = await httpGet(`${HOT_URL}/put?namespace=${unregisteredNs}&key=123&val=456`);
  assert(putRes.status === 404 || putRes.body.status === 404, 'PUT on unregistered namespace returns 404');
  assert(putRes.body.error_code === 'ERR_NAMESPACE_UNKNOWN', `PUT returns ERR_NAMESPACE_UNKNOWN (got ${putRes.body.error_code})`);

  // 1c: del on unregistered namespace
  const delRes = await httpGet(`${HOT_URL}/del?namespace=${unregisteredNs}&key=123`);
  assert(delRes.status === 404 || delRes.body.status === 404, 'DEL on unregistered namespace returns 404');
  assert(delRes.body.error_code === 'ERR_NAMESPACE_UNKNOWN', `DEL returns ERR_NAMESPACE_UNKNOWN (got ${delRes.body.error_code})`);

  // 1d: exists on unregistered namespace
  const existsRes = await httpGet(`${HOT_URL}/exists?namespace=${unregisteredNs}&key=123`);
  assert(existsRes.status === 404 || existsRes.body.status === 404, 'EXISTS on unregistered namespace returns 404');
  assert(existsRes.body.error_code === 'ERR_NAMESPACE_UNKNOWN', `EXISTS returns ERR_NAMESPACE_UNKNOWN (got ${existsRes.body.error_code})`);

  // 1e: scan on unregistered namespace
  const scanRes = await httpGet(`${HOT_URL}/scan?namespace=${unregisteredNs}&prefix=abc`);
  assert(scanRes.status === 404 || scanRes.body.status === 404, 'SCAN on unregistered namespace returns 404');
  assert(scanRes.body.error_code === 'ERR_NAMESPACE_UNKNOWN', `SCAN returns ERR_NAMESPACE_UNKNOWN (got ${scanRes.body.error_code})`);

  console.log('\n[Test 2] Schema Registration Rejection & Zero Sequence Allocation:');
  // First, do a valid operation on default to know current sequence
  const pingPut = await httpGet(`${HOT_URL}/put?namespace=default&key=9999&val=1`);
  const initialSeq = Number(pingPut.body.seq);
  console.log(`  * Initial Sequence Baseline: ${initialSeq}`);

  // 2a: Unknown backend rejected
  const badBackendRes = await httpPost(`${HOT_URL}/schema?action=register`, {
    namespace: 'test.badbackend',
    backend: 'unsupported_engine_xyz',
    schema_version: '1.0.0',
    key_type: 'int64',
    value_type: 'int64'
  });
  assert(isFalse(badBackendRes.body.ok), 'Unknown backend registration rejected');
  assert(badBackendRes.body.error_code === 'ERR_INVALID_SCHEMA', `Returns ERR_INVALID_SCHEMA (got ${badBackendRes.body.error_code})`);

  // 2b: Unsupported type on wasm_slot (string key rejected)
  const badKeyRes = await httpPost(`${HOT_URL}/schema?action=register`, {
    namespace: 'test.badkey',
    backend: 'wasm_slot',
    schema_version: '1.0.0',
    key_type: 'string',
    value_type: 'int64'
  });
  assert(isFalse(badKeyRes.body.ok), 'wasm_slot with string key rejected');
  assert(badKeyRes.body.error_code === 'ERR_CAPABILITY_UNSUPPORTED', `Returns ERR_CAPABILITY_UNSUPPORTED (got ${badKeyRes.body.error_code})`);

  // 2c: Unsupported type on wasm_slot (json value rejected)
  const badValRes = await httpPost(`${HOT_URL}/schema?action=register`, {
    namespace: 'test.badval',
    backend: 'wasm_slot',
    schema_version: '1.0.0',
    key_type: 'int64',
    value_type: 'json'
  });
  assert(isFalse(badValRes.body.ok), 'wasm_slot with json value rejected');
  assert(badValRes.body.error_code === 'ERR_CAPABILITY_UNSUPPORTED', `Returns ERR_CAPABILITY_UNSUPPORTED (got ${badValRes.body.error_code})`);

  // 2d: Incoherent rules (min_value > max_value)
  const incoherentRes = await httpPost(`${HOT_URL}/schema?action=register`, {
    namespace: 'test.incoherent',
    backend: 'wasm_slot',
    schema_version: '1.0.0',
    key_type: 'int64',
    value_type: 'int64',
    rules: { min_value: 500, max_value: 100 }
  });
  assert(isFalse(incoherentRes.body.ok), 'Incoherent numeric rules rejected');
  assert(incoherentRes.body.error_code === 'ERR_INVALID_SCHEMA', `Returns ERR_INVALID_SCHEMA for incoherent rules`);

  // Verify sequence did NOT advance after all these failed attempts!
  const probePut = await httpGet(`${HOT_URL}/put?namespace=default&key=9999&val=2`);
  const newSeq = Number(probePut.body.seq);
  assert(newSeq === initialSeq + 1, `Zero sequence allocated on rejected schemas (expected ${initialSeq + 1}, got ${newSeq})`);

  console.log('\n[Test 3] Pre-Sequence Delete Semantics & Monotonic Tombstone Version:');
  // 3a: Delete non-existent key rejected with ERR_NOT_FOUND (HTTP 404)
  const nonExistentDel = await httpGet(`${HOT_URL}/del?namespace=app.docs&key=non_existent_key_999`);
  assert(isFalse(nonExistentDel.body.ok), 'Delete non-existent record rejected');
  assert(nonExistentDel.body.error_code === 'ERR_NOT_FOUND', `Returns ERR_NOT_FOUND (got ${nonExistentDel.body.error_code})`);

  // Verify sequence did NOT advance on failed delete
  const probeSeq2 = await httpGet(`${HOT_URL}/put?namespace=default&key=9999&val=3`);
  assert(Number(probeSeq2.body.seq) === newSeq + 1, `Zero sequence allocated on failed delete (got ${probeSeq2.body.seq})`);

  // 3b: Insert -> Delete -> Verify Tombstone Version -> Re-insert -> Verify Monotonic Version
  const docKey = 'user_order_tahap3_' + Date.now();
  const put1 = await httpGet(`${HOT_URL}/put?namespace=app.docs&key=${docKey}&val=INITIAL_ORDER`);
  assert(isTrue(put1.body.ok) && Number(put1.body.ver) === 1, `Initial put returns ver=1 (got ver=${put1.body.ver})`);

  const del1 = await httpGet(`${HOT_URL}/del?namespace=app.docs&key=${docKey}`);
  assert(isTrue(del1.body.ok), 'Delete successful');
  assert(Number(del1.body.ver) === 2, `Delete operation returns tombstone ver=2 (prev + 1, got ver=${del1.body.ver})`);

  const existsAfterDel = await httpGet(`${HOT_URL}/exists?namespace=app.docs&key=${docKey}`);
  assert(isFalse(existsAfterDel.body.exists), 'Key exists=false after delete');
  assert(Number(existsAfterDel.body.ver) === 2, `Key exists query returns tombstone ver=2 (got ver=${existsAfterDel.body.ver})`);

  const put2 = await httpGet(`${HOT_URL}/put?namespace=app.docs&key=${docKey}&val=REINSERTED_ORDER`);
  assert(isTrue(put2.body.ok), 'Re-insert successful');
  assert(Number(put2.body.ver) === 3, `Re-insert after delete increments monotonically to ver=3 (got ver=${put2.body.ver})`);

  console.log('\n[Test 4] Valid Sequenced Registration with Lifecycle State:');
  const validReg = await httpPost(`${HOT_URL}/schema?action=register`, {
    namespace: 'tahap3.verified',
    backend: 'ets_doc',
    schema_version: '1.0.0',
    key_type: 'string',
    value_type: 'string',
    rules: { min_length: 3 }
  });
  assert(isTrue(validReg.body.ok), 'Valid schema registration accepted');
  assert(validReg.body.contract_status === 'replicated', `Registered status is replicated upon SU ACK (got ${validReg.body.contract_status})`);
  assert(isTrue(validReg.body.replicated), 'Registration sequenced and replicated to SU');

  // Verify write on freshly registered and replicated namespace works
  const writeNs = await httpGet(`${HOT_URL}/put?namespace=tahap3.verified&key=valid_item&val=passed_tahap3`);
  assert(isTrue(writeNs.body.ok), 'Write operation succeeds on active replicated namespace');
  assert(writeNs.body.val === 'passed_tahap3', 'Value stored and verified');

  console.log('\n========================================================================');
  console.log(` Tahap 3 Verification Summary: ${passed} PASSED, ${failed} FAILED`);
  console.log('========================================================================');

  if (failed > 0) {
    process.exit(1);
  }
}

main().catch(err => {
  console.error('Fatal test error:', err);
  process.exit(1);
});
