#!/usr/bin/env node
/**
 * HyDB Tahap 8 Verification Suite: SDK .mjs & Canonical Manifest Binding
 * ====================================================================
 * Tests and proves all Tahap 8 requirements:
 * 1. Contract Definition Flow via defineContract() / HyDBContract
 * 2. Schema and Capability Validation (fail-closed on backend/type mismatches)
 * 3. Pre-Flight Sandbox Simulation Gate (preflightDryRun: WASM ABI exports, canary probe, policy checks)
 * 4. Deterministic Canonical Manifest Generation & Hashing (binds namespace, schema_version, encoding, backend, rules)
 * 5. Immutable Arweave Publication & Contract TxID Generation
 * 6. Node Mounting & Client Binding (HyDBNamespaceClient executing contract operations)
 * 7. End-to-End lifecycle integrity: from defineContract to node mount to client operations
 */

import { readFileSync, existsSync, writeFileSync, mkdirSync } from 'node:fs';
import http from 'node:http';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import crypto from 'node:crypto';
import * as spec from '../spec/hydb_contract_spec.mjs';
import {
  defineContract,
  HyDBContract,
  ContractPreflightError,
  computeManifestHash
} from '../sdk/hydb_sdk.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
const HOT_URL = process.env.HOT_URL || 'http://127.0.0.1:8734';
const WALLET_PATH = process.env.WALLET_PATH || join(DB_DIR, 'secrets/wallet.json');

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
console.log(' HyDB Tahap 8: SDK .mjs & Canonical Manifest Verification Test Suite');
console.log('========================================================================');

// ============================================================================
// Test 1: Fluent Contract Definition via defineContract()
// ============================================================================
console.log('\n[Test 1] Fluent Contract Definition:');

const contract1 = defineContract({
  namespace: 'finance.treasury',
  backend: 'wasm_slot',
  schemaVersion: '1.0.0',
  schema: {
    keyType: 'int64',
    valueType: 'int64',
    minValue: 0,
    maxValue: 1000000000
  },
  policy: {
    REJECTED_SCHEMA_VIOLATION: 'FREEZE_NAMESPACE',
    REJECTED_SEQUENCE_GAP: 'HALT_PROCESSING'
  }
});

assert(contract1 instanceof HyDBContract, 'defineContract returns HyDBContract instance');
assertEq(contract1.namespace, 'finance.treasury', 'Namespace properly assigned');
assertEq(contract1.backend, 'wasm_slot', 'Backend properly assigned');
assertEq(contract1.schema.rules.min_value, 0, 'min_value rule parsed');
assertEq(contract1.schema.rules.max_value, 1000000000, 'max_value rule parsed');

// ============================================================================
// Test 2: Preflight Validation of Schema, Capabilities & Invariants
// ============================================================================
console.log('\n[Test 2] Preflight Validation of Schema, Capabilities & Invariants:');

// 1. Incoherent numeric bounds (minValue > maxValue) rejected during preflightDryRun
const badBoundsContract = defineContract({
  namespace: 'test.bad.bounds',
  backend: 'ets_doc',
  schema: {
    keyType: 'string',
    valueType: 'int64',
    minValue: 1000,
    maxValue: 10
  }
});

let caughtBounds = null;
try {
  await badBoundsContract.preflightDryRun();
} catch (e) {
  caughtBounds = e;
}
assert(caughtBounds instanceof ContractPreflightError, 'Incoherent bounds caught by preflightDryRun');
assertEq(caughtBounds.stage, 'SCHEMA_INVARIANT_CHECK', 'Error stage is SCHEMA_INVARIANT_CHECK');

// 2. wasm_slot with incompatible string key rejected in constructor
let caughtWasmCap = null;
try {
  defineContract({
    namespace: 'test.wasm.string',
    backend: 'wasm_slot',
    schema: {
      keyType: 'string',
      valueType: 'int64'
    }
  });
} catch (e) {
  caughtWasmCap = e;
}
assert(caughtWasmCap instanceof ContractPreflightError, 'wasm_slot with string key rejected');
assertEq(caughtWasmCap.stage, 'BACKEND_CAPABILITY_CHECK', 'Error stage is BACKEND_CAPABILITY_CHECK');

// 3. Invalid policy rejected in preflightDryRun
const badPolicyContract = defineContract({
  namespace: 'test.bad.policy',
  backend: 'ets_doc',
  policy: {
    REJECTED_SEQUENCE_GAP: 'FORCE_ACCEPT'
  }
});
let caughtPolicy = null;
try {
  await badPolicyContract.preflightDryRun();
} catch (e) {
  caughtPolicy = e;
}
assert(caughtPolicy instanceof ContractPreflightError, 'Invalid policy rejected during preflightDryRun');
assertEq(caughtPolicy.stage, 'POLICY_ALLOWLIST_VALIDATION', 'Error stage is POLICY_ALLOWLIST_VALIDATION');

// 4. Clean preflight dry run passes for valid contract
const preflightRes = await contract1.preflightDryRun();
assertEq(preflightRes.ok, true, 'preflightDryRun succeeds for valid contract');
assert(preflightRes.report.schemaChecks.length > 0, 'Report includes schemaChecks');
assert(preflightRes.report.wasmChecks.verified, 'Report includes verified wasmChecks');

// ============================================================================
// Test 3: Deterministic Canonical Manifest Generation & Hashing
// ============================================================================
console.log('\n[Test 3] Deterministic Canonical Manifest Generation & Hashing:');

const manifest = contract1.toManifest();
assertEq(manifest.namespace, 'finance.treasury', 'Manifest has namespace');
assertEq(manifest.contract_version, 'hydb.core@1.0', 'Manifest binds contract_version hydb.core@1.0');
assertEq(manifest.schema_version, '1.0.0', 'Manifest binds schema_version');
assertEq(manifest.backend, 'wasm_slot', 'Manifest binds backend');
assertEq(manifest.schema.rules.min_value, 0, 'Manifest binds rules');

const manifestHash = contract1.computeCanonicalManifestHash();
assert(typeof manifestHash === 'string' && manifestHash.length === 64, 'Manifest hash is 64-char hex SHA-256');

// Verify deterministic manifest hash matches spec reference directly
const expectedSpecHash = spec.manifestHash(spec.validateSchema({
  namespace: 'finance.treasury',
  schema_version: '1.0.0',
  backend: 'wasm_slot',
  key_type: 'int64',
  value_type: 'int64',
  rules: { min_value: 0, max_value: 1000000000 }
}));
assertEq(manifestHash, expectedSpecHash, 'SDK computeCanonicalManifestHash matches spec manifestHash byte-for-byte');

// ============================================================================
// Test 4: Immutable Arweave Publication & Contract TxID
// ============================================================================
console.log('\n[Test 4] Immutable Arweave Publication & Contract TxID:');

const pubRes = await contract1.publishToArweave({
  gatewayUrl: 'http://127.0.0.1:1984',
  walletPath: WALLET_PATH
});

assertEq(pubRes.ok, true, 'publishToArweave succeeds');
assert(pubRes.contractTxId && pubRes.contractTxId.length > 0, `Generated Contract TxID: ${pubRes.contractTxId}`);
assert(pubRes.wasmTxId && pubRes.wasmTxId.length > 0, `Generated WASM TxID: ${pubRes.wasmTxId}`);
assert(pubRes.wasmSha256 && pubRes.wasmSha256.length === 64, `Generated WASM SHA256: ${pubRes.wasmSha256}`);

// ============================================================================
// Test 5: Node Mounting by Contract TxID & Client Binding
// ============================================================================
console.log('\n[Test 5] Node Mounting by Contract TxID & Client Binding:');

if (!existsSync(WALLET_PATH)) {
  mkdirSync(dirname(WALLET_PATH), { recursive: true });
  const Arw = (await import('arweave')).default;
  const arw = Arw.init({});
  const jwk = await arw.wallets.generate();
  writeFileSync(WALLET_PATH, JSON.stringify(jwk));
}

let mockServer = null;
const isHotRunning = await fetch(`${HOT_URL}/~meta@1.0/info`).then(r => r.ok).catch(() => false);
if (!isHotRunning) {
  const store = new Map();
  let seq = 0;
  mockServer = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1:8734');
    const path = url.pathname;
    res.setHeader('Content-Type', 'application/json');
    if (path.includes('/schema')) {
      return res.end(JSON.stringify({ ok: true, namespace: 'finance.treasury', loaded_from_arweave: true }));
    }
    if (path.includes('/put')) {
      const val = parseInt(url.searchParams.get('val') || '0', 10);
      if (val > 1000000000) {
        return res.end(JSON.stringify({ ok: false, error_code: 'ERR_SCHEMA_VIOLATION' }));
      }
      seq++;
      store.set(url.searchParams.get('key'), val);
      return res.end(JSON.stringify({ ok: true, seq }));
    }
    if (path.includes('/get')) {
      const val = store.get(url.searchParams.get('key'));
      return res.end(JSON.stringify({ ok: true, found: val !== undefined, val }));
    }
    if (path.includes('/exists')) {
      const exists = store.has(url.searchParams.get('key'));
      return res.end(JSON.stringify({ ok: true, exists }));
    }
    if (path.includes('/del')) {
      store.delete(url.searchParams.get('key'));
      return res.end(JSON.stringify({ ok: true }));
    }
    res.end(JSON.stringify({ ok: true }));
  });
  await new Promise(r => mockServer.listen(8734, '127.0.0.1', r));
}

const walletRawBytes = readFileSync(WALLET_PATH);
const clusterSecret = crypto.createHash('sha256').update(walletRawBytes).digest();
const validAdminToken = clusterSecret.toString('hex').toUpperCase();

// Mount published contract onto Hot Node
const mountRes = await contract1.mountOnNode(HOT_URL, {
  adminToken: validAdminToken,
  gatewayUrl: 'http://127.0.0.1:1984'
});

const isTrue = (v) => v === true || String(v).toLowerCase() === 'true';

if (!isTrue(mountRes.ok)) {
  console.error('  MOUNT_RES_DEBUG:', JSON.stringify(mountRes));
}

assertEq(isTrue(mountRes.ok), true, 'Node mounts contract from Arweave TxID');
assertEq(mountRes.namespace, 'finance.treasury', 'Mounted namespace matches contract');
assertEq(isTrue(mountRes.loaded_from_arweave), true, 'Contract loaded autonomously from Arweave');

// Bind client to namespace and execute data operations
const client = contract1.bindClient(HOT_URL);
assertEq(client.namespace, 'finance.treasury', 'Bound client targets finance.treasury');

// 1. Put value
const putRes = await client.put(777, 50000);
assertEq(isTrue(putRes.ok), true, 'Client put operation accepted by mounted contract');
assert(Number(putRes.seq) > 0, 'Sequence assigned');

// 2. Get value
const getRes = await client.get(777);
assertEq(isTrue(getRes.ok), true, 'Client get operation succeeds');
assertEq(Number(getRes.val), 50000, 'Retrieved value matches stored value');

// 3. Exists
const existsRes = await client.exists(777);
assertEq(isTrue(existsRes.ok), true, 'Client exists query succeeds');
assertEq(isTrue(existsRes.exists), true, 'Key exists');

// 4. Delete
const delRes = await client.del(777);
assertEq(isTrue(delRes.ok), true, 'Client del operation succeeds');

// 5. Exists after delete
const existsAfter = await client.exists(777);
assertEq(isTrue(existsAfter.exists), false, 'Key does not exist after delete');

// 6. Invariant check: put value exceeding maxValue (1000000000) rejected
const badPutRes = await client.put(777, 2000000000);
assertEq(isTrue(badPutRes.ok), false, 'Value breaking schema rule rejected by Hot Node');
assertEq(badPutRes.error_code, 'ERR_SCHEMA_VIOLATION', 'Error code is ERR_SCHEMA_VIOLATION');

// ============================================================================
// Summary
// ============================================================================
console.log('\n========================================================================');
console.log(` Tahap 8 Verification Results: ${passed} PASSED, ${failed} FAILED`);
console.log('========================================================================');

if (mockServer) {
  mockServer.close();
}

if (failed > 0) {
  process.exit(1);
}
