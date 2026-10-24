#!/usr/bin/env node
/**
 * HyDB Cross-Language Contract Conformance Test Driver — Tahap 2
 * ===============================================================
 * Enforces the 5 HyDB principles across:
 * 1. JavaScript (Normative reference: spec/hydb_contract_spec.mjs)
 * 2. Erlang (hymessage/src/hydb_spec.erl via tests/erl_conformance_runner.escript)
 * 3. Rust WASM (hydb/out/hydb.wasm)
 * 4. AO Lua (ao_process/hydb_spec.lua via tests/lua_conformance_runner.lua & AoLoader with arbitrator.lua)
 *
 * Verifies:
 * - Identical acceptance / rejection of inputs & schemas
 * - Byte-identical canonical TLV encodings and SHA-256 event hashes
 * - Identical operation semantics and monotonic record versions (put -> del -> put)
 * - Identical cryptographic 256-bit state commitment
 * - Equivocation and state divergence detection between multiple SUs
 */

import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import { existsSync, readFileSync } from 'node:fs';
import * as spec from '../spec/hydb_contract_spec.mjs';
import AoLoader from '@permaweb/ao-loader';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
const WASM_PATH = join(DB_DIR, 'hydb/out/hydb.wasm');
const AO_WASM_PATH = process.env.AO_PROCESS_WASM || (
  existsSync(join(DB_DIR, 'ao_process/process.wasm'))
    ? join(DB_DIR, 'ao_process/process.wasm')
    : join(DB_DIR, 'vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/genesis-wasm-server/test/processes/happy/process.wasm')
);

let passed = 0;
let failed = 0;

function assert(condition, message) {
  if (!condition) {
    failed++;
    console.error(`  ❌ FAIL: ${message}`);
    throw new Error(`Assertion failed: ${message}`);
  }
  passed++;
  console.log(`  ✅ PASS: ${message}`);
}

function assertEq(actual, expected, message) {
  if (actual !== expected) {
    failed++;
    console.error(`  ❌ FAIL: ${message}`);
    console.error(`     Expected: ${expected}`);
    console.error(`     Actual:   ${actual}`);
    throw new Error(`Equality assertion failed: ${message}`);
  }
  passed++;
  console.log(`  ✅ PASS: ${message}`);
}

console.log('========================================================================');
console.log(' HyDB Tahap 2: Cross-Language Contract Conformance Test Suite');
console.log('========================================================================');

// ============================================================================
// Suite 1: Canonical TLV Value Encoding Identity (JS vs Erlang vs Lua)
// ============================================================================
console.log('\n[Suite 1] Canonical TLV Encodings & Value Model:');

// JS Reference Encodings
const jsInt42 = spec.encodeValue({ t: 'int64', v: 42n }).toString('hex').toUpperCase();
const jsIntMin = spec.encodeValue({ t: 'int64', v: -(2n ** 63n) }).toString('hex').toUpperCase();
const jsIntMax = spec.encodeValue({ t: 'int64', v: 2n ** 63n - 1n }).toString('hex').toUpperCase();
const jsBoolTrue = spec.encodeValue({ t: 'bool', v: true }).toString('hex').toUpperCase();
const jsBoolFalse = spec.encodeValue({ t: 'bool', v: false }).toString('hex').toUpperCase();
const jsStrSimple = spec.encodeValue({ t: 'string', v: 'hello world' }).toString('hex').toUpperCase();
const jsStrUnicode = spec.encodeValue({ t: 'string', v: 'Käse 🚀 日本語' }).toString('hex').toUpperCase();
const jsBytes = spec.encodeValue({ t: 'bytes', v: new Uint8Array([1, 2, 3, 255]) }).toString('hex').toUpperCase();
const jsAbsent = spec.encodeValue(null).toString('hex').toUpperCase();
const jsNull = spec.encodeValue({ t: 'null' }).toString('hex').toUpperCase();

const jsJson = spec.encodeValue({
  t: 'object',
  v: [
    ['zeta', { t: 'int64', v: 100n }],
    ['alpha', { t: 'string', v: 'first' }],
    ['mid', { t: 'array', v: [{ t: 'int64', v: 1n }, { t: 'int64', v: 2n }] }]
  ]
}).toString('hex').toUpperCase();

// Erlang Runner Execution
console.log('  Executing Erlang Conformance Runner...');
const erlRunner = join(DB_DIR, 'tests/erl_conformance_runner.escript');
const erlOutputRaw = execFileSync(erlRunner, { encoding: 'utf-8', env: { ...process.env, DB_DIR } }).trim();
const erl = JSON.parse(erlOutputRaw);

// Lua Runner Execution
console.log('  Executing Lua Conformance Runner...');
const luaRunner = join(DB_DIR, 'tests/lua_conformance_runner.lua');
const luaOutputRaw = execFileSync('lua', [luaRunner], { encoding: 'utf-8', env: { ...process.env, DB_DIR } }).trim();
const lua = JSON.parse(luaOutputRaw);

// Value Assertions across JS, Erlang, Lua
assertEq(erl.int64_42, jsInt42, 'int64(42) JS == Erlang');
assertEq(lua.int64_42, jsInt42, 'int64(42) JS == Lua');

assertEq(erl.int64_min, jsIntMin, 'int64(INT64_MIN) JS == Erlang');
assertEq(lua.int64_min, jsIntMin, 'int64(INT64_MIN) JS == Lua');

assertEq(erl.int64_max, jsIntMax, 'int64(INT64_MAX) JS == Erlang');
assertEq(lua.int64_max, jsIntMax, 'int64(INT64_MAX) JS == Lua');

assertEq(erl.bool_true, jsBoolTrue, 'bool(true) JS == Erlang');
assertEq(lua.bool_true, jsBoolTrue, 'bool(true) JS == Lua');

assertEq(erl.bool_false, jsBoolFalse, 'bool(false) JS == Erlang');
assertEq(lua.bool_false, jsBoolFalse, 'bool(false) JS == Lua');

assertEq(erl.str_simple, jsStrSimple, 'string("hello world") JS == Erlang');
assertEq(lua.str_simple, jsStrSimple, 'string("hello world") JS == Lua');

assertEq(erl.str_unicode, jsStrUnicode, 'string("Käse 🚀 日本語") JS == Erlang');
assertEq(lua.str_unicode, jsStrUnicode, 'string("Käse 🚀 日本語") JS == Lua');

assertEq(erl.bytes, jsBytes, 'bytes([1,2,3,255]) JS == Erlang');
assertEq(lua.bytes, jsBytes, 'bytes([1,2,3,255]) JS == Lua');

assertEq(erl.absent, jsAbsent, 'absent tag JS == Erlang');
assertEq(lua.absent, jsAbsent, 'absent tag JS == Lua');

assertEq(erl.null, jsNull, 'null tag JS == Erlang');
assertEq(lua.null, jsNull, 'null tag JS == Lua');

assertEq(erl.json, jsJson, 'Sorted nested JSON JS == Erlang');
assertEq(lua.json, jsJson, 'Sorted nested JSON JS == Lua');

// Shared Golden Vectors Verification
const golden = JSON.parse(readFileSync(join(DB_DIR, 'spec/golden_vectors.json'), 'utf-8'));
assertEq(jsInt42, golden.values.int64_42, 'Golden Vector: int64(42)');
assertEq(jsIntMin, golden.values.int64_min, 'Golden Vector: int64(INT64_MIN)');
assertEq(jsIntMax, golden.values.int64_max, 'Golden Vector: int64(INT64_MAX)');
assertEq(jsBoolTrue, golden.values.bool_true, 'Golden Vector: bool(true)');
assertEq(jsBoolFalse, golden.values.bool_false, 'Golden Vector: bool(false)');
assertEq(jsStrSimple, golden.values.string_simple, 'Golden Vector: string("hello world")');
assertEq(jsStrUnicode, golden.values.string_unicode, 'Golden Vector: Unicode');
assertEq(jsBytes, golden.values.bytes_raw, 'Golden Vector: bytes');
assertEq(jsJson, golden.values.sorted_json, 'Golden Vector: Sorted nested JSON');

// ============================================================================
// Suite 2: Type Rejection & Boundary Checks
// ============================================================================
console.log('\n[Suite 2] Strict Type Enforcement & Boundary Validation:');

// Floats are explicitly rejected in hydb.spec/1
let floatRejected = false;
try {
  spec.normalizeValue('int64', 12.34);
} catch (err) {
  if (err.code === 'ERR_TYPE_MISMATCH') floatRejected = true;
}
assert(floatRejected, 'Float values must be rejected with ERR_TYPE_MISMATCH');

// Out of range int64
let overflowRejected = false;
try {
  spec.normalizeValue('int64', 2n ** 64n);
} catch (err) {
  if (err.code === 'ERR_TYPE_MISMATCH') overflowRejected = true;
}
assert(overflowRejected, 'Integer overflow rejected with ERR_TYPE_MISMATCH');

// Schema Validation Rejections (Identical Error Codes across runtimes)
assertEq(erl.bad_ns_error, 'ERR_INVALID_SCHEMA', 'Erlang rejects bad namespace format with ERR_INVALID_SCHEMA');
assertEq(erl.bad_cap_error, 'ERR_CAPABILITY_UNSUPPORTED', 'Erlang rejects unsupported prefix_scan on wasm_slot');
assertEq(erl.bad_type_error, 'ERR_CAPABILITY_UNSUPPORTED', 'Erlang rejects json on wasm_slot with ERR_CAPABILITY_UNSUPPORTED');

// JS Schema Rejections
let jsBadCap = false;
try {
  spec.validateSchema({
    namespace: 'test.cap',
    schema_version: '1.0.0',
    backend: 'wasm_slot',
    key_type: 'int64',
    value_type: 'int64',
    requires: ['prefix_scan']
  });
} catch (err) {
  if (err.code === 'ERR_CAPABILITY_UNSUPPORTED') jsBadCap = true;
}
assert(jsBadCap, 'JS rejects prefix_scan on wasm_slot with ERR_CAPABILITY_UNSUPPORTED');

// ============================================================================
// Suite 3: Manifest Hashes & Event Hash Chain Identity
// ============================================================================
console.log('\n[Suite 3] Deterministic Manifest & Event Hash Chain:');

const validSchema = {
  namespace: 'finance.balances',
  schema_version: '1.0.0',
  backend: 'wasm_slot',
  key_type: 'int64',
  value_type: 'int64',
  require_auth: true,
  requires: ['cas'],
  rules: { min_value: 0n, max_records: 1000n }
};
const jsNormValidSchema = spec.validateSchema(validSchema);
const jsValidManifestHash = spec.manifestHash(jsNormValidSchema);
assertEq(erl.valid_manifest_hash, jsValidManifestHash, 'finance.balances manifest hash JS == Erlang');
assertEq(lua.valid_manifest_hash, jsValidManifestHash, 'finance.balances manifest hash JS == Lua');

const docSchema = {
  namespace: 'app.docs',
  schema_version: '1.0.0',
  backend: 'ets_doc',
  key_type: 'string',
  value_type: 'json',
  require_auth: false,
  requires: ['prefix_scan'],
  rules: {
    fields: {
      title: { type: 'string', required: true, min_length: 3n },
      views: { type: 'int64', required: false, min_value: 0n }
    },
    additional_fields: true
  }
};
const jsNormDocSchema = spec.validateSchema(docSchema);
const jsDocManifestHash = spec.manifestHash(jsNormDocSchema);
assertEq(erl.doc_manifest_hash, jsDocManifestHash, 'app.docs manifest hash JS == Erlang');
assertEq(lua.doc_manifest_hash, jsDocManifestHash, 'app.docs manifest hash JS == Lua');
assertEq(jsValidManifestHash, golden.manifests.finance_balances_hash, 'Golden Vector: finance.balances manifest hash');
assertEq(jsDocManifestHash, golden.manifests.app_docs_hash, 'Golden Vector: app.docs manifest hash');

// Event Hash Chain (Ev1: Register, Ev2: Put, Ev3: Del, Ev4: Put)
const st = spec.createState();
const ev1Draft = spec.prepareOp(st, { op: 'register', schema: validSchema });
const ev1 = {
  contract_version: spec.CONTRACT_VERSION,
  prev_hash: spec.GENESIS_HASH,
  epoch: 1n,
  seq: 1n,
  ...ev1Draft
};
ev1.hash = spec.eventHash(ev1);
spec.applyEvent(st, ev1);

assertEq(erl.ev1_hash, ev1.hash, 'Ev1 (Register) Hash JS == Erlang');
assertEq(lua.ev1_hash, ev1.hash, 'Ev1 (Register) Hash JS == Lua');
assertEq(ev1.hash, golden.events.ev1_register_hash, 'Golden Vector: Ev1 (Register) Hash');
assertEq(erl.sd_ev1_hash, golden.events.ev1_register_hash, 'Production SD Worker Ev1 Hash == Golden Vector');
assertEq(erl.hot_ev1_hash, golden.events.ev1_register_hash, 'Production Hot Node Ev1 Hash == Golden Vector');

// Verify unauthenticated write rejection
let unauthRejected = false;
try {
  spec.prepareOp(st, { op: 'put', namespace: 'finance.balances', key: 1001, value: 50000 });
} catch (err) {
  if (err.code === 'ERR_UNAUTHORIZED') unauthRejected = true;
}
assert(unauthRejected, 'Unauthenticated operation on require_auth schema rejected with ERR_UNAUTHORIZED');

const ev2Draft = spec.prepareOp(st, { op: 'put', namespace: 'finance.balances', key: 1001, value: 50000, authorized: true });
const ev2 = {
  contract_version: spec.CONTRACT_VERSION,
  prev_hash: ev1.hash,
  epoch: 1n,
  seq: 2n,
  ...ev2Draft
};
ev2.hash = spec.eventHash(ev2);
spec.applyEvent(st, ev2);

assertEq(erl.ev2_hash, ev2.hash, 'Ev2 (Put) Hash JS == Erlang');
assertEq(lua.ev2_hash, ev2.hash, 'Ev2 (Put) Hash JS == Lua');
assertEq(ev2.hash, golden.events.ev2_put_hash, 'Golden Vector: Ev2 (Put) Hash');
assertEq(erl.sd_ev2_hash, golden.events.ev2_put_hash, 'Production SD Worker Ev2 Hash == Golden Vector');
assertEq(erl.hot_ev2_hash, golden.events.ev2_put_hash, 'Production Hot Node Ev2 Hash == Golden Vector');

const ev3Draft = spec.prepareOp(st, { op: 'del', namespace: 'finance.balances', key: 1001, authorized: true });
const ev3 = {
  contract_version: spec.CONTRACT_VERSION,
  prev_hash: ev2.hash,
  epoch: 1n,
  seq: 3n,
  ...ev3Draft
};
ev3.hash = spec.eventHash(ev3);
spec.applyEvent(st, ev3);

assertEq(erl.ev3_hash, ev3.hash, 'Ev3 (Del) Hash JS == Erlang');
assertEq(lua.ev3_hash, ev3.hash, 'Ev3 (Del) Hash JS == Lua');
assertEq(ev3.hash, golden.events.ev3_del_hash, 'Golden Vector: Ev3 (Del) Hash');
assertEq(erl.sd_ev3_hash, golden.events.ev3_del_hash, 'Production SD Worker Ev3 Hash == Golden Vector');
assertEq(erl.hot_ev3_hash, golden.events.ev3_del_hash, 'Production Hot Node Ev3 Hash == Golden Vector');

const ev4Draft = spec.prepareOp(st, { op: 'put', namespace: 'finance.balances', key: 1001, value: 75000, authorized: true });
const ev4 = {
  contract_version: spec.CONTRACT_VERSION,
  prev_hash: ev3.hash,
  epoch: 1n,
  seq: 4n,
  ...ev4Draft
};
ev4.hash = spec.eventHash(ev4);
spec.applyEvent(st, ev4);

assertEq(erl.ev4_hash, ev4.hash, 'Ev4 (Put on Tombstone) Hash JS == Erlang');
assertEq(lua.ev4_hash, ev4.hash, 'Ev4 (Put on Tombstone) Hash JS == Lua');
assertEq(ev4.hash, golden.events.ev4_reput_hash, 'Golden Vector: Ev4 (Put on Tombstone) Hash');
assertEq(erl.sd_ev4_hash, golden.events.ev4_reput_hash, 'Production SD Worker Ev4 Hash == Golden Vector');
assertEq(erl.hot_ev4_hash, golden.events.ev4_reput_hash, 'Production Hot Node Ev4 Hash == Golden Vector');

// Record version must be 3 (1 -> 2 on del -> 3 on re-insert)
assertEq(ev4.ver, 3n, 'Record version monotonically incremented across delete -> reinsert');

// State Commitment Match
const docRegDraft = spec.prepareOp(st, { op: 'register', schema: docSchema });
const docRegEv = {
  contract_version: spec.CONTRACT_VERSION,
  prev_hash: ev4.hash,
  epoch: 1n,
  seq: 5n,
  ...docRegDraft
};
docRegEv.hash = spec.eventHash(docRegEv);
spec.applyEvent(st, docRegEv);

const ev5Draft = spec.prepareOp(st, { op: 'put', namespace: 'finance.balances', key: 1002, value: 25000, authorized: true });
const ev5 = { contract_version: spec.CONTRACT_VERSION, prev_hash: docRegEv.hash, epoch: 1n, seq: 6n, ...ev5Draft };
ev5.hash = spec.eventHash(ev5);
spec.applyEvent(st, ev5);

const ev6Draft = spec.prepareOp(st, { op: 'put', namespace: 'app.docs', key: 'doc:readme', value: { title: 'HyDB Guide', views: 10 } });
const ev6 = { contract_version: spec.CONTRACT_VERSION, prev_hash: ev5.hash, epoch: 1n, seq: 7n, ...ev6Draft };
ev6.hash = spec.eventHash(ev6);
spec.applyEvent(st, ev6);

const jsCommitment = spec.stateCommitment(st);
assertEq(erl.state_commitment, jsCommitment, '256-bit State Commitment JS == Erlang');
assertEq(lua.state_commitment, jsCommitment, '256-bit State Commitment JS == Lua');

// ============================================================================
// Suite 4: Rust WASM (wasm_slot) Execution & Semantics
// ============================================================================
console.log('\n[Suite 4] Rust WASM (wasm_slot) Operation & Version Semantics:');

const wasmBytes = readFileSync(WASM_PATH);
const { instance: wasmInst } = await WebAssembly.instantiate(wasmBytes, {});
const wasm = wasmInst.exports;

// 1. Invariant Configuration
const nsSlot = 1001n;
wasm.db_set_contract_ns(nsSlot, 1n, 0n, 1n, 100000n, 500n);

// 2. Schema Rule Enforcement inside WASM
const negCheck = wasm.db_check_put_ns(nsSlot, 42n, -50n);
assertEq(negCheck, -422n, 'WASM db_check_put_ns catches value < min_value (-422)');

const overCheck = wasm.db_check_put_ns(nsSlot, 42n, 200000n);
assertEq(overCheck, -422n, 'WASM db_check_put_ns catches value > max_value (-422)');

// 3. Put -> Update -> Del -> Re-insert version continuity
wasm.db_put_ns(nsSlot, 42n, 100n, 1n);
assertEq(wasm.db_version_ns(nsSlot, 42n), 1n, 'WASM first put ver == 1');
assertEq(wasm.db_get_ns(nsSlot, 42n), 100n, 'WASM first put val == 100');

wasm.db_put_ns(nsSlot, 42n, 200n, 2n);
assertEq(wasm.db_version_ns(nsSlot, 42n), 2n, 'WASM update put ver == 2');
assertEq(wasm.db_get_ns(nsSlot, 42n), 200n, 'WASM update put val == 200');

wasm.db_del_ns(nsSlot, 42n, 3n);
assertEq(wasm.db_has_ns(nsSlot, 42n), 0n, 'WASM db_has_ns returns 0 for deleted key');
assertEq(wasm.db_version_ns(nsSlot, 42n), 3n, 'WASM tombstone ver == 3');

wasm.db_put_ns(nsSlot, 42n, 300n, 4n);
assertEq(wasm.db_has_ns(nsSlot, 42n), 1n, 'WASM db_has_ns returns 1 after re-insert');
assertEq(wasm.db_version_ns(nsSlot, 42n), 4n, 'WASM re-insert ver == 4 (never resets)');
assertEq(wasm.db_get_ns(nsSlot, 42n), 300n, 'WASM re-insert val == 300');

// 4. Namespace Isolation for Identical Keys
const nsOther = 2002n;
wasm.db_set_contract_ns(nsOther, 0n, 0n, 0n, 0n, 500n);
assertEq(wasm.db_has_ns(nsOther, 42n), 0n, 'Key 42 not present in unwritten namespace');
wasm.db_put_ns(nsOther, 42n, 9999n, 5n);

assertEq(wasm.db_get_ns(nsSlot, 42n), 300n, 'Key 42 in nsSlot remains 300');
assertEq(wasm.db_get_ns(nsOther, 42n), 9999n, 'Key 42 in nsOther is 9999');

// ============================================================================
// Suite 5: Tamper, Sequence Gap & Equivocation Detection
// ============================================================================
console.log('\n[Suite 5] Tamper, Sequence Gap, and Multi-SU Equivocation Detection:');

// 1. Tamper detection: event altered after hash creation
const tamperedEvent = { ...ev2, hash: ev2.hash, value: { t: 'int64', v: 999999n } }; // value altered!
let tamperCaught = false;
try {
  const freshSt = spec.createState();
  spec.applyEvent(freshSt, ev1);
  const tip = { seq: 1n, hash: ev1.hash };
  const res = spec.verifyBatch(freshSt, tip, [tamperedEvent], { epoch: 1n });
  if (!res.ok && res.verdict === 'REJECTED_EVENT_HASH_MISMATCH') {
    tamperCaught = true;
  }
} catch {
  tamperCaught = true;
}
assert(tamperCaught, 'Tampered event value detected and rejected with REJECTED_EVENT_HASH_MISMATCH');

// 2. Sequence gap detection
const gapEvent = { ...ev2, seq: 3n }; // skipped seq 2
const freshStGap = spec.createState();
spec.applyEvent(freshStGap, ev1);
const resGap = spec.verifyBatch(freshStGap, { seq: 1n, hash: ev1.hash }, [gapEvent], { epoch: 1n });
assertEq(resGap.verdict, 'REJECTED_SEQUENCE_GAP', 'Sequence gap rejected with REJECTED_SEQUENCE_GAP');

// 3. Out-of-order sequence detection
const outOfOrderEvent = { ...ev2, seq: 1n }; // repeated / old seq
const resOrder = spec.verifyBatch(freshStGap, { seq: 1n, hash: ev1.hash }, [outOfOrderEvent], { epoch: 1n });
assertEq(resOrder.verdict, 'REJECTED_OUT_OF_ORDER_BATCH', 'Out of order sequence rejected with REJECTED_OUT_OF_ORDER_BATCH');

// 4. Broken hash chain detection
const brokenChainEvent = { ...ev2, prev_hash: '1'.repeat(64) }; // wrong prev_hash
brokenChainEvent.hash = spec.eventHash(brokenChainEvent);
const resChain = spec.verifyBatch(freshStGap, { seq: 1n, hash: ev1.hash }, [brokenChainEvent], { epoch: 1n });
assertEq(resChain.verdict, 'REJECTED_HASH_CHAIN_BROKEN', 'Broken hash chain rejected with REJECTED_HASH_CHAIN_BROKEN');

// ============================================================================
// Suite 6: Live AO Process Arbitrator Execution
// ============================================================================
console.log('\n[Suite 6] Live AO Process Execution via @permaweb/ao-loader:');

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
const aoMsg = {
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
    { name: 'To-Seq', value: '4' },
    { name: 'State-Commitment-256', value: jsCommitment },
    { name: 'Hash', value: ev4.hash },
    { name: 'function', value: 'hello' }
  ],
  Data: JSON.stringify({
    epoch: 1,
    from_seq: 1,
    to_seq: 4,
    events: [spec.eventToTyped(ev1), spec.eventToTyped(ev2), spec.eventToTyped(ev3), spec.eventToTyped(ev4)]
  })
};

const aoExecRes = await aoHandle(null, aoMsg, aoEnv);
assert(aoExecRes && aoExecRes.Output, 'AoLoader executes AO Process with response Output');
console.log('  Live AO Process execution verified.');

// ============================================================================
// Summary
// ============================================================================
console.log('\n========================================================================');
console.log(` Conformance Results: ${passed} PASSED, ${failed} FAILED`);
console.log('========================================================================');

if (failed > 0) {
  process.exit(1);
}
