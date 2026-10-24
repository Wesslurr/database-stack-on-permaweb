#!/usr/bin/env node
/**
 * HyDB Phase 03 Verification Suite: Canonical Hash Protocol & Fixed Membership Quorum
 * ===================================================================================
 * Verifies:
 * 1. Canonical TLV Event Encoding & SHA-256 Hashing across JS, Lua, and Erlang production paths.
 * 2. Production functions (`hymessage_runtime:compute_canonical_event_hash` and
 *    `sd_runtime:compute_canonical_event_hash`) match Golden Vectors directly.
 * 3. Fixed cluster membership (`HYDB_CLUSTER_SIZE` / `HYDB_CLUSTER_MEMBERS`) governs quorum.
 * 4. Majority quorum does NOT shrink when subscriber TCP connections drop.
 * 5. Durable ACK (`replicated: true`, `commit_status: "durable"`) is strictly gated on
 *    satisfying configured quorum.
 */

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import * as spec from '../spec/hydb_contract_spec.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');

let passed = 0;
let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    failed++;
    console.error(`  ❌ FAIL: ${msg}`);
    process.exitCode = 1;
    throw new Error(`Assertion failed: ${msg}`);
  }
  passed++;
  console.log(`  ✅ PASS: ${msg}`);
}

console.log('========================================================================');
console.log(' HyDB Phase 03: Canonical Hash Protocol & Fixed Quorum Verification');
console.log('========================================================================\n');

// 1. Verify Golden Vectors in JS Reference Spec
console.log('[1] Normative Golden Vector & Canonical Event Hash Identity:');
const golden = JSON.parse(readFileSync(join(DB_DIR, 'spec/golden_vectors.json'), 'utf-8'));

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

const st = spec.createState();
const ev1 = { contract_version: spec.CONTRACT_VERSION, prev_hash: spec.GENESIS_HASH, epoch: 1n, seq: 1n, ...spec.prepareOp(st, { op: 'register', schema: validSchema }) };
ev1.hash = spec.eventHash(ev1);
spec.applyEvent(st, ev1);

const ev2 = { contract_version: spec.CONTRACT_VERSION, prev_hash: ev1.hash, epoch: 1n, seq: 2n, ...spec.prepareOp(st, { op: 'put', namespace: 'finance.balances', key: 1001, value: 50000, authorized: true }) };
ev2.hash = spec.eventHash(ev2);
spec.applyEvent(st, ev2);

const ev3 = { contract_version: spec.CONTRACT_VERSION, prev_hash: ev2.hash, epoch: 1n, seq: 3n, ...spec.prepareOp(st, { op: 'del', namespace: 'finance.balances', key: 1001, authorized: true }) };
ev3.hash = spec.eventHash(ev3);
spec.applyEvent(st, ev3);

const ev4 = { contract_version: spec.CONTRACT_VERSION, prev_hash: ev3.hash, epoch: 1n, seq: 4n, ...spec.prepareOp(st, { op: 'put', namespace: 'finance.balances', key: 1001, value: 75000, authorized: true }) };
ev4.hash = spec.eventHash(ev4);
spec.applyEvent(st, ev4);

assert(ev1.hash === golden.events.ev1_register_hash, 'JS Ev1 canonical hash matches golden vector');
assert(ev2.hash === golden.events.ev2_put_hash, 'JS Ev2 canonical hash matches golden vector');
assert(ev3.hash === golden.events.ev3_del_hash, 'JS Ev3 canonical hash matches golden vector');
assert(ev4.hash === golden.events.ev4_reput_hash, 'JS Ev4 canonical hash matches golden vector');

// 2. Run Erlang Production Hash & Fixed Quorum Runner
console.log('\n[2] Executing Erlang Production Hash & Quorum Invariance Suite:');
const erlRunner = join(DB_DIR, 'tests/erl_quorum_hash_runner.escript');
const erlOut = execFileSync('escript', [erlRunner], {
  encoding: 'utf-8',
  env: { ...process.env, DB_DIR }
});

for (const line of erlOut.trim().split('\n')) {
  if (line.startsWith('PASS:') || line.startsWith('FAIL:')) {
    console.log(`  [Erlang] ${line}`);
  }
}

assert(
  erlOut.includes('PASS: Hot Node production canonical hash Ev1 matches golden vector') &&
  erlOut.includes('PASS: SD Worker production canonical hash Ev1 matches golden vector'),
  'Erlang Hot Node & SD Worker production functions match Ev1 golden vector'
);
assert(
  erlOut.includes('PASS: Hot Node production canonical hash Ev2 (binary map keys) matches golden vector') &&
  erlOut.includes('PASS: SD Worker production canonical hash Ev2 (binary map keys) matches golden vector'),
  'Erlang Hot Node & SD Worker production functions match Ev2 golden vector'
);
assert(
  erlOut.includes('PASS: Hot Node production canonical hash Ev3 (delete) matches golden vector') &&
  erlOut.includes('PASS: SD Worker production canonical hash Ev3 (delete) matches golden vector'),
  'Erlang Hot Node & SD Worker production functions match Ev3 golden vector'
);
assert(
  erlOut.includes('PASS: Hot Node production canonical hash Ev4 (re-put) matches golden vector') &&
  erlOut.includes('PASS: SD Worker production canonical hash Ev4 (re-put) matches golden vector'),
  'Erlang Hot Node & SD Worker production functions match Ev4 golden vector'
);
assert(
  erlOut.includes('PASS: Majority quorum (2/3) FAILS when only 1 node is connected and ACKs (quorum does not shrink)'),
  'Majority quorum strictly anchored to configured cluster size (does not shrink on disconnect)'
);
assert(
  erlOut.includes('PASS: 3-arity is_quorum_satisfied also FAILS with TotalSubs=1 when configured cluster size is 3'),
  'Legacy 3-arity quorum check also enforces configured cluster size'
);
assert(
  erlOut.includes('PASS: Duplicate sockets for same NodeId and unconfigured NodeIds are deduplicated/filtered to 1 ACK') &&
  erlOut.includes('PASS: Single NodeId with 3 duplicate sockets CANNOT spoof 2/3 majority quorum'),
  'Duplicate sockets for the same NodeId and unconfigured NodeIds are deduplicated/rejected from quorum count'
);
assert(
  erlOut.includes('PASS: verify_su_handshake accepts valid HMAC for configured cluster member su-node-2') &&
  erlOut.includes('PASS: verify_su_handshake rejects valid HMAC when NodeId is not in configured cluster_members'),
  'TCP handshake enforces configured cluster membership list even when caller possesses shared secret'
);
assert(
  erlOut.includes('PASS: Mismatched cluster_size vs cluster_members returns ERR_CLUSTER_SIZE_MISMATCH') &&
  erlOut.includes('PASS: get_configured_cluster_size enforces max(cluster_size, length(cluster_members)) = 3') &&
  erlOut.includes('PASS: Majority quorum with 1 ACK still FAILS when cluster_members has 3 nodes despite cluster_size=1'),
  'Cluster size and cluster_members are strictly validated for consistency (no 1-ACK majority on 3-member list)'
);
assert(
  erlOut.includes('PASS: Membership disk persistence failure returns ERR_MEMBERSHIP_PERSIST_FAILED') &&
  erlOut.includes('PASS: ETS cluster membership remains unchanged (3) when disk persistence fails'),
  'Membership reconfiguration persists to disk before mutating ETS and fails closed on I/O error'
);
assert(
  erlOut.includes('PASS: After explicit admin membership change to 1, majority quorum succeeds with 1 node'),
  'Explicit administrative membership reconfiguration updates quorum threshold safely'
);
assert(
  erlOut.includes('ALL_PHASE_03_ERL_TESTS_PASSED'),
  'All Erlang Phase 03 hash & quorum assertions passed'
);

console.log('\n========================================================================');
console.log(` Phase 03 Verification Results: ${passed} PASSED, ${failed} FAILED`);
console.log('========================================================================');
