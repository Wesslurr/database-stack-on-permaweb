#!/usr/bin/env node
/**
 * HyDB Phase 04 Verification Suite: Idempotency & Explicit Write Status Semantics
 * ===============================================================================
 * Verifies:
 * 1. `request_id` / `idempotency_key` support on `/put`, `/del`, and `/schema`.
 * 2. Retries with identical `request_id` do NOT duplicate state mutations or increment `seq`.
 * 3. Reusing `request_id` with different payload is rejected with 409 `ERR_IDEMPOTENCY_KEY_CONFLICT`.
 * 4. Explicit separation of write statuses: `accepted` (provisional in RAM),
 *    `durable` (quorum ACKed by SUs), and `unknown`.
 * 5. Client request status lookup (`request_status_from_req`) for dropped/timed-out responses.
 */

import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';

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
console.log(' HyDB Phase 04: Idempotency & Write Status Semantics Verification');
console.log('========================================================================\n');

const erlRunner = join(DB_DIR, 'tests/erl_idempotency_runner.escript');
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
  erlOut.includes('PASS: Retried PUT returns original seq 1 without incrementing sequence') &&
  erlOut.includes('PASS: Retried PUT returns original ver 1 without double mutation'),
  'Retrying PUT with identical request_id is strictly idempotent (no duplicate seq or version)'
);
assert(
  erlOut.includes('PASS: Idempotency conflict returns ERR_IDEMPOTENCY_KEY_CONFLICT'),
  'Reusing request_id with mismatched payload is rejected with 409 ERR_IDEMPOTENCY_KEY_CONFLICT'
);
assert(
  erlOut.includes("PASS: Without SU ACK, write_status is 'accepted'") &&
  erlOut.includes("PASS: After SU quorum ACK, request status lookup promotes write_status to 'durable'"),
  'Write status explicitly distinguishes accepted (provisional in RAM) vs durable (quorum ACKed)'
);
assert(
  erlOut.includes("PASS: Unknown request_id returns write_status='unknown'"),
  'Client status lookup returns unknown for unrecorded request_id'
);
assert(
  erlOut.includes('PASS: Retried DEL with same request_id succeeds instead of 404'),
  'Retrying DEL with identical request_id returns cached tombstone commit instead of 404 ERR_NOT_FOUND'
);
assert(
  erlOut.includes('PASS: Retried schema registration returns original seq 3'),
  'Retrying schema registration with identical request_id is strictly idempotent'
);
assert(
  erlOut.includes('PASS: Hot Node init([]) restores promoted epoch 3 from disk') &&
  erlOut.includes('PASS: Hot Node init([]) restores seq 4 from hot_wal.log') &&
  erlOut.includes('PASS: Retried PUT after Hot Node restart returns original seq 4 without re-sequencing'),
  'Hot Node production init([]) restores epoch, state, and request_id idempotency across process restarts'
);
assert(
  erlOut.includes('PASS: Missing request_id rejected with ERR_MISSING_REQUEST_ID in production profile') &&
  erlOut.includes('PASS: Idempotency cache size is bounded by HYDB_IDEMPOTENCY_MAX_ENTRIES'),
  'Production profile enforces mandatory request_id and bounds idempotency table memory growth'
);
assert(
  erlOut.includes('ALL_PHASE_04_ERL_TESTS_PASSED'),
  'All Erlang Phase 04 idempotency and write status assertions passed'
);

console.log('\n========================================================================');
console.log(` Phase 04 Verification Results: ${passed} PASSED, ${failed} FAILED`);
console.log('========================================================================');
