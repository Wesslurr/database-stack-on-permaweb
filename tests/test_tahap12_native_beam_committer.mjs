#!/usr/bin/env node
/**
 * HyDB Phase 12: Native In-BEAM AO Committer & Settlement Engine Verification
 * ============================================================================
 * Normative Guarantees Verified:
 * 1. Zero-Node.js in-BEAM Execution:
 *    - Native Erlang Arweave RSA JWK decoding and address derivation.
 *    - In-BEAM batch assembly directly from `sd_runtime` memory/ETS.
 * 2. Cryptographic Proof & Batch Integrity:
 *    - Canonical `compute_batch_id` SHA-256 calculation.
 *    - HMAC-SHA256 batch signing with derived cluster secret.
 * 3. Durable Cursor & Cold-Start Recovery:
 *    - Cursor atomic write and durable reload from disk.
 * 4. Native Erlang HTTP AO Dispatcher:
 *    - In-BEAM `httpc` dispatch to AO Scheduler/Gateway.
 *    - Accurate parsing of `COMMITTED` verdict and sequence progression.
 * 5. Genesis Bootstrap & Fail-Closed Guardrails:
 *    - Rejection on divergent Genesis Hash (`err_genesis_hash_mismatch`).
 *    - Health state machine (`healthy` -> `degraded`).
 */

import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import { execFileSync } from 'node:child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = resolve(__dirname, '..');

let passed = 0;
let failed = 0;

function assert(cond, msg) {
  if (cond) {
    passed++;
    console.log(`  ✅ PASS: ${msg}`);
  } else {
    failed++;
    console.error(`  ❌ FAIL: ${msg}`);
    throw new Error(`Assertion failed: ${msg}`);
  }
}

function runTests() {
  console.log('========================================================================');
  console.log(' HyDB Tahap 12: Native In-BEAM AO Committer & Gateway Verification');
  console.log('========================================================================\n');

  const escriptPath = join(ROOT_DIR, 'tests/erl_committer_runner.escript');
  assert(existsSync(escriptPath), 'tests/erl_committer_runner.escript exists');

  console.log('\n[Suite 1] Executing In-BEAM Committer Verification in Erlang VM:');
  const erlOut = execFileSync('escript', [escriptPath], {
    cwd: ROOT_DIR,
    encoding: 'utf8',
    env: {
      ...process.env,
      DB_DIR: ROOT_DIR,
      HYDB_PROFILE: 'test',
      HYDB_CLUSTER_SECRET: 'super_cluster_secret_min16bytes_ok!',
      HYDB_ADMIN_TOKEN: 'super_admin_token_min16bytes_ok!'
    }
  });

  assert(
    erlOut.includes('PASS: Erlang native Arweave address derivation matches address_su.txt 100%'),
    'Erlang native Arweave address derivation matches address_su.txt 100%'
  );
  assert(
    erlOut.includes('PASS: In-BEAM batch ID and HMAC signature match canonical vector'),
    'In-BEAM batch ID and HMAC signature match canonical vector'
  );
  assert(
    erlOut.includes('PASS: In-BEAM committer initializes with sequence 0'),
    'In-BEAM committer initializes with sequence 0'
  );
  assert(
    erlOut.includes('PASS: Durable disk cursor persisted and reloaded from disk atomically'),
    'Durable disk cursor persisted and reloaded from disk atomically'
  );
  assert(
    erlOut.includes('PASS: Native Erlang HTTP client dispatches CommitCheckpoint and parses COMMITTED verdict'),
    'Native Erlang HTTP client dispatches CommitCheckpoint and parses COMMITTED verdict'
  );
  assert(
    erlOut.includes('PASS: Genesis hash divergence triggers fail-closed error in Erlang committer'),
    'Genesis hash divergence triggers fail-closed error in Erlang committer'
  );
  assert(
    erlOut.includes('PASS: In-BEAM committer status reporting and health state machine verified'),
    'In-BEAM committer status reporting and health state machine verified'
  );
  assert(
    erlOut.includes('ALL TAHAP 12 IN-BEAM COMMITTER TESTS PASSED'),
    'All In-BEAM Committer lifecycle guarantees verified successfully'
  );

  console.log('\n========================================================================');
  console.log(` Tahap 12 Native In-BEAM Committer Results: ${passed} PASSED, ${failed} FAILED`);
  console.log('========================================================================\n');

  if (failed > 0) {
    process.exit(1);
  }
}

try {
  runTests();
} catch (err) {
  console.error('\nFatal Tahap 12 Test Error:', err);
  process.exit(1);
}
