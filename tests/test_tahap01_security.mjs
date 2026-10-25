#!/usr/bin/env node
/**
 * HyDB Phase 01 Security Verification Suite:
 * Control Endpoint Protection & Cluster Secret Hardening
 * ======================================================
 * Tests and verifies:
 * 1. Fatal fail-fast when required cluster secrets are missing (no guessable fallbacks).
 * 2. Elimination of HYDB_DEFAULT_CLUSTER_SECRET across all components.
 * 3. Strict authentication on SD control endpoints (/reset, /fence_epoch, /simulate_restart).
 * 4. Unauthenticated control calls rejected with 401/403 and state/epoch remain unchanged.
 * 5. Production profile blocks /reset with ERR_RESET_DISABLED.
 * 6. Authenticated administrative operations succeed when supplied valid token.
 */

import { execFileSync, spawnSync } from 'node:child_process';
import { readFileSync, existsSync } from 'node:fs';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
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
console.log(' HyDB Phase 01: Control Endpoint & Cluster Secret Security Test Suite');
console.log('========================================================================');

// ============================================================================
// Test 1: Fail-Closed Behavior on Missing Cluster Secret (No Guessable Fallback)
// ============================================================================
console.log('\n[Test 1] Cluster Secret Fail-Fast Invariants:');

// 1. Committer daemon must fail fast if WALLET_PATH points to non-existent file and HYDB_CLUSTER_SECRET is unset
const committerProbeCode = `
import { deriveClusterSecret } from './ao_process/committer.mjs';
try {
  deriveClusterSecret('/tmp/non_existent_wallet_hydb_test.json');
  console.log('UNEXPECTED_SUCCESS');
  process.exit(0);
} catch (err) {
  console.log('CAUGHT_EXPECTED_ERROR: ' + err.message);
  process.exit(42);
}
`;

const probeRes = spawnSync('node', ['--input-type=module', '-e', committerProbeCode], {
  cwd: DB_DIR,
  env: {
    ...process.env,
    WALLET_PATH: '/tmp/non_existent_wallet_hydb_test.json',
    HYDB_CLUSTER_SECRET: ''
  },
  encoding: 'utf8'
});

assertEq(probeRes.status, 42, 'Committer daemon exits with expected error status when secret is missing');
assert(probeRes.stdout.includes('CAUGHT_EXPECTED_ERROR: Fatal: missing cluster secret'), 'Committer daemon emits fatal missing secret error without falling back');

// 2. Committer daemon succeeds derivation when HYDB_CLUSTER_SECRET is explicitly provided
const committerSecretRes = spawnSync('node', ['--input-type=module', '-e', `
import { deriveClusterSecret } from './ao_process/committer.mjs';
const key = deriveClusterSecret('/tmp/non_existent_wallet_hydb_test.json');
console.log('DERIVED_LEN=' + key.length);
`], {
  cwd: DB_DIR,
  env: {
    ...process.env,
    WALLET_PATH: '/tmp/non_existent_wallet_hydb_test.json',
    HYDB_CLUSTER_SECRET: 'test_custom_explicit_cluster_secret_12345'
  },
  encoding: 'utf8'
});

assert(committerSecretRes.stdout.includes('DERIVED_LEN=32'), 'Cluster secret derivation generates 256-bit key from explicit env');

// ============================================================================
// Test 2: Erlang SD Control Endpoints Security Suite (Native BEAM Execution)
// ============================================================================
console.log('\n[Test 2] Native Erlang SD Control Endpoints & Fencing Security:');

const escriptPath = join(DB_DIR, 'tests/erl_security_runner.escript');
const erlOut = execFileSync('escript', [escriptPath], {
  cwd: DB_DIR,
  env: {
    ...process.env,
    DB_DIR,
    WALLET_PATH
  },
  encoding: 'utf8'
});

assert(erlOut.includes('PASS: default profile is fail-closed (production) when profile env vars are unset'), 'Default profile is fail-closed (production) when profile env vars are unset');
assert(erlOut.includes('PASS: production startup rejects missing HYDB_CLUSTER_SECRET without falling back to wallet'), 'Production startup rejects missing HYDB_CLUSTER_SECRET without wallet fallback');
assert(erlOut.includes('PASS: startup rejects HYDB_CLUSTER_SECRET shorter than 16 bytes'), 'Startup rejects HYDB_CLUSTER_SECRET shorter than 16 bytes');
assert(erlOut.includes('PASS: production startup rejects HYDB_ADMIN_TOKEN identical to HYDB_CLUSTER_SECRET'), 'Production startup enforces distinct HYDB_ADMIN_TOKEN and HYDB_CLUSTER_SECRET');
assert(erlOut.includes('PASS: unauthenticated verify_admin_req returns 401'), 'Erlang rejects unauthenticated requests with 401');
assert(erlOut.includes('PASS: forged admin_token and cluster secret rejected with 403'), 'Erlang rejects forged tokens and cluster secrets with 403');
assert(erlOut.includes('PASS: valid admin_token and x-admin-token headers return ok'), 'Erlang accepts valid admin token and x-admin-token header');
assert(erlOut.includes('PASS: valid Authorization: Bearer token returns ok'), 'Erlang accepts Authorization: Bearer header');
assert(erlOut.includes('PASS: unauthenticated fence_epoch rejected and epoch unchanged'), 'Unauthenticated fence_epoch rejected and preserves epoch');
assert(erlOut.includes('PASS: authenticated fence_epoch updated epoch'), 'Authenticated fence_epoch succeeds');
assert(erlOut.includes('PASS: reset and simulate_restart rejected in production profile'), 'Reset and simulate_restart rejected in production profile');
assert(erlOut.includes('PASS: reset succeeds in test profile with valid admin token'), 'Reset succeeds in test profile with valid admin token');

// ============================================================================
// Test 3: No Hardcoded Guessable Secrets in Repo
// ============================================================================
console.log('\n[Test 3] Absence of Guessable Secrets Audit:');

const grepRes = spawnSync('grep', ['-rn', '--exclude-dir=node_modules', '--exclude-dir=vendor', '--exclude-dir=.git', '--exclude=test_tahap01_security.mjs', 'HYDB_DEFAULT_CLUSTER_SECRET', '.'], {
  cwd: DB_DIR,
  encoding: 'utf8'
});

assertEq(grepRes.stdout.trim(), '', 'Zero occurrences of HYDB_DEFAULT_CLUSTER_SECRET remain in codebase');

// ============================================================================
// Summary
// ============================================================================
console.log('\n========================================================================');
console.log(` Phase 01 Security Verification Results: ${passed} PASSED, ${failed} FAILED`);
console.log('========================================================================\n');

if (failed > 0) {
  process.exit(1);
}
