#!/usr/bin/env node
/**
 * HyDB Phase 02 Verification Suite:
 * WAL Recovery & Corrupted Data Fail-Closed Handling
 * ==================================================
 * Tests and verifies:
 * 1. Monotonic sequence, prev_hash, event hash, epoch replay invariants.
 * 2. Dynamic state_root recomputation from validated state (never blindly trust WAL contents).
 * 3. Clear differentiation between clean EOF crash truncation vs mid-file corruption.
 * 4. Immediate transition to degraded status & write refusal on corrupted/inconsistent WAL.
 * 5. Full automated suite for valid WAL, EOF truncation, record tampering, sequence gaps, and root forgery.
 */

import { execFileSync } from 'node:child_process';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';

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

console.log('========================================================================');
console.log(' HyDB Phase 02: WAL Recovery & Corrupted Data Fail-Closed Test Suite');
console.log('========================================================================');

const escriptPath = join(DB_DIR, 'tests/erl_recovery_runner.escript');
const erlOut = execFileSync('escript', [escriptPath], {
  cwd: DB_DIR,
  env: {
    ...process.env,
    DB_DIR,
    WALLET_PATH
  },
  encoding: 'utf8'
});

assert(erlOut.includes('PASS: valid WAL recovers exact 3 entries and matching dynamic state_root'),
  'Valid WAL recovery reconstructs exact state and matches recomputed state_root');

assert(erlOut.includes('PASS: EOF truncation detected cleanly, node recovers up to last valid record and stays healthy'),
  'Crash mid-write (EOF truncation) recovers valid prefix and preserves healthy status');

assert(erlOut.includes('PASS: mid-file corruption puts node into degraded status and stops write readiness'),
  'Mid-file corruption immediately marks node degraded and stops write readiness');

assert(erlOut.includes('PASS: sequence gap halts recovery and sets health to degraded'),
  'Sequence gaps halt recovery and put node into degraded status');

assert(erlOut.includes('PASS: broken hash chain halts recovery and sets health to degraded'),
  'Broken hash chains halt recovery and put node into degraded status');

assert(erlOut.includes('PASS: tampered record value detected via hash mismatch and sets health to degraded'),
  'Tampered record values detected via hash mismatch');

assert(erlOut.includes('PASS: forged state_root detected by dynamic recomputation and sets health to degraded'),
  'Forged state_root in WAL detected by dynamic recomputation and sets health to degraded');

console.log('\n========================================================================');
console.log(` Phase 02 Recovery Verification Results: ${passed} PASSED, ${failed} FAILED`);
console.log('========================================================================\n');

if (failed > 0) {
  process.exit(1);
}
