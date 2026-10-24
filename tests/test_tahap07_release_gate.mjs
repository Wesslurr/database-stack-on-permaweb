/**
 * Phase 07 Release Gate & Documentation Synchronization Audit
 *
 * Verifies:
 * 1. Baseline commit (83c76f9) and protocol version (hydb.core@1.0 / hydb.spec/1) are accurately documented; stale baseline ccce578 is absent.
 * 2. Every guarantee is explicitly classified as Implemented, Tested, or Proven under model.
 * 3. Quorum assumptions, crash recovery & fail-closed WAL behavior, durability boundaries, timeout/idempotency rules, and AO settlement status meanings are documented.
 * 4. Unqualified absolute claims ("Zero Data Loss", "Irreversible Finality", "Cannot be reverted") are eliminated from guarantee tables and replaced with explicit scope boundaries.
 * 5. All test files and implementation files referenced in the Guarantee & Verification Matrix exist on disk and are wired into package.json and .github/workflows/ci.yml.
 * 6. Production runtime files contain no fallback to default cluster secrets.
 */

import { readFileSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, '..');

let passed = 0;
let failed = 0;

function assert(cond, msg) {
  if (cond) {
    passed++;
    console.log(`  ✓ ${msg}`);
  } else {
    failed++;
    console.error(`  ✗ FAIL: ${msg}`);
  }
}

console.log('\n================================================================');
console.log('  PHASE 07: DOCUMENTATION SYNCHRONIZATION & RELEASE GATE AUDIT');
console.log('================================================================\n');

const protocolDocPath = resolve(ROOT, 'docs/PROTOCOL_AND_FAILURE_MODEL.md');
const readmePath = resolve(ROOT, 'README.md');
const ciWorkflowPath = resolve(ROOT, '.github/workflows/ci.yml');
const pkgJsonPath = resolve(ROOT, 'package.json');

assert(existsSync(protocolDocPath), 'docs/PROTOCOL_AND_FAILURE_MODEL.md exists');
assert(existsSync(readmePath), 'README.md exists');
assert(existsSync(ciWorkflowPath), '.github/workflows/ci.yml exists');
assert(existsSync(pkgJsonPath), 'package.json exists');

const protocolDoc = readFileSync(protocolDocPath, 'utf8');
const readmeDoc = readFileSync(readmePath, 'utf8');
const ciWorkflow = readFileSync(ciWorkflowPath, 'utf8');
const pkgJson = JSON.parse(readFileSync(pkgJsonPath, 'utf8'));

// ---------------------------------------------------------------------------
// 1. Baseline Commit & Protocol Versioning
// ---------------------------------------------------------------------------
console.log('[1] Checking Baseline Commit & Protocol Version...');
assert(protocolDoc.includes('83c76f9'), 'PROTOCOL_AND_FAILURE_MODEL.md references true baseline commit 83c76f9');
assert(!protocolDoc.includes('ccce578'), 'PROTOCOL_AND_FAILURE_MODEL.md does NOT reference stale commit ccce578');
assert(readmeDoc.includes('83c76f9'), 'README.md references true baseline commit 83c76f9');
assert(!readmeDoc.includes('ccce578'), 'README.md does NOT reference stale commit ccce578');
assert(
  protocolDoc.includes('hydb.core@1.0') && protocolDoc.includes('hydb.spec/1'),
  'PROTOCOL_AND_FAILURE_MODEL.md specifies protocol versions hydb.core@1.0 and hydb.spec/1'
);

// ---------------------------------------------------------------------------
// 2. Guarantee Classification Labels (Implemented, Tested, Proven under model)
// ---------------------------------------------------------------------------
console.log('\n[2] Checking Guarantee Classification Labels...');
assert(protocolDoc.includes('`Implemented`'), 'Defines Implemented guarantee classification');
assert(protocolDoc.includes('`Tested`'), 'Defines Tested guarantee classification');
assert(protocolDoc.includes('`Proven under model`'), 'Defines Proven under model guarantee classification');
assert(readmeDoc.includes('`Implemented`') && readmeDoc.includes('`Tested`') && readmeDoc.includes('`Proven under model`'),
  'README.md includes Implemented / Tested / Proven under model classifications');

// ---------------------------------------------------------------------------
// 3. Required Technical Semantics & Failure Model Documentation
// ---------------------------------------------------------------------------
console.log('\n[3] Checking Quorum, Crash Recovery, Durability, Timeout & AO Status Documentation...');
assert(
  protocolDoc.includes('HYDB_CLUSTER_SIZE') && protocolDoc.includes('HYDB_CLUSTER_MEMBERS'),
  'Documents fixed cluster membership configuration (HYDB_CLUSTER_SIZE / HYDB_CLUSTER_MEMBERS)'
);
assert(
  protocolDoc.includes('ERR_DURABILITY_UNSATISFIED'),
  'Documents fail-closed durability rejection (ERR_DURABILITY_UNSATISFIED) when quorum is not met'
);
assert(
  protocolDoc.includes('degraded') && protocolDoc.includes('ERR_WAL_CORRUPTED_DEGRADED') && protocolDoc.includes('canonical_hash_64'),
  'Documents fail-closed WAL corruption handling (degraded state, ERR_WAL_CORRUPTED_DEGRADED, dynamic state_root recomputation)'
);
assert(
  protocolDoc.includes('eof_truncated_bytes') && protocolDoc.includes('truncate_clean_eof'),
  'Documents distinction between clean EOF crash truncation and mid-file WAL corruption'
);
assert(
  protocolDoc.includes('`accepted`') && protocolDoc.includes('`durable`') && protocolDoc.includes('`unknown`') && protocolDoc.includes('provisional'),
  'Documents write statuses (accepted, durable, unknown) and provisional visibility rules'
);
assert(
  protocolDoc.includes('request_id') && protocolDoc.includes('/request_status') && protocolDoc.includes('ERR_IDEMPOTENCY_KEY_CONFLICT'),
  'Documents request_id idempotency, /request_status lookup after timeout, and ERR_IDEMPOTENCY_KEY_CONFLICT'
);
assert(
  protocolDoc.includes('`submitted`') &&
  protocolDoc.includes('`rejected`') &&
  protocolDoc.includes('`verified`') &&
  protocolDoc.includes('`finalized`') &&
  protocolDoc.includes('arlocal_local') &&
  protocolDoc.includes('ao_network'),
  'Documents AO settlement statuses (submitted, rejected, verified, finalized) and arlocal_local vs ao_network distinction'
);
assert(
  protocolDoc.includes('committer_cursor.json') && protocolDoc.includes('DUPLICATE_BATCH_IDEMPOTENT'),
  'Documents durable committer cursor persistence and idempotent batch retry handling'
);

// ---------------------------------------------------------------------------
// 4. Removal of Unqualified Absolute Claims
// ---------------------------------------------------------------------------
console.log('\n[4] Checking Removal of Unqualified Absolute Claims...');
const lines = protocolDoc.split('\n');
const unqualifiedZeroDataLossLines = lines.filter(
  (l) => /zero data loss/i.test(l) && !/no unqualified/i.test(l)
);
const unqualifiedIrreversibleLines = lines.filter(
  (l) => (/irreversible finality/i.test(l) || /cannot be reverted/i.test(l)) && !/no unqualified/i.test(l)
);
assert(
  unqualifiedZeroDataLossLines.length === 0,
  'No unqualified "Zero Data Loss" claims in PROTOCOL_AND_FAILURE_MODEL.md'
);
assert(
  unqualifiedIrreversibleLines.length === 0,
  'No unqualified "Irreversible Finality / Cannot be reverted" claims in PROTOCOL_AND_FAILURE_MODEL.md'
);
assert(
  !/zero data loss/i.test(readmeDoc) && !/cannot be reverted/i.test(readmeDoc),
  'No unqualified "Zero Data Loss" or "Cannot be reverted" claims in README.md'
);

// ---------------------------------------------------------------------------
// 5. Traceability of Referenced Files, NPM Scripts & CI Steps
// ---------------------------------------------------------------------------
console.log('\n[5] Checking Referenced Files, NPM Scripts & CI Workflow Alignment...');
const requiredFiles = [
  '.github/workflows/ci.yml',
  'package.json',
  'sd/src/sd_runtime.erl',
  'hymessage/src/hymessage_runtime.erl',
  'spec/hydb_contract_spec.mjs',
  'spec/golden_vectors.json',
  'hydb/src/lib.rs',
  'ao_process/arbitrator.lua',
  'ao_process/committer.mjs',
  'ao_process/committer_daemon.mjs',
  'tests/chaos_node_daemon.escript',
  'tests/contract_conformance.mjs',
  'tests/test_tahap01_security.mjs',
  'tests/test_tahap02_wal_recovery.mjs',
  'tests/test_tahap03_quorum_hash.mjs',
  'tests/test_tahap04_idempotency.mjs',
  'tests/fault_injection_chaos.mjs',
  'tests/test_tahap06_ao_committer.mjs',
  'tests/test_tahap07_release_gate.mjs'
];

for (const relPath of requiredFiles) {
  assert(existsSync(resolve(ROOT, relPath)), `Required artifact exists on disk: ${relPath}`);
}

const requiredScripts = [
  'test:security',
  'test:recovery',
  'test:quorum',
  'test:idempotency',
  'test:chaos',
  'test:committer',
  'test:release-gate',
  'test:conformance',
  'test:ao',
  'test:sdk',
  'test:business'
];

for (const scriptName of requiredScripts) {
  assert(Boolean(pkgJson.scripts?.[scriptName]), `package.json defines script "${scriptName}"`);
  assert(ciWorkflow.includes(`npm run ${scriptName}`), `.github/workflows/ci.yml executes "npm run ${scriptName}"`);
}

assert(
  ciWorkflow.includes('run: npm ci') && !ciWorkflow.includes('npm ci || npm install'),
  '.github/workflows/ci.yml enforces strict reproducible "npm ci" without fallback'
);

// ---------------------------------------------------------------------------
// 6. Production Source Code Release Gate (No Default Secret Fallback)
// ---------------------------------------------------------------------------
console.log('\n[6] Checking Production Source Code Release Gate Invariants...');
const hymessageSrc = readFileSync(resolve(ROOT, 'hymessage/src/hymessage_runtime.erl'), 'utf8');
const sdSrc = readFileSync(resolve(ROOT, 'sd/src/sd_runtime.erl'), 'utf8');
const committerSrc = readFileSync(resolve(ROOT, 'ao_process/committer.mjs'), 'utf8');

const forbiddenFallbackToken = ['HYDB', 'DEFAULT', 'CLUSTER', 'SECRET'].join('_');
assert(
  !hymessageSrc.includes(forbiddenFallbackToken) && !sdSrc.includes(forbiddenFallbackToken),
  'Neither hymessage_runtime.erl nor sd_runtime.erl contains default cluster secret fallback'
);
assert(
  committerSrc.includes('settlement_status') && committerSrc.includes('verified') && committerSrc.includes('finalized'),
  'ao_process/committer.mjs enforces explicit settlement_status, verified, and finalized fields'
);

console.log('\n================================================================');
console.log(`  PHASE 07 RELEASE GATE SUMMARY: ${passed} passed, ${failed} failed`);
console.log('================================================================\n');

if (failed > 0) {
  process.exit(1);
}
