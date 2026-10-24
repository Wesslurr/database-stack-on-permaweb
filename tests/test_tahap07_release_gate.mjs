/**
 * Phase 07 Release Gate: Semantic, Behavioral & Documentation Synchronization Audit
 *
 * Verifies:
 * 1. Baseline commit (83c76f9) and protocol version (hydb.core@1.0 / hydb.spec/1) are accurately documented; stale baseline ccce578 is absent.
 * 2. Every guarantee is explicitly classified as Implemented, Tested, or Cross-Language Golden-Vector Tested (no overclaimed formal model checking).
 * 3. Quorum membership identity, physical WAL EOF truncation repair & fail-closed disk I/O, durable epoch & idempotency across restarts, and AO L1 finality verification are documented.
 * 4. Unqualified absolute claims ("Zero Data Loss", "Irreversible Finality", "Cannot be reverted") are eliminated from guarantee tables and replaced with explicit scope boundaries.
 * 5. Multi-replica docker-compose.yml (3 SD replicas + explicit secrets/profile) and CI Rust->WASM build + golden vector provenance steps are wired.
 * 6. Live Behavioral & Semantic Verification across JS, WASM, AO Committer, and Erlang BEAM runtimes.
 */

import { readFileSync, existsSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { buildGoldenVectors } from '../spec/generate_golden_vectors.mjs';
import { verifyArbitratorEvaluation, verifyArweaveL1Finality } from '../ao_process/committer.mjs';

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
console.log('  PHASE 07: SEMANTIC & BEHAVIORAL RELEASE GATE AUDIT');
console.log('================================================================\n');

const protocolDocPath = resolve(ROOT, 'docs/PROTOCOL_AND_FAILURE_MODEL.md');
const readmePath = resolve(ROOT, 'README.md');
const ciWorkflowPath = resolve(ROOT, '.github/workflows/ci.yml');
const pkgJsonPath = resolve(ROOT, 'package.json');
const composePath = resolve(ROOT, 'docker-compose.yml');
const envExamplePath = resolve(ROOT, '.env.example');

assert(existsSync(protocolDocPath), 'docs/PROTOCOL_AND_FAILURE_MODEL.md exists');
assert(existsSync(readmePath), 'README.md exists');
assert(existsSync(ciWorkflowPath), '.github/workflows/ci.yml exists');
assert(existsSync(pkgJsonPath), 'package.json exists');
assert(existsSync(composePath), 'docker-compose.yml exists');
assert(existsSync(envExamplePath), '.env.example exists');

const protocolDoc = readFileSync(protocolDocPath, 'utf8');
const readmeDoc = readFileSync(readmePath, 'utf8');
const ciWorkflow = readFileSync(ciWorkflowPath, 'utf8');
const pkgJson = JSON.parse(readFileSync(pkgJsonPath, 'utf8'));
const composeDoc = readFileSync(composePath, 'utf8');
const envExampleDoc = readFileSync(envExamplePath, 'utf8');

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
// 2. Guarantee Classification Labels (Implemented, Tested, Cross-Language Golden-Vector Tested)
// ---------------------------------------------------------------------------
console.log('\n[2] Checking Honest Guarantee Classification Labels...');
assert(protocolDoc.includes('`Implemented`'), 'Defines Implemented guarantee classification');
assert(protocolDoc.includes('`Tested`'), 'Defines Tested guarantee classification');
assert(
  protocolDoc.includes('`Cross-Language Golden-Vector Tested`'),
  'Defines Cross-Language Golden-Vector Tested guarantee classification'
);
assert(
  !protocolDoc.includes('Proven under model') && !readmeDoc.includes('Proven under model'),
  'Neither PROTOCOL_AND_FAILURE_MODEL.md nor README.md overclaims formal model checking ("Proven under model")'
);

// ---------------------------------------------------------------------------
// 3. Required Technical Semantics & Failure Model Documentation
// ---------------------------------------------------------------------------
console.log('\n[3] Checking Quorum, Crash Recovery, Durability, Timeout & AO Status Documentation...');
assert(
  protocolDoc.includes('HYDB_CLUSTER_SIZE') &&
    protocolDoc.includes('HYDB_CLUSTER_MEMBERS') &&
    protocolDoc.includes('ERR_NODE_NOT_IN_CLUSTER_MEMBERS'),
  'Documents fixed cluster membership configuration and NodeId handshake verification'
);
assert(
  protocolDoc.includes('ERR_DURABILITY_TIMEOUT'),
  'Documents fail-closed durability rejection (ERR_DURABILITY_TIMEOUT) when quorum is not met'
);
assert(
  protocolDoc.includes('degraded') &&
    protocolDoc.includes('ERR_WAL_CORRUPTED_DEGRADED') &&
    protocolDoc.includes('repair_truncated_wal_file') &&
    protocolDoc.includes('missing_state_root'),
  'Documents physical WAL EOF truncation repair, mandatory state_root check, and fail-closed degraded mode'
);
assert(
  protocolDoc.includes('`accepted`') &&
    protocolDoc.includes('`durable`') &&
    protocolDoc.includes('`unknown`') &&
    protocolDoc.includes('provisional'),
  'Documents write statuses (accepted, durable, unknown) and provisional visibility rules'
);
assert(
  protocolDoc.includes('request_id') &&
    protocolDoc.includes('/request_status') &&
    protocolDoc.includes('ERR_IDEMPOTENCY_KEY_CONFLICT') &&
    protocolDoc.includes('ERR_MISSING_REQUEST_ID'),
  'Documents mandatory production request_id, /request_status lookup, and ERR_IDEMPOTENCY_KEY_CONFLICT'
);
assert(
  protocolDoc.includes('`submitted`') &&
    protocolDoc.includes('`rejected`') &&
    protocolDoc.includes('`verified`') &&
    protocolDoc.includes('`finalized`') &&
    protocolDoc.includes('MISSING_ARBITRATOR_PROOF_FIELDS') &&
    protocolDoc.includes('diverged_local_ahead_of_ao'),
  'Documents AO settlement statuses, mandatory proof fields check, and cursor divergence detection'
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
// 5. Multi-Replica Compose, Env Example, CI Rust->WASM Build & Scripts
// ---------------------------------------------------------------------------
console.log('\n[5] Checking Multi-Replica Compose, .env.example & CI Rust->WASM Pipeline...');
assert(
  composeDoc.includes('sd-node-1:') &&
    composeDoc.includes('sd-node-2:') &&
    composeDoc.includes('sd-node-3:') &&
    composeDoc.includes('HYDB_CLUSTER_MEMBERS') &&
    composeDoc.includes('HYDB_CLUSTER_SIZE=${HYDB_CLUSTER_SIZE:-3}') &&
    composeDoc.includes('HYDB_ADMIN_TOKEN') &&
    composeDoc.includes('HYDB_CLUSTER_SECRET') &&
    composeDoc.includes('~sd@1.0'),
  'docker-compose.yml configures a 3-replica SD cluster with explicit secrets, profile, and ~sd@1.0 endpoints'
);
assert(
  envExampleDoc.includes('HYDB_PROFILE=production') &&
    envExampleDoc.includes('HYDB_CLUSTER_SECRET=') &&
    envExampleDoc.includes('HYDB_ADMIN_TOKEN=') &&
    envExampleDoc.includes('HYDB_CLUSTER_MEMBERS='),
  '.env.example documents HYDB_PROFILE, HYDB_CLUSTER_SECRET, HYDB_ADMIN_TOKEN, and HYDB_CLUSTER_MEMBERS'
);

const requiredScripts = [
  'build:wasm',
  'verify:vectors',
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
assert(
  ciWorkflow.includes('wasm32-unknown-unknown'),
  '.github/workflows/ci.yml installs Rust wasm32-unknown-unknown target to build hydb.wasm from source'
);

// ---------------------------------------------------------------------------
// 6. Production Source Code Release Gate (No Default Secret Fallback)
// ---------------------------------------------------------------------------
console.log('\n[6] Checking Production Source Code Release Gate Invariants...');
const hymessageSrc = readFileSync(resolve(ROOT, 'hymessage/src/hymessage_runtime.erl'), 'utf8');
const sdSrc = readFileSync(resolve(ROOT, 'sd/src/sd_runtime.erl'), 'utf8');

const forbiddenFallbackToken = ['HYDB', 'DEFAULT', 'CLUSTER', 'SECRET'].join('_');
assert(
  !hymessageSrc.includes(forbiddenFallbackToken) && !sdSrc.includes(forbiddenFallbackToken),
  'Neither hymessage_runtime.erl nor sd_runtime.erl contains default cluster secret fallback'
);

// ---------------------------------------------------------------------------
// 7. Live Executable Semantic & Behavioral Verification
// ---------------------------------------------------------------------------
console.log('\n[7] Executing Live Semantic & Behavioral Release Gate...');

// 7A. Golden Vectors Provenance
const diskGolden = JSON.parse(readFileSync(resolve(ROOT, 'spec/golden_vectors.json'), 'utf8'));
const computedGolden = buildGoldenVectors();
assert(
  JSON.stringify(diskGolden) === JSON.stringify(computedGolden),
  'Live execution of buildGoldenVectors() matches spec/golden_vectors.json 100%'
);

// 7B. Rust WASM Binary Execution Check
const wasmBytes = readFileSync(resolve(ROOT, 'hydb/out/hydb.wasm'));
const wasmMod = new WebAssembly.Module(wasmBytes);
const wasmInst = new WebAssembly.Instance(wasmMod, {});
const wasmExports = wasmInst.exports;
wasmExports.db_reset();
const rootAfterPut = wasmExports.db_put(101n, 5000n, 1n);
const readBack = wasmExports.db_get(101n);
assert(
  readBack === 5000n && rootAfterPut !== 0n,
  'Live execution of compiled hydb/out/hydb.wasm executes db_put/db_get and updates 64-bit Merkle-XOR root'
);

// 7C. AO Committer Behavioral Verification (Missing Proof Fields & L1 Finality Gate)
const fakeBatch = {
  from_seq: 1,
  to_seq: 2,
  proposed_hash: 'A'.repeat(64),
  proposed_state_root: '12345'
};
const missingHashEval = verifyArbitratorEvaluation(
  { Messages: [{ Status: 'COMMITTED', OfficialSeq: '2', OfficialStateRoot: '12345' }] },
  fakeBatch
);
assert(
  missingHashEval.verified === false && missingHashEval.verdict === 'MISSING_ARBITRATOR_PROOF_FIELDS',
  'Live verifyArbitratorEvaluation rejects COMMITTED message missing OfficialHash'
);

const unverifiedFinalityEval = verifyArbitratorEvaluation(
  {
    Messages: [
      {
        Status: 'COMMITTED',
        OfficialSeq: '2',
        OfficialHash: 'A'.repeat(64),
        OfficialStateRoot: '12345'
      }
    ]
  },
  fakeBatch,
  { environment: 'ao_network', finalityProof: { block_height: 999999, confirmations: 50 } }
);
assert(
  unverifiedFinalityEval.verified === true && unverifiedFinalityEval.finalized === false,
  'Live verifyArbitratorEvaluation refuses to set finalized=true from unverified caller finalityProof'
);

const l1VerifyFail = await verifyArweaveL1Finality('tx_fake_1', fakeBatch, {
  environment: 'ao_network',
  l1GatewayFetchFn: async () => ({ status: 404 })
});
assert(l1VerifyFail.verified === false, 'Live verifyArweaveL1Finality rejects unconfirmed L1 transaction');

// 7D. Live Erlang BEAM Probe: Fail-Closed Default Profile & Constant-Time Token Check
const beamProbe = spawnSync(
  'erl',
  [
    '-noshell',
    '-eval',
    `{ok, hydb_spec, B0} = compile:file("${resolve(ROOT, 'hymessage/src/hydb_spec.erl')}", [binary]), code:load_binary(hydb_spec, "hydb_spec.erl", B0), ` +
      `{ok, hydb_contract, B1} = compile:file("${resolve(ROOT, 'hymessage/src/hydb_contract.erl')}", [binary]), code:load_binary(hydb_contract, "hydb_contract.erl", B1), ` +
      `{ok, sd_runtime, B2} = compile:file("${resolve(ROOT, 'sd/src/sd_runtime.erl')}", [binary]), code:load_binary(sd_runtime, "sd_runtime.erl", B2), ` +
      `{ok, hymessage_runtime, B3} = compile:file("${resolve(ROOT, 'hymessage/src/hymessage_runtime.erl')}", [binary]), code:load_binary(hymessage_runtime, "hymessage_runtime.erl", B3), ` +
      'os:unsetenv("HYDB_PROFILE"), os:unsetenv("HYDB_ENV"), os:unsetenv("NODE_ENV"), ' +
      'P1 = sd_runtime:is_test_profile(), P2 = hymessage_runtime:is_test_profile(), ' +
      'C1 = sd_runtime:constant_time_equals(<<"secret-token-1234">>, <<"secret-token-1234">>), ' +
      'C2 = sd_runtime:constant_time_equals(<<"secret-token-1234">>, <<"secret-token-9999">>), ' +
      'io:format("PROBE:~p:~p:~p:~p~n", [P1, P2, C1, C2]), halt(0).'
  ],
  { encoding: 'utf8' }
);
assert(
  beamProbe.stdout.includes('PROBE:false:false:true:false'),
  'Live Erlang BEAM probe confirms default fail-closed profile (false, false) and constant_time_equals correctness'
);

console.log('\n================================================================');
console.log(`  PHASE 07 RELEASE GATE SUMMARY: ${passed} passed, ${failed} failed`);
console.log('================================================================\n');

if (failed > 0) {
  process.exit(1);
}
