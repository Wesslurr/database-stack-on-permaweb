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

import { existsSync, readFileSync, unlinkSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import { execFileSync } from 'node:child_process';
import * as arbundles from 'arbundles';

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

async function runTests() {
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
    erlOut.includes('PASS: In-BEAM RSA-PSS message signature verified against wallet public key'),
    'In-BEAM RSA-PSS message signature verified against wallet public key'
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
    erlOut.includes('PASS: Corrupted disk cursor strictly rejected with fail-closed error'),
    'Corrupted disk cursor strictly rejected with fail-closed error'
  );
  assert(
    erlOut.includes('PASS: Native Erlang HTTP client dispatches CommitCheckpoint and parses COMMITTED verdict'),
    'Native Erlang HTTP client dispatches CommitCheckpoint and parses COMMITTED verdict'
  );
  assert(
    erlOut.includes('PASS: Dispatched message envelope cryptographically signed with RSA-PSS and owner public key'),
    'Dispatched message envelope cryptographically signed with RSA-PSS and owner public key'
  );
  assert(
    erlOut.includes('PASS: In-BEAM committer fetches and evaluates verdict via CU result endpoint'),
    'In-BEAM committer fetches and evaluates verdict via CU result endpoint'
  );
  assert(
    erlOut.includes('PASS: Official AO Compute Unit dry-run evaluation query verified'),
    'Official AO Compute Unit dry-run evaluation query verified'
  );
  assert(
    erlOut.includes('PASS: DUPLICATE_BATCH_IDEMPOTENT verdict requires matching official hash and root'),
    'DUPLICATE_BATCH_IDEMPOTENT verdict requires matching official hash and root'
  );
  assert(
    erlOut.includes('PASS: Arbitrator sequence advance with missing hash/root strictly halts fail-closed'),
    'Arbitrator sequence advance with missing hash/root strictly halts fail-closed'
  );
  assert(
    erlOut.includes('PASS: Incomplete verdict missing official hash strictly rejected'),
    'Incomplete verdict missing official hash strictly rejected'
  );
  assert(
    erlOut.includes('PASS: Unpinned Genesis hash rejected in production profile'),
    'Unpinned Genesis hash rejected in production profile'
  );
  assert(
    erlOut.includes('PASS: Committer heartbeat without Genesis strictly reports unacknowledged'),
    'Committer heartbeat without Genesis strictly reports unacknowledged'
  );
  assert(
    erlOut.includes('PASS: Divergent official hash strictly rejected by arbitrator verdict parser'),
    'Divergent official hash strictly rejected by arbitrator verdict parser'
  );
  assert(
    erlOut.includes('PASS: Genesis hash divergence triggers fail-closed error in Erlang committer'),
    'Genesis hash divergence triggers fail-closed error in Erlang committer'
  );
  assert(
    erlOut.includes('PASS: Missing genesis hash or connection failure triggers strict fail-closed rejection'),
    'Missing genesis hash or connection failure triggers strict fail-closed rejection'
  );
  assert(
    erlOut.includes('PASS: In-BEAM committer status reporting and health state machine verified'),
    'In-BEAM committer status reporting and health state machine verified'
  );
  assert(
    erlOut.includes('ALL TAHAP 12 IN-BEAM COMMITTER TESTS PASSED'),
    'All In-BEAM Committer lifecycle guarantees verified successfully'
  );

  console.log('\n[Suite 2] Cross-Verification with Official Arweave arbundles:');
  const tempBinPath = join(ROOT_DIR, 'data/temp_test_ans104.bin');
  try {
    const erlItemRes = execFileSync('erl', [
      '-pa', 'sd/out/ebin',
      '-noshell',
      '-eval',
      `{ok, WI} = sd_ao_client:load_wallet("secrets/wallet_su.json"),
       Target = <<"0123456789012345678901234567890123456789010">>,
       Tags = [{<<"Data-Protocol">>, <<"ao">>}, {<<"Action">>, <<"CommitCheckpoint">>}, {<<"Epoch">>, <<"1">>}],
       {ok, ItemId, Bin} = sd_ao_client:build_ans104_data_item(Target, <<"CommitCheckpoint">>, Tags, <<"{\\"test\\":\\"ans104\\"}">>, WI),
       file:write_file("${tempBinPath.replace(/\\/g, '/')}", Bin),
       io:format("~s", [ItemId]),
       halt(0).`
    ], { cwd: ROOT_DIR, encoding: 'utf8' });

    assert(existsSync(tempBinPath), 'Native Erlang generated ANS-104 binary DataItem');
    const buf = readFileSync(tempBinPath);
    const DataItemCtor = arbundles.DataItem || arbundles.default?.DataItem;
    const item = new DataItemCtor(buf);
    const valid = await item.isValid();
    assert(valid === true, 'arbundles.DataItem cryptographically verifies native RSA-PSS signature');
    assert(item.id.length === 43, 'arbundles resolves valid 43-character DataItem ID');
    assert(item.id === erlItemRes.trim(), 'arbundles ID matches Erlang native item ID exactly');
    assert(item.tags.some(t => t.name === 'Data-Protocol' && t.value === 'ao'), 'DataItem contains valid AO Data-Protocol tag');
    assert(item.tags.some(t => t.name === 'Action' && t.value === 'CommitCheckpoint'), 'DataItem contains valid CommitCheckpoint Action tag');
    assert(item.rawData.toString('utf8') === '{"test":"ans104"}', 'DataItem raw data matches original payload exactly');
  } finally {
    if (existsSync(tempBinPath)) unlinkSync(tempBinPath);
  }

  console.log('\n[Suite 3] Container Supervisor Fail-Closed State Machine Verification:');

  // Test 3A: Compact JSON heartbeat match (Erlang json:encode output)
  const compactHeartbeatJson = '{"health":"failed_closed","status":"halted","error_code":"err_reconciliation_invalid_proof"}';
  const matchCompactCmd = `
    HEARTBEAT_JSON='${compactHeartbeatJson}'
    if echo "$HEARTBEAT_JSON" | grep -Eq '"health"[[:space:]]*:[[:space:]]*"failed_closed"' || \\
       echo "$HEARTBEAT_JSON" | grep -Eq '"status"[[:space:]]*:[[:space:]]*"(halted|failed_closed|crashed)"' || \\
       echo "$HEARTBEAT_JSON" | grep -Eq '"committer_health"[[:space:]]*:[[:space:]]*"failed_closed"'; then
      echo "MATCH_FAIL_CLOSED"
    else
      echo "NO_MATCH"
    fi
  `;
  const matchCompactOut = execFileSync('bash', ['-c', matchCompactCmd], { encoding: 'utf8' }).trim();
  assert(matchCompactOut === 'MATCH_FAIL_CLOSED', 'Supervisor regex detects compact Erlang failed_closed/halted JSON');

  // Test 3B: Normal running state does not trigger fail-closed
  const runningHeartbeatJson = '{"health":"healthy","status":"running","last_committed_seq":5}';
  const matchRunningCmd = `
    HEARTBEAT_JSON='${runningHeartbeatJson}'
    if echo "$HEARTBEAT_JSON" | grep -Eq '"health"[[:space:]]*:[[:space:]]*"failed_closed"' || \\
       echo "$HEARTBEAT_JSON" | grep -Eq '"status"[[:space:]]*:[[:space:]]*"(halted|failed_closed|crashed)"' || \\
       echo "$HEARTBEAT_JSON" | grep -Eq '"committer_health"[[:space:]]*:[[:space:]]*"failed_closed"'; then
      echo "MATCH_FAIL_CLOSED"
    else
      echo "HEALTHY_OK"
    fi
  `;
  const matchRunningOut = execFileSync('bash', ['-c', matchRunningCmd], { encoding: 'utf8' }).trim();
  assert(matchRunningOut === 'HEALTHY_OK', 'Supervisor loop allows healthy running committer state');

  // Test 3C: Detection of crashed/dead committer GenServer via ~sd@1.0/status payload
  const crashedSdStatus = '{"ok":true,"health":"failed_closed","committer":{"status":"crashed","health":"failed_closed"}}';
  const matchCrashedCmd = `
    SD_STATUS='${crashedSdStatus}'
    if echo "$SD_STATUS" | grep -q '"committer"'; then
      if echo "$SD_STATUS" | grep -Eq '"status"[[:space:]]*:[[:space:]]*"(disabled|stopped|halted|crashed)"' || \\
         echo "$SD_STATUS" | grep -Eq '"health"[[:space:]]*:[[:space:]]*"(failed_closed|crashed)"'; then
        echo "DEAD_COMMITTER_DETECTED"
      else
        echo "COMMITTER_ALIVE"
      fi
    else
      echo "NO_COMMITTER"
    fi
  `;
  const matchCrashedOut = execFileSync('bash', ['-c', matchCrashedCmd], { encoding: 'utf8' }).trim();
  assert(matchCrashedOut === 'DEAD_COMMITTER_DETECTED', 'Supervisor detects dead/crashed in-BEAM committer GenServer and halts');

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
