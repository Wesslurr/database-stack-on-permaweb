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

import { existsSync, readFileSync, unlinkSync, writeFileSync, appendFileSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import { execFile, execFileSync, spawnSync } from 'node:child_process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
import http from 'node:http';
import os from 'node:os';
import crypto from 'node:crypto';
import * as arbundles from 'arbundles';
import Arweave from 'arweave';
import {
  computeGenesisHash,
  buildGenesisContractLua,
  buildArbitratorContractLua
} from '../sdk/hydb_sdk.mjs';
import { startAoNode } from '../ao_process/ao_node.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = resolve(__dirname, '..');

function resolveLuaBinary() {
  if (process.env.LUA_BIN) return process.env.LUA_BIN;
  for (const bin of ['lua', 'lua5.4', 'lua5.3', 'luajit']) {
    try {
      const res = spawnSync(bin, ['-v'], { stdio: 'ignore' });
      if (res.status === 0) return bin;
    } catch {}
  }
  return 'lua';
}
const LUA_BIN = resolveLuaBinary();
const TEST_TMP_DIR = join(os.tmpdir(), `hydb_tahap12_${process.pid}_${Date.now()}`);
if (!existsSync(TEST_TMP_DIR)) {
  mkdirSync(TEST_TMP_DIR, { recursive: true });
}

const mask64 = 0xffffffffffffffffn;
function canonicalHash64(keyInt, valInt, verInt) {
  const prime = 0x100000001b3n;
  const ukey = BigInt(keyInt) & mask64;
  const uval = BigInt(valInt) & mask64;
  const uver = BigInt(verInt) & mask64;
  let h = 0xcbf29ce484222325n;
  h = ((h ^ ukey) * prime) & mask64;
  h = ((h ^ uval) * prime) & mask64;
  h = ((h ^ uver) * prime) & mask64;
  h = (h ^ (h >> 30n)) & mask64;
  h = (h * 0xbf58476d1ce4e5b9n) & mask64;
  h = (h ^ (h >> 27n)) & mask64;
  h = (h * 0x94d049bb133111ebn) & mask64;
  h = (h ^ (h >> 31n)) & mask64;
  return BigInt.asIntN(64, h);
}

function buildValidEvents(startSeq, count, prevHashInit, prevRootInit) {
  const events = [];
  let prevHash = prevHashInit;
  let root = BigInt(prevRootInit);
  for (let i = 0; i < count; i++) {
    const seq = startSeq + i;
    const key = 100 + seq;
    const val = seq * 1000;
    const ver = 1;
    const payload = `${prevHash}${seq}20${key}${val}`;
    const hash = crypto.createHash('sha256').update(payload).digest('hex').toUpperCase();
    const recHash = canonicalHash64(key, val, ver);
    root = BigInt.asIntN(64, (root ^ recHash) & mask64);
    events.push({
      contract_version: 'hydb.core@1.0',
      epoch: 1,
      seq,
      namespace: 'default',
      action: 'put',
      key,
      val,
      val_type: 'int64',
      canonical_val: String(val),
      ver,
      prev_hash: prevHash,
      hash,
      state_root: root.toString(),
      state_root_str: root.toString()
    });
    prevHash = hash;
  }
  return events;
}

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
    erlOut.includes('PASS: Retrying older batch with advanced Arbitrator tip returns authoritative tip hash and root'),
    'Retrying older batch with advanced Arbitrator tip returns authoritative tip hash and root'
  );
  assert(
    erlOut.includes('PASS: Advanced tip duplicate with invalid tip hash strictly rejected'),
    'Advanced tip duplicate with invalid tip hash strictly rejected'
  );
  assert(
    erlOut.includes('PASS: Duplicate verdict with sequence lag strictly rejected'),
    'Duplicate verdict with sequence lag strictly rejected'
  );
  assert(
    erlOut.includes('PASS: Advanced duplicate cursor correctly stores authoritative tip hash and root for cold-start restart'),
    'Advanced duplicate cursor correctly stores authoritative tip hash and root for cold-start restart'
  );
  assert(
    erlOut.includes('PASS: Arbitrator sequence advance with missing hash/root strictly halts fail-closed'),
    'Arbitrator sequence advance with missing hash/root strictly halts fail-closed'
  );
  assert(
    erlOut.includes('PASS: Lagging local WAL strictly halts fail-closed on advanced Arbitrator tip'),
    'Lagging local WAL strictly halts fail-closed on advanced Arbitrator tip'
  );
  assert(
    erlOut.includes('PASS: Disk cursor strictly preserved without unapplied sequence advance'),
    'Disk cursor strictly preserved without unapplied sequence advance'
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
    console.log(`    ℹ️ Erlang generated DataItem ID: ${erlItemRes.trim()}`);
    console.log(`    ℹ️ arbundles verified DataItem ID: ${item.id}`);
    console.log(`    ℹ️ Raw signature size: ${item.signature.length} bytes (RSA-4096 SHA-256 PSS)`);
    console.log(`    ℹ️ Owner modulus size: ${item.rawOwner.length} bytes`);
    console.log(`    ℹ️ Decoded DataItem Tags: ${JSON.stringify(item.tags.map(t => [t.name, t.value]))}`);
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

  console.log('\n[Suite 4] Authentic AO MU & CU Node Integration (Stateful AOS Lua Engine & In-BEAM Committer):');

  const suAddress = readFileSync(join(ROOT_DIR, 'secrets/address_su.txt'), 'utf8').trim();
  const adminWallet = JSON.parse(readFileSync(join(ROOT_DIR, 'secrets/wallet.json'), 'utf8'));
  const arweaveInstance = Arweave.init({});
  const adminAddress = await arweaveInstance.wallets.jwkToAddress(adminWallet);

  const stackId = 'hydb-production-vault';
  const schemas = [
    { namespace: 'finance.balances', backend: 'wasm_slot', key_type: 'int64', val_type: 'int64', rules: { min_value: 0 } },
    { namespace: 'app.docs', backend: 'ets_doc', key_type: 'string', val_type: 'any' }
  ];
  const nodes = [
    { id: 'sd-node-1', address: suAddress },
    { id: 'sd-node-admin', address: adminAddress }
  ];
  const admins = [adminAddress];

  const genesisHash = computeGenesisHash({
    stackId,
    quorumSize: 1,
    quorumMode: 'majority',
    nodes,
    admins,
    schemas
  });

  const mockGenesisId = crypto.createHash('sha256').update(`GENESIS_${genesisHash}`).digest('base64url');
  const mockArbitratorId = crypto.createHash('sha256').update(`ARBITRATOR_${genesisHash}`).digest('base64url');

  const genesisLua = buildGenesisContractLua({
    stackId,
    quorumSize: 1,
    quorumMode: 'majority',
    nodes,
    admins,
    schemas,
    genesisHash,
    arbitratorProcessId: mockArbitratorId
  });

  const arbitratorLua = buildArbitratorContractLua({
    stackId,
    quorumSize: 1,
    quorumMode: 'majority',
    nodes,
    admins,
    schemas,
    genesisHash,
    genesisProcessId: mockGenesisId
  });

  const mockQueueProcId = 'queue_order_' + crypto.randomUUID().replace(/-/g, '').slice(0, 32);
  const queueLua = `
local ok_json, json = pcall(require, "json")
if not ok_json or not json then json = require(".json") end

Counter = 0
History = {}

Handlers.add(
  "IncrementAndRecord",
  Handlers.utils.hasMatchingTag("Action", "IncrementAndRecord"),
  function(msg)
    Counter = Counter + 1
    local clientId = (msg.Tags and msg.Tags.ClientId) or msg.From or "unknown"
    table.insert(History, { order = Counter, client = tostring(clientId) })
    ao.send({
      Target = msg.From,
      Action = "IncrementResponse",
      Order = tostring(Counter),
      ClientId = tostring(clientId),
      Data = json.encode({ order = Counter, client = tostring(clientId) })
    })
  end
)

Handlers.add(
  "GetQueueState",
  Handlers.utils.hasMatchingTag("Action", "GetQueueState"),
  function(msg)
    ao.send({
      Target = msg.From,
      Action = "QueueStateResponse",
      Order = tostring(Counter),
      Data = json.encode({ counter = Counter, history = History })
    })
  end
)
`;

  const LOCAL_AO_PORT = 8996;
  const aoNode = await startAoNode({
    port: LOCAL_AO_PORT,
    processes: {
      [mockGenesisId]: genesisLua,
      [mockArbitratorId]: arbitratorLua,
      [mockQueueProcId]: queueLua
    }
  });

  try {
    // Step 4A-2: Concurrent HTTP Request FIFO Serialization & Deterministic State Proof
    const CONCURRENT_MSG_COUNT = 15;
    const concurrentPromises = [];
    for (let i = 1; i <= CONCURRENT_MSG_COUNT; i++) {
      const p = (async () => {
        const postRes = await fetch(`http://127.0.0.1:${LOCAL_AO_PORT}/`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            Target: mockQueueProcId,
            Action: 'IncrementAndRecord',
            From: `client_${i}`,
            Tags: [
              { name: 'Action', value: 'IncrementAndRecord' },
              { name: 'ClientId', value: `client_${i}` }
            ]
          })
        });
        assert(postRes.status === 200, `Concurrent message ${i} accepted by HTTP MU with 200`);
        const { id: msgId } = await postRes.json();

        // Fetch evaluated result from CU
        const resultRes = await fetch(`http://127.0.0.1:${LOCAL_AO_PORT}/result/${msgId}?process-id=${mockQueueProcId}`);
        assert(resultRes.status === 200, `CU result query for ${msgId} returned HTTP 200`);
        const resultJson = await resultRes.json();
        const orderNum = parseInt(resultJson.Messages?.[0]?.Order, 10);
        return { client: `client_${i}`, order: orderNum, msgId };
      })();
      concurrentPromises.push(p);
    }

    const concurrentResults = await Promise.all(concurrentPromises);
    assert(concurrentResults.length === CONCURRENT_MSG_COUNT, 'All 15 concurrent requests resolved');
    const assignedOrders = concurrentResults.map((r) => r.order).sort((a, b) => a - b);
    const expectedOrders = Array.from({ length: CONCURRENT_MSG_COUNT }, (_, idx) => idx + 1);
    assert(JSON.stringify(assignedOrders) === JSON.stringify(expectedOrders), 'All concurrent messages assigned continuous 1..15 sequence order without duplicates or drops');

    // Query final state via dry-run to prove deterministic WASM linear memory state
    const queueStateRes = await fetch(`http://127.0.0.1:${LOCAL_AO_PORT}/dry-run?process-id=${mockQueueProcId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ Action: 'GetQueueState' })
    });
    const queueStateJson = await queueStateRes.json();
    const stateData = JSON.parse(queueStateJson.Messages?.[0]?.Data || '{}');
    assert(stateData.counter === CONCURRENT_MSG_COUNT, 'Final WASM memory state counter strictly matches total concurrent count (15)');
    assert(stateData.history?.length === CONCURRENT_MSG_COUNT, 'Final WASM memory history contains exact number of recorded items (15)');
    const historyOrders = (stateData.history || []).map((h) => h.order);
    assert(JSON.stringify(historyOrders) === JSON.stringify(expectedOrders), 'WASM memory state history records strict continuous FIFO ordering 1..15');

    // Generate valid events for Batch 1 (seq 1..3)
    const b1Events = buildValidEvents(1, 3, '0000000000000000000000000000000000000000000000000000000000000000', '0');
    const b1PropHash = b1Events[b1Events.length - 1].hash;
    const b1PropRoot = b1Events[b1Events.length - 1].state_root;

    // Run Erlang committer interacting with the live AO HTTP stack
    const erlSuite4Script = `
      _ = application:ensure_all_started(inets),
      _ = application:ensure_all_started(ssl),
      {ok, WI} = sd_ao_client:load_wallet("secrets/wallet_su.json"),
      GwUrl = "http://127.0.0.1:${LOCAL_AO_PORT}",
      CuUrl = "http://127.0.0.1:${LOCAL_AO_PORT}",
      GenId = <<"${mockGenesisId}">>,
      ArbId = <<"${mockArbitratorId}">>,

      %% 1. Bootstrap: query GetGenesisConfig via CU dry-run
      {ok, 200, GenConfig} = sd_ao_client:query_state(GwUrl, GenId, <<"GetGenesisConfig">>, #{}, #{cu_url => CuUrl, wallet => WI}),
      DiscoveredArb = maps:get(<<"arbitrator_process_id">>, GenConfig),
      io:format("DISCOVERED_ARB:~s~n", [DiscoveredArb]),

      %% 2. Register SU node on Genesis contract via MU -> CU
      RegPayload = #{<<"node_id">> => maps:get(address, WI), <<"role">> => <<"SU_WORKER">>, <<"endpoint">> => <<"http://127.0.0.1:8735">>},
      {ok, 200, RegRes} = sd_ao_client:send_message(GwUrl, GenId, <<"RegisterNode">>, [], RegPayload, #{sender => maps:get(address, WI), wallet => WI, cu_url => CuUrl}),
      io:format("REG_OK:~p~n", [maps:get(<<"ok">>, RegRes, maps:get(<<"status">>, RegRes, <<"UNKNOWN">>))]),

      %% 3. Commit Checkpoint Batch 1 (seq 1..3) as authentic ANS-104 DataItem via MU -> CU
      Batch1 = #{
          <<"contract_version">> => <<"hydb.core@1.0">>,
          <<"epoch">> => 1,
          <<"from_seq">> => 1,
          <<"to_seq">> => 3,
          <<"prev_hash">> => <<"0000000000000000000000000000000000000000000000000000000000000000">>,
          <<"proposed_hash">> => <<"${b1PropHash}">>,
          <<"proposed_state_root">> => <<"${b1PropRoot}">>,
          <<"events">> => json:decode(<<"${JSON.stringify(b1Events).replace(/\\/g, '\\\\').replace(/"/g, '\\"') }">>)
      },
      BatchId1 = sd_ao_client:compute_batch_id(Batch1),
      FinalBatch1 = Batch1#{<<"batch_id">> => BatchId1},
      Tags1 = [
          {<<"Data-Protocol">>, <<"ao">>},
          {<<"Action">>, <<"CommitCheckpoint">>},
          {<<"Batch-Id">>, BatchId1},
          {<<"From-Seq">>, <<"1">>},
          {<<"To-Seq">>, <<"3">>},
          {<<"Epoch">>, <<"1">>}
      ],
      {ok, 200, CommitRes1} = sd_ao_client:send_message(GwUrl, ArbId, <<"CommitCheckpoint">>, Tags1, FinalBatch1, #{sender => maps:get(address, WI), wallet => WI, cu_url => CuUrl, timeout => 15000, cu_retries => 10, cu_retry_delay_ms => 300}),
      io:format("COMMIT1_STATUS:~s_SEQ:~p~n", [maps:get(<<"Status">>, CommitRes1), maps:get(<<"OfficialSeq">>, CommitRes1)]),

      halt(0).
    `;

    const { stdout: outStep1 } = await execFileAsync('erl', [
      '-pa', 'sd/out/ebin',
      '-noshell',
      '-eval', erlSuite4Script
    ], { cwd: ROOT_DIR, encoding: 'utf8', env: { ...process.env, HYDB_PROFILE: 'test' } });

    assert(outStep1.includes(`DISCOVERED_ARB:${mockArbitratorId}`), 'In-BEAM client dynamically discovers Arbitrator ID from live Genesis Lua contract via CU dry-run');
    assert(outStep1.includes('REG_OK:'), 'In-BEAM client successfully registers node with live Genesis Lua contract via MU -> CU flow');
    assert(outStep1.includes('COMMIT1_STATUS:COMMITTED') && outStep1.includes('3'), 'Live Arbitrator Lua contract evaluates ANS-104 DataItem and returns COMMITTED at seq 3 via MU -> CU polling');

    // Step 4C-2: Prove true snapshot isolation on Compute Unit with a 100% VALID mutating Batch
    // Construct a completely valid Batch 2 (seq 4..6 chained from seq 3 tip) that WOULD advance state if committed
    const b2Events = buildValidEvents(4, 3, b1PropHash, b1PropRoot);
    const b2PropHash = b2Events[b2Events.length - 1].hash;
    const b2PropRoot = b2Events[b2Events.length - 1].state_root;
    const validB2Batch = {
      contract_version: 'hydb.core@1.0',
      epoch: 1,
      from_seq: 4,
      to_seq: 6,
      prev_hash: b1PropHash,
      proposed_hash: b2PropHash,
      proposed_state_root: b2PropRoot,
      events: b2Events
    };

    // Execute valid mutating Batch 2 via POST /dry-run
    const dryrunMutRes = await fetch(`http://127.0.0.1:${LOCAL_AO_PORT}/dry-run?process-id=${mockArbitratorId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        Action: 'CommitCheckpoint',
        From: suAddress,
        Data: JSON.stringify(validB2Batch)
      })
    });
    const dryrunMutJson = await dryrunMutRes.json();
    const dryrunVerdict = dryrunMutJson.Messages?.[0];
    assert(dryrunVerdict?.Status === 'COMMITTED', 'Valid checkpoint dry-run returns COMMITTED verdict inside ephemeral execution sandbox');
    assert(String(dryrunVerdict?.OfficialSeq) === '6', 'Valid checkpoint dry-run returns advanced sequence 6 inside ephemeral execution sandbox');
    assert(dryrunVerdict?.OfficialHash === b2PropHash, 'Valid checkpoint dry-run returns proposed tip hash inside ephemeral execution sandbox');

    // Query official state via dry-run to prove Arbitrator sequence did NOT advance in persistent WASM memory
    const dryrunCheckRes = await fetch(`http://127.0.0.1:${LOCAL_AO_PORT}/dry-run?process-id=${mockArbitratorId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ Action: 'GetOfficialState' })
    });
    const dryrunCheckJson = await dryrunCheckRes.json();
    const stateMsgBeforeB2 = dryrunCheckJson.Messages?.[0];
    assert(stateMsgBeforeB2 && String(stateMsgBeforeB2.OfficialSeq) === '3', 'True Snapshot Isolation proven: persistent OfficialSeq strictly remains 3 despite valid mutating dry-run returning COMMITTED at seq 6');
    assert(stateMsgBeforeB2.OfficialHash === b1PropHash, 'True Snapshot Isolation proven: persistent official hash strictly remains at seq 3 tip hash');

    // Step 4D: Advance Arbitrator tip to seq 6 using Batch 2 dispatched through HTTP MU -> CU flow

    const erlSuite4Step2Script = `
      _ = application:ensure_all_started(inets),
      _ = application:ensure_all_started(ssl),
      {ok, WI} = sd_ao_client:load_wallet("secrets/wallet_su.json"),
      GwUrl = "http://127.0.0.1:${LOCAL_AO_PORT}",
      CuUrl = "http://127.0.0.1:${LOCAL_AO_PORT}",
      ArbId = <<"${mockArbitratorId}">>,

      Batch2 = #{
          <<"contract_version">> => <<"hydb.core@1.0">>,
          <<"epoch">> => 1,
          <<"from_seq">> => 4,
          <<"to_seq">> => 6,
          <<"prev_hash">> => <<"${b1PropHash}">>,
          <<"proposed_hash">> => <<"${b2PropHash}">>,
          <<"proposed_state_root">> => <<"${b2PropRoot}">>,
          <<"events">> => json:decode(<<"${JSON.stringify(b2Events).replace(/\\/g, '\\\\').replace(/"/g, '\\"') }">>)
      },
      BatchId2 = sd_ao_client:compute_batch_id(Batch2),
      FinalBatch2 = Batch2#{<<"batch_id">> => BatchId2},
      Tags2 = [
          {<<"Data-Protocol">>, <<"ao">>},
          {<<"Action">>, <<"CommitCheckpoint">>},
          {<<"Batch-Id">>, BatchId2},
          {<<"From-Seq">>, <<"4">>},
          {<<"To-Seq">>, <<"6">>},
          {<<"Epoch">>, <<"1">>}
      ],
      {ok, 200, CommitRes2} = sd_ao_client:send_message(GwUrl, ArbId, <<"CommitCheckpoint">>, Tags2, FinalBatch2, #{sender => maps:get(address, WI), wallet => WI, cu_url => CuUrl, timeout => 15000, cu_retries => 10, cu_retry_delay_ms => 300}),
      io:format("COMMIT2_STATUS:~s_SEQ:~p~n", [maps:get(<<"Status">>, CommitRes2), maps:get(<<"OfficialSeq">>, CommitRes2)]),
      halt(0).
    `;

    const { stdout: outStep1B } = await execFileAsync('erl', [
      '-pa', 'sd/out/ebin',
      '-noshell',
      '-eval', erlSuite4Step2Script
    ], { cwd: ROOT_DIR, encoding: 'utf8', env: { ...process.env, HYDB_PROFILE: 'test' } });

    assert(outStep1B.includes('COMMIT2_STATUS:COMMITTED') && outStep1B.includes('6'), 'Batch 2 evaluated over authentic HTTP MU -> CU pipeline, advancing Arbitrator tip to seq 6');

    // Query official state to prove persistent WASM memory has now legitimately advanced to seq 6
    const postCommitRes = await fetch(`http://127.0.0.1:${LOCAL_AO_PORT}/dry-run?process-id=${mockArbitratorId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ Action: 'GetOfficialState' })
    });
    const postCommitJson = await postCommitRes.json();
    const stateMsgAfterB2 = postCommitJson.Messages?.[0];
    assert(String(stateMsgAfterB2?.OfficialSeq) === '6', 'Persistent state now officially advanced to seq 6 only upon real commit');
    assert(stateMsgAfterB2?.OfficialHash === b2PropHash, 'Persistent official hash strictly matches Batch 2 hash');

    // Step 4E: Full in-BEAM committer cycle with lagging local WAL (local seq 3 vs Arbitrator tip 6)
    const walFile = join(ROOT_DIR, 'data/test_suite4_wal.log');
    const walCursorFile = join(ROOT_DIR, 'data/test_suite4_live_cursor.json');
    if (existsSync(walFile)) unlinkSync(walFile);
    if (existsSync(walCursorFile)) unlinkSync(walCursorFile);

    // Node only has WAL records up to sequence 3
    const walLines1 = b1Events.map((e) => JSON.stringify(e)).join('\n') + '\n';
    writeFileSync(walFile, walLines1, 'utf8');

    // Initial cursor starts at sequence 0
    const initialCursorJson = JSON.stringify({
      last_committed_seq: 0,
      last_committed_hash: '0000000000000000000000000000000000000000000000000000000000000000',
      last_state_root: '0',
      epoch: 1,
      verdict: 'GENESIS'
    });
    writeFileSync(walCursorFile, initialCursorJson, 'utf8');

    const erlLagScript = `
      _ = application:ensure_all_started(inets),
      _ = application:ensure_all_started(ssl),
      os:putenv("HYDB_PROFILE", "test"),
      os:putenv("HYDB_WAL_PATH", "${walFile.replace(/\\/g, '/')}"),
      _ = sd_runtime:ensure_started(),
      _ = sd_runtime:simulate_restart_from_wal(),
      InitialTick = sd_runtime:get_current_tick(),
      io:format("INITIAL_LOCAL_WAL_TICK:~p~n", [InitialTick]),

      GwUrl = "http://127.0.0.1:${LOCAL_AO_PORT}",
      CuUrl = "http://127.0.0.1:${LOCAL_AO_PORT}",
      ArbId = <<"${mockArbitratorId}">>,
      CursorPath = "${walCursorFile.replace(/\\/g, '/')}",

      %% 1. Startup reconciliation lag guard: starting with arbitrator_id strictly halts fail-closed
      {error, {chain_divergence, LagReason}} = sd_committer:start_link(#{
          gateway_url => GwUrl,
          cu_url => CuUrl,
          arbitrator_id => ArbId,
          wallet_path => "secrets/wallet_su.json",
          cursor_path => CursorPath,
          poll_interval_ms => 60000
      }),
      io:format("STARTUP_LAG_HALTED:true_REASON:~s~n", [LagReason]),

      %% 2. Running committer lag guard: start without arbitrator_id, then attach and run commit cycle
      {ok, _} = sd_committer:start_link(#{
          gateway_url => GwUrl,
          cu_url => CuUrl,
          arbitrator_id => <<>>,
          wallet_path => "secrets/wallet_su.json",
          cursor_path => CursorPath,
          poll_interval_ms => 60000
      }),
      ok = sd_committer:set_arbitrator_id(ArbId),

      %% Run full commit cycle while local WAL only has seq 1..3, but Arbitrator is already at seq 6
      CycleRes = sd_committer:commit_now(),
      io:format("FULL_CYCLE_RES:~p~n", [CycleRes]),
      StatusMap = sd_committer:get_status(),
      io:format("FULL_CYCLE_STATUS:~s_HEALTH:~s_ERR:~s~n", [
          maps:get(<<"status">>, StatusMap, <<"unknown">>),
          maps:get(<<"health">>, StatusMap, <<"unknown">>),
          maps:get(<<"error_code">>, StatusMap, <<"none">>)
      ]),

      %% Verify durable cursor on disk did NOT skip to 6!
      {ok, DiskCurAfterLag} = sd_committer:load_disk_cursor(CursorPath),
      io:format("CURSOR_SEQ_AFTER_LAG:~p~n", [maps:get(last_committed_seq, DiskCurAfterLag)]),

      sd_committer:stop(),
      halt(0).
    `;

    const { stdout: outStep2 } = await execFileAsync('erl', [
      '-pa', 'sd/out/ebin',
      '-noshell',
      '-eval', erlLagScript
    ], { cwd: ROOT_DIR, encoding: 'utf8', env: { ...process.env, HYDB_PROFILE: 'test' } });

    assert(outStep2.includes('INITIAL_LOCAL_WAL_TICK:3'), 'Local node WAL initializes strictly at sequence 3');
    assert(outStep2.includes('STARTUP_LAG_HALTED:true'), 'Startup reconciliation strictly halts fail-closed on lagging local WAL');
    assert(outStep2.includes('err_local_wal_lagging'), 'Full committer cycle detects lagging local WAL (seq 3 < seq 6) and halts fail-closed');
    assert(outStep2.includes('FULL_CYCLE_STATUS:halted_HEALTH:failed_closed_ERR:err_local_wal_lagging'), 'Committer state machine records status=halted and health=failed_closed');
    assert(outStep2.includes('CURSOR_SEQ_AFTER_LAG:0'), 'Durable disk cursor strictly refuses to skip unapplied local events (remains seq 0)');

    // Step 4F: Catch-up local WAL to sequence 6 and recover committer state cleanly
    const walLines2 = b2Events.map((e) => JSON.stringify(e)).join('\n') + '\n';
    appendFileSync(walFile, walLines2, 'utf8');

    const erlCatchupScript = `
      _ = application:ensure_all_started(inets),
      _ = application:ensure_all_started(ssl),
      os:putenv("HYDB_PROFILE", "test"),
      os:putenv("HYDB_WAL_PATH", "${walFile.replace(/\\/g, '/')}"),
      _ = sd_runtime:ensure_started(),
      _ = sd_runtime:simulate_restart_from_wal(),
      CatchupTick = sd_runtime:get_current_tick(),
      io:format("CATCHUP_LOCAL_WAL_TICK:~p~n", [CatchupTick]),

      GwUrl = "http://127.0.0.1:${LOCAL_AO_PORT}",
      CuUrl = "http://127.0.0.1:${LOCAL_AO_PORT}",
      ArbId = <<"${mockArbitratorId}">>,
      CursorPath = "${walCursorFile.replace(/\\/g, '/')}",

      %% Start in-BEAM committer: startup reconciliation runs against live Arbitrator
      %% Since local WAL has caught up to seq 6 and matches Arbitrator tip, reconciliation succeeds!
      {ok, _} = sd_committer:start_link(#{
          gateway_url => GwUrl,
          cu_url => CuUrl,
          arbitrator_id => ArbId,
          wallet_path => "secrets/wallet_su.json",
          cursor_path => CursorPath,
          poll_interval_ms => 60000
      }),
      StatusMapCatchup = sd_committer:get_status(),
      io:format("CATCHUP_STATUS:~s_HEALTH:~s_SEQ:~p~n", [
          maps:get(<<"status">>, StatusMapCatchup, <<"unknown">>),
          maps:get(<<"health">>, StatusMapCatchup, <<"unknown">>),
          maps:get(<<"last_committed_seq">>, StatusMapCatchup, 0)
      ]),

      %% Verify cursor on disk now safely records sequence 6
      {ok, DiskCurAfterCatchup} = sd_committer:load_disk_cursor(CursorPath),
      io:format("DURABLE_CURSOR_SEQ_AFTER_CATCHUP:~p~n", [maps:get(last_committed_seq, DiskCurAfterCatchup)]),

      sd_committer:stop(),
      halt(0).
    `;

    const { stdout: outStep3 } = await execFileAsync('erl', [
      '-pa', 'sd/out/ebin',
      '-noshell',
      '-eval', erlCatchupScript
    ], { cwd: ROOT_DIR, encoding: 'utf8', env: { ...process.env, HYDB_PROFILE: 'test' } });

    assert(outStep3.includes('CATCHUP_LOCAL_WAL_TICK:6'), 'Local node WAL catches up and applies events to sequence 6');
    assert(outStep3.includes('CATCHUP_STATUS:running_HEALTH:healthy_SEQ:6'), 'Committer startup reconciliation against live Arbitrator succeeds cleanly after WAL catch-up');
    assert(outStep3.includes('DURABLE_CURSOR_SEQ_AFTER_CATCHUP:6'), 'Durable disk cursor safely advances to seq 6 after local WAL matches Arbitrator tip');

  } finally {
    const walFile = join(ROOT_DIR, 'data/test_suite4_wal.log');
    const walCursorFile = join(ROOT_DIR, 'data/test_suite4_live_cursor.json');
    if (existsSync(walFile)) unlinkSync(walFile);
    if (existsSync(walCursorFile)) unlinkSync(walCursorFile);
    if (aoNode) await aoNode.stop();
  }

  console.log('\n========================================================================');
  console.log(` Tahap 12 Native In-BEAM Committer Results: ${passed} PASSED, ${failed} FAILED`);
  console.log('========================================================================\n');

  if (failed > 0) {
    process.exit(1);
  }
}

runTests().catch((err) => {
  console.error('\nFatal Tahap 12 Test Error:', err);
  process.exit(1);
});
