#!/usr/bin/env node
/**
 * HyDB Phase 06: Honest AO Settlement, Status Lifecycle & Durable Cursor Suite
 * ============================================================================
 * Verifies all Phase 06 roadmap requirements:
 * 1. `message()` returning a message ID alone NEVER returns `confirmed: true`.
 * 2. Explicit distinction between `submitted`, `evaluated` (`rejected`),
 *    `verified`, and `finalized`, plus `pending`/`unknown` evaluation states.
 * 3. Explicit distinction between `arlocal_local` test gateways and `ao_network`
 *    L1 finality.
 * 4. Idempotent checkpoint batch retries over `(from_seq, to_seq, proposed_hash)`
 *    against the real Lua `ao_process/arbitrator.lua` without duplicate checkpoints.
 * 5. Durable `lastCommittedSeq` cursor persistence and startup reconciliation
 *    with AO `GetOfficialState` across daemon restarts.
 */

import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';
import Arweave from 'arweave';

import {
  computeBatchId,
  detectAoEnvironment,
  commitCheckpointBatch,
  queryOfficialAoState,
  verifyArbitratorEvaluation,
  verifyArweaveL1Finality,
  getOrCreateProcessId,
  isTestProfile,
  loadDurableCursor,
  saveDurableCursor,
  reconcileCursorWithAO,
  processCommitterCycle,
  setCommittedCursor,
  getCommittedCursor,
  acquireCursorLock,
  releaseCursorLock,
  writeHeartbeatStatus,
  readHeartbeatStatus,
  resolveDefaultCursorPath,
  isFatalFailClosedError,
  FATAL_FAIL_CLOSED_EXIT_CODE
} from '../ao_process/committer.mjs';
import { deployAoProcess } from '../ao_process/deploy_ao_process.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = resolve(__dirname, '..');

function resolveLuaBinary() {
  if (process.env.LUA_BIN) return process.env.LUA_BIN;
  for (const bin of ['lua', 'lua5.3', 'lua5.4', 'luajit']) {
    const check = spawnSync(bin, ['-v'], { encoding: 'utf-8' });
    if (check.status === 0) return bin;
  }
  return 'lua';
}
const LUA_BIN = resolveLuaBinary();

let passed = 0;
let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error(`  ❌ FAIL: ${msg}`);
    failed++;
    throw new Error(`Phase 06 verification failed: ${msg}`);
  }
  console.log(`  ✅ PASS: ${msg}`);
  passed++;
}

function canonicalHash64(keyInt, valInt, verInt) {
  const mask64 = 0xffffffffffffffffn;
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
    root = BigInt.asIntN(64, (root ^ recHash) & 0xffffffffffffffffn);
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

/**
 * Stateful Lua 5.3 Arbitrator Harness that executes real `ao_process/arbitrator.lua`
 * across a sequence of AO messages in a single Lua session or via state snapshot.
 */
class LuaArbitratorHarness {
  constructor() {
    this.historyMessages = [];
  }

  async evaluate(msg) {
    this.historyMessages.push(msg);
    const luaRunnerScript = `
      package.path = "${ROOT_DIR.replace(/\\/g, '/')}/ao_process/?.lua;" .. package.path
      local spec = dofile("${ROOT_DIR.replace(/\\/g, '/')}/ao_process/hydb_spec.lua")
      package.loaded["hydb_spec"] = spec

      -- Minimal JSON codec using JS-injected messages table
      local arbit = dofile("${ROOT_DIR.replace(/\\/g, '/')}/ao_process/arbitrator.lua")
    `;
    // Execute via a temporary Lua script that feeds `this.historyMessages` into `arbitrator.lua`
    const tmpLua = join(TEST_TMP_DIR, 'run_arbit.lua');
    const luaSource = this.generateLuaScript(this.historyMessages);
    writeFileSync(tmpLua, luaSource, 'utf-8');
    const res = spawnSync(LUA_BIN, [tmpLua], { encoding: 'utf-8' });
    if (res.status !== 0) {
      throw new Error(`Lua Arbitrator execution failed (${LUA_BIN}): ${res.stderr || res.error?.message}`);
    }
    return JSON.parse(res.stdout.trim());
  }

  generateLuaScript(msgs) {
    // Serialize messages directly into Lua table constructors so Lua doesn't need external C json library
    const toLuaVal = (v) => {
      if (v === null || v === undefined) return 'nil';
      if (typeof v === 'boolean') return v ? 'true' : 'false';
      if (typeof v === 'number') return String(v);
      if (typeof v === 'string') {
        return JSON.stringify(v);
      }
      if (Array.isArray(v)) {
        return '{ ' + v.map(toLuaVal).join(', ') + ' }';
      }
      if (typeof v === 'object') {
        const entries = Object.entries(v).map(([k, val]) => `[${JSON.stringify(k)}] = ${toLuaVal(val)}`);
        return '{ ' + entries.join(', ') + ' }';
      }
      return 'nil';
    };

    const parsedMsgs = msgs.map((m) => {
      let decodedData = {};
      if (typeof m.Data === 'string') {
        try {
          decodedData = JSON.parse(m.Data);
        } catch {
          decodedData = {};
        }
      } else if (m.Data && typeof m.Data === 'object') {
        decodedData = m.Data;
      }
      return { ...m, _decodedData: decodedData };
    });

    return `
      package.path = "${ROOT_DIR.replace(/\\/g, '/')}/ao_process/?.lua;" .. package.path
      local spec = dofile("${ROOT_DIR.replace(/\\/g, '/')}/ao_process/hydb_spec.lua")
      package.loaded["hydb_spec"] = spec

      local decoded_Registry = {}
      package.loaded["json"] = {
        decode = function(s)
          return decoded_Registry[s] or {}
        end,
        encode = function(t)
          return "{}"
        end
      }

      local arbit = dofile("${ROOT_DIR.replace(/\\/g, '/')}/ao_process/arbitrator.lua")
      local msgs = ${toLuaVal(parsedMsgs)}
      local last_res = nil
      for idx, m in ipairs(msgs) do
        local key = "DATA_" .. tostring(idx)
        decoded_Registry[key] = m._decodedData
        m.Data = key
        last_res = arbit.handle(m)
      end

      local resp = (last_res and last_res.responses and last_res.responses[#last_res.responses]) or {}
      local function esc(s)
        if s == nil then return "null" end
        return string.format("%q", tostring(s))
      end
      io.write(string.format(
        '{"official_seq":%d,"current_epoch":%d,"responses":[{"Action":%s,"Status":%s,"SubStatus":%s,"OfficialSeq":%s,"OfficialStateRoot":%s,"OfficialHash":%s,"OfficialCommitment256":%s,"CheckpointsCount":%s,"DeduplicatedEvents":%s,"Reason":%s}]}',
        last_res.official_seq or 0,
        last_res.current_epoch or 1,
        esc(resp.Action),
        esc(resp.Status),
        esc(resp.SubStatus),
        esc(resp.OfficialSeq),
        esc(resp.OfficialStateRoot),
        esc(resp.OfficialHash),
        esc(resp.OfficialCommitment256),
        esc(resp.CheckpointsCount),
        esc(resp.DeduplicatedEvents),
        esc(resp.Reason)
      ))
    `;
  }
}

const TEST_TMP_DIR = join(tmpdir(), `hydb_phase06_${process.pid}_${Date.now()}`);
mkdirSync(TEST_TMP_DIR, { recursive: true });

async function runPhase06Tests() {
  console.log('========================================================================');
  console.log(' HyDB Phase 06: AO Settlement Verification & Durable Cursor Test Suite');
  console.log('========================================================================\n');

  process.env.HYDB_CLUSTER_SECRET = 'phase06-test-cluster-secret-2026';
  const GENESIS = '0000000000000000000000000000000000000000000000000000000000000000';

  const events1to3 = buildValidEvents(1, 3, GENESIS, 0n);
  const batch1to3 = {
    su_id: 'SU_NODE_1',
    epoch: 1,
    from_seq: 1,
    to_seq: 3,
    proposed_state_root: events1to3[2].state_root_str,
    proposed_hash: events1to3[2].hash,
    events: events1to3
  };

  // --------------------------------------------------------------------------
  console.log('[1] No Premature Confirmation on Mere `message()` Submission:');
  // --------------------------------------------------------------------------
  const subOnlyRes = await commitCheckpointBatch(batch1to3, {
    useAoConnect: true,
    processId: 'PROC_TEST_01',
    messageFn: async () => 'msg_id_submitted_only_123',
    skipResultLookup: true
  });

  assert(subOnlyRes.tx_id === 'msg_id_submitted_only_123', 'Message ID returned from transport');
  assert(subOnlyRes.confirmed === false, 'confirmed is strictly FALSE when only message() succeeded');
  assert(subOnlyRes.verified === false, 'verified is strictly FALSE without Arbitrator evaluation');
  assert(subOnlyRes.finalized === false, 'finalized is strictly FALSE');
  assert(subOnlyRes.settlement_status === 'submitted', 'settlement_status is "submitted"');
  assert(subOnlyRes.evaluation_status === 'unknown' || subOnlyRes.evaluation_status === 'pending', 'evaluation_status is pending/unknown');

  const pendingCuRes = await commitCheckpointBatch(batch1to3, {
    useAoConnect: true,
    processId: 'PROC_TEST_01',
    messageFn: async () => 'msg_id_pending_cu_456',
    resultFn: async () => ({ Messages: [], Output: {} })
  });
  assert(pendingCuRes.confirmed === false, 'confirmed is FALSE when CU result has no evaluation messages yet');
  assert(pendingCuRes.settlement_status === 'submitted', 'settlement_status remains "submitted"');
  assert(pendingCuRes.evaluation_status === 'pending', 'evaluation_status is "pending"');

  const timeoutCuRes = await commitCheckpointBatch(batch1to3, {
    useAoConnect: true,
    processId: 'PROC_TEST_01',
    messageFn: async () => 'msg_id_timeout_cu_789',
    resultFn: async () => {
      throw new Error('CU evaluation timeout');
    }
  });
  assert(timeoutCuRes.confirmed === false, 'confirmed is FALSE when CU evaluation times out');
  assert(timeoutCuRes.evaluation_status === 'unknown', 'evaluation_status is "unknown" on CU error');

  // --------------------------------------------------------------------------
  console.log('\n[2] Real Lua Arbitrator Verification, ArLocal vs AO Finality, & Idempotent Batch Retries:');
  // --------------------------------------------------------------------------
  const luaArbit = new LuaArbitratorHarness();

  // First commit of batch 1..3 on ArLocal environment
  const verifiedArLocalRes = await commitCheckpointBatch(batch1to3, {
    useAoConnect: true,
    environment: 'arlocal_local',
    processId: 'PROC_TEST_01',
    messageFn: async () => 'msg_batch_1_3_first',
    evaluatorFn: (m) => luaArbit.evaluate(m)
  });

  assert(verifiedArLocalRes.ok === true, 'Valid batch 1..3 accepted by Lua Arbitrator');
  assert(verifiedArLocalRes.verdict === 'COMMITTED', 'Arbitrator verdict is COMMITTED');
  assert(verifiedArLocalRes.verified === true, 'Batch is cryptographically verified');
  assert(verifiedArLocalRes.confirmed === true, 'confirmed is TRUE after verified COMMITTED verdict');
  assert(verifiedArLocalRes.settlement_status === 'verified', 'settlement_status is "verified"');
  assert(verifiedArLocalRes.environment === 'arlocal_local', 'Environment classified as "arlocal_local"');
  assert(verifiedArLocalRes.finalized === false, 'arlocal_local simulation does NOT claim L1 finalized=true');
  assert(verifiedArLocalRes.idempotent_replay === false, 'First commit is not an idempotent replay');

  // Retry the exact same batch 1..3 with raw unverified finalityProof -> must NOT grant finalized: true without L1 gateway verification!
  const unverifiedL1ProofRes = await commitCheckpointBatch(batch1to3, {
    useAoConnect: true,
    environment: 'ao_network',
    finalityProof: { block_height: 1550000, confirmations: 12 },
    processId: 'PROC_TEST_01',
    messageFn: async () => 'msg_batch_1_3_unverified_l1',
    evaluatorFn: (m) => luaArbit.evaluate(m)
  });
  assert(
    unverifiedL1ProofRes.verified === true && unverifiedL1ProofRes.finalized === false,
    'Raw caller-injected finalityProof without gateway verification is NOT accepted as finalized=true'
  );

  // Retry the exact same batch 1..3 with authoritative Arweave L1 gateway verification (/tx/<id>/status & /tx/<id> full commitment tags)
  const retrySameBatchRes = await commitCheckpointBatch(batch1to3, {
    useAoConnect: true,
    environment: 'ao_network',
    minConfirmations: 5,
    l1Gateways: ['https://arweave.net', 'https://ar-io.dev'],
    l1GatewayFetchFn: async (url) => {
      if (url.endsWith('/status')) {
        return {
          status: 200,
          json: async () => ({
            block_height: 1550000,
            number_of_confirmations: 12,
            block_indep_hash: 'B'.repeat(48)
          })
        };
      }
      return {
        status: 200,
        json: async () => ({
          id: 'msg_batch_1_3_retry',
          target: 'PROC_TEST_01',
          tags: [
            { name_decoded: 'Action', value_decoded: 'CommitCheckpoint' },
            { name_decoded: 'Recipient', value_decoded: 'PROC_TEST_01' },
            { name_decoded: 'Batch-Id', value_decoded: verifiedArLocalRes.batch_id },
            { name_decoded: 'From-Seq', value_decoded: String(batch1to3.from_seq) },
            { name_decoded: 'To-Seq', value_decoded: String(batch1to3.to_seq) },
            { name_decoded: 'State-Root', value_decoded: String(batch1to3.proposed_state_root) },
            { name_decoded: 'Hash', value_decoded: String(batch1to3.proposed_hash) }
          ]
        })
      };
    },
    processId: 'PROC_TEST_01',
    messageFn: async () => 'msg_batch_1_3_retry',
    evaluatorFn: (m) => luaArbit.evaluate(m)
  });

  assert(retrySameBatchRes.ok === true, 'Idempotent retry of batch 1..3 succeeded');
  assert(retrySameBatchRes.verdict === 'DUPLICATE_BATCH_IDEMPOTENT', 'Arbitrator verdict is DUPLICATE_BATCH_IDEMPOTENT');
  assert(retrySameBatchRes.idempotent_replay === true, 'idempotent_replay is TRUE on duplicate batch retry');
  assert(retrySameBatchRes.verified === true, 'Duplicate batch is verified');
  assert(retrySameBatchRes.confirmed === true, 'Duplicate batch is confirmed');
  assert(retrySameBatchRes.finalized === true, 'With ao_network + multi-gateway L1 proof & full commitment tags, finalized is TRUE');
  assert(retrySameBatchRes.settlement_status === 'finalized', 'settlement_status is "finalized"');
  assert(retrySameBatchRes.batch_id === verifiedArLocalRes.batch_id, 'Batch-Id is deterministic and identical across retries');

  // Verify L1 finality rejects mismatched TX ID, mismatched Process ID, mismatched commitment tags, and multi-gateway disagreement
  const baseValidTxBody = {
    id: 'msg_l1_binding_test',
    target: 'PROC_TEST_01',
    tags: [
      { name_decoded: 'Action', value_decoded: 'CommitCheckpoint' },
      { name_decoded: 'Recipient', value_decoded: 'PROC_TEST_01' },
      { name_decoded: 'Batch-Id', value_decoded: verifiedArLocalRes.batch_id },
      { name_decoded: 'From-Seq', value_decoded: String(batch1to3.from_seq) },
      { name_decoded: 'To-Seq', value_decoded: String(batch1to3.to_seq) },
      { name_decoded: 'State-Root', value_decoded: String(batch1to3.proposed_state_root) },
      { name_decoded: 'Hash', value_decoded: String(batch1to3.proposed_hash) }
    ]
  };
  const validStatusBody = {
    block_height: 1550000,
    number_of_confirmations: 12,
    block_indep_hash: 'B'.repeat(48)
  };

  const txIdMismatchL1 = await verifyArweaveL1Finality('msg_l1_binding_test', verifiedArLocalRes.batch_id, {
    processId: 'PROC_TEST_01',
    expectedBatch: batch1to3,
    fetchFn: async (url) => ({
      status: 200,
      json: async () => (url.endsWith('/status') ? validStatusBody : { ...baseValidTxBody, id: 'wrong_tx_id_999' })
    })
  });
  assert(
    txIdMismatchL1.finalized === false && txIdMismatchL1.reason === 'L1_TX_ID_MISMATCH',
    'verifyArweaveL1Finality rejects L1 transaction when tx.id does not match expected message/bundle ID'
  );

  const procMismatchL1 = await verifyArweaveL1Finality('msg_l1_binding_test', verifiedArLocalRes.batch_id, {
    processId: 'PROC_TEST_01',
    expectedBatch: batch1to3,
    fetchFn: async (url) => ({
      status: 200,
      json: async () => (url.endsWith('/status') ? validStatusBody : { ...baseValidTxBody, target: 'WRONG_PROC_999' })
    })
  });
  assert(
    procMismatchL1.finalized === false && procMismatchL1.reason === 'L1_PROCESS_ID_MISMATCH',
    'verifyArweaveL1Finality rejects L1 transaction when target/Recipient does not match target AO Process ID'
  );

  const tagMismatchL1 = await verifyArweaveL1Finality('msg_l1_binding_test', verifiedArLocalRes.batch_id, {
    processId: 'PROC_TEST_01',
    expectedBatch: batch1to3,
    fetchFn: async (url) => ({
      status: 200,
      json: async () =>
        url.endsWith('/status')
          ? validStatusBody
          : {
              ...baseValidTxBody,
              tags: baseValidTxBody.tags.map((t) =>
                t.name_decoded === 'State-Root' ? { ...t, value_decoded: '999999999' } : t
              )
            }
    })
  });
  assert(
    tagMismatchL1.finalized === false && tagMismatchL1.reason === 'L1_COMMITMENT_TAGS_MISMATCH',
    'verifyArweaveL1Finality rejects L1 transaction when State-Root/Hash commitment tags mismatch batch'
  );

  const gatewaySplitBrainL1 = await verifyArweaveL1Finality('msg_l1_binding_test', verifiedArLocalRes.batch_id, {
    processId: 'PROC_TEST_01',
    expectedBatch: batch1to3,
    l1Gateways: ['https://arweave.net', 'https://ar-io.dev'],
    fetchFn: async (url) => ({
      status: 200,
      json: async () =>
        url.endsWith('/status')
          ? {
              ...validStatusBody,
              block_indep_hash: url.startsWith('https://arweave.net') ? 'A'.repeat(48) : 'C'.repeat(48)
            }
          : baseValidTxBody
    })
  });
  assert(
    gatewaySplitBrainL1.finalized === false && gatewaySplitBrainL1.reason === 'MULTI_GATEWAY_CONSENSUS_FAILED',
    'verifyArweaveL1Finality rejects L1 finality when multiple gateways disagree on block_indep_hash'
  );

  // Verify real Arweave cryptographic transaction signing & local HTTP gateway L1 verification
  const arweave = Arweave.init({ host: '127.0.0.1', port: 1984, protocol: 'http' });
  const walletPath = join(ROOT_DIR, 'secrets/wallet.json');
  const jwk = existsSync(walletPath)
    ? JSON.parse(readFileSync(walletPath, 'utf-8'))
    : await arweave.wallets.generate();
  const realTx = await arweave.createTransaction(
    {
      target: 'PROC_TEST_01'.padEnd(43, '0').slice(0, 43),
      data: JSON.stringify(batch1to3),
      last_tx: 'A'.repeat(43),
      reward: '1000000'
    },
    jwk
  );
  const realTargetProc = realTx.target;
  realTx.addTag('Action', 'CommitCheckpoint');
  realTx.addTag('Recipient', realTargetProc);
  realTx.addTag('Batch-Id', verifiedArLocalRes.batch_id);
  realTx.addTag('From-Seq', String(batch1to3.from_seq));
  realTx.addTag('To-Seq', String(batch1to3.to_seq));
  realTx.addTag('State-Root', String(batch1to3.proposed_state_root));
  realTx.addTag('Hash', String(batch1to3.proposed_hash));
  await arweave.transactions.sign(realTx, jwk);
  const cryptoVerified = await arweave.transactions.verify(realTx);
  assert(cryptoVerified === true, 'Real Arweave L1 transaction signed and RSA-PSS signature cryptographically verified');

  const localGatewayServer = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'application/json');
    if (req.url === `/tx/${realTx.id}/status`) {
      res.end(
        JSON.stringify({
          block_height: 1600123,
          number_of_confirmations: 15,
          block_indep_hash: 'D'.repeat(48)
        })
      );
      return;
    }
    if (req.url === `/tx/${realTx.id}`) {
      res.end(JSON.stringify(realTx));
      return;
    }
    res.statusCode = 404;
    res.end(JSON.stringify({ error: 'not_found' }));
  });
  await new Promise((r) => localGatewayServer.listen(0, '127.0.0.1', r));
  const gwPort = localGatewayServer.address().port;
  try {
    const realHttpL1Proof = await verifyArweaveL1Finality(realTx.id, verifiedArLocalRes.batch_id, {
      environment: 'ao_network',
      gatewayUrl: `http://127.0.0.1:${gwPort}`,
      processId: realTargetProc,
      expectedBatch: batch1to3,
      minConfirmations: 10
    });
    assert(
      realHttpL1Proof.finalized === true &&
        realHttpL1Proof.tx_id === realTx.id &&
        realHttpL1Proof.process_id === realTargetProc &&
        realHttpL1Proof.confirmations === 15,
      'Real signed Arweave L1 transaction with base64url tags verified over live HTTP gateway'
    );
    assert(
      realHttpL1Proof.trust_boundary === 'single_gateway_trusted_provider',
      'Single gateway proof is marked with trust_boundary=single_gateway_trusted_provider'
    );
    assert(
      realHttpL1Proof.multi_gateway_consensus === false,
      'Single gateway proof sets multi_gateway_consensus=false'
    );

    const singleGwReqMulti = await verifyArweaveL1Finality(realTx.id, verifiedArLocalRes.batch_id, {
      environment: 'ao_network',
      gatewayUrl: `http://127.0.0.1:${gwPort}`,
      requireMultiGateway: true,
      processId: realTargetProc,
      expectedBatch: batch1to3
    });
    assert(
      singleGwReqMulti.verified === false &&
        singleGwReqMulti.error_code === 'MULTI_GATEWAY_CONSENSUS_REQUIRED',
      'verifyArweaveL1Finality rejects single gateway when requireMultiGateway=true'
    );
  } finally {
    localGatewayServer.close();
  }

  // Verify Arbitrator response missing OfficialHash or OfficialStateRoot is rejected (no silent fallback to proposed values)
  const missingProofFieldsRes = verifyArbitratorEvaluation(
    {
      Messages: [
        {
          Status: 'COMMITTED',
          OfficialSeq: '3'
          // Missing OfficialHash and OfficialStateRoot!
        }
      ]
    },
    batch1to3
  );
  assert(
    missingProofFieldsRes.verified === false &&
      missingProofFieldsRes.confirmed === false &&
      missingProofFieldsRes.verdict === 'MISSING_ARBITRATOR_PROOF_FIELDS',
    'Arbitrator evaluation missing OfficialHash or OfficialStateRoot is rejected with MISSING_ARBITRATOR_PROOF_FIELDS'
  );

  // Verify CheckpointsCount in Arbitrator is still 1 (no duplicate checkpoint created!)
  const officialAfterRetry = await queryOfficialAoState({
    evaluatorFn: (m) => luaArbit.evaluate(m)
  });
  assert(officialAfterRetry.ok === true && officialAfterRetry.official_seq === 3, 'AO OfficialSeq remains 3 after duplicate batch retry');

  // Submit a tampered/conflicting batch for seq 1..3 -> must be rejected as EQUIVOCATION_PROVEN
  const conflictingBatch = {
    ...batch1to3,
    proposed_hash: 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'
  };
  const conflictRes = await commitCheckpointBatch(conflictingBatch, {
    useAoConnect: true,
    processId: 'PROC_TEST_01',
    messageFn: async () => 'msg_batch_1_3_conflict',
    evaluatorFn: (m) => luaArbit.evaluate(m)
  });
  assert(conflictRes.ok === false, 'Conflicting duplicate batch rejected');
  assert(conflictRes.settlement_status === 'rejected', 'settlement_status is "rejected"');
  assert(conflictRes.verdict === 'EQUIVOCATION_PROVEN', 'Verdict is EQUIVOCATION_PROVEN');
  assert(conflictRes.verified === false && conflictRes.confirmed === false, 'Rejected batch has verified=false and confirmed=false');

  // --------------------------------------------------------------------------
  console.log('\n[3] Durable Cursor Persistence & Daemon Restart Recovery:');
  // --------------------------------------------------------------------------
  const cursorFile = join(TEST_TMP_DIR, 'committer_cursor.json');
  const daemonLuaArbit = new LuaArbitratorHarness();

  const events4to5 = buildValidEvents(4, 2, events1to3[2].hash, BigInt(events1to3[2].state_root_str));
  const allWalEvents = [...events1to3, ...events4to5];

  // Mock SU HTTP server responder serving `allWalEvents`
  const mockSuFetch = async (url) => {
    if (url.endsWith('/status')) {
      return {
        status: 200,
        body: {
          ok: true,
          epoch: 1,
          latest_tick_ram: allWalEvents.length,
          state_root_str: allWalEvents[allWalEvents.length - 1].state_root_str,
          latest_hash: allWalEvents[allWalEvents.length - 1].hash
        }
      };
    }
    const u = new URL(url);
    const fromSeq = Number(u.searchParams.get('from_seq') || 1);
    const toSeq = Number(u.searchParams.get('to_seq') || allWalEvents.length);
    const slice = allWalEvents.filter((e) => e.seq >= fromSeq && e.seq <= toSeq);
    const lastEv = slice[slice.length - 1];
    return {
      status: 200,
      body: {
        ok: true,
        su_id: 'SU_NODE_1',
        epoch: 1,
        from_seq: fromSeq,
        to_seq: toSeq,
        proposed_hash: lastEv?.hash || '',
        proposed_state_root: lastEv?.state_root_str || '0',
        events_json: JSON.stringify(slice)
      }
    };
  };

  // 3A: If commit evaluation is pending (not verified), daemon MUST NOT advance cursor
  setCommittedCursor(null);
  const unverifiedCycle = await processCommitterCycle({
    cursorPath: cursorFile,
    batchMax: 3,
    fetchFn: mockSuFetch,
    useAoConnect: true,
    processId: 'PROC_DAEMON_01',
    messageFn: async () => 'msg_unverified_cycle',
    skipResultLookup: true
  });
  assert(unverifiedCycle.ok === false && unverifiedCycle.cursor_advanced === false, 'Daemon refused to advance cursor when evaluation was pending/unverified');
  assert(getCommittedCursor() === 0, 'In-memory cursor remained at 0');
  assert(!existsSync(cursorFile), 'Disk cursor file was not prematurely written on unverified submission');

  // 3B: Run cycle for seq 1..3 with real Lua Arbitrator verification -> cursor advances to 3 and persists to disk
  const verifiedCycle1 = await processCommitterCycle({
    cursorPath: cursorFile,
    batchMax: 3,
    fetchFn: mockSuFetch,
    useAoConnect: true,
    processId: 'PROC_DAEMON_01',
    messageFn: async () => 'msg_verified_cycle_1',
    evaluatorFn: (m) => daemonLuaArbit.evaluate(m)
  });
  assert(verifiedCycle1.ok === true && verifiedCycle1.cursor_advanced === true, 'Daemon advanced cursor after verified COMMITTED verdict');
  assert(verifiedCycle1.from_seq === 1 && verifiedCycle1.to_seq === 3, 'Cycle 1 committed seq 1..3');
  assert(verifiedCycle1.last_committed_seq === 3, 'last_committed_seq is 3');

  const diskAfterCycle1 = loadDurableCursor(cursorFile);
  assert(diskAfterCycle1.last_committed_seq === 3 && diskAfterCycle1.source === 'disk_cursor', 'Durable cursor file on disk stores last_committed_seq=3');

  // 3C: Simulate Daemon OS Restart (clear in-memory cursor) -> next cycle must resume from seq 4..5, NOT seq 1!
  setCommittedCursor(null);
  const verifiedCycle2 = await processCommitterCycle({
    cursorPath: cursorFile,
    batchMax: 3,
    fetchFn: mockSuFetch,
    useAoConnect: true,
    processId: 'PROC_DAEMON_01',
    messageFn: async () => 'msg_verified_cycle_2',
    evaluatorFn: (m) => daemonLuaArbit.evaluate(m)
  });
  assert(verifiedCycle2.ok === true && verifiedCycle2.cursor_advanced === true, 'Restarted daemon completed cycle 2');
  assert(
    verifiedCycle2.from_seq === 4 && verifiedCycle2.to_seq === 5,
    `Restarted daemon resumed from seq=4..5 without re-sending 1..3 (got ${verifiedCycle2.from_seq}..${verifiedCycle2.to_seq})`
  );
  assert(verifiedCycle2.last_committed_seq === 5, 'Durable cursor advanced to 5');

  // 3D: Simulate stale local cursor file (e.g., disk has seq=2, but AO Arbitrator OfficialSeq is 5)
  saveDurableCursor(
    {
      last_committed_seq: 2,
      last_committed_hash: events1to3[1].hash,
      last_state_root: events1to3[1].state_root_str,
      epoch: 1
    },
    cursorFile
  );
  setCommittedCursor(null);
  const reconciled = await reconcileCursorWithAO({
    cursorPath: cursorFile,
    evaluatorFn: (m) => daemonLuaArbit.evaluate(m)
  });
  assert(reconciled.last_committed_seq === 5, 'reconcileCursorWithAO synchronized stale disk cursor (2) up to AO OfficialSeq (5)');
  assert(reconciled.source === 'ao_official_reconciled', 'Reconciliation source marked as ao_official_reconciled');

  // 3E: Simulate diverged local cursor ahead of AO OfficialSeq (e.g., disk has seq=99, AO has seq=5)
  saveDurableCursor(
    {
      last_committed_seq: 99,
      last_committed_hash: events1to3[1].hash,
      last_state_root: events1to3[1].state_root_str,
      epoch: 1
    },
    cursorFile
  );
  setCommittedCursor(null);
  const divergedReconcile = await reconcileCursorWithAO({
    cursorPath: cursorFile,
    evaluatorFn: (m) => daemonLuaArbit.evaluate(m)
  });
  assert(
    divergedReconcile.mismatch_detected === true &&
      divergedReconcile.reconciliation_status === 'diverged_local_ahead_of_ao',
    'reconcileCursorWithAO detects and flags when local disk cursor (99) is ahead of AO OfficialSeq (5)'
  );
  setCommittedCursor(null);
  const divergedCycle = await processCommitterCycle({
    cursorPath: cursorFile,
    fetchFn: mockSuFetch,
    evaluatorFn: (m) => daemonLuaArbit.evaluate(m)
  });
  assert(
    divergedCycle.ok === false && divergedCycle.error_code === 'ERR_CURSOR_AHEAD_OF_AO_OFFICIAL_SEQ',
    'processCommitterCycle halts with ERR_CURSOR_AHEAD_OF_AO_OFFICIAL_SEQ when local cursor is ahead of AO'
  );

  // [4] State Root and Hash Verification on Matching Sequence (Fork/Corruption Fail-Closed)
  saveDurableCursor(
    {
      last_committed_seq: 5,
      last_committed_hash: 'CORRUPTED_OR_FORKED_LOCAL_HASH_00000000000000000000000000000000',
      last_state_root: '999999999',
      epoch: 1,
      last_batch_id: 'FORKED_BATCH_ID'
    },
    cursorFile
  );
  setCommittedCursor(null);

  const stateMismatchReconcile = await reconcileCursorWithAO({
    cursorPath: cursorFile,
    evaluatorFn: (m) => daemonLuaArbit.evaluate(m)
  });
  assert(
    stateMismatchReconcile.mismatch_detected === true &&
      stateMismatchReconcile.reconciliation_status === 'diverged_state_mismatch' &&
      stateMismatchReconcile.error_code === 'ERR_CURSOR_AO_STATE_MISMATCH',
    'reconcileCursorWithAO flags ERR_CURSOR_AO_STATE_MISMATCH when seq matches but hash or state root diverges'
  );

  setCommittedCursor(null);
  const stateMismatchCycle = await processCommitterCycle({
    cursorPath: cursorFile,
    fetchFn: mockSuFetch,
    evaluatorFn: (m) => daemonLuaArbit.evaluate(m)
  });
  assert(
    stateMismatchCycle.ok === false &&
      stateMismatchCycle.cursor_advanced === false &&
      stateMismatchCycle.error_code === 'ERR_CURSOR_AO_STATE_MISMATCH',
    'processCommitterCycle halts fail-closed on state mismatch without advancing cursor'
  );

  // [5] startCommitterDaemon Fail-Closed on Divergence
  let daemonFailedClosed = false;
  let daemonExitCode = null;
  const { startCommitterDaemon } = await import('../ao_process/committer.mjs');
  try {
    await startCommitterDaemon({
      cursorPath: cursorFile,
      evaluatorFn: (m) => daemonLuaArbit.evaluate(m),
      allowLocalAhead: false
    });
  } catch (err) {
    daemonExitCode = err.exitCode;
    if (err.message.includes('Committer daemon fail-closed')) {
      daemonFailedClosed = true;
    }
  }
  assert(daemonFailedClosed === true, 'startCommitterDaemon throws and halts fail-closed when state divergence is detected');
  assert(daemonExitCode === FATAL_FAIL_CLOSED_EXIT_CODE, `startCommitterDaemon sets exitCode=${FATAL_FAIL_CLOSED_EXIT_CODE} on fatal fail-closed`);

  // Verify committer_status.json records halted / failed_closed state
  const statusAfterHalt = readHeartbeatStatus(cursorFile);
  assert(
    statusAfterHalt &&
      statusAfterHalt.status === 'halted' &&
      statusAfterHalt.health === 'failed_closed',
    'Heartbeat status records status=halted and health=failed_closed on fatal divergence'
  );

  // [5B] Committer Mutual Exclusion via Cursor File Lock
  const lockResult1 = acquireCursorLock(cursorFile);
  assert(lockResult1.acquired === true, 'First committer worker acquires cursor lock cleanly');
  const lockResult2 = acquireCursorLock(cursorFile);
  assert(
    lockResult2.acquired === false && lockResult2.heldByPid === process.pid,
    'Second committer worker is rejected with lock held by active PID'
  );
  releaseCursorLock(cursorFile);
  const lockResult3 = acquireCursorLock(cursorFile);
  assert(lockResult3.acquired === true, 'Cursor lock can be re-acquired after release');
  releaseCursorLock(cursorFile);

  // [5C] Production Gate: Prohibit auto-creation of AO Arbitrator Process
  const prevProfile = process.env.HYDB_PROFILE;
  const prevEnvProc = process.env.AO_PROCESS_ID;
  delete process.env.AO_PROCESS_ID;
  process.env.HYDB_PROFILE = 'production';
  let autoCreateBlocked = false;
  try {
    await getOrCreateProcessId({ processIdFile: join(TEST_TMP_DIR, 'nonexistent_proc_id.txt') });
  } catch (err) {
    if (err.message.includes('ERR_AO_PROCESS_ID_REQUIRED')) {
      autoCreateBlocked = true;
    }
  }
  process.env.HYDB_PROFILE = prevProfile;
  if (prevEnvProc !== undefined) process.env.AO_PROCESS_ID = prevEnvProc;
  assert(
    autoCreateBlocked === true,
    'getOrCreateProcessId rejects silent auto-creation in production mode (ERR_AO_PROCESS_ID_REQUIRED)'
  );

  // [5D] Committer Disabled via HYDB_COMMITTER_ENABLED=false
  process.env.HYDB_COMMITTER_ENABLED = 'false';
  const disabledRes = await startCommitterDaemon({ cursorPath: cursorFile });
  delete process.env.HYDB_COMMITTER_ENABLED;
  assert(
    disabledRes.ok === true && disabledRes.status === 'disabled',
    'startCommitterDaemon cleanly exits with status=disabled when HYDB_COMMITTER_ENABLED=false'
  );

  // [5E] SD Node Launcher Native In-BEAM Committer Verification (No Node.js Sidecar Required)
  const startSdScript = join(ROOT_DIR, 'scripts/start-sd-node.sh');
  const mockRunDir = join(TEST_TMP_DIR, 'mock_sd_run');
  mkdirSync(mockRunDir, { recursive: true });

  const beamCheck = spawnSync('bash', [startSdScript, '--check-committer'], {
    env: {
      ...process.env,
      HYDB_COMMITTER_ENABLED: 'true',
      NODE_SD_RUN_DIR: mockRunDir
    },
    encoding: 'utf-8'
  });
  const combinedOutput = `${beamCheck.stdout || ''}\n${beamCheck.stderr || ''}`;
  assert(
    beamCheck.status === 0 &&
      combinedOutput.includes('AO Committer is enabled natively inside HyperBEAM (sd_committer)'),
    'start-sd-node.sh --check-committer confirms native In-BEAM committer without external Node.js sidecar'
  );

  // [6] Daemon Default Option Propagation to commitCheckpointBatch
  let capturedCommitOptions = null;
  const mockCommitFn = async (batch, opts) => {
    capturedCommitOptions = opts;
    return { ok: false, verified: false, reason: 'mock_test_inspection' };
  };
  setCommittedCursor(5);
  process.env.VERIFY_L1_FINALITY = 'true';
  process.env.L1_GATEWAYS = 'https://arweave.net,https://ar-io.dev';
  process.env.REQUIRE_MULTI_GATEWAY = 'true';
  process.env.MIN_CONFIRMATIONS = '3';

  // Advance SU tick so a batch is generated
  const event6 = buildValidEvents(6, 1, allWalEvents[4].hash, BigInt(allWalEvents[4].state_root_str))[0];
  allWalEvents.push(event6);
  await processCommitterCycle({
    cursorPath: cursorFile,
    fetchFn: mockSuFetch,
    commitFn: mockCommitFn,
    allowLocalAhead: true
  });
  delete process.env.VERIFY_L1_FINALITY;
  delete process.env.L1_GATEWAYS;
  delete process.env.REQUIRE_MULTI_GATEWAY;
  delete process.env.MIN_CONFIRMATIONS;

  assert(
    capturedCommitOptions !== null &&
      capturedCommitOptions.verifyL1Finality === true &&
      Array.isArray(capturedCommitOptions.l1Gateways) &&
      capturedCommitOptions.l1Gateways.length === 2 &&
      capturedCommitOptions.requireMultiGateway === true &&
      capturedCommitOptions.minConfirmations === 3,
    'processCommitterCycle propagates L1 finality & multi-gateway consensus options to commitCheckpointBatch'
  );

  // [7] Production Hardening: Fatal Fail-Closed Errors, Startup AO Unreachable Gate, & Access Control
  console.log('\n[7] Production Hardening: Fatal Error Discrimination & Access Control:');

  // 7A. Error Classification: Fatal Fail-Closed vs Transient Network Retries
  const fatalErr1 = new Error('Fatal: AO_PROCESS_ID required in production');
  fatalErr1.code = 'ERR_AO_PROCESS_ID_REQUIRED';
  assert(isFatalFailClosedError(fatalErr1) === true, 'ERR_AO_PROCESS_ID_REQUIRED is classified as fatal fail-closed');

  const fatalErr2 = new Error('State unreachable');
  fatalErr2.code = 'ERR_AO_STATE_UNREACHABLE';
  assert(isFatalFailClosedError(fatalErr2) === true, 'ERR_AO_STATE_UNREACHABLE is classified as fatal fail-closed');

  const fatalErr3 = new Error('Local cursor state diverges');
  fatalErr3.code = 'ERR_CURSOR_AO_STATE_MISMATCH';
  assert(isFatalFailClosedError(fatalErr3) === true, 'ERR_CURSOR_AO_STATE_MISMATCH is classified as fatal fail-closed');

  const fatalErr4 = new Error('Lock conflict');
  fatalErr4.code = 'ERR_COMMITTER_LOCK_HELD';
  assert(isFatalFailClosedError(fatalErr4) === true, 'ERR_COMMITTER_LOCK_HELD is classified as fatal fail-closed');

  const transientErr = new Error('Fetch failed: connect ECONNREFUSED 127.0.0.1:8735');
  transientErr.code = 'ECONNREFUSED';
  assert(isFatalFailClosedError(transientErr) === false, 'Network timeout/refusal (ECONNREFUSED) is classified as transient retry');

  // 7B. Strict Production Startup Gate: Unreachable AO State Triggers Fail-Closed
  const strictReconcileRes = await reconcileCursorWithAO({
    cursorPath: cursorFile,
    strictReconcile: true,
    queryOfficialStateFn: async () => ({ ok: false, status: 'error', reason: 'AO Compute Unit timeout' })
  });
  assert(
    strictReconcileRes.mismatch_detected === true &&
      strictReconcileRes.reconciliation_status === 'diverged_ao_unreachable' &&
      strictReconcileRes.error_code === 'ERR_AO_STATE_UNREACHABLE',
    'Startup reconciliation in strict/production mode halts fail-closed if authoritative AO state is unreachable'
  );

  // 7C. Contract Access Restriction in arbitrator.lua
  const luaSource = readFileSync(join(ROOT_DIR, 'ao_process/arbitrator.lua'), 'utf-8');
  const suWorkerAddr = 'yWLs9VNxqM1R3tUQpVNtu0_1J-Z3zWt371kIUrsTMOg';
  const operatorAddr = 'Davi6NRutvUMmKFQw0zOy_-n1Hjdm2KswfiHVjwx8wI';

  // Extract AuthorizedAdmins table
  const adminsMatch = luaSource.match(/AuthorizedAdmins\s*=\s*AuthorizedAdmins\s*or\s*\{([^}]+)\}/);
  assert(adminsMatch !== null, 'AuthorizedAdmins table exists in arbitrator.lua');
  const adminsTableContent = adminsMatch ? adminsMatch[1] : '';

  assert(!adminsTableContent.includes(suWorkerAddr), 'AuthorizedAdmins does NOT grant admin permissions to SU worker node');
  assert(adminsTableContent.includes(operatorAddr), 'AuthorizedAdmins includes designated Operator wallet');

  // Extract AuthorizedNodes table
  const nodesMatch = luaSource.match(/AuthorizedNodes\s*=\s*AuthorizedNodes\s*or\s*\{([^}]+)\}/);
  assert(nodesMatch !== null, 'AuthorizedNodes table exists in arbitrator.lua');
  const nodesTableContent = nodesMatch ? nodesMatch[1] : '';
  assert(nodesTableContent.includes(suWorkerAddr), 'AuthorizedNodes includes authorized SU worker node');

  // 7D. AO Network Mainnet Guard in deployAoProcess
  let mainnetGuardBlocked = false;
  try {
    await deployAoProcess({
      network: 'mainnet',
      dryRun: true,
      moduleId: '',
      scheduler: '',
      walletPath: join(ROOT_DIR, 'secrets/wallet.json')
    });
  } catch (err) {
    if (err.message.includes('Testnet module/scheduler IDs cannot be used on mainnet')) {
      mainnetGuardBlocked = true;
    }
  }
  assert(
    mainnetGuardBlocked === true,
    'deployAoProcess refuses to default to testnet module/scheduler when target network is mainnet'
  );

  console.log('\n========================================================================');
  console.log(` Phase 06 AO Settlement & Cursor Results: ${passed} PASSED, ${failed} FAILED`);
  console.log('========================================================================\n');
}

runPhase06Tests()
  .catch((err) => {
    console.error('\nFatal Phase 06 Error:', err);
    process.exit(1);
  })
  .finally(() => {
    try {
      rmSync(TEST_TMP_DIR, { recursive: true, force: true });
    } catch {}
  });
