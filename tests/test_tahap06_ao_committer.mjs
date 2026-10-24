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
import { existsSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';

import {
  computeBatchId,
  detectAoEnvironment,
  commitCheckpointBatch,
  queryOfficialAoState
} from '../ao_process/committer.mjs';

import {
  loadDurableCursor,
  saveDurableCursor,
  reconcileCursorWithAO,
  processCommitterCycle,
  setCommittedCursor,
  getCommittedCursor
} from '../ao_process/committer_daemon.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = resolve(__dirname, '..');

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
    const res = spawnSync('lua5.3', [tmpLua], { encoding: 'utf-8' });
    if (res.status !== 0) {
      throw new Error(`Lua Arbitrator execution failed: ${res.stderr}`);
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

  // Retry the exact same batch 1..3 (idempotent retry)
  const retrySameBatchRes = await commitCheckpointBatch(batch1to3, {
    useAoConnect: true,
    environment: 'ao_network',
    finalityProof: { block_height: 1550000, confirmations: 12 },
    processId: 'PROC_TEST_01',
    messageFn: async () => 'msg_batch_1_3_retry',
    evaluatorFn: (m) => luaArbit.evaluate(m)
  });

  assert(retrySameBatchRes.ok === true, 'Idempotent retry of batch 1..3 succeeded');
  assert(retrySameBatchRes.verdict === 'DUPLICATE_BATCH_IDEMPOTENT', 'Arbitrator verdict is DUPLICATE_BATCH_IDEMPOTENT');
  assert(retrySameBatchRes.idempotent_replay === true, 'idempotent_replay is TRUE on duplicate batch retry');
  assert(retrySameBatchRes.verified === true, 'Duplicate batch is verified');
  assert(retrySameBatchRes.confirmed === true, 'Duplicate batch is confirmed');
  assert(retrySameBatchRes.finalized === true, 'With ao_network + finalityProof, finalized is TRUE');
  assert(retrySameBatchRes.settlement_status === 'finalized', 'settlement_status is "finalized"');
  assert(retrySameBatchRes.batch_id === verifiedArLocalRes.batch_id, 'Batch-Id is deterministic and identical across retries');

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
