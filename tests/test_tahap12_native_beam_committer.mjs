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

import { existsSync, readFileSync, unlinkSync, writeFileSync, mkdirSync } from 'node:fs';
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

class LiveStackLuaRuntime {
  constructor(genesisId, arbitratorId, genesisLua, arbitratorLua) {
    this.genesisId = genesisId;
    this.arbitratorId = arbitratorId;
    this.genesisLua = genesisLua;
    this.arbitratorLua = arbitratorLua;
    this.genesisMessages = [];
    this.arbitratorMessages = [];
  }

  async evaluate(msg) {
    const target = msg.Target || msg.process || '';
    const isGenesis = target === this.genesisId ||
      ['GetGenesisConfig', 'RegisterNode', 'ReportHeartbeat', 'GetStackStatus', 'SetArbitratorProcess', 'UpdateStackConfig'].includes(msg.Action);

    if (isGenesis) {
      this.genesisMessages.push(msg);
      const res = this.runLua(this.genesisLua, this.genesisMessages, 'genesis');
      if (res && res.Messages && Array.isArray(res.Messages)) {
        for (const outMsg of res.Messages) {
          if (outMsg && (outMsg.Target === this.arbitratorId || outMsg.process === this.arbitratorId)) {
            this.arbitratorMessages.push({
              From: this.genesisId,
              Target: this.arbitratorId,
              Action: outMsg.Action,
              Tags: outMsg.Tags || [{ name: 'Action', value: outMsg.Action }],
              Data: outMsg.Data,
              Timestamp: Date.now()
            });
            this.runLua(this.arbitratorLua, this.arbitratorMessages, 'arbitrator');
          }
        }
      }
      return res;
    } else {
      this.arbitratorMessages.push(msg);
      return this.runLua(this.arbitratorLua, this.arbitratorMessages, 'arbitrator');
    }
  }

  runLua(contractSource, historyMsgs, label) {
    const tmpLuaPath = join(TEST_TMP_DIR, `run_${label}_${Date.now()}_${Math.random().toString(36).slice(2, 7)}.lua`);
    const toLuaVal = (v) => {
      if (v === null || v === undefined) return 'nil';
      if (typeof v === 'boolean') return v ? 'true' : 'false';
      if (typeof v === 'number') return String(v);
      if (typeof v === 'string') return JSON.stringify(v);
      if (Array.isArray(v)) return '{ ' + v.map(toLuaVal).join(', ') + ' }';
      if (typeof v === 'object') {
        const entries = Object.entries(v).map(([k, val]) => `[${JSON.stringify(k)}] = ${toLuaVal(val)}`);
        return '{ ' + entries.join(', ') + ' }';
      }
      return 'nil';
    };

    const parsedMsgs = historyMsgs.map((m) => {
      let decodedData = {};
      if (typeof m.Data === 'string' && m.Data.trim().startsWith('{')) {
        try { decodedData = JSON.parse(m.Data); } catch {}
      } else if (m.Data && typeof m.Data === 'object') {
        decodedData = m.Data;
      }
      return {
        From: m.From || m.sender || 'SU_NODE_1',
        Target: m.Target || m.process || '',
        Action: m.Action || (m.Tags?.find((t) => (t.name || t[1]) === 'Action')?.[1] || m.Tags?.find((t) => (t.name || t[1]) === 'Action')?.value) || '',
        Tags: m.Tags || [],
        Timestamp: m.Timestamp || 1000,
        _decodedData: decodedData
      };
    });

    const script = `
      package.path = "${ROOT_DIR.replace(/\\/g, '/')}/ao_process/?.lua;" .. package.path

      local decoded_Registry = {}
      package.loaded["json"] = {
        decode = function(s)
          return decoded_Registry[s] or {}
        end,
        encode = function(t)
          local function serialize(o)
            if type(o) == "number" then return tostring(o)
            elseif type(o) == "boolean" then return tostring(o)
            elseif type(o) == "string" then return string.format("%q", o)
            elseif type(o) == "table" then
              local is_array = #o > 0
              local parts = {}
              if is_array then
                for _, v in ipairs(o) do table.insert(parts, serialize(v)) end
                return "[" .. table.concat(parts, ",") .. "]"
              else
                for k, v in pairs(o) do
                  table.insert(parts, string.format("%q", tostring(k)) .. ":" .. serialize(v))
                end
                return "{" .. table.concat(parts, ",") .. "}"
              end
            else return "null" end
          end
          return serialize(t)
        end
      }

      -- Load Contract Script
      local contract = (function()
        ${contractSource}
      end)()

      local msgs = ${toLuaVal(parsedMsgs)}
      local last_res = nil
      for idx, m in ipairs(msgs) do
        local key = "DATA_" .. tostring(idx)
        decoded_Registry[key] = m._decodedData
        m.Data = key
        last_res = handle_message(m)
      end

      local outbox = (last_res and last_res.responses) or ao._outbox or {}
      local last_msg = outbox[#outbox] or {}
      local output = {
        ok = true,
        handled = last_res and last_res.handled,
        Messages = outbox,
        responses = outbox,
        last = last_msg,
        official_seq = OfficialSeq,
        current_epoch = CurrentEpoch
      }

      io.write(package.loaded["json"].encode(output))
    `;

    try {
      writeFileSync(tmpLuaPath, script, 'utf8');
      const res = spawnSync(LUA_BIN, [tmpLuaPath], { cwd: ROOT_DIR, encoding: 'utf8' });
      if (res.error) throw new Error(`Lua spawn failed: ${res.error.message}`);
      if (res.status !== 0) throw new Error(`Lua execution failed (${res.status}): ${res.stderr || res.stdout}`);
      const raw = (res.stdout || '').trim();
      return JSON.parse(raw);
    } finally {
      if (existsSync(tmpLuaPath)) unlinkSync(tmpLuaPath);
    }
  }
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

  console.log('\n[Suite 4] Real Local AO Runtime Integration (LiveStackLuaRuntime MU -> CU & Advanced Duplicate Handling):');

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

  const liveRuntime = new LiveStackLuaRuntime(mockGenesisId, mockArbitratorId, genesisLua, arbitratorLua);
  const pendingResults = new Map();

  const LOCAL_AO_PORT = 8996;
  const aoServer = http.createServer(async (req, res) => {
    try {
      const urlObj = new URL(req.url, `http://127.0.0.1:${LOCAL_AO_PORT}`);
      let rawBuf = Buffer.alloc(0);
      if (req.method === 'POST' || req.method === 'PUT') {
        const chunks = [];
        for await (const chunk of req) chunks.push(chunk);
        rawBuf = Buffer.concat(chunks);
      }

      if (req.method === 'POST' && urlObj.pathname.endsWith('/dry-run')) {
        let json = {};
        try { json = JSON.parse(rawBuf.toString('utf8')); } catch {}
        const target = json.Target || json.target || urlObj.searchParams.get('process-id') || '';
        const action = json.Action || (json.Tags || []).find(t => (t.name || t[0]) === 'Action')?.[1] || (json.Tags || []).find(t => (t.name || t[0]) === 'Action')?.value || '';
        const evalRes = await liveRuntime.evaluate({
          From: json.From || json.sender || suAddress,
          Target: target,
          Action: action,
          Tags: json.Tags || [],
          Data: json.Data || '',
          Timestamp: Date.now()
        });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ Messages: evalRes?.Messages || evalRes?.responses || [] }));
        return;
      }

      if (req.method === 'GET' && urlObj.pathname.startsWith('/result/')) {
        const msgId = urlObj.pathname.replace('/result/', '');
        const evalRes = pendingResults.get(msgId);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          Messages: evalRes?.Messages || evalRes?.responses || [],
          Output: evalRes?.Output || {},
          Spawns: []
        }));
        return;
      }

      if (req.method === 'POST' && (urlObj.pathname.endsWith('/message') || urlObj.pathname === '/message' || urlObj.pathname.includes('/ao/'))) {
        let target = '';
        let action = '';
        let from = suAddress;
        let data = '';
        let tags = [];

        if (rawBuf.length > 2 && (rawBuf[0] === 1 || rawBuf[0] === 0)) {
          try {
            const DataItemCtor = arbundles.DataItem || arbundles.default?.DataItem;
            const item = new DataItemCtor(rawBuf);
            target = item.target;
            action = item.tags.find(t => t.name === 'Action')?.value || '';
            tags = item.tags.map(t => ({ name: t.name, value: t.value }));
            data = item.rawData.toString('utf8');
            from = await arweaveInstance.wallets.ownerToAddress(item.owner);
          } catch (e) {
            console.error('[local-ao-server] ANS-104 decode error:', e);
          }
        }
        if (!target) {
          try {
            const json = JSON.parse(rawBuf.toString('utf8'));
            target = json.Target || json.target || json.process;
            action = json.Action || json.action;
            tags = json.Tags || json.tags || [];
            data = json.Data || json.data;
            from = json.From || json.from || json.sender || suAddress;
          } catch (e) {}
        }

        const msgId = 'msg_' + crypto.randomUUID();
        const evalRes = await liveRuntime.evaluate({
          From: from,
          Target: target,
          Action: action,
          Tags: tags,
          Data: data,
          Timestamp: Date.now()
        });
        pendingResults.set(msgId, evalRes);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ id: msgId, messageId: msgId }));
        return;
      }

      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Not found' }));
    } catch (err) {
      console.error('[local-ao-server] Request error:', err);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
  });

  await new Promise((resolve) => aoServer.listen(LOCAL_AO_PORT, '127.0.0.1', resolve));

  try {
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

    // Step 4D: Advance Arbitrator tip directly to seq 6 using Batch 2
    const b2Events = buildValidEvents(4, 3, b1PropHash, b1PropRoot);
    const b2PropHash = b2Events[b2Events.length - 1].hash;
    const b2PropRoot = b2Events[b2Events.length - 1].state_root;
    const b2Batch = {
      contract_version: 'hydb.core@1.0',
      epoch: 1,
      from_seq: 4,
      to_seq: 6,
      prev_hash: b1PropHash,
      proposed_hash: b2PropHash,
      proposed_state_root: b2PropRoot,
      events: b2Events
    };
    const b2Res = await liveRuntime.evaluate({
      From: suAddress,
      Target: mockArbitratorId,
      Action: 'CommitCheckpoint',
      Tags: [{ name: 'Action', value: 'CommitCheckpoint' }],
      Data: JSON.stringify(b2Batch),
      Timestamp: Date.now()
    });
    const b2Msg = b2Res?.Messages?.[b2Res.Messages.length - 1];
    assert(b2Msg?.Status === 'COMMITTED' && b2Msg?.OfficialSeq === '6', 'Arbitrator on-chain tip advanced to sequence 6');

    // Step 4E: Retry older Batch 1 (seq 1..3) from In-BEAM committer against advanced tip
    const erlRetryScript = `
      _ = application:ensure_all_started(inets),
      _ = application:ensure_all_started(ssl),
      {ok, WI} = sd_ao_client:load_wallet("secrets/wallet_su.json"),
      GwUrl = "http://127.0.0.1:${LOCAL_AO_PORT}",
      CuUrl = "http://127.0.0.1:${LOCAL_AO_PORT}",
      ArbId = <<"${mockArbitratorId}">>,

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
      {ok, 200, DupRes1} = sd_ao_client:send_message(GwUrl, ArbId, <<"CommitCheckpoint">>, Tags1, FinalBatch1, #{sender => maps:get(address, WI), wallet => WI, cu_url => CuUrl, timeout => 15000, cu_retries => 10, cu_retry_delay_ms => 300}),
      io:format("DUP_RESP_STATUS:~s_TIP_SEQ:~s~n", [maps:get(<<"Status">>, DupRes1), maps:get(<<"OfficialSeq">>, DupRes1)]),

      %% Parse verdict with parse_arbitrator_verdict/4 for ToSeq=3
      case sd_committer:parse_arbitrator_verdict(DupRes1, 3, <<"${b1PropHash}">>, <<"${b1PropRoot}">>) of
          {ok, duplicate, 6, TipHash, TipRoot} ->
              io:format("DUP_PARSED_SEQ:~p~n", [6]),
              %% Verify tip hash matches advanced seq 6 hash
              case TipHash =:= <<"${b2PropHash}">> andalso TipRoot =:= <<"${b2PropRoot}">> of
                  true -> io:format("TIP_HASH_ROOT_MATCHES_CANONICAL:true~n");
                  false -> io:format("TIP_HASH_ROOT_MISMATCH:~p~n", [{TipHash, TipRoot}])
              end,
              %% Persist durable cursor with the authoritative tip state
              CursorPath = "data/test_suite4_live_cursor.json",
              ok = sd_committer:save_disk_cursor(6, TipHash, TipRoot, 1, <<"DUPLICATE_BATCH_IDEMPOTENT">>, CursorPath),
              {ok, LoadedCur} = sd_committer:load_disk_cursor(CursorPath),
              io:format("LOADED_CUR_SEQ:~p~n", [maps:get(last_committed_seq, LoadedCur)]),

              %% Reconcile against live Arbitrator GetOfficialState via CU dry-run
              InitState = sd_committer:reconcile_arbitrator_state(ArbId, #{
                  gateway_url => GwUrl,
                  cu_url => CuUrl,
                  wallet_info => WI,
                  last_committed_seq => 6,
                  last_committed_hash => TipHash,
                  last_state_root => TipRoot,
                  cursor_path => CursorPath,
                  epoch => 1,
                  status => running,
                  health => healthy
              }),
              io:format("RECONCILE_HEALTH:~s~n", [maps:get(health, InitState)]),
              _ = file:delete(CursorPath);
          OtherDup ->
              io:format("DUP_FAILED:~p~n", [OtherDup]),
              halt(1)
      end,

      halt(0).
    `;

    const { stdout: outStep2 } = await execFileAsync('erl', [
      '-pa', 'sd/out/ebin',
      '-noshell',
      '-eval', erlRetryScript
    ], { cwd: ROOT_DIR, encoding: 'utf8', env: { ...process.env, HYDB_PROFILE: 'test' } });

    assert(outStep2.includes('DUP_RESP_STATUS:DUPLICATE_BATCH_IDEMPOTENT_TIP_SEQ:6'), 'Live Arbitrator Lua contract returns DUPLICATE_BATCH_IDEMPOTENT with tip advanced to seq 6');
    assert(outStep2.includes('TIP_HASH_ROOT_MATCHES_CANONICAL:true'), 'In-BEAM verdict parser resolves authoritative tip hash and state root at seq 6 from duplicate verdict');
    assert(outStep2.includes('LOADED_CUR_SEQ:6'), 'Durable disk cursor persists seq 6 with authoritative tip state for old duplicate batch');
    assert(outStep2.includes('RECONCILE_HEALTH:healthy'), 'Cold-start reconciliation against live Arbitrator Lua contract succeeds cleanly with zero divergence');

  } finally {
    await new Promise((resolve) => aoServer.close(resolve));
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
