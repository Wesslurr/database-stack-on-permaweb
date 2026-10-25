#!/usr/bin/env node
/**
 * Autonomous Background Committer Daemon — Lapisan 4 / AO Bridge (Phase 06)
 * =========================================================================
 * Normative Guarantees (Phase 06):
 * 1. Durable Cursor Persistence:
 *    - Persists `last_committed_seq`, `last_committed_hash`, `last_state_root`,
 *      `epoch`, and `last_batch_id` atomically to disk (`HYDB_COMMITTER_CURSOR_PATH`).
 *    - Never blindly assumes `lastCommittedSeq = 0` on daemon restart.
 * 2. Startup Reconciliation with AO Official Checkpoint State:
 *    - Reconciles disk cursor with `GetOfficialState` on the AO Arbitrator process
 *      upon startup or recovery (`Math.max(diskCursor.last_committed_seq, officialSeq)`).
 * 3. Strict Verification Gate Before Cursor Advancement:
 *    - Only advances `lastCommittedSeq` when `commitCheckpointBatch` returns
 *      `verified: true` (verdict `COMMITTED` or `DUPLICATE_BATCH_IDEMPOTENT`).
 *    - If settlement status is `submitted` (`evaluation_status: "pending"` / `"unknown"`)
 *      or `rejected`, the cursor is NOT advanced, preserving batch retry safety.
 * 4. Idempotent Batch Retry:
 *    - Retries use deterministic `(from_seq, to_seq, proposed_hash)` batch identity.
 */

import {
  readFileSync,
  writeFileSync,
  renameSync,
  unlinkSync,
  existsSync,
  mkdirSync,
  openSync,
  fsyncSync,
  closeSync
} from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join, basename } from 'node:path';
import crypto from 'node:crypto';
import {
  commitCheckpointBatch,
  computeBatchId,
  queryOfficialAoState,
  getOrCreateProcessId,
  isTestProfile,
  resolveDefaultWalletPath,
  getWallet,
  createDataItemSigner,
  getAoInstance
} from './committer.mjs';

export const FATAL_FAIL_CLOSED_EXIT_CODE = 78;

/**
 * Checks whether an error is an unrecoverable configuration/state divergence error
 * that must cause the daemon to fail closed immediately (exit code 78) instead of retrying.
 */
export function isFatalFailClosedError(err) {
  if (!err) return false;
  if (err.exitCode === FATAL_FAIL_CLOSED_EXIT_CODE) return true;
  const fatalCodes = [
    'ERR_AO_PROCESS_ID_REQUIRED',
    'ERR_INVALID_PROCESS_ID',
    'ERR_CURSOR_AO_STATE_MISMATCH',
    'ERR_CURSOR_AHEAD_OF_AO_OFFICIAL_SEQ',
    'ERR_AO_STATE_UNREACHABLE',
    'ERR_COMMITTER_LOCK_HELD',
    'ERR_UNAUTHORIZED_NODE',
    'ERR_UNAUTHORIZED_ADMIN',
    'ERR_FRAUD_PROOF_DETECTED',
    'ERR_GENESIS_HASH_MISMATCH',
    'ERR_ARBITRATOR_NOT_RESOLVED_FROM_GENESIS',
    'ERR_GENESIS_STATE_UNREACHABLE'
  ];
  if (err.code && fatalCodes.includes(err.code)) return true;
  const msg = String(err.message || '');
  if (
    msg.includes('ERR_AO_PROCESS_ID_REQUIRED') ||
    msg.includes('Auto-creation of AO processes is strictly prohibited in production') ||
    msg.includes('fail-closed') ||
    msg.includes('REJECTED_UNAUTHORIZED_NODE') ||
    msg.includes('REJECTED_UNAUTHORIZED_ADMIN') ||
    msg.includes('ERR_GENESIS_HASH_MISMATCH') ||
    msg.includes('ERR_ARBITRATOR_NOT_RESOLVED_FROM_GENESIS')
  ) {
    return true;
  }
  return false;
}

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
const WALLET_PATH = resolveDefaultWalletPath();

export function resolveDefaultCursorPath() {
  if (process.env.HYDB_COMMITTER_CURSOR_PATH) {
    return process.env.HYDB_COMMITTER_CURSOR_PATH;
  }
  if (!isTestProfile()) {
    return join(DB_DIR, 'data/committer/committer_cursor.json');
  }
  return join(DB_DIR, 'ao_process/committer_cursor.json');
}

const DEFAULT_CURSOR_PATH = resolveDefaultCursorPath();

function isProcessAlive(pid) {
  if (!pid || typeof pid !== 'number' || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return e.code === 'EPERM';
  }
}

/**
 * Acquire local mutual-exclusion lock for the committer cursor directory.
 *
 * ARCHITECTURAL NOTICE (Fail-Closed Fencing):
 * This is a local OS process lock ensuring only a single committer daemon operates against
 * a designated storage unit directory at any time on this host. It is NOT a distributed fencing lease.
 * In a multi-replica cluster (sd-node-1, sd-node-2, sd-node-3), only the designated committer owner
 * (HYDB_COMMITTER_ENABLED=true on sd-node-1) should run the committer. Running active committers across
 * multiple nodes without a distributed coordinator lease is prohibited to prevent split-brain.
 */
export function acquireCursorLock(cursorPath = DEFAULT_CURSOR_PATH) {
  const dir = dirname(cursorPath);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  const lockPath = join(dir, 'committer.lock');
  try {
    const fd = openSync(lockPath, 'wx');
    const payload = JSON.stringify({ pid: process.pid, acquired_at: new Date().toISOString() });
    writeFileSync(fd, payload, 'utf-8');
    fsyncSync(fd);
    closeSync(fd);
    return { acquired: true, lockPath };
  } catch (err) {
    if (err.code === 'EEXIST') {
      try {
        const raw = readFileSync(lockPath, 'utf-8');
        const lockData = JSON.parse(raw);
        const lockPid = Number(lockData?.pid);
        if (lockPid && isProcessAlive(lockPid)) {
          return {
            acquired: false,
            lockPath,
            heldByPid: lockPid,
            reason: `Committer daemon lock held by active PID ${lockPid}`
          };
        }
        // Stale lock detected (process died without unlocking)
        console.warn(`[CommitterDaemon] Recovering stale lockfile from dead PID ${lockPid}`);
        const fd = openSync(lockPath, 'w');
        const payload = JSON.stringify({
          pid: process.pid,
          acquired_at: new Date().toISOString(),
          recovered_from_stale_pid: lockPid
        });
        writeFileSync(fd, payload, 'utf-8');
        fsyncSync(fd);
        closeSync(fd);
        return { acquired: true, lockPath, recoveredStale: true };
      } catch (parseErr) {
        return { acquired: false, lockPath, reason: `Could not inspect existing lock: ${parseErr.message}` };
      }
    }
    throw err;
  }
}

export function releaseCursorLock(cursorPath = DEFAULT_CURSOR_PATH) {
  try {
    const dir = dirname(cursorPath);
    const lockPath = join(dir, 'committer.lock');
    if (existsSync(lockPath)) {
      try {
        const raw = readFileSync(lockPath, 'utf-8');
        const lockData = JSON.parse(raw);
        if (Number(lockData?.pid) === process.pid) {
          unlinkSync(lockPath);
        }
      } catch {
        unlinkSync(lockPath);
      }
    }
  } catch {
    // Non-fatal
  }
}

export function writeHeartbeatStatus(statusData, cursorPath = DEFAULT_CURSOR_PATH) {
  try {
    const dir = dirname(cursorPath);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
    const statusFile = join(dir, 'committer_status.json');
    const payload = {
      pid: process.pid,
      updated_at: new Date().toISOString(),
      ...statusData
    };
    writeFileSync(statusFile, JSON.stringify(payload, null, 2), 'utf-8');
  } catch {
    // Non-fatal
  }
}

export function readHeartbeatStatus(cursorPath = DEFAULT_CURSOR_PATH) {
  try {
    const dir = dirname(cursorPath);
    const statusFile = join(dir, 'committer_status.json');
    if (!existsSync(statusFile)) return null;
    return JSON.parse(readFileSync(statusFile, 'utf-8'));
  } catch {
    return null;
  }
}

const args = process.argv.slice(2);
function getArg(flag, envKey, def) {
  const idx = args.indexOf(flag);
  if (idx !== -1 && args[idx + 1]) return args[idx + 1];
  return process.env[envKey] || def;
}

const SU_SCHEDULER_URL = getArg('--su-url', 'SU_URL', 'http://127.0.0.1:8735/~sd@1.0');
const INTERVAL_MS = parseInt(getArg('--interval', 'INTERVAL_MS', '3000'), 10);
const BATCH_MAX = parseInt(getArg('--batch-max', 'BATCH_MAX', '100'), 10);
const RUN_ONCE = args.includes('--once') || process.env.ONCE === 'true';

let lastCommittedSeq = null; // Loaded durably or reconciled before first cycle
let isRunning = true;

async function httpFetch(url) {
  try {
    const res = await fetch(url, { headers: { Accept: 'application/json' } });
    const text = await res.text();
    try {
      return { status: res.status, body: JSON.parse(text) };
    } catch {
      return { status: res.status, body: text };
    }
  } catch (err) {
    return { status: 0, error: err.message };
  }
}

export function loadDurableCursor(cursorPath = DEFAULT_CURSOR_PATH) {
  if (!existsSync(cursorPath)) {
    return {
      last_committed_seq: 0,
      last_committed_hash: '0000000000000000000000000000000000000000000000000000000000000000',
      last_state_root: '0',
      epoch: 1,
      last_batch_id: null,
      source: 'default_genesis'
    };
  }
  try {
    const raw = JSON.parse(readFileSync(cursorPath, 'utf-8'));
    const seq = Number(raw.last_committed_seq ?? 0);
    if (!Number.isInteger(seq) || seq < 0) {
      throw new Error(`Invalid last_committed_seq in cursor file: ${raw.last_committed_seq}`);
    }
    return {
      last_committed_seq: seq,
      last_committed_hash: String(raw.last_committed_hash || ''),
      last_state_root: String(raw.last_state_root || '0'),
      epoch: Number(raw.epoch || 1),
      last_batch_id: raw.last_batch_id || null,
      updated_at: raw.updated_at || null,
      source: 'disk_cursor'
    };
  } catch (err) {
    throw new Error(`Corrupted committer cursor file at ${cursorPath}: ${err.message}`);
  }
}

export function saveDurableCursor(cursorState, cursorPath = DEFAULT_CURSOR_PATH) {
  const dir = dirname(cursorPath);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  const payload = {
    last_committed_seq: Number(cursorState.last_committed_seq ?? 0),
    last_committed_hash: String(cursorState.last_committed_hash ?? ''),
    last_state_root: String(cursorState.last_state_root ?? '0'),
    epoch: Number(cursorState.epoch ?? 1),
    last_batch_id: cursorState.last_batch_id ?? null,
    verdict: cursorState.verdict ?? null,
    updated_at: new Date().toISOString()
  };
  const tmpPath = `${cursorPath}.tmp.${process.pid}`;
  const fd = openSync(tmpPath, 'w');
  try {
    writeFileSync(fd, JSON.stringify(payload, null, 2), 'utf-8');
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(tmpPath, cursorPath);
  try {
    const dirFd = openSync(dir, 'r');
    try {
      fsyncSync(dirFd);
    } finally {
      closeSync(dirFd);
    }
  } catch {
    // Directory fsync may not be supported on all platforms/filesystems
  }
  return payload;
}

/**
 * Bootstrap node with Genesis Stack if AO_GENESIS_ID or process_genesis_id.txt is configured.
 */
export async function bootstrapGenesisStackNode(options = {}) {
  const genesisId = options.genesisProcessId || process.env.AO_GENESIS_ID;
  const procIdFile = options.processIdFile || join(DB_DIR, 'ao_process/process_id.txt');
  const genesisFile = join(dirname(procIdFile), 'process_genesis_id.txt');
  const targetGenesisId = (genesisId && genesisId.trim()) || (existsSync(genesisFile) ? readFileSync(genesisFile, 'utf-8').trim() : null);

  if (!targetGenesisId) return null;

  try {
    const config = await queryOfficialAoState({
      ...options,
      processId: targetGenesisId,
      action: 'GetGenesisConfig'
    });

    if (config && config.ok) {
      // 1. Check Genesis Hash against expected if provided or required
      const procIdFile = options.processIdFile || join(DB_DIR, 'ao_process/process_id.txt');
      const hashFile = join(dirname(procIdFile), 'genesis_hash.txt');
      const expectedHash = options.expectedGenesisHash ||
        process.env.AO_GENESIS_HASH ||
        process.env.HYDB_GENESIS_HASH ||
        (existsSync(hashFile) ? readFileSync(hashFile, 'utf8').trim() : null);

      if (expectedHash && config.genesis?.genesis_hash) {
        if (config.genesis.genesis_hash.toUpperCase() !== expectedHash.toUpperCase()) {
          const err = new Error(`ERR_GENESIS_HASH_MISMATCH: Genesis hash mismatch (expected ${expectedHash}, got ${config.genesis.genesis_hash})`);
          err.code = 'ERR_GENESIS_HASH_MISMATCH';
          throw err;
        }
      } else if (!expectedHash && (options.strictGenesisHash === true || options.requireGenesisHash === true)) {
        const err = new Error('ERR_GENESIS_HASH_REQUIRED: AO_GENESIS_HASH is required to verify Genesis Stack in strict mode');
        err.code = 'ERR_GENESIS_HASH_REQUIRED';
        throw err;
      }

      // 2. Discover and bind Arbitrator process ID
      const arbitratorId = config.arbitrator_process_id;
      if (arbitratorId) {
        options.processId = arbitratorId;
        process.env.AO_PROCESS_ID = arbitratorId;
      }

      // 3. Auto-register node with Genesis stack
      let nodeAddress = options.nodeId || options.address;
      let signer = options.signer;
      let jwk = null;
      if (!nodeAddress) {
        try {
          const w = await getWallet(options.walletPath || WALLET_PATH);
          nodeAddress = w.address;
          jwk = w.jwk;
        } catch {}
      }

      if (nodeAddress) {
        try {
          const regPayload = {
            node_id: nodeAddress,
            role: 'storage_unit',
            endpoint: options.suUrl || process.env.SU_ENDPOINT || 'http://127.0.0.1:8735'
          };
          let regResp = null;
          if (typeof options.evaluatorFn === 'function') {
            const evRes = await options.evaluatorFn({
              Target: targetGenesisId,
              Action: 'RegisterNode',
              From: nodeAddress,
              Tags: [
                { name: 'Action', value: 'RegisterNode' },
                { name: 'Data-Protocol', value: 'ao' },
                { name: 'Variant', value: 'ao.TN.1' }
              ],
              Data: JSON.stringify(regPayload)
            });
            const lastMsg = evRes?.Messages?.[evRes.Messages.length - 1] || evRes?.responses?.[evRes.responses?.length - 1];
            if (lastMsg && lastMsg.Data) {
              try { regResp = JSON.parse(lastMsg.Data); } catch {}
            }
          } else if (typeof options.messageFn === 'function') {
            await options.messageFn({
              process: targetGenesisId,
              tags: [
                { name: 'Action', value: 'RegisterNode' },
                { name: 'Data-Protocol', value: 'ao' },
                { name: 'Variant', value: 'ao.TN.1' }
              ],
              data: JSON.stringify(regPayload)
            });
          } else if (options.ao || options.useAoConnect || process.env.USE_AO_CONNECT === 'true') {
            const aoClient = getAoInstance(options);
            if (!signer && jwk) signer = createDataItemSigner(jwk);
            if (signer) {
              await aoClient.message({
                process: targetGenesisId,
                signer,
                tags: [
                  { name: 'Action', value: 'RegisterNode' },
                  { name: 'Data-Protocol', value: 'ao' },
                  { name: 'Variant', value: 'ao.TN.1' }
                ],
                data: JSON.stringify(regPayload)
              });
            }
          }

          if (regResp && (regResp.ok === false || regResp.status === 'REJECTED_UNAUTHORIZED_NODE')) {
            const regErr = new Error(`ERR_UNAUTHORIZED_NODE: Genesis rejected node registration for ${nodeAddress}: ${regResp.status || 'unknown'}`);
            regErr.code = 'ERR_UNAUTHORIZED_NODE';
            throw regErr;
          }
        } catch (regErr) {
          if (regErr.code === 'ERR_UNAUTHORIZED_NODE') {
            throw regErr;
          }
          console.warn(`[Genesis Bootstrap] Note: Node registration returned: ${regErr.message}`);
          if (options.strictRegistration === true) {
            throw regErr;
          }
        }
      }

      console.log(
        `[Genesis Bootstrap] Bound to Stack '${config.genesis?.stack_id || targetGenesisId}' ` +
        `(Quorum=${config.genesis?.quorum_size}, Arbitrator=${config.arbitrator_process_id || 'unbound'})`
      );
      return {
        ok: true,
        targetGenesisId,
        arbitrator_process_id: arbitratorId,
        config
      };
    }
  } catch (err) {
    if (err.code === 'ERR_GENESIS_HASH_MISMATCH' || err.code === 'ERR_UNAUTHORIZED_NODE' || err.code === 'ERR_GENESIS_HASH_REQUIRED') {
      throw err;
    }
    console.warn(`[Genesis Bootstrap] Note: Genesis discovery returned: ${err.message}`);
    return { ok: false, error: err.message };
  }
  return null;
}

/**
 * Reports periodic node heartbeat and operational metrics to Genesis Stack.
 */
export async function reportNodeHeartbeatToGenesis(targetGenesisId, heartbeatData = {}, options = {}) {
  const genesisId = targetGenesisId || options.genesisProcessId || process.env.AO_GENESIS_ID;
  if (!genesisId) return null;

  let sender = heartbeatData.node_id || options.sender;
  let signer = options.signer;
  let jwk = null;
  if (!sender) {
    try {
      const w = await getWallet(options.walletPath || WALLET_PATH);
      sender = w.address;
      jwk = w.jwk;
    } catch {
      sender = 'SU_NODE_1';
    }
  }

  const payload = {
    node_id: sender,
    health: heartbeatData.health || 'ACTIVE',
    throughput: heartbeatData.throughput ?? 0,
    last_seq: heartbeatData.last_seq ?? (lastCommittedSeq || 0),
    endpoint: options.suUrl || process.env.SU_ENDPOINT || 'http://127.0.0.1:8735',
    ...heartbeatData
  };

  try {
    if (typeof options.evaluatorFn === 'function') {
      return await options.evaluatorFn({
        Target: genesisId,
        Action: 'ReportHeartbeat',
        From: sender,
        Tags: [
          { name: 'Action', value: 'ReportHeartbeat' },
          { name: 'Data-Protocol', value: 'ao' },
          { name: 'Variant', value: 'ao.TN.1' }
        ],
        Data: JSON.stringify(payload)
      });
    }

    if (typeof options.messageFn === 'function') {
      return await options.messageFn({
        process: genesisId,
        tags: [
          { name: 'Action', value: 'ReportHeartbeat' },
          { name: 'Data-Protocol', value: 'ao' },
          { name: 'Variant', value: 'ao.TN.1' }
        ],
        data: JSON.stringify(payload)
      });
    }

    if (options.ao || options.useAoConnect || process.env.USE_AO_CONNECT === 'true') {
      const aoClient = getAoInstance(options);
      if (!signer && jwk) signer = createDataItemSigner(jwk);
      if (signer) {
        return await aoClient.message({
          process: genesisId,
          signer,
          tags: [
            { name: 'Action', value: 'ReportHeartbeat' },
            { name: 'Data-Protocol', value: 'ao' },
            { name: 'Variant', value: 'ao.TN.1' }
          ],
          data: JSON.stringify(payload)
        });
      }
    }
  } catch (err) {
    console.warn(`[Genesis Heartbeat] Report error: ${err.message}`);
    return { ok: false, error: err.message };
  }
  return null;
}

/**
 * Reconcile durable local cursor with the authoritative OfficialSeq on the AO Arbitrator.
 */
export async function reconcileCursorWithAO(options = {}) {
  const cursorPath = options.cursorPath || DEFAULT_CURSOR_PATH;
  const diskCursor = loadDurableCursor(cursorPath);
  const aoState = await queryOfficialAoState(options);

  if (aoState && aoState.ok && Number.isInteger(aoState.official_seq) && aoState.official_seq >= 0) {
    if (aoState.official_seq > diskCursor.last_committed_seq) {
      const reconciled = saveDurableCursor(
        {
          last_committed_seq: aoState.official_seq,
          last_committed_hash: aoState.official_hash || diskCursor.last_committed_hash,
          last_state_root: aoState.official_state_root || diskCursor.last_state_root,
          epoch: aoState.current_epoch || diskCursor.epoch,
          last_batch_id: diskCursor.last_batch_id,
          verdict: 'RECONCILED_FROM_AO'
        },
        cursorPath
      );
      lastCommittedSeq = reconciled.last_committed_seq;
      return {
        ...reconciled,
        source: 'ao_official_reconciled',
        reconciliation_status: 'advanced_from_ao',
        mismatch_detected: false,
        previous_disk_seq: diskCursor.last_committed_seq
      };
    }

    if (aoState.official_seq === diskCursor.last_committed_seq && aoState.official_seq > 0) {
      const aoHash = String(aoState.official_hash || '').toUpperCase();
      const localHash = String(diskCursor.last_committed_hash || '').toUpperCase();
      const aoRoot = String(aoState.official_state_root ?? '0');
      const localRoot = String(diskCursor.last_state_root ?? '0');

      const hashMismatch = aoHash && localHash && aoHash !== localHash;
      const rootMismatch = aoRoot && localRoot && aoRoot !== localRoot;

      if (hashMismatch || rootMismatch) {
        lastCommittedSeq = diskCursor.last_committed_seq;
        const diverged = {
          ...diskCursor,
          reconciliation_status: 'diverged_state_mismatch',
          mismatch_detected: true,
          error_code: 'ERR_CURSOR_AO_STATE_MISMATCH',
          ao_official_seq: aoState.official_seq,
          ao_official_hash: aoState.official_hash,
          ao_official_state_root: aoState.official_state_root,
          local_disk_seq: diskCursor.last_committed_seq,
          local_disk_hash: diskCursor.last_committed_hash,
          local_disk_state_root: diskCursor.last_state_root,
          reason: `Local cursor state diverges from AO official state at seq ${aoState.official_seq} (hashMismatch=${Boolean(hashMismatch)}, rootMismatch=${Boolean(rootMismatch)})`
        };
        if (options.throwOnDivergence === true) {
          throw new Error(diverged.reason);
        }
        return diverged;
      }
    }

    if (diskCursor.last_committed_seq > aoState.official_seq) {
      lastCommittedSeq = diskCursor.last_committed_seq;
      const diverged = {
        ...diskCursor,
        reconciliation_status: 'diverged_local_ahead_of_ao',
        mismatch_detected: true,
        error_code: 'ERR_CURSOR_AHEAD_OF_AO_OFFICIAL_SEQ',
        ao_official_seq: aoState.official_seq,
        local_disk_seq: diskCursor.last_committed_seq,
        reason: `Local cursor seq (${diskCursor.last_committed_seq}) is ahead of AO OfficialSeq (${aoState.official_seq})`
      };
      if (options.throwOnDivergence === true) {
        throw new Error(diverged.reason);
      }
      return diverged;
    }
  } else {
    // Authoritative AO state could not be verified (unknown / error / unreachable)
    const isProd = !isTestProfile() || options.strictReconcile === true || process.env.HYDB_STRICT_AO === 'true';
    if (isProd) {
      lastCommittedSeq = diskCursor.last_committed_seq;
      const diverged = {
        ...diskCursor,
        reconciliation_status: 'diverged_ao_unreachable',
        mismatch_detected: true,
        error_code: 'ERR_AO_STATE_UNREACHABLE',
        ao_status: aoState?.status || 'unknown',
        local_disk_seq: diskCursor.last_committed_seq,
        reason: `Production fail-closed: Authoritative AO state cannot be verified on startup (${aoState?.status || 'unknown'}: ${aoState?.reason || 'unreachable'}). Process ID: ${options.processId || process.env.AO_PROCESS_ID || 'unset'}`
      };
      if (options.throwOnDivergence === true) {
        throw new Error(diverged.reason);
      }
      return diverged;
    }
  }

  lastCommittedSeq = diskCursor.last_committed_seq;
  return {
    ...diskCursor,
    reconciliation_status: 'in_sync',
    mismatch_detected: false
  };
}

export function getCommittedCursor() {
  return lastCommittedSeq;
}

export function setCommittedCursor(seq) {
  lastCommittedSeq = seq === null || seq === undefined ? null : Number(seq);
}

export function computeBatchSig(batch, secretKey) {
  const events = batch.events || [];
  const eventHashes = events.map((e) => e.hash || '').join(':');
  const payload = [
    batch.su_id || 'SU_NODE_1',
    batch.epoch || 1,
    batch.from_seq,
    batch.to_seq,
    batch.proposed_state_root,
    batch.proposed_hash,
    eventHashes
  ].join('|');
  return crypto.createHmac('sha256', secretKey).update(payload).digest('hex').toUpperCase();
}

export function deriveClusterSecret(walletPath = WALLET_PATH) {
  if (existsSync(walletPath)) {
    try {
      const walletBytes = readFileSync(walletPath);
      return crypto.createHash('sha256').update(walletBytes).digest();
    } catch (e) {
      throw new Error(`Failed to read cluster WALLET_PATH (${walletPath}): ${e.message}`);
    }
  } else if (process.env.HYDB_CLUSTER_SECRET && process.env.HYDB_CLUSTER_SECRET.trim().length > 0) {
    return crypto.createHash('sha256').update(Buffer.from(process.env.HYDB_CLUSTER_SECRET.trim())).digest();
  } else {
    throw new Error('Fatal: missing cluster secret. Set WALLET_PATH or HYDB_CLUSTER_SECRET environment variable.');
  }
}

export async function processCommitterCycle(options = {}) {
  const cursorPath = options.cursorPath || DEFAULT_CURSOR_PATH;
  const suUrl = options.suUrl || SU_SCHEDULER_URL;
  const batchMax = options.batchMax || BATCH_MAX;
  const fetchFn = options.fetchFn || httpFetch;

  // Ensure cursor is initialized from durable storage or AO state before first cycle
  if (lastCommittedSeq === null || options.forceReconcile || options.reconcileEveryCycle) {
    const reconcileRes = await reconcileCursorWithAO({ ...options, cursorPath });
    if (reconcileRes && reconcileRes.mismatch_detected && !options.allowLocalAhead) {
      writeHeartbeatStatus({
        status: 'halted',
        health: 'failed_closed',
        error_code: reconcileRes.error_code || 'ERR_CURSOR_DIVERGENCE',
        reason: reconcileRes.reason,
        last_committed_seq: lastCommittedSeq
      }, cursorPath);
      return {
        ok: false,
        skipped: true,
        cursor_advanced: false,
        error_code: reconcileRes.error_code || 'ERR_CURSOR_DIVERGENCE',
        mismatch_detected: true,
        reconciliation_status: reconcileRes.reconciliation_status,
        ao_official_seq: reconcileRes.ao_official_seq,
        lastCommitted: lastCommittedSeq,
        reason: reconcileRes.reason
      };
    }
  }

  const suStatus = await fetchFn(`${suUrl}/status`);
  if (suStatus.status !== 200 || !suStatus.body) {
    writeHeartbeatStatus({
      status: 'waiting_su',
      health: 'healthy',
      last_committed_seq: lastCommittedSeq,
      reason: 'SU node unreachable'
    }, cursorPath);
    return { skipped: true, reason: 'SU node unreachable', lastCommitted: lastCommittedSeq };
  }

  const latestTick = Number(suStatus.body.latest_tick_ram ?? suStatus.body.latest_tick ?? suStatus.body.tick ?? 0);
  const epoch = Number(suStatus.body.epoch || 1);

  if (latestTick <= lastCommittedSeq) {
    writeHeartbeatStatus({
      status: 'idle',
      health: 'healthy',
      current_seq: latestTick,
      last_committed_seq: lastCommittedSeq
    }, cursorPath);
    return { skipped: true, currentSeq: latestTick, lastCommitted: lastCommittedSeq };
  }

  const fromSeq = lastCommittedSeq + 1;
  const toSeq = Math.min(latestTick, fromSeq + batchMax - 1);

  const batchRes = await fetchFn(`${suUrl}/wal_batch?from_seq=${fromSeq}&to_seq=${toSeq}`);
  if (batchRes.status !== 200 || !batchRes.body) {
    return { skipped: true, reason: 'Failed to fetch batch from SU', lastCommitted: lastCommittedSeq };
  }

  let events = batchRes.body.events;
  if (!Array.isArray(events) && typeof batchRes.body.events_json === 'string') {
    try {
      events = JSON.parse(batchRes.body.events_json);
    } catch {
      events = [];
    }
  }

  if (!Array.isArray(events) || events.length === 0) {
    return { skipped: true, reason: 'No events returned in batch window', lastCommitted: lastCommittedSeq };
  }

  const lastEvent = events[events.length - 1];
  const proposedStateRoot = String(
    lastEvent.state_root_str ??
      lastEvent.state_root ??
      batchRes.body.proposed_state_root ??
      suStatus.body.state_root_str ??
      '0'
  );
  const proposedHash = String(lastEvent.hash || batchRes.body.proposed_hash || suStatus.body.latest_hash || '');

  const secretKey = deriveClusterSecret(options.walletPath || WALLET_PATH);

  const batchData = {
    su_id: batchRes.body.su_id || 'SU_NODE_1',
    epoch,
    from_seq: fromSeq,
    to_seq: toSeq,
    proposed_state_root: proposedStateRoot,
    proposed_hash: proposedHash,
    events
  };
  batchData.batch_id = computeBatchId(batchData);
  batchData.batch_sig = computeBatchSig(batchData, secretKey);

  const commitOptions = {
    ...options,
    verifyL1Gateway:
      options.verifyL1Gateway ??
      (process.env.VERIFY_L1_FINALITY === 'true' || options.verifyL1Finality === true),
    verifyL1Finality:
      options.verifyL1Finality ??
      (process.env.VERIFY_L1_FINALITY === 'true' || options.verifyL1Gateway === true),
    l1Gateways:
      options.l1Gateways ||
      (process.env.L1_GATEWAYS
        ? process.env.L1_GATEWAYS.split(',').map((s) => s.trim()).filter(Boolean)
        : undefined),
    minConfirmations:
      options.minConfirmations ??
      (process.env.MIN_CONFIRMATIONS ? parseInt(process.env.MIN_CONFIRMATIONS, 10) : 1),
    requireMultiGateway:
      options.requireMultiGateway ??
      (process.env.REQUIRE_MULTI_GATEWAY === 'true')
  };

  const commitFn = options.commitFn || commitCheckpointBatch;
  const commitRes = await commitFn(batchData, commitOptions);

  // CRITICAL PHASE 06 GATE:
  // Only advance `lastCommittedSeq` and persist cursor if evaluation proof is VERIFIED!
  if (commitRes && commitRes.ok && commitRes.verified === true) {
    const newSeq = Number(commitRes.official_seq ?? toSeq);
    const updatedCursor = saveDurableCursor(
      {
        last_committed_seq: Math.max(toSeq, newSeq),
        last_committed_hash: proposedHash,
        last_state_root: proposedStateRoot,
        epoch,
        last_batch_id: batchData.batch_id,
        verdict: commitRes.verdict
      },
      cursorPath
    );
    lastCommittedSeq = updatedCursor.last_committed_seq;

    // Report heartbeat to Genesis Stack to maintain active quorum status
    let heartbeatOk = true;
    let heartbeatError = null;
    const targetGenesisId = options.targetGenesisId || options.genesisProcessId || process.env.AO_GENESIS_ID;
    if (targetGenesisId) {
      try {
        const hbRes = await reportNodeHeartbeatToGenesis(targetGenesisId, {
          last_seq: lastCommittedSeq,
          health: 'ACTIVE'
        }, options);
        if (hbRes && hbRes.ok !== false) {
          const lastMsg = hbRes?.Messages?.[hbRes.Messages.length - 1] || hbRes?.responses?.[hbRes.responses?.length - 1];
          if (lastMsg && lastMsg.Data) {
            try {
              const parsed = JSON.parse(lastMsg.Data);
              if (parsed.ok === false) {
                heartbeatOk = false;
                heartbeatError = parsed.status || 'HEARTBEAT_REJECTED';
              }
            } catch {}
          }
        } else {
          heartbeatOk = false;
          heartbeatError = (hbRes && hbRes.error) || 'HEARTBEAT_FAILED';
        }
      } catch (hbErr) {
        heartbeatOk = false;
        heartbeatError = hbErr.message;
      }
    }

    writeHeartbeatStatus({
      status: 'running',
      health: heartbeatOk ? 'healthy' : 'degraded',
      heartbeat_acknowledged: heartbeatOk,
      heartbeat_error: heartbeatError,
      last_committed_seq: lastCommittedSeq,
      verdict: commitRes.verdict,
      from_seq: fromSeq,
      to_seq: toSeq,
      batch_id: batchData.batch_id
    }, cursorPath);
    return {
      ok: true,
      cursor_advanced: true,
      settlement_status: commitRes.settlement_status,
      evaluation_status: commitRes.evaluation_status,
      verdict: commitRes.verdict,
      idempotent_replay: Boolean(commitRes.idempotent_replay),
      from_seq: fromSeq,
      to_seq: toSeq,
      last_committed_seq: lastCommittedSeq,
      commit: commitRes,
      heartbeat_acknowledged: heartbeatOk,
      health: heartbeatOk ? 'healthy' : 'degraded'
    };
  }

  // Evaluation pending/unknown or rejected -> do NOT advance cursor!
  writeHeartbeatStatus({
    status: 'retry_pending',
    health: 'healthy',
    last_committed_seq: lastCommittedSeq,
    verdict: commitRes?.verdict || null,
    reason: commitRes?.reason || 'Batch evaluation not verified; cursor held at previous sequence'
  }, cursorPath);
  return {
    ok: false,
    cursor_advanced: false,
    settlement_status: commitRes?.settlement_status || 'unknown',
    evaluation_status: commitRes?.evaluation_status || 'unknown',
    verdict: commitRes?.verdict || null,
    from_seq: fromSeq,
    to_seq: toSeq,
    last_committed_seq: lastCommittedSeq,
    reason: commitRes?.reason || 'Batch evaluation not verified; cursor held at previous sequence',
    commit: commitRes
  };
}

export async function startCommitterDaemon(options = {}) {
  const cursorPath = options.cursorPath || DEFAULT_CURSOR_PATH;

  // 1. Single Committer Owner Gate: Only run if HYDB_COMMITTER_ENABLED is not explicitly false
  const enabledEnv = process.env.HYDB_COMMITTER_ENABLED;
  const isEnabled = options.enabled !== undefined ? options.enabled : (enabledEnv !== 'false');
  if (!isEnabled) {
    console.log('[CommitterDaemon] Disabled on this node (HYDB_COMMITTER_ENABLED == false). Exiting gracefully.');
    return { ok: true, enabled: false, status: 'disabled' };
  }

  // 2. Mutual Exclusion Gate: Enforce single active worker per cursor storage volume via file lock
  if (!options.skipLock) {
    const lockRes = acquireCursorLock(cursorPath);
    if (!lockRes.acquired) {
      console.error(`[Fatal CommitterDaemon Error] ${lockRes.reason}`);
      const lockErr = new Error(`Committer daemon lock conflict: ${lockRes.reason}`);
      lockErr.code = 'ERR_COMMITTER_LOCK_HELD';
      lockErr.exitCode = FATAL_FAIL_CLOSED_EXIT_CODE;
      throw lockErr;
    }
  }

  console.log('========================================================================');
  console.log(' HyDB Autonomous Committer Daemon (Storage Unit -> AO Settlement)');
  console.log(` SU Endpoint: ${options.suUrl || SU_SCHEDULER_URL}`);
  console.log(` Cursor File: ${cursorPath}`);
  console.log(` Interval:    ${options.intervalMs || INTERVAL_MS}ms`);
  console.log(` Run Once:    ${options.runOnce !== undefined ? options.runOnce : RUN_ONCE}`);
  console.log('========================================================================\n');

  try {
    const genesisRes = await bootstrapGenesisStackNode(options);
    if (genesisRes && genesisRes.arbitrator_process_id) {
      options.processId = genesisRes.arbitrator_process_id;
      process.env.AO_PROCESS_ID = genesisRes.arbitrator_process_id;
      options.targetGenesisId = genesisRes.targetGenesisId;
    }
    const initialCursor = await reconcileCursorWithAO(options);
    if (initialCursor && initialCursor.mismatch_detected && !options.allowLocalAhead) {
      console.error('[Fatal CommitterDaemon Error] Cursor reconciliation mismatch detected:', initialCursor.reason);
      writeHeartbeatStatus({
        status: 'halted',
        health: 'failed_closed',
        error_code: initialCursor.error_code || 'ERR_CURSOR_DIVERGENCE',
        reason: initialCursor.reason,
        last_committed_seq: initialCursor.last_committed_seq
      }, cursorPath);
      const failClosedErr = new Error(`Committer daemon fail-closed: ${initialCursor.reason}`);
      failClosedErr.code = initialCursor.error_code || 'ERR_CURSOR_DIVERGENCE';
      failClosedErr.exitCode = FATAL_FAIL_CLOSED_EXIT_CODE;
      throw failClosedErr;
    }
    console.log(
      `[CommitterDaemon] Initialized cursor at seq=${initialCursor.last_committed_seq} (source=${initialCursor.source})`
    );

    writeHeartbeatStatus({
      status: 'idle',
      health: 'healthy',
      last_committed_seq: initialCursor.last_committed_seq
    }, cursorPath);

    const isRunOnce = options.runOnce !== undefined ? options.runOnce : RUN_ONCE;
    if (isRunOnce) {
      try {
        const res = await processCommitterCycle(options);
        console.log('[CommitterDaemon] Cycle complete:', res);
        return res;
      } catch (err) {
        if (isFatalFailClosedError(err)) {
          console.error('[Fatal CommitterDaemon Error] Fatal fail-closed error during run-once cycle:', err.message);
          writeHeartbeatStatus({
            status: 'halted',
            health: 'failed_closed',
            error_code: err.code || 'ERR_FATAL_CYCLE_ERROR',
            reason: err.message,
            last_committed_seq: lastCommittedSeq
          }, cursorPath);
          err.exitCode = FATAL_FAIL_CLOSED_EXIT_CODE;
          throw err;
        }
        throw err;
      }
    }

    const interval = options.intervalMs || INTERVAL_MS;
    while (isRunning) {
      try {
        await processCommitterCycle(options);
      } catch (err) {
        if (isFatalFailClosedError(err)) {
          console.error('[Fatal CommitterDaemon Error] Fatal fail-closed error during cycle:', err.message);
          writeHeartbeatStatus({
            status: 'halted',
            health: 'failed_closed',
            error_code: err.code || 'ERR_FATAL_CYCLE_ERROR',
            reason: err.message,
            last_committed_seq: lastCommittedSeq
          }, cursorPath);
          err.exitCode = FATAL_FAIL_CLOSED_EXIT_CODE;
          throw err;
        }
        console.error('[CommitterDaemon] Cycle error (transient, will retry):', err.message);
        writeHeartbeatStatus({
          status: 'cycle_error',
          health: 'healthy',
          last_committed_seq: lastCommittedSeq,
          last_error: err.message
        }, cursorPath);
      }
      await new Promise((r) => setTimeout(r, interval));
    }
  } finally {
    if (!options.skipLock) {
      releaseCursorLock(cursorPath);
    }
  }
}

if (basename(process.argv[1] || '') === 'committer_daemon.mjs') {
  const activeCursorPath = resolveDefaultCursorPath();
  const cleanup = (code = 0) => {
    isRunning = false;
    releaseCursorLock(activeCursorPath);
    writeHeartbeatStatus({ status: 'stopped', health: 'healthy' }, activeCursorPath);
    process.exit(code);
  };
  process.on('SIGINT', () => cleanup(0));
  process.on('SIGTERM', () => cleanup(0));
  process.stdin.on('end', () => {
    console.log('[CommitterDaemon] Supervisor stdin pipe closed. Gracefully stopping daemon.');
    cleanup(0);
  });
  startCommitterDaemon().catch((err) => {
    console.error('[Fatal CommitterDaemon Error]', err.message);
    const exitCode =
      err.exitCode ||
      (isFatalFailClosedError(err)
        ? FATAL_FAIL_CLOSED_EXIT_CODE
        : 1);
    releaseCursorLock(activeCursorPath);
    process.exit(exitCode);
  });
}
