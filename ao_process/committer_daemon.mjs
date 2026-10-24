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

import { readFileSync, writeFileSync, renameSync, existsSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join, basename } from 'node:path';
import crypto from 'node:crypto';
import { commitCheckpointBatch, computeBatchId, queryOfficialAoState } from './committer.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
const WALLET_PATH = process.env.WALLET_PATH || join(DB_DIR, 'secrets/wallet.json');
const DEFAULT_CURSOR_PATH =
  process.env.HYDB_COMMITTER_CURSOR_PATH || join(DB_DIR, 'ao_process/committer_cursor.json');

const args = process.argv.slice(2);
function getArg(flag, envKey, def) {
  const idx = args.indexOf(flag);
  if (idx !== -1 && args[idx + 1]) return args[idx + 1];
  return process.env[envKey] || def;
}

const SU_SCHEDULER_URL = getArg('--su-url', 'SU_URL', 'http://127.0.0.1:8735/~scheduler@1.0');
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
  writeFileSync(tmpPath, JSON.stringify(payload, null, 2), 'utf-8');
  renameSync(tmpPath, cursorPath);
  return payload;
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
        previous_disk_seq: diskCursor.last_committed_seq
      };
    }
  }

  lastCommittedSeq = diskCursor.last_committed_seq;
  return diskCursor;
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
  if (lastCommittedSeq === null || options.forceReconcile) {
    await reconcileCursorWithAO({ ...options, cursorPath });
  }

  const suStatus = await fetchFn(`${suUrl}/status`);
  if (suStatus.status !== 200 || !suStatus.body) {
    return { skipped: true, reason: 'SU node unreachable', lastCommitted: lastCommittedSeq };
  }

  const latestTick = Number(suStatus.body.latest_tick_ram ?? suStatus.body.latest_tick ?? suStatus.body.tick ?? 0);
  const epoch = Number(suStatus.body.epoch || 1);

  if (latestTick <= lastCommittedSeq) {
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

  const commitFn = options.commitFn || commitCheckpointBatch;
  const commitRes = await commitFn(batchData, options);

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
      commit: commitRes
    };
  }

  // Evaluation pending/unknown or rejected -> do NOT advance cursor!
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
  console.log('========================================================================');
  console.log(' HyDB Autonomous Committer Daemon (Storage Unit -> AO Settlement)');
  console.log(` SU Endpoint: ${SU_SCHEDULER_URL}`);
  console.log(` Cursor File: ${options.cursorPath || DEFAULT_CURSOR_PATH}`);
  console.log(` Interval:    ${INTERVAL_MS}ms`);
  console.log(` Run Once:    ${RUN_ONCE}`);
  console.log('========================================================================\n');

  const initialCursor = await reconcileCursorWithAO(options);
  console.log(
    `[CommitterDaemon] Initialized cursor at seq=${initialCursor.last_committed_seq} (source=${initialCursor.source})`
  );

  if (RUN_ONCE) {
    const res = await processCommitterCycle(options);
    console.log('[CommitterDaemon] Cycle complete:', res);
    return res;
  }

  while (isRunning) {
    try {
      await processCommitterCycle(options);
    } catch (err) {
      console.error('[CommitterDaemon] Cycle error:', err.message);
    }
    await new Promise((r) => setTimeout(r, INTERVAL_MS));
  }
}

if (basename(process.argv[1] || '') === 'committer_daemon.mjs') {
  process.on('SIGINT', () => {
    isRunning = false;
    process.exit(0);
  });
  process.on('SIGTERM', () => {
    isRunning = false;
    process.exit(0);
  });
  startCommitterDaemon().catch((err) => {
    console.error('[Fatal CommitterDaemon Error]', err);
    process.exit(1);
  });
}
