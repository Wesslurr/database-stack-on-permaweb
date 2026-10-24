#!/usr/bin/env node
/**
 * Autonomous Background Committer Daemon — Lapisan 4 / AO Bridge
 * ==============================================================
 * Continuously monitors the Storage Unit for newly durably committed sequences,
 * automatically packages them into signed Checkpoint Batches, and anchors
 * them to the decentralized AO Process on Arweave.
 *
 * Runs completely decoupled and independently as a standalone service / sidecar.
 *
 * Configuration (Environment or CLI arguments):
 *   SU_URL / --su-url            Storage Unit URL (default: http://127.0.0.1:8735/~scheduler@1.0)
 *   HOT_URL / --hot-url          Hot Node URL (default: http://127.0.0.1:8734/~hymessage@1.0)
 *   ARWEAVE_GATEWAY / --gateway  Arweave Gateway URL (default: http://127.0.0.1:1984)
 *   INTERVAL_MS / --interval     Polling interval in milliseconds (default: 3000)
 *   BATCH_MAX / --batch-max      Maximum events per batch (default: 100)
 *   ONCE / --once                Run a single check & commit cycle and exit
 */

import { readFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import crypto from 'node:crypto';
import { commitCheckpointBatch, getOrCreateProcessId } from './committer.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
const WALLET_PATH = process.env.WALLET_PATH || join(DB_DIR, 'secrets/wallet.json');

const args = process.argv.slice(2);
function getArg(flag, envKey, def) {
  const idx = args.indexOf(flag);
  if (idx !== -1 && args[idx + 1]) return args[idx + 1];
  return process.env[envKey] || def;
}

const SU_SCHEDULER_URL = getArg('--su-url', 'SU_URL', 'http://127.0.0.1:8735/~scheduler@1.0');
const HOT_URL = getArg('--hot-url', 'HOT_URL', 'http://127.0.0.1:8734/~hymessage@1.0');
const INTERVAL_MS = parseInt(getArg('--interval', 'INTERVAL_MS', '3000'), 10);
const BATCH_MAX = parseInt(getArg('--batch-max', 'BATCH_MAX', '100'), 10);
const RUN_ONCE = args.includes('--once') || process.env.ONCE === 'true';

let lastCommittedSeq = 0;
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

function computeBatchSig(batch, secretKey) {
  const events = batch.events || [];
  const eventHashes = events.map(e => e.hash || '').join(':');
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

export async function processCommitterCycle() {
  const suStatus = await httpFetch(`${SU_SCHEDULER_URL}/status`);
  if (suStatus.status !== 200 || !suStatus.body) {
    return { skipped: true, reason: 'SU node unreachable' };
  }

  const latestTick = Number(suStatus.body.latest_tick_ram || suStatus.body.tick || 0);
  const epoch = Number(suStatus.body.epoch || 1);

  if (latestTick <= lastCommittedSeq) {
    return { skipped: true, currentSeq: latestTick, lastCommitted: lastCommittedSeq };
  }

  const fromSeq = lastCommittedSeq + 1;
  const toSeq = Math.min(latestTick, fromSeq + BATCH_MAX - 1);

  const batchRes = await httpFetch(`${SU_SCHEDULER_URL}/wal_batch?from_seq=${fromSeq}&to_seq=${toSeq}`);
  if (batchRes.status !== 200 || !batchRes.body || !batchRes.body.events || batchRes.body.events.length === 0) {
    return { skipped: true, reason: 'No events returned in batch window' };
  }

  const events = batchRes.body.events;
  const lastEvent = events[events.length - 1];
  const proposedStateRoot = String(lastEvent.state_root || lastEvent.db_root_hash || suStatus.body.raw_root_hash || '0');
  const proposedHash = String(lastEvent.hash || suStatus.body.latest_hash || '');

  // Derive cluster secret for HMAC signature - NO guessable fallback permitted
  const secretKey = deriveClusterSecret(WALLET_PATH);

  const batchData = {
    su_id: 'SU_NODE_1',
    epoch,
    from_seq: fromSeq,
    to_seq: toSeq,
    proposed_state_root: proposedStateRoot,
    proposed_hash: proposedHash,
    events
  };
  batchData.batch_sig = computeBatchSig(batchData, secretKey);

  console.log(`[CommitterDaemon] Packaging batch Seq ${fromSeq}..${toSeq} (${events.length} events, Epoch ${epoch})...`);
  const commitRes = await commitCheckpointBatch(batchData);
  lastCommittedSeq = toSeq;

  console.log(`[CommitterDaemon] Checkpoint Seq ${fromSeq}..${toSeq} committed to AO! TxID: ${commitRes.tx_id}, Block: ${commitRes.block_height}`);
  return { ok: true, from_seq: fromSeq, to_seq: toSeq, commit: commitRes };
}

export async function startCommitterDaemon() {
  console.log('========================================================================');
  console.log(' HyDB Autonomous Committer Daemon (Storage Unit -> AO Settlement)');
  console.log(` SU Endpoint: ${SU_SCHEDULER_URL}`);
  console.log(` Interval:    ${INTERVAL_MS}ms`);
  console.log(` Run Once:    ${RUN_ONCE}`);
  console.log('========================================================================\n');

  if (RUN_ONCE) {
    const res = await processCommitterCycle();
    console.log('[CommitterDaemon] Cycle complete:', res);
    return;
  }

  while (isRunning) {
    try {
      await processCommitterCycle();
    } catch (err) {
      console.error('[CommitterDaemon] Cycle error:', err.message);
    }
    await new Promise(r => setTimeout(r, INTERVAL_MS));
  }
}

// Run if called as main script
if (process.argv[1]?.endsWith('committer_daemon.mjs')) {
  process.on('SIGINT', () => { isRunning = false; process.exit(0); });
  process.on('SIGTERM', () => { isRunning = false; process.exit(0); });
  startCommitterDaemon().catch(err => {
    console.error('[Fatal CommitterDaemon Error]', err);
    process.exit(1);
  });
}
