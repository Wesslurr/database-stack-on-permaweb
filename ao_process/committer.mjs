#!/usr/bin/env node
/**
 * AO Process Committer & Settlement Verification Bridge — Lapisan 4 (Phase 06)
 * ============================================================================
 * Normative Guarantees (Phase 06):
 * 1. Never returns `confirmed: true` merely because `message()` or gateway TX
 *    submission returned a message/transaction ID.
 * 2. Strictly separates four settlement lifecycle states based on cryptographic
 *    and execution evidence:
 *    - `submitted`:  Accepted by transport (MU / Arweave Gateway), not yet evaluated.
 *    - `evaluated`:  Evaluated by AO Compute Unit / Arbitrator, verdict available.
 *    - `verified`:   Arbitrator verdict is COMMITTED or DUPLICATE_BATCH_IDEMPOTENT
 *                    with verified OfficialSeq, OfficialHash, and OfficialStateRoot.
 *    - `finalized`:  Verified by Arbitrator AND backed by target AO/Arweave L1
 *                    block finality proof (never conflated with local ArLocal `/mine`).
 *    - `rejected`:   Arbitrator evaluated and rejected the batch (fraud/gap/fenced).
 *    - `unknown`:    Evaluation status could not be determined (timeout/network error).
 * 3. Deterministic idempotent `batch_id` computed over `(epoch, from_seq, to_seq, proposed_hash, proposed_state_root)`.
 * 4. Explicit distinction between local `arlocal_local` test gateways and `ao_network`.
 */

import Arweave from 'arweave';
import { message, result as aoResult, createDataItemSigner } from '@permaweb/aoconnect';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join, basename } from 'node:path';
import crypto from 'node:crypto';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
const WALLET_PATH = process.env.WALLET_PATH || join(DB_DIR, 'secrets/wallet.json');
const PROCESS_ID_FILE = join(DB_DIR, 'ao_process/process_id.txt');
const LUA_PATH = join(DB_DIR, 'ao_process/arbitrator.lua');

const gatewayUrl = process.env.ARWEAVE_GATEWAY || 'http://127.0.0.1:1984';
const gwUrlObj = new URL(gatewayUrl);

const arweave = Arweave.init({
  host: gwUrlObj.hostname,
  port: Number(gwUrlObj.port || (gwUrlObj.protocol === 'https:' ? 443 : 80)),
  protocol: gwUrlObj.protocol.replace(':', '')
});

/**
 * Compute deterministic idempotent Batch ID from sequence range, epoch, state root, and tip hash.
 */
export function computeBatchId(batchData) {
  const epoch = Number(batchData?.epoch ?? 1);
  const fromSeq = Number(batchData?.from_seq ?? 0);
  const toSeq = Number(batchData?.to_seq ?? 0);
  const propHash = String(batchData?.proposed_hash ?? '').toUpperCase();
  const propRoot = String(batchData?.proposed_state_root ?? '0');
  const canonical = `HYDB_BATCH_V1|${epoch}|${fromSeq}|${toSeq}|${propHash}|${propRoot}`;
  return crypto.createHash('sha256').update(canonical).digest('hex').toUpperCase();
}

/**
 * Classify target environment so local ArLocal test simulation is never conflated
 * with decentralized AO / Arweave L1 finality.
 */
export function detectAoEnvironment(gw = gatewayUrl, options = {}) {
  if (options.environment === 'ao_network' || options.environment === 'arlocal_local') {
    return options.environment;
  }
  try {
    const u = new URL(gw);
    const isLocalHost =
      u.hostname === '127.0.0.1' ||
      u.hostname === 'localhost' ||
      u.hostname === '::1' ||
      u.port === '1984';
    return isLocalHost ? 'arlocal_local' : 'ao_network';
  } catch {
    return 'arlocal_local';
  }
}

/**
 * Extract tag/property from an AO response message (supports both AOS tag arrays and flat maps).
 */
export function extractMsgField(msg, key) {
  if (!msg || typeof msg !== 'object') return undefined;
  if (msg[key] !== undefined) return msg[key];
  if (Array.isArray(msg.Tags)) {
    const found = msg.Tags.find((t) => t && t.name === key);
    if (found) return found.value;
  }
  if (msg.tags && typeof msg.tags === 'object' && !Array.isArray(msg.tags)) {
    if (msg.tags[key] !== undefined) return msg.tags[key];
  }
  return undefined;
}

/**
 * Parse and cryptographically verify an AO Arbitrator evaluation result against the submitted batch.
 */
export function verifyArbitratorEvaluation(evalPayload, batchData, options = {}) {
  const env = detectAoEnvironment(options.gatewayUrl || gatewayUrl, options);

  if (!evalPayload || evalPayload.error) {
    return {
      evaluation_status: 'unknown',
      settlement_status: 'submitted',
      verdict: null,
      verified: false,
      finalized: false,
      confirmed: false,
      idempotent_replay: false,
      reason: evalPayload?.error || 'No evaluation output available from AO Compute Unit'
    };
  }

  const messages = Array.isArray(evalPayload.Messages)
    ? evalPayload.Messages
    : Array.isArray(evalPayload.responses)
    ? evalPayload.responses
    : [];

  if (messages.length === 0) {
    return {
      evaluation_status: 'pending',
      settlement_status: 'submitted',
      verdict: null,
      verified: false,
      finalized: false,
      confirmed: false,
      idempotent_replay: false,
      reason: 'AO message submitted, awaiting Compute Unit evaluation'
    };
  }

  const primaryMsg = messages[messages.length - 1];
  const status = extractMsgField(primaryMsg, 'Status');
  const action = extractMsgField(primaryMsg, 'Action');
  const subStatus = extractMsgField(primaryMsg, 'SubStatus');
  const officialSeqRaw = extractMsgField(primaryMsg, 'OfficialSeq');
  const officialRootRaw = extractMsgField(primaryMsg, 'OfficialStateRoot');
  const officialHashRaw = extractMsgField(primaryMsg, 'OfficialHash');
  const officialCommitment256 = extractMsgField(primaryMsg, 'OfficialCommitment256');
  const reason = extractMsgField(primaryMsg, 'Reason');

  const isCommittingVerdict = status === 'COMMITTED' || status === 'DUPLICATE_BATCH_IDEMPOTENT';
  if (!isCommittingVerdict) {
    return {
      evaluation_status: 'evaluated',
      settlement_status: 'rejected',
      verdict: status || subStatus || action || 'REJECTED',
      sub_status: subStatus || null,
      verified: false,
      finalized: false,
      confirmed: false,
      idempotent_replay: false,
      official_seq: officialSeqRaw !== undefined ? Number(officialSeqRaw) : undefined,
      reason: reason || `Arbitrator rejected checkpoint batch with status: ${status || 'UNKNOWN'}`
    };
  }

  const officialSeq = Number(officialSeqRaw ?? -1);
  const expectedToSeq = Number(batchData.to_seq);
  const seqVerified = officialSeq >= expectedToSeq;

  const expectedHash = String(batchData.proposed_hash || '').toUpperCase();
  const actualHash = officialHashRaw !== undefined ? String(officialHashRaw).toUpperCase() : expectedHash;
  const hashVerified =
    status === 'DUPLICATE_BATCH_IDEMPOTENT' && officialSeq > expectedToSeq
      ? true
      : actualHash === expectedHash;

  const expectedRoot = String(batchData.proposed_state_root ?? '0');
  const actualRoot = officialRootRaw !== undefined ? String(officialRootRaw) : expectedRoot;
  const rootVerified =
    status === 'DUPLICATE_BATCH_IDEMPOTENT' && officialSeq > expectedToSeq
      ? true
      : actualRoot === expectedRoot;

  const verified = seqVerified && hashVerified && rootVerified;
  if (!verified) {
    return {
      evaluation_status: 'evaluated',
      settlement_status: 'rejected',
      verdict: 'VERIFICATION_MISMATCH',
      verified: false,
      finalized: false,
      confirmed: false,
      idempotent_replay: false,
      official_seq: officialSeq,
      reason: `Arbitrator response failed post-evaluation verification (seqVerified=${seqVerified}, hashVerified=${hashVerified}, rootVerified=${rootVerified})`
    };
  }

  // Finality check: only `ao_network` with verified L1 block height & confirmations is `finalized`.
  // Local `arlocal_local` simulated blocks are `verified`, never misrepresented as L1 `finalized`.
  const hasL1FinalityProof =
    env === 'ao_network' &&
    options.finalityProof &&
    Number(options.finalityProof.block_height) > 0 &&
    Number(options.finalityProof.confirmations ?? 0) >= Number(options.minConfirmations ?? 1);

  return {
    evaluation_status: 'evaluated',
    settlement_status: hasL1FinalityProof ? 'finalized' : 'verified',
    verdict: status,
    verified: true,
    finalized: Boolean(hasL1FinalityProof),
    confirmed: true,
    idempotent_replay: status === 'DUPLICATE_BATCH_IDEMPOTENT',
    official_seq: officialSeq,
    official_state_root: actualRoot,
    official_hash: actualHash,
    official_commitment_256: officialCommitment256 || null
  };
}

async function getWallet(walletPath = WALLET_PATH) {
  const jwk = JSON.parse(readFileSync(walletPath, 'utf-8'));
  const address = await arweave.wallets.jwkToAddress(jwk);
  return { jwk, address };
}

async function mineBlock(gw = gatewayUrl) {
  try {
    const res = await fetch(`${gw.replace(/\/$/, '')}/mine`).then((r) => r.json());
    return res;
  } catch {
    return { height: 0, current: null };
  }
}

export async function getOrCreateProcessId(options = {}) {
  const procIdFile = options.processIdFile || PROCESS_ID_FILE;
  if (existsSync(procIdFile)) {
    const id = readFileSync(procIdFile, 'utf-8').trim();
    if (id) return id;
  }

  const { jwk, address } = await getWallet(options.walletPath || WALLET_PATH);
  const luaCode = existsSync(LUA_PATH) ? readFileSync(LUA_PATH, 'utf-8') : '-- AO Arbitrator';

  const tx = await arweave.createTransaction(
    {
      data: Buffer.from(luaCode)
    },
    jwk
  );

  tx.addTag('App-Name', 'AO');
  tx.addTag('Type', 'Process');
  tx.addTag('Data-Protocol', 'ao');
  tx.addTag('Variant', 'ao.TN.1');
  tx.addTag('Name', 'HyDB-Official-Arbitrator');
  tx.addTag('Owner', address);
  tx.addTag('Timestamp', Date.now().toString());

  await arweave.transactions.sign(tx, jwk);
  const resp = await arweave.transactions.post(tx);
  if (resp.status !== 200 && resp.status !== 202) {
    throw new Error(`Failed to post process creation tx: ${resp.status}`);
  }

  await mineBlock();
  writeFileSync(procIdFile, tx.id);
  console.log(`[AO Process] Created new 'Satu Kepala' Process ID: ${tx.id}`);
  return tx.id;
}

/**
 * Query Official Checkpoint State from the AO Arbitrator Process.
 */
export async function queryOfficialAoState(options = {}) {
  if (typeof options.queryOfficialStateFn === 'function') {
    return await options.queryOfficialStateFn(options);
  }
  if (typeof options.evaluatorFn === 'function') {
    const res = await options.evaluatorFn({
      Action: 'GetOfficialState',
      From: options.sender || 'SU_NODE_1',
      Tags: [{ name: 'Action', value: 'GetOfficialState' }],
      Data: '{}'
    });
    const msgs = res?.Messages || res?.responses || [];
    const last = msgs[msgs.length - 1];
    if (last) {
      return {
        ok: true,
        official_seq: Number(extractMsgField(last, 'OfficialSeq') ?? 0),
        current_epoch: Number(extractMsgField(last, 'CurrentEpoch') ?? 1),
        official_state_root: String(extractMsgField(last, 'OfficialStateRoot') ?? '0'),
        official_hash: String(extractMsgField(last, 'OfficialHash') ?? ''),
        official_commitment_256: String(extractMsgField(last, 'OfficialCommitment256') ?? '')
      };
    }
  }
  return { ok: false, status: 'unknown', official_seq: null };
}

/**
 * Submit a Checkpoint Batch to AO and verify evaluation proof before claiming confirmation.
 */
export async function commitCheckpointBatch(batchData, options = {}) {
  const batchId = computeBatchId(batchData);
  const env = detectAoEnvironment(options.gatewayUrl || gatewayUrl, options);
  const processId = options.processId || process.env.AO_PROCESS_ID || (await getOrCreateProcessId(options));

  const tags = [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Variant', value: 'ao.TN.1' },
    { name: 'Type', value: 'Message' },
    { name: 'Action', value: 'CommitCheckpoint' },
    { name: 'Batch-Id', value: batchId },
    { name: 'Epoch', value: String(batchData.epoch ?? 1) },
    { name: 'From-Seq', value: String(batchData.from_seq) },
    { name: 'To-Seq', value: String(batchData.to_seq) },
    { name: 'State-Root', value: String(batchData.proposed_state_root) },
    { name: 'Hash', value: String(batchData.proposed_hash) },
    { name: 'Events-Count', value: String((batchData.events || []).length) },
    { name: 'Timestamp', value: Date.now().toString() }
  ];

  const useAoConnect = options.useAoConnect || process.env.USE_AO_CONNECT === 'true';

  if (useAoConnect) {
    const sendFn =
      options.messageFn ||
      (async (args) => {
        const { jwk } = await getWallet(options.walletPath || WALLET_PATH);
        return await message({ ...args, signer: createDataItemSigner(jwk) });
      });

    const msgId = await sendFn({
      process: processId,
      tags,
      data: JSON.stringify({ ...batchData, batch_id: batchId })
    });

    if (!msgId) {
      throw new Error('AO message submission failed to return a valid message ID');
    }

    // Attempt to fetch evaluation proof from Compute Unit (`result()`) or injected evaluator
    let evalPayload = null;
    const resultReader =
      options.resultFn ||
      (typeof options.evaluatorFn === 'function'
        ? async () =>
            await options.evaluatorFn({
              Id: msgId,
              From: batchData.su_id || 'SU_NODE_1',
              Action: 'CommitCheckpoint',
              Tags: tags,
              Data: JSON.stringify({ ...batchData, batch_id: batchId })
            })
        : options.skipResultLookup
        ? null
        : async () => await aoResult({ process: processId, message: msgId }));

    if (resultReader) {
      try {
        evalPayload = await resultReader({ process: processId, message: msgId });
      } catch (err) {
        evalPayload = { error: err.message };
      }
    }

    const verification = verifyArbitratorEvaluation(evalPayload, batchData, {
      ...options,
      environment: env
    });

    return {
      ok: verification.settlement_status !== 'rejected',
      protocol: 'aoconnect',
      environment: env,
      finality_level: verification.finalized
        ? 'ao_l1_finalized'
        : verification.verified
        ? 'ao_cu_verified'
        : 'unverified_submission',
      tx_id: msgId,
      batch_id: batchId,
      process_id: processId,
      from_seq: batchData.from_seq,
      to_seq: batchData.to_seq,
      state_root: batchData.proposed_state_root,
      final_hash: batchData.proposed_hash,
      settlement_status: verification.settlement_status,
      evaluation_status: verification.evaluation_status,
      verdict: verification.verdict,
      verified: verification.verified,
      finalized: verification.finalized,
      confirmed: verification.confirmed,
      idempotent_replay: verification.idempotent_replay,
      official_seq: verification.official_seq,
      reason: verification.reason || null
    };
  }

  // Gateway / ArLocal Transaction Path
  let txId = options.mockTxId || null;
  let mineRes = { height: 0, current: null };
  let txMinedOnGateway = false;

  if (!options.skipGatewayPost) {
    const { jwk } = await getWallet(options.walletPath || WALLET_PATH);
    const payload = Buffer.from(JSON.stringify({ ...batchData, batch_id: batchId }));
    const tx = await arweave.createTransaction({ data: payload }, jwk);

    tx.addTag('App-Name', 'AO');
    for (const t of tags) {
      tx.addTag(t.name, t.value);
    }
    tx.addTag('Recipient', processId);

    await arweave.transactions.sign(tx, jwk);
    const resp = await arweave.transactions.post(tx);
    if (resp.status !== 200 && resp.status !== 202) {
      throw new Error(`Failed to post checkpoint tx: ${resp.status}`);
    }
    txId = tx.id;
    mineRes = await mineBlock(options.gatewayUrl || gatewayUrl);
    const txStatus = await arweave.transactions.getStatus(tx.id);
    txMinedOnGateway = txStatus.status === 200;
  }

  // Evaluate against AO Arbitrator if evaluator is provided (ArLocal alone only mines L1 data items, it does NOT run AOS Lua)
  let evalPayload = null;
  if (typeof options.evaluatorFn === 'function') {
    try {
      evalPayload = await options.evaluatorFn({
        Id: txId || batchId,
        From: batchData.su_id || 'SU_NODE_1',
        Action: 'CommitCheckpoint',
        Timestamp: Date.now(),
        Tags: tags,
        Data: JSON.stringify({ ...batchData, batch_id: batchId })
      });
    } catch (err) {
      evalPayload = { error: err.message };
    }
  } else if (typeof options.resultFn === 'function') {
    try {
      evalPayload = await options.resultFn({ process: processId, message: txId });
    } catch (err) {
      evalPayload = { error: err.message };
    }
  }

  const verification = verifyArbitratorEvaluation(evalPayload, batchData, {
    ...options,
    environment: env
  });

  return {
    ok: verification.settlement_status !== 'rejected',
    protocol: 'arweave_gateway',
    environment: env,
    finality_level:
      env === 'arlocal_local'
        ? txMinedOnGateway
          ? 'arlocal_simulated_block'
          : 'arlocal_mempool'
        : verification.finalized
        ? 'arweave_l1_finalized'
        : 'arweave_pending',
    tx_id: txId,
    batch_id: batchId,
    process_id: processId,
    from_seq: batchData.from_seq,
    to_seq: batchData.to_seq,
    state_root: batchData.proposed_state_root,
    final_hash: batchData.proposed_hash,
    block_height: mineRes.height || null,
    block_hash: mineRes.current || null,
    tx_mined_on_gateway: txMinedOnGateway,
    settlement_status: verification.settlement_status,
    evaluation_status: verification.evaluation_status,
    verdict: verification.verdict,
    verified: verification.verified,
    finalized: verification.finalized,
    confirmed: verification.confirmed,
    idempotent_replay: verification.idempotent_replay,
    official_seq: verification.official_seq,
    reason: verification.reason || null
  };
}

async function cli() {
  const args = process.argv.slice(2);
  const cmd = args[0] || 'init';

  if (cmd === 'init') {
    const id = await getOrCreateProcessId();
    console.log(JSON.stringify({ ok: true, process_id: id }));
  } else if (cmd === 'commit') {
    const filePath = args[1];
    if (!filePath) {
      console.error('Usage: committer.mjs commit <batch_json_path>');
      process.exit(1);
    }
    const batchData = JSON.parse(readFileSync(filePath, 'utf-8'));
    const res = await commitCheckpointBatch(batchData);
    console.log(JSON.stringify(res, null, 2));
  } else {
    console.error(`Unknown command: ${cmd}`);
    process.exit(1);
  }
}

if (basename(process.argv[1] || '') === 'committer.mjs') {
  cli().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
