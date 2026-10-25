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
import { connect, message, result as aoResult, createDataItemSigner } from '@permaweb/aoconnect';
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
 * Returns an aoconnect client instance bound to target MU, CU, and Gateway units.
 * Supports explicit AO_NETWORK configuration ('mainnet' | 'testnet' | 'local').
 */
export function getAoInstance(options = {}) {
  const isAoNetwork = detectAoEnvironment(options.gatewayUrl || gatewayUrl, options) === 'ao_network';
  const targetNetwork = (options.network || process.env.AO_NETWORK || 'testnet').toLowerCase();

  let defaultMu = undefined;
  let defaultCu = undefined;
  let defaultGw = options.gatewayUrl || gatewayUrl;

  if (isAoNetwork) {
    if (targetNetwork === 'mainnet') {
      defaultMu = process.env.AO_MU_URL || 'https://mu.ao-mainnet.xyz';
      defaultCu = process.env.AO_CU_URL || 'https://cu.ao-mainnet.xyz';
      defaultGw = process.env.ARWEAVE_GATEWAY || 'https://arweave.net';
    } else {
      defaultMu = process.env.AO_MU_URL || 'https://mu.ao-testnet.xyz';
      defaultCu = process.env.AO_CU_URL || 'https://cu.ao-testnet.xyz';
      defaultGw = process.env.ARWEAVE_GATEWAY || 'https://arweave.net';
    }
  }

  const muUrl = options.muUrl || process.env.AO_MU_URL || defaultMu;
  const cuUrl = options.cuUrl || process.env.AO_CU_URL || defaultCu;
  const gwUrl = options.gatewayUrl || defaultGw;

  const cfg = {};
  if (muUrl) cfg.MU_URL = muUrl;
  if (cuUrl) cfg.CU_URL = cuUrl;
  if (gwUrl) cfg.GATEWAY_URL = gwUrl;

  return connect(cfg);
}

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

  const hasSeqField =
    officialSeqRaw !== undefined && officialSeqRaw !== null && String(officialSeqRaw).trim() !== '';
  const hasHashField =
    officialHashRaw !== undefined && officialHashRaw !== null && String(officialHashRaw).trim() !== '';
  const hasRootField =
    officialRootRaw !== undefined && officialRootRaw !== null && String(officialRootRaw).trim() !== '';

  if (!hasSeqField || !hasHashField || !hasRootField) {
    return {
      evaluation_status: 'evaluated',
      settlement_status: 'rejected',
      verdict: 'MISSING_ARBITRATOR_PROOF_FIELDS',
      verified: false,
      finalized: false,
      confirmed: false,
      idempotent_replay: false,
      official_seq: hasSeqField ? Number(officialSeqRaw) : undefined,
      reason: `Arbitrator response is missing mandatory proof fields (hasSeq=${hasSeqField}, hasHash=${hasHashField}, hasRoot=${hasRootField})`
    };
  }

  const officialSeq = Number(officialSeqRaw);
  const expectedToSeq = Number(batchData.to_seq);
  const seqVerified = Number.isInteger(officialSeq) && officialSeq >= expectedToSeq;

  const expectedHash = String(batchData.proposed_hash || '').toUpperCase();
  const actualHash = String(officialHashRaw).toUpperCase();
  const hashVerified =
    status === 'DUPLICATE_BATCH_IDEMPOTENT' && officialSeq > expectedToSeq
      ? actualHash.length === 64
      : expectedHash.length > 0 && actualHash === expectedHash;

  const expectedRoot = String(batchData.proposed_state_root ?? '0');
  const actualRoot = String(officialRootRaw);
  const rootVerified =
    status === 'DUPLICATE_BATCH_IDEMPOTENT' && officialSeq > expectedToSeq
      ? actualRoot.length > 0
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

  // Finality check: only `ao_network` with gateway-verified L1 block_indep_hash, block_height,
  // confirmations, and matching Batch-Id tag is `finalized`.
  // Local `arlocal_local` simulated blocks or unverified caller-injected finalityProof objects
  // are never accepted as L1 `finalized`.
  const l1Proof = options.verifiedL1Finality;
  const hasL1FinalityProof =
    env === 'ao_network' &&
    l1Proof &&
    l1Proof.verified === true &&
    typeof l1Proof.block_indep_hash === 'string' &&
    l1Proof.block_indep_hash.length >= 32 &&
    Number(l1Proof.block_height) > 0 &&
    Number(l1Proof.number_of_confirmations ?? 0) >= Number(options.minConfirmations ?? 1);

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
    official_commitment_256: officialCommitment256 || null,
    l1_finality: hasL1FinalityProof ? l1Proof : null
  };
}

/**
 * Verify authoritative Arweave L1 transaction finality, message/bundle identity,
 * target AO Process binding, full checkpoint commitment tags, and optional multi-gateway consensus.
 */
export async function verifyArweaveL1Finality(l1TxId, batchDataOrId, options = {}) {
  const batchData =
    batchDataOrId && typeof batchDataOrId === 'object'
      ? batchDataOrId
      : options.expectedBatch || {};
  const explicitBatchId = typeof batchDataOrId === 'string' ? batchDataOrId : options.batchId;

  const env = detectAoEnvironment(options.gatewayUrl || gatewayUrl, {
    ...options,
    environment: options.environment || (options.fetchFn || options.l1GatewayFetchFn || options.gatewayUrl ? 'ao_network' : undefined)
  });
  if (env !== 'ao_network' || !l1TxId) {
    return { verified: false, finalized: false, error_code: 'NOT_AO_NETWORK', reason: 'Not ao_network or missing l1TxId' };
  }
  const fetchImpl =
    options.l1GatewayFetchFn ||
    options.fetchFn ||
    (typeof options.verifyL1Gateway === 'boolean' && !options.verifyL1Gateway ? null : fetch);
  if (!fetchImpl) {
    return { verified: false, finalized: false, error_code: 'NO_GATEWAY_VERIFIER', reason: 'No L1 gateway verifier configured' };
  }

  const rawGateways = Array.isArray(options.l1Gateways) && options.l1Gateways.length > 0
    ? options.l1Gateways
    : (process.env.L1_GATEWAYS ? process.env.L1_GATEWAYS.split(',').map((s) => s.trim()).filter(Boolean) : null) ||
      [options.gatewayUrl || gatewayUrl];
  const gateways = [...new Set(rawGateways.map((g) => String(g).replace(/\/$/, '')).filter(Boolean))];
  if (gateways.length === 0) {
    return { verified: false, finalized: false, error_code: 'NO_GATEWAY_URLS', reason: 'No L1 gateway URLs configured' };
  }

  if ((options.requireMultiGateway === true || process.env.REQUIRE_MULTI_GATEWAY === 'true') && gateways.length < 2) {
    return {
      verified: false,
      finalized: false,
      error_code: 'MULTI_GATEWAY_CONSENSUS_REQUIRED',
      reason: 'Multi-gateway consensus is required but fewer than 2 distinct gateways were provided',
      gateways_count: gateways.length
    };
  }

  const expectedBatchId = explicitBatchId || computeBatchId(batchData);
  const expectedMessageId = options.expectedMessageId || l1TxId;
  const expectedProcessId = options.processId || null;
  const minConf = Number(options.minConfirmations ?? 1);

  const decodeTag = (str) => {
    if (typeof str !== 'string') return '';
    try {
      const dec = Buffer.from(str, 'base64url').toString('utf-8');
      if (Buffer.from(dec, 'utf-8').toString('base64url') === str.replace(/=+$/, '')) {
        return dec;
      }
      return str;
    } catch {
      return String(str || '');
    }
  };

  try {
    let canonicalBlockHash = null;
    let canonicalBlockHeight = null;
    let minObservedConfirmations = Infinity;

    for (const gw of gateways) {
      const statusRes = await fetchImpl(`${gw}/tx/${l1TxId}/status`);
      let blockHeight = 0;
      let confirmations = 0;
      let blockIndepHash = '';
      let isGqlVerified = false;
      let gqlTags = [];

      if (statusRes && statusRes.status === 200) {
        const statusBody = await statusRes.json();
        blockHeight = Number(statusBody?.block_height ?? 0);
        confirmations = Number(statusBody?.number_of_confirmations ?? 0);
        blockIndepHash = String(statusBody?.block_indep_hash ?? '');
      } else {
        // Fallback: Query GraphQL Gateway for ANS-104 DataItems on live Permaweb
        try {
          const gqlRes = await fetchImpl(`${gw}/graphql`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              query: `query { transaction(id: "${l1TxId}") { id block { id height timestamp } tags { name value } } }`
            })
          });
          if (gqlRes && gqlRes.status === 200) {
            const gqlData = await gqlRes.json();
            const txGql = gqlData?.data?.transaction;
            if (txGql && txGql.block && Number(txGql.block.height) > 0) {
              blockHeight = Number(txGql.block.height);
              blockIndepHash = String(txGql.block.id || '');
              gqlTags = txGql.tags || [];
              isGqlVerified = true;
              try {
                const infoRes = await fetchImpl(`${gw}/info`);
                if (infoRes && infoRes.status === 200) {
                  const infoJson = await infoRes.json();
                  const netHeight = Number(infoJson?.height ?? 0);
                  confirmations = netHeight >= blockHeight ? (netHeight - blockHeight + 1) : 1;
                } else {
                  confirmations = 1;
                }
              } catch {
                confirmations = 1;
              }
            }
          }
        } catch {
          // ignore GQL error and fall through
        }

        if (!isGqlVerified) {
          return {
            verified: false,
            finalized: false,
            error_code: 'L1_STATUS_HTTP_ERROR',
            reason: `L1 tx status on gateway ${gw} returned HTTP ${statusRes?.status ?? 0}`
          };
        }
      }

      if (blockHeight <= 0 || confirmations < minConf || blockIndepHash.length < 32) {
        return {
          verified: false,
          finalized: false,
          error_code: 'INSUFFICIENT_L1_FINALITY',
          reason: `Insufficient L1 finality proof on ${gw} (height=${blockHeight}, confirmations=${confirmations}, min=${minConf})`
        };
      }

      if (canonicalBlockHash === null) {
        canonicalBlockHash = blockIndepHash;
        canonicalBlockHeight = blockHeight;
      } else if (blockIndepHash !== canonicalBlockHash || blockHeight !== canonicalBlockHeight) {
        return {
          verified: false,
          finalized: false,
          error_code: 'MULTI_GATEWAY_CONSENSUS_FAILED',
          reason: 'MULTI_GATEWAY_CONSENSUS_FAILED',
          details: `Multi-gateway L1 consensus mismatch on ${gw}: expected block ${canonicalBlockHeight}/${canonicalBlockHash}, got ${blockHeight}/${blockIndepHash}`
        };
      }
      minObservedConfirmations = Math.min(minObservedConfirmations, confirmations);

      const tagMap = new Map();
      let actualTxId = null;
      let bundleItems = [];
      let txBody = null;

      if (isGqlVerified) {
        actualTxId = l1TxId;
        for (const t of gqlTags) {
          const name = t.name;
          const value = t.value;
          if (name) tagMap.set(name, String(value ?? ''));
        }
      } else {
        const txRes = await fetchImpl(`${gw}/tx/${l1TxId}`);
        if (!txRes || txRes.status !== 200) {
          return {
            verified: false,
            finalized: false,
            error_code: 'L1_TX_HTTP_ERROR',
            reason: `L1 tx metadata lookup on ${gw} failed with HTTP ${txRes?.status ?? 0}`
          };
        }
        txBody = await txRes.json();
        const rawTags = Array.isArray(txBody?.tags) ? txBody.tags : [];
        for (const t of rawTags) {
          const name = t.name_decoded || decodeTag(t.name) || t.name;
          const value = t.value_decoded || decodeTag(t.value) || t.value;
          if (name) tagMap.set(name, String(value ?? ''));
        }
        actualTxId = txBody?.id ? String(txBody.id) : null;
        bundleItems = Array.isArray(txBody?.bundle_items) ? txBody.bundle_items.map(String) : [];
      }
      const bundledIn = tagMap.get('Bundled-In') || null;
      const idMatchesDirect = actualTxId === expectedMessageId;
      const idMatchesBundle =
        actualTxId === l1TxId &&
        (bundleItems.includes(expectedMessageId) || (bundledIn && bundledIn === l1TxId && tagMap.get('Message-Id') === expectedMessageId));

      if (!actualTxId || (!idMatchesDirect && !idMatchesBundle)) {
        return {
          verified: false,
          finalized: false,
          error_code: 'L1_TX_ID_MISMATCH',
          reason: 'L1_TX_ID_MISMATCH',
          details: `L1 transaction ID (${actualTxId || 'missing'}) is not bound to submitted AO message (${expectedMessageId})`
        };
      }

      // 2. Verify Target AO Process Binding
      if (expectedProcessId) {
        const targetProc = String(txBody?.target || tagMap.get('Recipient') || tagMap.get('Process') || '');
        if (targetProc !== String(expectedProcessId)) {
          return {
            verified: false,
            finalized: false,
            error_code: 'L1_PROCESS_ID_MISMATCH',
            reason: 'L1_PROCESS_ID_MISMATCH',
            details: `L1 transaction target/Recipient (${targetProc || 'missing'}) does not match AO Process ID (${expectedProcessId})`
          };
        }
      }

      // 3. Verify Full Cryptographic Checkpoint Commitment Tags
      if (tagMap.get('Batch-Id') !== expectedBatchId) {
        return {
          verified: false,
          finalized: false,
          error_code: 'L1_COMMITMENT_TAGS_MISMATCH',
          reason: 'L1_COMMITMENT_TAGS_MISMATCH',
          details: `L1 transaction ${l1TxId} does not carry matching Batch-Id tag (${expectedBatchId})`
        };
      }
      if (tagMap.get('Action') !== 'CommitCheckpoint') {
        return {
          verified: false,
          finalized: false,
          error_code: 'L1_COMMITMENT_TAGS_MISMATCH',
          reason: 'L1_COMMITMENT_TAGS_MISMATCH',
          details: `L1 transaction ${l1TxId} missing or invalid Action tag (${tagMap.get('Action') || 'missing'})`
        };
      }
      if (
        batchData.from_seq !== undefined &&
        (tagMap.get('From-Seq') !== String(batchData.from_seq) || tagMap.get('To-Seq') !== String(batchData.to_seq))
      ) {
        return {
          verified: false,
          finalized: false,
          error_code: 'L1_COMMITMENT_TAGS_MISMATCH',
          reason: 'L1_COMMITMENT_TAGS_MISMATCH',
          details: `L1 transaction ${l1TxId} sequence range tags (${tagMap.get('From-Seq')}..${tagMap.get('To-Seq')}) do not match batch (${batchData.from_seq}..${batchData.to_seq})`
        };
      }
      if (
        batchData.proposed_state_root !== undefined &&
        tagMap.get('State-Root') !== String(batchData.proposed_state_root)
      ) {
        return {
          verified: false,
          finalized: false,
          error_code: 'L1_COMMITMENT_TAGS_MISMATCH',
          reason: 'L1_COMMITMENT_TAGS_MISMATCH',
          details: `L1 transaction ${l1TxId} State-Root tag (${tagMap.get('State-Root')}) does not match batch (${batchData.proposed_state_root})`
        };
      }
      if (
        batchData.proposed_hash !== undefined &&
        (tagMap.get('Hash') || '').toUpperCase() !== String(batchData.proposed_hash || '').toUpperCase()
      ) {
        return {
          verified: false,
          finalized: false,
          error_code: 'L1_COMMITMENT_TAGS_MISMATCH',
          reason: 'L1_COMMITMENT_TAGS_MISMATCH',
          details: `L1 transaction ${l1TxId} Hash tag (${tagMap.get('Hash')}) does not match batch (${batchData.proposed_hash})`
        };
      }
    }

    return {
      verified: true,
      finalized: true,
      tx_id: l1TxId,
      message_id: expectedMessageId,
      process_id: expectedProcessId,
      block_height: canonicalBlockHeight,
      confirmations: minObservedConfirmations,
      number_of_confirmations: minObservedConfirmations,
      block_indep_hash: canonicalBlockHash,
      batch_id: expectedBatchId,
      gateways_verified: gateways.length,
      multi_gateway_consensus: gateways.length >= 2,
      trust_boundary: gateways.length >= 2 ? 'multi_gateway_consensus' : 'single_gateway_trusted_provider'
    };
  } catch (err) {
    return { verified: false, reason: `L1 gateway verification error: ${err.message}` };
  }
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

export function isTestProfile() {
  const p = process.env.HYDB_PROFILE || process.env.HYDB_ENV;
  if (p === 'prod' || p === 'production') return false;
  if (p === 'test') return true;
  if (process.env.NODE_ENV === 'test') return true;
  return false;
}

export async function getOrCreateProcessId(options = {}) {
  const envProcId = options.processId || process.env.AO_PROCESS_ID;
  if (envProcId && envProcId.trim()) {
    return envProcId.trim();
  }

  const procIdFile = options.processIdFile || PROCESS_ID_FILE;
  if (existsSync(procIdFile)) {
    const id = readFileSync(procIdFile, 'utf-8').trim();
    if (id) return id;
  }

  // Dynamic stack resolution: If AO_GENESIS_ID or process_genesis_id.txt is set, resolve ArbitratorProcessId from Genesis
  const genesisId = options.genesisProcessId || process.env.AO_GENESIS_ID;
  const genesisFile = join(dirname(procIdFile), 'process_genesis_id.txt');
  const targetGenesisId = (genesisId && genesisId.trim()) || (existsSync(genesisFile) ? readFileSync(genesisFile, 'utf-8').trim() : null);

  if (targetGenesisId) {
    try {
      const cfg = typeof options.queryGenesisFn === 'function'
        ? await options.queryGenesisFn(targetGenesisId, options)
        : await queryOfficialAoState({ ...options, processId: targetGenesisId, action: 'GetGenesisConfig' });
      const arbId = cfg?.arbitrator_process_id || cfg?.genesis?.arbitrator_process_id;
      if (arbId && arbId.trim()) {
        return arbId.trim();
      }
    } catch {}
  }

  if (process.env.AO_GENESIS_ID && process.env.AO_GENESIS_ID.trim()) {
    return process.env.AO_GENESIS_ID.trim();
  }

  // CRITICAL PRODUCTION GATE:
  // Auto-creation of AO Arbitrator Process is prohibited in production to prevent rogue or unanchored processes.
  if (!isTestProfile() && options.allowAutoCreate !== true) {
    const err = new Error(
      'Fatal: AO_PROCESS_ID environment variable or process_id.txt is required in production. ' +
      'Auto-creation of AO processes is strictly prohibited in production mode (ERR_AO_PROCESS_ID_REQUIRED).'
    );
    err.code = 'ERR_AO_PROCESS_ID_REQUIRED';
    throw err;
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
 * Reconciles local sequence against the authoritative AO Arbitrator via dryrun / CU read.
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
        status: 'ok',
        official_seq: Number(extractMsgField(last, 'OfficialSeq') ?? 0),
        current_epoch: Number(extractMsgField(last, 'CurrentEpoch') ?? 1),
        official_state_root: String(extractMsgField(last, 'OfficialStateRoot') ?? '0'),
        official_hash: String(extractMsgField(last, 'OfficialHash') ?? ''),
        official_commitment_256: String(extractMsgField(last, 'OfficialCommitment256') ?? '')
      };
    }
  }

  // Live AO Query via dryrun / CU read
  const processId =
    options.processId ||
    process.env.AO_PROCESS_ID ||
    process.env.AO_GENESIS_ID ||
    (await getOrCreateProcessId(options).catch(() => null));

  if (processId) {
    try {
      const aoClient = getAoInstance(options);
      const res = await aoClient.dryrun({
        process: processId,
        tags: [
          { name: 'Action', value: 'GetOfficialState' },
          { name: 'Data-Protocol', value: 'ao' },
          { name: 'Variant', value: 'ao.TN.1' }
        ],
        data: '{}'
      });
      const msgs = res?.Messages || [];
      const last = msgs[msgs.length - 1];
      if (last) {
        return {
          ok: true,
          status: 'ok',
          official_seq: Number(extractMsgField(last, 'OfficialSeq') ?? 0),
          current_epoch: Number(extractMsgField(last, 'CurrentEpoch') ?? 1),
          official_state_root: String(extractMsgField(last, 'OfficialStateRoot') ?? '0'),
          official_hash: String(extractMsgField(last, 'OfficialHash') ?? ''),
          official_commitment_256: String(extractMsgField(last, 'OfficialCommitment256') ?? '')
        };
      }
      return { ok: false, status: 'empty_response', reason: 'AO dryrun returned no messages', processId };
    } catch (err) {
      return { ok: false, status: 'error', reason: `AO dryrun failed: ${err.message}`, error: err, processId };
    }
  }

  return {
    ok: false,
    status: 'unknown',
    reason: 'Official AO query hook not configured and AO_PROCESS_ID not available.',
    official_seq: null
  };
}

/**
 * Submit a Checkpoint Batch to AO and verify evaluation proof before claiming confirmation.
 */
export async function commitCheckpointBatch(batchData, options = {}) {
  const batchId = computeBatchId(batchData);
  const env = detectAoEnvironment(options.gatewayUrl || gatewayUrl, options);
  const processId = options.processId || process.env.AO_PROCESS_ID || process.env.AO_GENESIS_ID || (await getOrCreateProcessId(options));

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

  const useAoConnect = options.useAoConnect || process.env.USE_AO_CONNECT === 'true' || env === 'ao_network';

  if (useAoConnect) {
    const aoClient = getAoInstance(options);
    const sendFn =
      options.messageFn ||
      (async (args) => {
        const { jwk } = await getWallet(options.walletPath || WALLET_PATH);
        return await aoClient.message({ ...args, signer: createDataItemSigner(jwk) });
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
        : async () => await aoClient.result({ process: processId, message: msgId }));

    if (resultReader) {
      try {
        evalPayload = await resultReader({ process: processId, message: msgId });
      } catch (err) {
        evalPayload = { error: err.message };
      }
    }

    const shouldVerifyL1 =
      env === 'ao_network' &&
      (options.l1TxId || msgId) &&
      (options.verifyL1Gateway === true ||
        options.verifyL1Finality === true ||
        process.env.VERIFY_L1_FINALITY === 'true' ||
        Boolean(options.l1GatewayFetchFn));

    let verifiedL1Finality = null;
    if (shouldVerifyL1) {
      verifiedL1Finality = await verifyArweaveL1Finality(options.l1TxId || msgId, batchData, {
        ...options,
        batchId,
        processId,
        expectedMessageId: msgId
      });
    }

    const verification = verifyArbitratorEvaluation(evalPayload, batchData, {
      ...options,
      environment: env,
      verifiedL1Finality
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

  const shouldVerifyL1Tx =
    env === 'ao_network' &&
    (options.l1TxId || txId) &&
    (options.verifyL1Gateway === true ||
      options.verifyL1Finality === true ||
      process.env.VERIFY_L1_FINALITY === 'true' ||
      Boolean(options.l1GatewayFetchFn));

  let verifiedL1Finality = null;
  if (shouldVerifyL1Tx) {
    verifiedL1Finality = await verifyArweaveL1Finality(options.l1TxId || txId, batchData, {
      ...options,
      batchId,
      processId,
      expectedMessageId: txId
    });
  }

  const verification = verifyArbitratorEvaluation(evalPayload, batchData, {
    ...options,
    environment: env,
    verifiedL1Finality
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
