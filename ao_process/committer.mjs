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
export { createDataItemSigner };
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

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
export function resolveDefaultWalletPath() {
  if (process.env.WALLET_PATH) return process.env.WALLET_PATH;
  const suWallet = join(DB_DIR, 'secrets/wallet_su.json');
  if (existsSync(suWallet)) return suWallet;
  return join(DB_DIR, 'secrets/wallet.json');
}

const WALLET_PATH = resolveDefaultWalletPath();
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

export async function getWallet(walletPath = WALLET_PATH) {
  if (process.env.WALLET_JSON && process.env.WALLET_JSON.trim()) {
    try {
      const jwk = JSON.parse(process.env.WALLET_JSON);
      const address = await arweave.wallets.jwkToAddress(jwk);
      return { jwk, address };
    } catch {}
  }
  let targetPath = walletPath;
  if (!existsSync(targetPath)) {
    const fallback = targetPath.includes('wallet_su.json')
      ? join(DB_DIR, 'secrets/wallet.json')
      : join(DB_DIR, 'secrets/wallet_su.json');
    if (existsSync(fallback)) {
      targetPath = fallback;
    }
  }
  const jwk = JSON.parse(readFileSync(targetPath, 'utf-8'));
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

    // Never return targetGenesisId as if it were the Arbitrator
    if (!isTestProfile()) {
      const err = new Error(
        `Fatal: Failed to resolve ArbitratorProcessId from Genesis Process ${targetGenesisId} (ERR_ARBITRATOR_NOT_RESOLVED_FROM_GENESIS).`
      );
      err.code = 'ERR_ARBITRATOR_NOT_RESOLVED_FROM_GENESIS';
      throw err;
    }
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
  const action = options.action || 'GetOfficialState';

  if (typeof options.queryOfficialStateFn === 'function') {
    return await options.queryOfficialStateFn(options);
  }
  if (typeof options.evaluatorFn === 'function') {
    const res = await options.evaluatorFn({
      Action: action,
      From: options.sender || 'SU_NODE_1',
      Tags: [{ name: 'Action', value: action }],
      Data: '{}'
    });
    const msgs = res?.Messages || res?.responses || [];
    const last = msgs[msgs.length - 1];
    if (last) {
      if (action === 'GetGenesisConfig') {
        let dataObj = {};
        try { if (last.Data) dataObj = JSON.parse(last.Data); } catch {}
        return {
          ok: true,
          status: 'ok',
          arbitrator_process_id: dataObj.arbitrator_process_id || extractMsgField(last, 'ArbitratorProcessId') || '',
          genesis: dataObj.genesis || {},
          authorized_nodes: dataObj.authorized_nodes || {},
          authorized_admins: dataObj.authorized_admins || {},
          schemas: dataObj.schemas || {}
        };
      }
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
    (await getOrCreateProcessId(options).catch(() => null));

  if (processId) {
    try {
      const aoClient = getAoInstance(options);
      const res = await aoClient.dryrun({
        process: processId,
        tags: [
          { name: 'Action', value: action },
          { name: 'Data-Protocol', value: 'ao' },
          { name: 'Variant', value: 'ao.TN.1' }
        ],
        data: '{}'
      });
      const msgs = res?.Messages || [];
      const last = msgs[msgs.length - 1];
      if (last) {
        if (action === 'GetGenesisConfig') {
          let dataObj = {};
          try { if (last.Data) dataObj = JSON.parse(last.Data); } catch {}
          return {
            ok: true,
            status: 'ok',
            arbitrator_process_id: dataObj.arbitrator_process_id || extractMsgField(last, 'ArbitratorProcessId') || '',
            genesis: dataObj.genesis || {},
            authorized_nodes: dataObj.authorized_nodes || {},
            authorized_admins: dataObj.authorized_admins || {},
            schemas: dataObj.schemas || {}
          };
        }
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
              From: options.sender || batchData.su_id || 'SU_NODE_1',
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
        From: options.sender || batchData.su_id || 'SU_NODE_1',
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



// ============================================================================
// Autonomous Background Committer & Durable Cursor Persistence
// ============================================================================

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



async function cli() {
  const args = process.argv.slice(2);
  const cmd = args[0];

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
    // Autonomous Committer Daemon Runner Mode
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
      console.log('[Committer] Supervisor stdin pipe closed. Gracefully stopping daemon.');
      cleanup(0);
    });
    startCommitterDaemon().catch((err) => {
      console.error('[Fatal Committer Error]', err.message);
      const exitCode =
        err.exitCode ||
        (isFatalFailClosedError(err)
          ? FATAL_FAIL_CLOSED_EXIT_CODE
          : 1);
      releaseCursorLock(activeCursorPath);
      process.exit(exitCode);
    });
  }
}

if (basename(process.argv[1] || '') === 'committer.mjs') {
  cli().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
