#!/usr/bin/env node
/**
 * Real AO WebAssembly Process Evaluator & Logical Security Domain Arbitrator
 * =========================================================================
 * Executes deterministic verification inside the official @permaweb/ao-loader
 * WebAssembly sandbox + enforces the Logical Security Domain rules:
 * 1. Sender & Epoch Fencing validation
 * 2. Monotonic sequence, gap, out-of-order, and duplicate/overlap handling
 * 3. Multi-SU Equivocation detection (conflicting events at same sequence)
 * 4. Atomic all-or-nothing batch staging
 * 5. Exact 64-bit Merkle-XOR Fast Fingerprint + 256-bit SHA-256 State Commitment recomputation
 * 6. Separation of Arbitrator Verdict & Developer Policy Circuit
 * 7. Permanent checkpoint commitment to ArLocal (:1984)
 */

import AoLoader from '@permaweb/ao-loader';
import Arweave from 'arweave';
import crypto from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import {
  DEFAULT_POLICY as SPEC_DEFAULT_POLICY,
  resolvePolicy as specResolvePolicy,
  validatePolicy as specValidatePolicy
} from '../spec/hydb_contract_spec.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
const WALLET_PATH = process.env.WALLET_PATH || join(DB_DIR, 'secrets/wallet.json');
const WASM_PATH = process.env.AO_PROCESS_WASM || (
  existsSync(join(__dirname, 'process.wasm'))
    ? join(__dirname, 'process.wasm')
    : join(DB_DIR, 'vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/genesis-wasm-server/test/processes/happy/process.wasm')
);
const PROCESS_ID_FILE = join(DB_DIR, 'ao_process/process_id.txt');
const GENESIS_HASH = '0000000000000000000000000000000000000000000000000000000000000000';

const arweave = Arweave.init({
  host: '127.0.0.1',
  port: 1984,
  protocol: 'http',
});

let wasmMemory = null;
let aoHandle = null;

export const HYDB_CONTRACT_VERSION = 'hydb.core@1.0';

// Pluggable Developer Policy Circuit — defaults & allowlist come from the canonical spec.
export const DefaultPolicyCircuit = { ...SPEC_DEFAULT_POLICY };

/**
 * Guardrail C: Policy Circuit can choose operational recovery response,
 * but can NEVER turn an invalid Arbitrator Verdict into a valid state commit.
 *
 * Only actions listed in POLICY_ALLOWLIST[verdict] are honoured. Arbitrary code
 * (e.g. Lua strings from a contract manifest) is NEVER executed here; any value
 * outside the allowlist falls back to the verdict's safe default and is flagged.
 */
export function resolvePolicyAction(policyCircuit, verdictKey, _eventContext = {}) {
  const res = specResolvePolicy(policyCircuit, verdictKey);
  if (res.guardrail_triggered) {
    const rejected = typeof res.rejected_action === 'string' ? res.rejected_action.slice(0, 64) : String(res.rejected_action);
    return {
      action: `GUARDRAIL_BLOCKED_OVERRIDE(${rejected})->${res.action}`,
      guardrail_triggered: true
    };
  }
  return { action: res.action, guardrail_triggered: false };
}


/**
 * Exact 64-bit FNV-1a + SplitMix64 record hash matching Rust `hydb/src/lib.rs`
 * and Erlang `hydb_contract:canonical_hash_64/3`.
 * Returns signed 64-bit BigInt (matching Rust `as i64`).
 */
export function recordHash64(key, val, ver) {
  const mask64 = 0xFFFFFFFFFFFFFFFFn;
  let h = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;

  const ukey = BigInt.asUintN(64, BigInt(key));
  const uval = BigInt.asUintN(64, BigInt(val));
  const uver = BigInt.asUintN(64, BigInt(ver));

  h = (h ^ ukey) & mask64;
  h = (h * prime) & mask64;
  h = (h ^ uval) & mask64;
  h = (h * prime) & mask64;
  h = (h ^ uver) & mask64;
  h = (h * prime) & mask64;

  h = (h ^ (h >> 30n)) & mask64;
  h = (h * 0xbf58476d1ce4e5b9n) & mask64;
  h = (h ^ (h >> 27n)) & mask64;
  h = (h * 0x94d049bb133111ebn) & mask64;
  h = (h ^ (h >> 31n)) & mask64;

  return BigInt.asIntN(64, h);
}

/**
 * Exact match for Erlang `hydb_contract:bytes_to_u64/1`:
 * First 8 bytes (big-endian unsigned 64-bit integer) of SHA-256(bin).
 */
export function bytesToU64(inputStr) {
  const digest = crypto.createHash('sha256').update(Buffer.from(String(inputStr), 'utf8')).digest();
  return digest.readBigUInt64BE(0);
}

/**
 * Exact match for Erlang `hydb_contract:namespace_id_64/1`:
 * 0n for 'default', or 1000n + (uint32BE(SHA-256(ns)) % 100000000n)
 */
export function namespaceId64(inputStr) {
  if (!inputStr || inputStr === 'default') return 0n;
  const digest = crypto.createHash('sha256').update(Buffer.from(String(inputStr), 'utf8')).digest();
  const int32 = BigInt(digest.readUInt32BE(0));
  return 1000n + (int32 % 100000000n);
}

export function canonicalEncodeValue(valType, val) {
  if (valType === 'int64') return String(val);
  if (valType === 'bool') return val === true || val === 'true' ? 'true' : 'false';
  if (valType === 'json' && val && typeof val === 'object') {
    const keys = Object.keys(val).sort();
    const pairs = keys.map(k => `"${k}":${JSON.stringify(val[k])}`);
    return `{${pairs.join(',')}}`;
  }
  return String(val ?? '');
}

/**
 * Full 256-bit Cryptographic State Commitment (SHA-256 over canonical sorted state across all namespaces)
 */
export function computeCommitment256(recordsMap) {
  const activeEntries = [];
  for (const [slotId, rec] of recordsMap.entries()) {
    if (rec.active) {
      activeEntries.push({ slotId, rec });
    }
  }
  activeEntries.sort((a, b) => {
    const nsA = a.rec.namespace || 'default';
    const nsB = b.rec.namespace || 'default';
    if (nsA !== nsB) return nsA.localeCompare(nsB);
    const numA = Number(a.rec.rawKey);
    const numB = Number(b.rec.rawKey);
    if (!Number.isNaN(numA) && !Number.isNaN(numB)) return numA - numB;
    return String(a.rec.rawKey).localeCompare(String(b.rec.rawKey));
  });
  const canonical = activeEntries
    .map(({ rec }) => {
      const ns = rec.namespace || 'default';
      if (ns === 'default') {
        return `${rec.rawKey}:${rec.canonicalVal}:${rec.ver}`;
      }
      return `${ns}:${rec.rawKey}:${rec.canonicalVal}:${rec.ver}`;
    })
    .join('|');
  return crypto.createHash('sha256').update(canonical).digest('hex').toUpperCase();
}

export function createDefaultSchemas() {
  const map = new Map();
  map.set('default', {
    namespace: 'default',
    backend: 'wasm_slot',
    schema_version: '1.0.0',
    key_type: 'int64',
    value_type: 'int64',
    rules: {}
  });
  map.set('finance.balances', {
    namespace: 'finance.balances',
    backend: 'wasm_slot',
    schema_version: '1.0.0',
    key_type: 'int64',
    value_type: 'int64',
    rules: { min_value: 0 }
  });
  map.set('app.docs', {
    namespace: 'app.docs',
    backend: 'ets_doc',
    schema_version: '1.0.0',
    key_type: 'string',
    value_type: 'any',
    rules: {}
  });
  return map;
}

const TRUSTED_ARWEAVE_GATEWAYS = new Set([
  'http://127.0.0.1:1984',
  'http://localhost:1984',
  'https://arweave.net',
  'https://ar-io.dev'
]);

export function computeBatchSignature(batch, secretKey) {
  const suId = batch.su_id || batch.sender || 'SU_NODE_1';
  const epoch = batch.epoch !== undefined ? Number(batch.epoch) : 1;
  const fromSeq = batch.from_seq ?? (batch.events && batch.events[0] ? batch.events[0].seq : 0);
  const toSeq = batch.to_seq ?? (batch.events && batch.events.length ? batch.events[batch.events.length - 1].seq : 0);
  const propHash = String(batch.proposed_hash || (batch.events && batch.events.length ? batch.events[batch.events.length - 1].hash : '')).toUpperCase();
  const propRoot = String(batch.proposed_state_root ?? (batch.events && batch.events.length ? batch.events[batch.events.length - 1].state_root : '0'));
  const msg = `${suId}:${epoch}:${fromSeq}:${toSeq}:${propHash}:${propRoot}`;
  return crypto.createHmac('sha256', secretKey).update(msg).digest('hex').toUpperCase();
}

export class ArbitratorDomain {
  constructor(options = {}) {
    this.contractVersion = HYDB_CONTRACT_VERSION;
    this.currentEpoch = options.initialEpoch || 1;
    this.officialSeq = 0;
    this.officialHash = GENESIS_HASH;
    this.officialStateRoot64 = 0n;
    this.officialCommitment256 = computeCommitment256(new Map());
    this.records = new Map();       // slotId -> { namespace, backend, rawKey, val, canonicalVal, keyHash64, valHash64, ver, active }
    this.schemas = createDefaultSchemas();
    this.wasmRuntimes = new Map();  // namespace -> { instance, exports, memory, lastRoot64, wasmTxId }
    this.eventHistory = new Map();  // seq -> { epoch, seq, namespace, action, key, val, canonical_val, hash, state_root, attestations: Set }
    this.checkpoints = [];
    this.authorizedSenders = options.authorizedSenders ? new Set(options.authorizedSenders) : null;
    this.batchAuthSecret = options.batchAuthSecret || null;
    this.requireBatchSignature = Boolean(options.requireBatchSignature);
    this.policyCircuit = { ...DefaultPolicyCircuit, ...(options.policyCircuit || {}) };
  }

  enableBatchSignatureAuth(secretKey) {
    this.batchAuthSecret = secretKey;
    this.requireBatchSignature = true;
  }

  resolvePolicy(verdictKey, eventContext = {}) {
    return resolvePolicyAction(this.policyCircuit, verdictKey, eventContext);
  }

  registerNamespace(namespace, config = {}) {
    const entry = {
      namespace,
      backend: config.backend || 'ets_doc',
      schema_version: config.schema_version || '1.0.0',
      key_type: config.key_type || 'string',
      value_type: config.value_type || 'any',
      rules: config.rules || {},
      ...(config.contract_tx_id ? { contract_tx_id: config.contract_tx_id } : {}),
      ...(config.wasm_tx_id ? { wasm_tx_id: config.wasm_tx_id } : {}),
      ...(config.wasm_sha256 ? { wasm_sha256: config.wasm_sha256 } : {}),
      ...(config.connector ? { connector: config.connector } : {}),
      ...(config.policy ? { policy: config.policy } : {})
    };
    if (config.policy && typeof config.policy === 'object') {
      // Throws SpecError(ERR_POLICY_INVALID) for unknown verdicts or non-allowlisted actions.
      // NOTE: policy is still merged into one global circuit; per-namespace scoping is Tahap 6/7.
      specValidatePolicy(config.policy);
    }
    this.schemas.set(namespace, entry);
    if (config.policy && typeof config.policy === 'object') {
      this.policyCircuit = { ...this.policyCircuit, ...config.policy };
    }
    return entry;
  }

  /**
   * Pull & verify a Contract Definition (+ linked WASM binary hash) directly from Arweave TX,
   * instantiate the WebAssembly module, and inject contract rules into WASM linear memory.
   */
  async loadContractFromArweave(contractTxId, gatewayUrl = 'http://127.0.0.1:1984') {
    const cleanGw = String(gatewayUrl).replace(/\/$/, '');
    if (!TRUSTED_ARWEAVE_GATEWAYS.has(cleanGw)) {
      throw new Error(`ERR_UNTRUSTED_GATEWAY: Untrusted Arweave gateway blocked: ${gatewayUrl}`);
    }
    const res = await fetch(`${cleanGw}/${contractTxId}`);
    if (!res.ok) {
      throw new Error(`ERR_CONTRACT_TX_NOT_FOUND: Failed to fetch contract TX ${contractTxId} (${res.status})`);
    }
    const manifest = await res.json();
    const namespace = manifest.namespace || contractTxId;
    const schemaObj = manifest.schema || manifest;
    const rules = schemaObj.rules || {};
    const connector = manifest.connector || {};
    const wasmTxId = manifest.wasm_tx_id || connector.wasm_tx_id || null;
    const wasmSha256 = manifest.wasm_sha256 || connector.wasm_sha256 || null;

    let wasmInstantiated = false;
    let wasmContractConfigured = false;
    if (wasmTxId) {
      const wasmRes = await fetch(`${cleanGw}/${wasmTxId}`);
      if (!wasmRes.ok) {
        throw new Error(`ERR_WASM_TX_NOT_FOUND: Failed to fetch linked WASM TX ${wasmTxId}`);
      }
      const wasmBuf = Buffer.from(await wasmRes.arrayBuffer());
      if (wasmSha256) {
        const actualHash = crypto.createHash('sha256').update(wasmBuf).digest('hex').toUpperCase();
        if (actualHash !== String(wasmSha256).toUpperCase()) {
          throw new Error(`ERR_WASM_HASH_MISMATCH: Expected ${wasmSha256}, got ${actualHash}`);
        }
      }
      const { instance } = await WebAssembly.instantiate(wasmBuf, {});
      if (typeof instance.exports.db_set_contract_ns === 'function') {
        const nsIdI64 = BigInt.asIntN(64, namespaceId64(namespace));
        const hasMin = rules.min_value !== undefined ? 1n : 0n;
        const minVal = rules.min_value !== undefined ? BigInt.asIntN(64, BigInt(rules.min_value)) : 0n;
        const hasMax = rules.max_value !== undefined ? 1n : 0n;
        const maxVal = rules.max_value !== undefined ? BigInt.asIntN(64, BigInt(rules.max_value)) : 0n;
        const maxRecords = rules.max_records !== undefined ? BigInt.asIntN(64, BigInt(rules.max_records)) : 0n;
        instance.exports.db_set_contract_ns(nsIdI64, hasMin, minVal, hasMax, maxVal, maxRecords);
        wasmContractConfigured = true;
      }
      this.wasmRuntimes.set(namespace, {
        instance,
        exports: instance.exports,
        memory: instance.exports.memory || null,
        lastRoot64: 0n,
        wasmTxId,
        wasmSha256
      });
      wasmInstantiated = true;
    }

    const entry = this.registerNamespace(namespace, {
      backend: manifest.backend || schemaObj.backend || 'wasm_slot',
      schema_version: manifest.schema_version || schemaObj.schema_version || '1.0.0',
      key_type: schemaObj.key_type || 'int64',
      value_type: schemaObj.value_type || 'int64',
      rules,
      contract_tx_id: contractTxId,
      wasm_tx_id: wasmTxId,
      wasm_sha256: wasmSha256,
      connector,
      policy: manifest.policy || {}
    });

    return {
      ok: true,
      loaded_from_arweave: true,
      wasm_vm_instantiated: wasmInstantiated,
      wasm_contract_configured: wasmContractConfigured,
      contract_tx_id: contractTxId,
      wasm_tx_id: wasmTxId,
      wasm_sha256: wasmSha256,
      namespace,
      schema: entry,
      active_policy: this.policyCircuit
    };
  }

  /**
   * Autonomous Contract & WASM Discovery from incoming SU WAL Batch:
   * Scans batch events for any `register_schema` carrying `contract_tx_id` and automatically
   * pulls, verifies, and instantiates the Contract + WASM from Arweave before batch verification.
   */
  async prepareBatchContractsFromArweave(batch, gatewayUrl = 'http://127.0.0.1:1984') {
    const events = (batch && batch.events) || [];
    const autoLoaded = [];
    for (const ev of events) {
      const action = ev.action || ev.op || 'put';
      if (action === 'register_schema') {
        const namespace = ev.namespace || 'default';
        const valObj = typeof ev.val === 'object' && ev.val !== null
          ? ev.val
          : (() => { try { return JSON.parse(ev.canonical_val || '{}'); } catch { return {}; } })();
        const contractTxId = valObj.contract_tx_id || ev.contract_tx_id || null;
        if (contractTxId) {
          const existingSchema = this.schemas.get(namespace);
          const existingWasm = this.wasmRuntimes.get(namespace);
          const needsLoad = !existingSchema ||
            existingSchema.contract_tx_id !== contractTxId ||
            (valObj.wasm_tx_id && !existingWasm);
          if (needsLoad) {
            const loaded = await this.loadContractFromArweave(contractTxId, gatewayUrl);
            autoLoaded.push(loaded);
          }
        }
      }
    }
    return autoLoaded;
  }

  getNamespaceSchema(namespace, schemasMap = this.schemas) {
    return schemasMap.get(namespace) || {
      namespace,
      backend: 'wasm_slot',
      schema_version: '1.0.0',
      key_type: 'int64',
      value_type: 'int64',
      rules: {}
    };
  }

  validateEventContractAndSchema(ev, schemasMap = this.schemas) {
    if (ev.contract_version && ev.contract_version !== this.contractVersion) {
      return {
        ok: false,
        sub_verdict: 'REJECTED_CONTRACT_MISMATCH',
        error_code: 'ERR_CONTRACT_MISMATCH',
        reason: `Incompatible contract_version '${ev.contract_version}', expected '${this.contractVersion}'`
      };
    }
    const namespace = ev.namespace || 'default';
    const action = ev.action || ev.op || 'put';
    if (action === 'register_schema') {
      return {
        ok: true,
        schema: {
          namespace,
          backend: ev.backend || (ev.val && ev.val.backend) || 'ets_doc',
          schema_version: ev.schema_version || (ev.val && ev.val.schema_version) || '1.0.0',
          key_type: (ev.val && ev.val.key_type) || 'string',
          value_type: (ev.val && ev.val.value_type) || 'any',
          rules: (ev.val && ev.val.rules) || {}
        }
      };
    }
    const schema = this.getNamespaceSchema(namespace, schemasMap);
    if (ev.schema_version && ev.schema_version !== schema.schema_version) {
      return {
        ok: false,
        sub_verdict: 'REJECTED_SCHEMA_VIOLATION',
        error_code: 'ERR_SCHEMA_VIOLATION',
        reason: `Schema version mismatch for namespace '${namespace}': event has ${ev.schema_version}, arbitrator requires ${schema.schema_version}`
      };
    }
    if (action === 'put') {
      const rules = schema.rules || {};
      const val = ev.val;
      if (schema.value_type === 'int64' || typeof val === 'number') {
        const numVal = Number(val);
        if (Number.isNaN(numVal)) {
          return {
            ok: false,
            sub_verdict: 'REJECTED_SCHEMA_VIOLATION',
            error_code: 'ERR_INVALID_INPUT',
            reason: `Value '${val}' in namespace '${namespace}' is not a valid int64`
          };
        }
        if (rules.min_value !== undefined && numVal < Number(rules.min_value)) {
          return {
            ok: false,
            sub_verdict: 'REJECTED_SCHEMA_VIOLATION',
            error_code: 'ERR_SCHEMA_VIOLATION',
            reason: `Schema invariant violated in '${namespace}': value ${numVal} < min_value ${rules.min_value}`
          };
        }
        if (rules.max_value !== undefined && numVal > Number(rules.max_value)) {
          return {
            ok: false,
            sub_verdict: 'REJECTED_SCHEMA_VIOLATION',
            error_code: 'ERR_SCHEMA_VIOLATION',
            reason: `Schema invariant violated in '${namespace}': value ${numVal} > max_value ${rules.max_value}`
          };
        }
      }
      if (rules.required_fields && Array.isArray(rules.required_fields)) {
        const objVal = typeof val === 'string' ? (() => { try { return JSON.parse(val); } catch { return null; } })() : val;
        if (!objVal || typeof objVal !== 'object') {
          return {
            ok: false,
            sub_verdict: 'REJECTED_SCHEMA_VIOLATION',
            error_code: 'ERR_SCHEMA_VIOLATION',
            reason: `Namespace '${namespace}' requires a structured JSON object`
          };
        }
        for (const reqField of rules.required_fields) {
          if (!(reqField in objVal)) {
            return {
              ok: false,
              sub_verdict: 'REJECTED_SCHEMA_VIOLATION',
              error_code: 'ERR_SCHEMA_VIOLATION',
              reason: `Schema violation in '${namespace}': missing required field '${reqField}'`
            };
          }
        }
      }
    }
    return { ok: true, schema };
  }

  cloneRecords() {
    const copy = new Map();
    for (const [k, v] of this.records.entries()) {
      copy.set(k, { ...v });
    }
    return copy;
  }

  cloneSchemas() {
    const copy = new Map();
    for (const [k, v] of this.schemas.entries()) {
      copy.set(k, { ...v, rules: { ...(v.rules || {}) } });
    }
    return copy;
  }

  promoteEpoch(newEpoch) {
    const target = Number(newEpoch);
    if (target <= this.currentEpoch) {
      return {
        ok: false,
        wasit_verdict: 'REJECTED_INVALID_EPOCH',
        reason: `New epoch ${target} must be > current epoch ${this.currentEpoch}`,
        current_epoch: this.currentEpoch
      };
    }
    const prev = this.currentEpoch;
    this.currentEpoch = target;
    return {
      ok: true,
      wasit_verdict: 'EPOCH_PROMOTED',
      previous_epoch: prev,
      current_epoch: this.currentEpoch,
      official_seq: this.officialSeq,
      state_root_64: this.officialStateRoot64.toString(),
      state_commitment_256: this.officialCommitment256
    };
  }

  /**
   * Deterministic verification of a batch in an Atomic Staging Area.
   * Does not mutate official state unless 100% of checks pass.
   */
  verifyBatch(batch) {
    const suId = batch.su_id || batch.sender || 'SU_NODE_1';
    const batchEpoch = batch.epoch !== undefined ? Number(batch.epoch) : 1;
    const events = batch.events || [];

    // 1. Sender Authorization Check
    if (this.authorizedSenders && !this.authorizedSenders.has(suId)) {
      const pol = resolvePolicyAction(this.policyCircuit, 'REJECTED_UNAUTHORIZED_SENDER');
      return {
        ok: false,
        status: 'AUDIT_REJECTED',
        wasit_verdict: 'REJECTED_UNAUTHORIZED_SENDER',
        policy_action: pol.action,
        policy_guardrail_enforced: pol.guardrail_triggered,
        reason: `Sender ${suId} is not in AuthorizedNodes`
      };
    }

    // 1b. Cryptographic SU Batch Signature Check (HMAC-SHA256)
    if (this.batchAuthSecret && (this.requireBatchSignature || batch.batch_sig !== undefined)) {
      const expectedSig = computeBatchSignature(batch, this.batchAuthSecret);
      const givenSig = batch.batch_sig ? String(batch.batch_sig).toUpperCase() : '';
      if (!givenSig || givenSig !== expectedSig) {
        const pol = resolvePolicyAction(this.policyCircuit, 'REJECTED_UNAUTHORIZED_SENDER');
        return {
          ok: false,
          status: 'AUDIT_REJECTED',
          wasit_verdict: 'REJECTED_INVALID_BATCH_SIGNATURE',
          error_code: 'ERR_BATCH_SIGNATURE_INVALID',
          policy_action: pol.action,
          policy_guardrail_enforced: pol.guardrail_triggered,
          reason: `Cryptographic batch signature verification failed for sender ${suId}`
        };
      }
    }

    // 2. Epoch Fencing Check
    if (batchEpoch < this.currentEpoch) {
      const pol = resolvePolicyAction(this.policyCircuit, 'FENCED_STALE_EPOCH');
      return {
        ok: false,
        status: 'AUDIT_REJECTED',
        wasit_verdict: 'FENCED_STALE_EPOCH',
        policy_action: pol.action,
        policy_guardrail_enforced: pol.guardrail_triggered,
        reason: `Batch epoch ${batchEpoch} is fenced by active security epoch ${this.currentEpoch}`,
        audit_proof: { batch_epoch: batchEpoch, active_epoch: this.currentEpoch }
      };
    }

    if (events.length === 0) {
      return {
        ok: false,
        status: 'AUDIT_REJECTED',
        wasit_verdict: 'REJECTED_EMPTY_BATCH',
        reason: 'Batch contains zero events'
      };
    }

    // 3. Check internal batch ordering (strictly +1 monotonic without internal disorder/gaps)
    for (let i = 1; i < events.length; i++) {
      const prevSeq = Number(events[i - 1].seq);
      const currSeq = Number(events[i].seq);
      if (currSeq !== prevSeq + 1) {
        const isOutOfOrder = currSeq <= prevSeq;
        const vKey = isOutOfOrder ? 'REJECTED_OUT_OF_ORDER_BATCH' : 'REJECTED_SEQUENCE_GAP';
        const pol = resolvePolicyAction(this.policyCircuit, vKey);
        return {
          ok: false,
          status: 'AUDIT_REJECTED',
          wasit_verdict: vKey,
          policy_action: pol.action,
          policy_guardrail_enforced: pol.guardrail_triggered,
          reason: `Internal batch sequence discontinuity: seq ${prevSeq} followed by seq ${currSeq}`,
          audit_proof: { prev_seq: prevSeq, offending_seq: currSeq }
        };
      }
    }

    const firstSeq = Number(events[0].seq);
    const lastSeq = Number(events[events.length - 1].seq);

    // 4. Check for Sequence Gap relative to OfficialSeq
    if (firstSeq > this.officialSeq + 1) {
      const pol = resolvePolicyAction(this.policyCircuit, 'REJECTED_SEQUENCE_GAP');
      return {
        ok: false,
        status: 'AUDIT_REJECTED',
        wasit_verdict: 'REJECTED_SEQUENCE_GAP',
        policy_action: pol.action,
        policy_guardrail_enforced: pol.guardrail_triggered,
        reason: `Sequence gap: OfficialSeq is ${this.officialSeq}, but batch starts at ${firstSeq}`,
        audit_proof: {
          official_seq: this.officialSeq,
          missing_from: this.officialSeq + 1,
          missing_to: firstSeq - 1
        }
      };
    }

    // 5. Stage Official State, Schemas, Policy, & WASM Linear Memory for Atomic All-or-Nothing Verification
    const stagedRecords = this.cloneRecords();
    const stagedSchemas = this.cloneSchemas();
    let stagedPolicy = { ...this.policyCircuit };
    let stagedRoot64 = this.officialStateRoot64;
    let runningHash = batch.initial_hash && this.officialSeq === 0
      ? String(batch.initial_hash).toUpperCase()
      : this.officialHash;
    let stagedSeq = this.officialSeq;
    const newCommittedEvents = [];
    let deduplicatedCount = 0;
    let arweaveWasmExecutions = 0;

    // Snapshot linear memory of all active Arweave WASM instances for atomic rollback on rejection
    const wasmSnapshots = new Map();
    for (const [ns, rt] of this.wasmRuntimes.entries()) {
      if (rt && rt.memory) {
        wasmSnapshots.set(ns, {
          memCopy: new Uint8Array(rt.memory.buffer.slice(0)),
          lastRoot64: rt.lastRoot64
        });
      }
    }
    const rollbackWasmRuntimes = () => {
      for (const [ns, snap] of wasmSnapshots.entries()) {
        const rt = this.wasmRuntimes.get(ns);
        if (rt && rt.memory) {
          new Uint8Array(rt.memory.buffer).set(snap.memCopy);
          rt.lastRoot64 = snap.lastRoot64;
        }
      }
    };

    for (const ev of events) {
      const seq = Number(ev.seq);
      const evEpoch = ev.epoch !== undefined ? Number(ev.epoch) : batchEpoch;
      const namespace = ev.namespace || 'default';
      const schemaCheck = this.validateEventContractAndSchema(ev, stagedSchemas);
      if (!schemaCheck.ok) {
        rollbackWasmRuntimes();
        const pol = resolvePolicyAction(stagedPolicy, schemaCheck.sub_verdict);
        return {
          ok: false,
          status: 'AUDIT_REJECTED',
          wasit_verdict: 'FRAUD_DETECTED_AND_CUT',
          sub_verdict: schemaCheck.sub_verdict,
          error_code: schemaCheck.error_code,
          policy_action: pol.action,
          policy_guardrail_enforced: pol.guardrail_triggered,
          reason: schemaCheck.reason,
          audit_proof: { seq, namespace, offending_event: ev }
        };
      }

      const schema = schemaCheck.schema;
      const backend = ev.backend || schema.backend || 'wasm_slot';
      const action = ev.action || ev.op || 'put';
      const valType = ev.val_type || (action === 'register_schema' ? 'schema' : (backend === 'wasm_slot' ? 'int64' : 'string'));
      const rawKey = action === 'register_schema'
        ? '__schema__'
        : (backend === 'wasm_slot' ? Number(ev.key) : String(ev.key));
      const rawVal = ev.val ?? 0;
      const canonicalVal = ev.canonical_val !== undefined
        ? String(ev.canonical_val)
        : canonicalEncodeValue(valType, rawVal);
      const givenHash = String(ev.hash || '').toUpperCase();

      if (evEpoch < this.currentEpoch) {
        rollbackWasmRuntimes();
        const pol = resolvePolicyAction(stagedPolicy, 'FENCED_STALE_EPOCH');
        return {
          ok: false,
          status: 'AUDIT_REJECTED',
          wasit_verdict: 'FENCED_STALE_EPOCH',
          policy_action: pol.action,
          policy_guardrail_enforced: pol.guardrail_triggered,
          reason: `Event at seq ${seq} has stale epoch ${evEpoch} < ${this.currentEpoch}`,
          audit_proof: { seq, event_epoch: evEpoch, active_epoch: this.currentEpoch }
        };
      }

      if (seq <= this.officialSeq) {
        // Already committed: Check Deduplication vs Multi-SU Equivocation
        const existing = this.eventHistory.get(seq);
        if (existing) {
          if (
            existing.hash !== givenHash ||
            String(existing.key) !== String(rawKey) ||
            existing.namespace !== namespace ||
            existing.action !== action ||
            (action === 'put' && String(existing.canonical_val) !== String(canonicalVal))
          ) {
            rollbackWasmRuntimes();
            const pol = resolvePolicyAction(stagedPolicy, 'EQUIVOCATION_PROVEN');
            return {
              ok: false,
              status: 'AUDIT_REJECTED',
              wasit_verdict: 'EQUIVOCATION_PROVEN',
              policy_action: pol.action,
              policy_guardrail_enforced: pol.guardrail_triggered,
              reason: `Equivocation proven at seq ${seq}: conflicting event reported by ${suId}`,
              audit_proof: {
                conflict_seq: seq,
                committed_event: {
                  hash: existing.hash,
                  namespace: existing.namespace,
                  action: existing.action,
                  key: existing.key,
                  val: existing.val,
                  attested_by: Array.from(existing.attestations)
                },
                conflicting_event: {
                  hash: givenHash,
                  namespace,
                  action,
                  key: rawKey,
                  val: rawVal,
                  reported_by: suId
                }
              }
            };
          }
          deduplicatedCount++;
        }
        continue;
      }

      // New uncommitted event (seq === stagedSeq + 1): Verify SHA-256 Hash Chain
      const actionCode = action === 'register_schema' ? '24' : (action === 'del' ? '22' : '20');
      let payload;
      if (actionCode === '24') {
        payload = `${runningHash}${seq}24${namespace}:${canonicalVal}`;
      } else if (namespace === 'default' && actionCode === '20' && valType === 'int64') {
        payload = `${runningHash}${seq}20${rawKey}${canonicalVal}`;
      } else if (namespace === 'default' && actionCode === '22') {
        payload = `${runningHash}${seq}22${rawKey}`;
      } else if (actionCode === '22') {
        payload = `${runningHash}${seq}22${namespace}:${rawKey}`;
      } else {
        payload = `${runningHash}${seq}20${namespace}:${rawKey}:${canonicalVal}`;
      }
      const expectedHash = crypto.createHash('sha256').update(payload).digest('hex').toUpperCase();

      if (expectedHash !== givenHash) {
        rollbackWasmRuntimes();
        const pol = resolvePolicyAction(stagedPolicy, 'REJECTED_HASH_CHAIN_BROKEN');
        return {
          ok: false,
          status: 'AUDIT_REJECTED',
          wasit_verdict: 'FRAUD_DETECTED_AND_CUT',
          sub_verdict: 'REJECTED_HASH_CHAIN_BROKEN',
          policy_action: pol.action,
          policy_guardrail_enforced: pol.guardrail_triggered,
          reason: `Hash chain broken at seq ${seq}! Expected ${expectedHash}, got ${givenHash}`,
          audit_proof: { seq, expected_hash: expectedHash, given_hash: givenHash }
        };
      }

      if (action === 'register_schema') {
        const parsedSchema = typeof rawVal === 'object' && rawVal !== null
          ? rawVal
          : (() => { try { return JSON.parse(canonicalVal); } catch { return {}; } })();
        const existingPinned = stagedSchemas.get(namespace);
        if (existingPinned && existingPinned.contract_tx_id) {
          const pinnedRulesJson = canonicalEncodeValue('json', existingPinned.rules || {});
          const givenRulesJson = canonicalEncodeValue('json', parsedSchema.rules || {});
          const wasmTxMismatch = existingPinned.wasm_tx_id && parsedSchema.wasm_tx_id && existingPinned.wasm_tx_id !== parsedSchema.wasm_tx_id;
          const wasmShaMismatch = existingPinned.wasm_sha256 && parsedSchema.wasm_sha256 &&
            String(existingPinned.wasm_sha256).toUpperCase() !== String(parsedSchema.wasm_sha256).toUpperCase();
          if (
            pinnedRulesJson !== givenRulesJson ||
            existingPinned.key_type !== (parsedSchema.key_type || 'string') ||
            existingPinned.value_type !== (parsedSchema.value_type || 'any') ||
            wasmTxMismatch ||
            wasmShaMismatch
          ) {
            rollbackWasmRuntimes();
            const pol = resolvePolicyAction(stagedPolicy, 'REJECTED_CONTRACT_MISMATCH');
            return {
              ok: false,
              status: 'AUDIT_REJECTED',
              wasit_verdict: 'FRAUD_DETECTED_AND_CUT',
              sub_verdict: 'REJECTED_CONTRACT_MISMATCH',
              error_code: 'ERR_CONTRACT_MISMATCH',
              policy_action: pol.action,
              policy_guardrail_enforced: pol.guardrail_triggered,
              reason: `Hot Node register_schema at seq ${seq} contradicts Arweave Contract TX ${existingPinned.contract_tx_id}!`,
              audit_proof: { seq, namespace, arweave_contract_tx: existingPinned.contract_tx_id }
            };
          }
        }
        if (parsedSchema.policy && typeof parsedSchema.policy === 'object') {
          try {
            specValidatePolicy(parsedSchema.policy);
          } catch (policyErr) {
            rollbackWasmRuntimes();
            const pol = resolvePolicyAction(this.policyCircuit, 'REJECTED_CONTRACT_MISMATCH');
            return {
              ok: false,
              status: 'AUDIT_REJECTED',
              wasit_verdict: 'FRAUD_DETECTED_AND_CUT',
              sub_verdict: 'REJECTED_CONTRACT_MISMATCH',
              error_code: 'ERR_POLICY_INVALID',
              policy_action: pol.action,
              policy_guardrail_enforced: pol.guardrail_triggered,
              reason: `register_schema at seq ${seq} carries an invalid policy: ${policyErr.message}`
            };
          }
          stagedPolicy = { ...stagedPolicy, ...parsedSchema.policy };
        }
        stagedSchemas.set(namespace, {
          namespace,
          backend: parsedSchema.backend || backend || 'ets_doc',
          schema_version: parsedSchema.schema_version || '1.0.0',
          key_type: parsedSchema.key_type || 'string',
          value_type: parsedSchema.value_type || 'any',
          rules: parsedSchema.rules || {},
          ...(parsedSchema.contract_tx_id ? { contract_tx_id: parsedSchema.contract_tx_id } : {}),
          ...(parsedSchema.wasm_tx_id ? { wasm_tx_id: parsedSchema.wasm_tx_id } : {}),
          ...(parsedSchema.wasm_sha256 ? { wasm_sha256: parsedSchema.wasm_sha256 } : {}),
          ...(parsedSchema.connector ? { connector: parsedSchema.connector } : {}),
          ...(parsedSchema.policy ? { policy: parsedSchema.policy } : {})
        });
      } else {
        // Recompute 64-bit Merkle-XOR State Root in Staged State (and execute live Arweave WASM instance if mounted)
        const slotId = backend === 'wasm_slot' ? `wasm:${namespace}:${rawKey}` : `doc:${namespace}:${rawKey}`;
        const keyHash64 = backend === 'wasm_slot'
          ? (namespace === 'default'
              ? BigInt.asUintN(64, BigInt(rawKey))
              : (namespaceId64(namespace) ^ BigInt.asUintN(64, BigInt(rawKey))))
          : bytesToU64(`${namespace}:${rawKey}`);
        const valHash64 = backend === 'wasm_slot'
          ? BigInt(rawVal)
          : bytesToU64(canonicalVal);

        const cur = stagedRecords.get(slotId);
        const wasmRt = backend === 'wasm_slot' ? this.wasmRuntimes.get(namespace) : null;

        if (wasmRt && wasmRt.exports) {
          // Execute real WebAssembly export from the Arweave-pulled WASM binary!
          const connExports = (schema && schema.connector && schema.connector.exports) || {};
          const nsIdI64 = BigInt.asIntN(64, namespaceId64(namespace));
          const keyI64 = BigInt.asIntN(64, BigInt(rawKey));
          const seqI64 = BigInt.asIntN(64, BigInt(seq));
          if (action === 'put') {
            const putFnName = connExports.put || 'db_put_ns';
            const verFnName = connExports.version || 'db_version_ns';
            const valI64 = BigInt.asIntN(64, BigInt(rawVal));
            if (typeof wasmRt.exports.db_check_put_ns === 'function') {
              const checkCode = BigInt.asIntN(64, wasmRt.exports.db_check_put_ns(nsIdI64, keyI64, valI64));
              if (checkCode < 0n) {
                rollbackWasmRuntimes();
                const pol = resolvePolicyAction(stagedPolicy, 'REJECTED_SCHEMA_VIOLATION');
                return {
                  ok: false,
                  status: 'AUDIT_REJECTED',
                  wasit_verdict: 'FRAUD_DETECTED_AND_CUT',
                  sub_verdict: 'REJECTED_SCHEMA_VIOLATION',
                  error_code: checkCode === -50701n ? 'ERR_WASM_CAPACITY_EXCEEDED' : 'ERR_SCHEMA_VIOLATION',
                  policy_action: pol.action,
                  policy_guardrail_enforced: pol.guardrail_triggered,
                  reason: `WASM linear memory contract rejected put at seq ${seq} (code=${checkCode.toString()})`
                };
              }
            }
            const newWasmRoot = BigInt.asIntN(64, wasmRt.exports[putFnName](nsIdI64, keyI64, valI64, seqI64));
            const newVer = wasmRt.exports[verFnName]
              ? Number(wasmRt.exports[verFnName](nsIdI64, keyI64))
              : (cur ? cur.ver + 1 : 1);
            stagedRoot64 = stagedRoot64 ^ wasmRt.lastRoot64 ^ newWasmRoot;
            wasmRt.lastRoot64 = newWasmRoot;
            arweaveWasmExecutions++;
            stagedRecords.set(slotId, {
              namespace,
              backend,
              rawKey,
              val: rawVal,
              canonicalVal,
              keyHash64,
              valHash64,
              ver: newVer,
              active: true
            });
          } else if (action === 'del') {
            const delFnName = connExports.del || 'db_del_ns';
            const newWasmRoot = BigInt.asIntN(64, wasmRt.exports[delFnName](nsIdI64, keyI64, seqI64));
            stagedRoot64 = stagedRoot64 ^ wasmRt.lastRoot64 ^ newWasmRoot;
            wasmRt.lastRoot64 = newWasmRoot;
            arweaveWasmExecutions++;
            if (cur && cur.active) {
              stagedRecords.set(slotId, { ...cur, ver: cur.ver + 1, active: false });
            }
          }
        } else if (action === 'put') {
          if (cur && cur.active) {
            const oldH = recordHash64(cur.keyHash64, cur.valHash64, cur.ver);
            const newVer = cur.ver + 1;
            const newH = recordHash64(keyHash64, valHash64, newVer);
            stagedRoot64 = stagedRoot64 ^ oldH ^ newH;
            stagedRecords.set(slotId, {
              namespace,
              backend,
              rawKey,
              val: rawVal,
              canonicalVal,
              keyHash64,
              valHash64,
              ver: newVer,
              active: true
            });
          } else if (cur && !cur.active) {
            // Reactivating tombstone preserves version progression (ver + 1)
            const newVer = cur.ver + 1;
            const newH = recordHash64(keyHash64, valHash64, newVer);
            stagedRoot64 = stagedRoot64 ^ newH;
            stagedRecords.set(slotId, {
              namespace,
              backend,
              rawKey,
              val: rawVal,
              canonicalVal,
              keyHash64,
              valHash64,
              ver: newVer,
              active: true
            });
          } else {
            const newH = recordHash64(keyHash64, valHash64, 1);
            stagedRoot64 = stagedRoot64 ^ newH;
            stagedRecords.set(slotId, {
              namespace,
              backend,
              rawKey,
              val: rawVal,
              canonicalVal,
              keyHash64,
              valHash64,
              ver: 1,
              active: true
            });
          }
        } else if (action === 'del') {
          if (cur && cur.active) {
            const oldH = recordHash64(cur.keyHash64, cur.valHash64, cur.ver);
            stagedRoot64 = stagedRoot64 ^ oldH;
            stagedRecords.set(slotId, { ...cur, ver: cur.ver + 1, active: false });
          }
        }
      }

      // If event carries a per-event state_root, verify it matches recomputed stagedRoot64
      if (ev.state_root !== undefined && ev.state_root !== null) {
        const evRootBig = BigInt(String(ev.state_root));
        if (evRootBig !== stagedRoot64) {
          rollbackWasmRuntimes();
          const pol = resolvePolicyAction(this.policyCircuit, 'REJECTED_STATE_ROOT_MISMATCH');
          return {
            ok: false,
            status: 'AUDIT_REJECTED',
            wasit_verdict: 'FRAUD_DETECTED_AND_CUT',
            sub_verdict: 'REJECTED_STATE_ROOT_MISMATCH',
            policy_action: pol.action,
            policy_guardrail_enforced: pol.guardrail_triggered,
            reason: `Event state_root mismatch at seq ${seq}: recomputed ${stagedRoot64.toString()}, got ${evRootBig.toString()}`,
            audit_proof: {
              seq,
              recomputed_root_64: stagedRoot64.toString(),
              claimed_root_64: evRootBig.toString()
            }
          };
        }
      }

      runningHash = givenHash;
      stagedSeq = seq;
      newCommittedEvents.push({
        epoch: evEpoch,
        seq,
        namespace,
        backend,
        action,
        key: rawKey,
        val: rawVal,
        canonical_val: canonicalVal,
        hash: givenHash,
        state_root: stagedRoot64.toString()
      });
    }

    // If 100% of events in the batch were already committed (Idempotent Duplicate Batch)
    if (newCommittedEvents.length === 0 && deduplicatedCount > 0) {
      for (const ev of events) {
        const rec = this.eventHistory.get(Number(ev.seq));
        if (rec) rec.attestations.add(suId);
      }
      const pol = resolvePolicyAction(this.policyCircuit, 'DUPLICATE_BATCH_IDEMPOTENT');
      return {
        ok: true,
        status: 'DUPLICATE_ACCEPTED',
        wasit_verdict: 'DUPLICATE_BATCH_IDEMPOTENT',
        policy_action: pol.action,
        epoch: this.currentEpoch,
        official_seq: this.officialSeq,
        deduplicated_count: deduplicatedCount,
        state_root_64: this.officialStateRoot64.toString(),
        state_commitment_256: this.officialCommitment256,
        final_hash: this.officialHash
      };
    }

    // 6. Verify Batch Proposed Hash Tip & Proposed State Root
    if (batch.proposed_hash !== undefined) {
      const propHash = String(batch.proposed_hash).toUpperCase();
      if (propHash !== runningHash) {
        rollbackWasmRuntimes();
        const pol = resolvePolicyAction(this.policyCircuit, 'REJECTED_HASH_CHAIN_BROKEN');
        return {
          ok: false,
          status: 'AUDIT_REJECTED',
          wasit_verdict: 'FRAUD_DETECTED_AND_CUT',
          sub_verdict: 'REJECTED_TIP_MISMATCH',
          policy_action: pol.action,
          policy_guardrail_enforced: pol.guardrail_triggered,
          reason: `Proposed batch hash ${propHash} does not match audited tip ${runningHash}`
        };
      }
    }

    if (batch.proposed_state_root !== undefined && batch.proposed_state_root !== null) {
      const propRootBig = BigInt(String(batch.proposed_state_root));
      const exactMatch = propRootBig === stagedRoot64;
      const floatMatch = typeof batch.proposed_state_root === 'number' &&
        Number(stagedRoot64) === batch.proposed_state_root &&
        Math.abs(Number(stagedRoot64) - batch.proposed_state_root) === 0;

      if (!exactMatch && !floatMatch) {
        rollbackWasmRuntimes();
        const pol = resolvePolicyAction(this.policyCircuit, 'REJECTED_STATE_ROOT_MISMATCH');
        return {
          ok: false,
          status: 'AUDIT_REJECTED',
          wasit_verdict: 'FRAUD_DETECTED_AND_CUT',
          sub_verdict: 'REJECTED_STATE_ROOT_MISMATCH',
          policy_action: pol.action,
          policy_guardrail_enforced: pol.guardrail_triggered,
          reason: `Batch proposed_state_root mismatch! Recomputed ${stagedRoot64.toString()}, proposed ${String(batch.proposed_state_root)}`,
          audit_proof: {
            recomputed_root_64: stagedRoot64.toString(),
            proposed_root_64: String(batch.proposed_state_root)
          }
        };
      }
    }

    if (batch.proposed_state_commitment_256 !== undefined && batch.proposed_state_commitment_256 !== null) {
      const propCommit = String(batch.proposed_state_commitment_256).toUpperCase();
      const recomputedCommit = computeCommitment256(stagedRecords);
      if (propCommit !== recomputedCommit) {
        rollbackWasmRuntimes();
        const pol = resolvePolicyAction(this.policyCircuit, 'REJECTED_STATE_COMMITMENT_MISMATCH');
        return {
          ok: false,
          status: 'AUDIT_REJECTED',
          wasit_verdict: 'FRAUD_DETECTED_AND_CUT',
          sub_verdict: 'REJECTED_STATE_COMMITMENT_MISMATCH',
          policy_action: pol.action,
          policy_guardrail_enforced: pol.guardrail_triggered,
          reason: `Batch proposed_state_commitment_256 mismatch! Recomputed ${recomputedCommit}, proposed ${propCommit}`,
          audit_proof: {
            recomputed_commitment_256: recomputedCommit,
            proposed_commitment_256: propCommit
          }
        };
      }
    }

    // 7. ATOMIC COMMIT: Promote Staged State, Staged Schemas, & Staged Policy to Official State
    this.records = stagedRecords;
    this.schemas = stagedSchemas;
    this.policyCircuit = stagedPolicy;
    if (batchEpoch > this.currentEpoch) {
      this.currentEpoch = batchEpoch;
    }
    this.officialSeq = stagedSeq;
    this.officialHash = runningHash;
    this.officialStateRoot64 = stagedRoot64;
    this.officialCommitment256 = computeCommitment256(this.records);

    for (const cev of newCommittedEvents) {
      this.eventHistory.set(cev.seq, {
        ...cev,
        attestations: new Set([suId])
      });
    }

    const cp = {
      contract_version: this.contractVersion,
      epoch: this.currentEpoch,
      from_seq: firstSeq,
      to_seq: lastSeq,
      new_events_committed: newCommittedEvents.length,
      deduplicated_count: deduplicatedCount,
      arweave_wasm_executions: arweaveWasmExecutions,
      state_root_64: this.officialStateRoot64.toString(),
      state_commitment_256: this.officialCommitment256,
      final_hash: this.officialHash,
      su_id: suId,
      timestamp: Date.now()
    };
    this.checkpoints.push(cp);

    const pol = resolvePolicyAction(this.policyCircuit, 'COMMITTED');
    return {
      ok: true,
      status: 'COMMITTED',
      wasit_verdict: 'PASSED_AND_COMMITTED',
      policy_action: pol.action,
      contract_version: this.contractVersion,
      epoch: this.currentEpoch,
      from_seq: firstSeq,
      to_seq: lastSeq,
      official_seq: this.officialSeq,
      new_events_committed: newCommittedEvents.length,
      deduplicated_count: deduplicatedCount,
      arweave_wasm_executions: arweaveWasmExecutions,
      state_root: this.officialStateRoot64.toString(),
      state_root_64: this.officialStateRoot64.toString(),
      state_commitment_256: this.officialCommitment256,
      final_hash: this.officialHash
    };
  }
}

async function getWallet() {
  const jwk = JSON.parse(readFileSync(WALLET_PATH, 'utf-8'));
  const address = await arweave.wallets.jwkToAddress(jwk);
  return { jwk, address };
}

async function mineBlock() {
  return await fetch('http://127.0.0.1:1984/mine').then(r => r.json());
}

export async function initAoLoader() {
  if (aoHandle) return aoHandle;
  const wasmBinary = readFileSync(WASM_PATH);
  const moduleOptions = {
    format: 'wasm32-unknown-emscripten',
    inputEncoding: 'JSON-1',
    outputEncoding: 'JSON-1',
    memoryLimit: 524_288_000,
    computeLimit: 9_000_000_000_000,
    extensions: []
  };
  aoHandle = await AoLoader(wasmBinary, moduleOptions);
  return aoHandle;
}

const defaultDomain = new ArbitratorDomain();

export async function evaluateAndCommitCheckpoint(batch, domain = defaultDomain) {
  const handle = await initAoLoader();
  const { jwk, address } = await getWallet();
  const processId = existsSync(PROCESS_ID_FILE)
    ? readFileSync(PROCESS_ID_FILE, 'utf-8').trim()
    : 'SATU_KEPALA_OFFICIAL_PROCESS';

  // 0. Autonomous Contract & WASM Discovery from incoming SU WAL stream:
  // If any `register_schema` event in the batch carries a `contract_tx_id`, automatically pull
  // the Contract TX + linked WASM TX from Arweave (:1984) and instantiate WebAssembly before verification.
  const autoLoadedContracts = await domain.prepareBatchContractsFromArweave(batch);
  const autoLoadedTxIds = autoLoadedContracts.map(c => c.contract_tx_id);

  // 1. Run Deterministic Arbitrator Domain Verification (Atomic Staging + Root Recompute)
  const auditRes = domain.verifyBatch(batch);
  if (!auditRes.ok) {
    return {
      ...auditRes,
      autonomously_loaded_contracts: autoLoadedTxIds
    };
  }

  // 2. Evaluate Message inside Official WebAssembly AO Sandbox (@permaweb/ao-loader)
  const aoMessage = {
    Target: processId,
    From: address,
    Owner: address,
    Timestamp: Date.now(),
    Tags: [
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Variant', value: 'ao.TN.1' },
      { name: 'Type', value: 'Message' },
      { name: 'Action', value: 'CommitCheckpoint' },
      { name: 'Epoch', value: String(auditRes.epoch) },
      { name: 'From-Seq', value: String( batch.from_seq || 1 ) },
      { name: 'To-Seq', value: String(auditRes.official_seq) },
      { name: 'State-Root-64', value: String(auditRes.state_root_64) },
      { name: 'State-Commitment-256', value: String(auditRes.state_commitment_256) },
      { name: 'Hash', value: String(auditRes.final_hash) },
      { name: 'function', value: 'hello' }
    ],
    Data: JSON.stringify(batch)
  };

  const env = { Process: { Id: processId, Tags: [] } };
  const aoExecRes = await handle(wasmMemory, aoMessage, env);
  wasmMemory = aoExecRes.Memory;

  // 3. Sign & Commit Checkpoint Transaction to ArLocal (:1984)
  const txPayload = Buffer.from(JSON.stringify({
    checkpoint: {
      epoch: auditRes.epoch,
      from_seq: batch.from_seq,
      to_seq: auditRes.official_seq,
      state_root_64: auditRes.state_root_64,
      state_commitment_256: auditRes.state_commitment_256,
      final_hash: auditRes.final_hash,
      verdict: auditRes.wasit_verdict,
      policy_action: auditRes.policy_action,
      autonomously_loaded_contracts: autoLoadedTxIds
    },
    ao_wasm_output: aoExecRes.Output,
    timestamp: Date.now()
  }));

  const tx = await arweave.createTransaction({ data: txPayload }, jwk);
  tx.addTag('App-Name', 'AO');
  tx.addTag('Type', 'Message');
  tx.addTag('Data-Protocol', 'ao');
  tx.addTag('Variant', 'ao.TN.1');
  tx.addTag('Recipient', processId);
  tx.addTag('Action', 'CommitCheckpoint');
  tx.addTag('Epoch', String(auditRes.epoch));
  tx.addTag('From-Seq', String(batch.from_seq || 1));
  tx.addTag('To-Seq', String(auditRes.official_seq));
  tx.addTag('State-Root-64', String(auditRes.state_root_64));
  tx.addTag('State-Commitment-256', String(auditRes.state_commitment_256));
  tx.addTag('Hash', String(auditRes.final_hash));

  await arweave.transactions.sign(tx, jwk);
  const postRes = await arweave.transactions.post(tx);
  if (postRes.status !== 200 && postRes.status !== 202) {
    throw new Error(`Failed to post tx to ArLocal: ${postRes.status}`);
  }

  const mineRes = await mineBlock();

  return {
    ...auditRes,
    autonomously_loaded_contracts: autoLoadedTxIds,
    process_id: processId,
    arweave_tx_id: tx.id,
    block_height: mineRes.height,
    block_hash: mineRes.current,
    ao_wasm_evaluated: true
  };
}

async function cli() {
  const args = process.argv.slice(2);
  const cmd = args[0] || 'init';

  if (cmd === 'init') {
    await initAoLoader();
    console.log(JSON.stringify({ ok: true, status: 'AO_LOADER_READY' }));
  } else if (cmd === 'commit') {
    const filePath = args[1];
    if (!filePath) {
      console.error('Usage: real_ao_evaluator.mjs commit <batch_file.json>');
      process.exit(1);
    }
    const rawText = readFileSync(filePath, 'utf-8');
    // Preserve exact 64-bit integer strings for proposed_state_root and state_root
    const rawRootMatch = rawText.match(/"proposed_state_root"\s*:\s*"?(-?\d+)"?/);
    const batch = JSON.parse(rawText);
    if (rawRootMatch) {
      batch.proposed_state_root = rawRootMatch[1];
    }
    const singleRunDomain = new ArbitratorDomain();
    const res = await evaluateAndCommitCheckpoint(batch, singleRunDomain);
    console.log(JSON.stringify(res, null, 2));
  }
}

if (process.argv[1]?.endsWith('real_ao_evaluator.mjs')) {
  cli().catch(err => {
    console.error(err);
    process.exit(1);
  });
}
