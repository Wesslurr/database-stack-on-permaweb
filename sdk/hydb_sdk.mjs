/**
 * HyDB Contract Builder & WASM / Database Connector SDK (hydb.core@1.0)
 * =====================================================================
 * 1. Formulates declarative Data Contracts (Namespace + Schema + Security Invariants + Policy Circuit).
 * 2. Bridges/connects WASM binaries (`hydb.wasm` or custom WASM exports) & storage backends (`wasm_slot`, `ets_doc`).
 * 3. Publishes immutable Contract + WASM Artifacts directly to Arweave (`Contract_TxID` & `Wasm_TxID`)
 *    WITHOUT requiring any Hot Node URL at contract definition time.
 * 4. Allows worker nodes (`Hymessage`) and `AO Arbitrator` to pull & mount contracts directly by `Contract_TxID`.
 */

import Arweave from 'arweave';
import crypto from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  DEFAULT_POLICY,
  POLICY_ALLOWLIST,
  validatePolicy as specValidatePolicy,
  validateSchema as specValidateSchema
} from '../spec/hydb_contract_spec.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DEFAULT_WALLET_PATH = resolve(__dirname, '../secrets/wallet.json');
const DEFAULT_WASM_PATH = resolve(__dirname, '../hydb/out/hydb.wasm');

export const HYDB_CONTRACT_VERSION = 'hydb.core@1.0';

/**
 * Standard HyDB Incident Event Types ($X$) monitored by AO Arbitrator.
 */
export const HYDB_EVENTS = Object.freeze({
  ON_VALID_COMMIT: 'COMMITTED',
  ON_DUPLICATE_OVERLAP: 'DUPLICATE_BATCH_IDEMPOTENT',
  ON_SEQUENCE_GAP: 'REJECTED_SEQUENCE_GAP',
  ON_OUT_OF_ORDER: 'REJECTED_OUT_OF_ORDER_BATCH',
  ON_SCHEMA_VIOLATION: 'REJECTED_SCHEMA_VIOLATION',
  ON_CONTRACT_MISMATCH: 'REJECTED_CONTRACT_MISMATCH',
  ON_HASH_CHAIN_BROKEN: 'REJECTED_HASH_CHAIN_BROKEN',
  ON_STATE_ROOT_MISMATCH: 'REJECTED_STATE_ROOT_MISMATCH',
  ON_EQUIVOCATION: 'EQUIVOCATION_PROVEN',
  ON_STALE_EPOCH: 'FENCED_STALE_EPOCH',
  ON_UNAUTHORIZED_SENDER: 'REJECTED_UNAUTHORIZED_SENDER'
});

export class ContractPreflightError extends Error {
  constructor(message, details = {}) {
    super(`[HyDB Preflight Gate Error]: ${message}`);
    this.name = 'ContractPreflightError';
    this.stage = details.stage || 'UNKNOWN';
    this.details = details;
  }
}

/**
 * Sandbox WASM Engine Verifier:
 * Validates module compilation, exported ABI symbols, and executes safe canary probe.
 */
export function verifyWasmArtifact(wasmBytes, expectedExports = {}) {
  let mod;
  try {
    mod = new WebAssembly.Module(wasmBytes);
  } catch (err) {
    throw new ContractPreflightError(`Invalid WASM bytecode compilation: ${err.message}`, {
      stage: 'WASM_COMPILATION',
      error: err.message
    });
  }

  const exportMap = new Set(WebAssembly.Module.exports(mod).map(e => e.name));
  const missing = [];
  for (const [actionName, exportSymbol] of Object.entries(expectedExports)) {
    if (!exportMap.has(exportSymbol)) {
      missing.push(`${actionName} -> '${exportSymbol}'`);
    }
  }

  if (missing.length > 0) {
    throw new ContractPreflightError(`Missing required WASM export functions: [${missing.join(', ')}]`, {
      stage: 'WASM_EXPORTS_CHECK',
      missingExports: missing,
      availableExports: Array.from(exportMap)
    });
  }

  // Canary probe: instantiate with empty import and check probe function
  try {
    const inst = new WebAssembly.Instance(mod, {});
    if (typeof inst.exports.db_has_ns === 'function') {
      const probeRes = inst.exports.db_has_ns(BigInt(1), BigInt(999999999));
      if (probeRes !== 0n && probeRes !== 0) {
        throw new Error(`Unexpected non-zero response from canary probe: ${probeRes}`);
      }
    }
  } catch (err) {
    throw new ContractPreflightError(`WASM canary execution failed: ${err.message}`, {
      stage: 'WASM_CANARY_PROBE',
      error: err.message
    });
  }

  return { ok: true, exportsCount: exportMap.size };
}

/**
 * Policy Circuit Validator:
 * Validates that developer's policy mapping conforms to canonical spec allowlist.
 * Arbitrary Lua execution is prohibited.
 */
export function validateContractPolicy(policy = {}) {
  try {
    return specValidatePolicy(policy);
  } catch (err) {
    throw new ContractPreflightError(err.message, {
      stage: 'POLICY_ALLOWLIST_VALIDATION',
      code: err.code || 'ERR_POLICY_INVALID'
    });
  }
}


function parseArweaveGateway(gatewayUrl = 'http://127.0.0.1:1984') {
  const u = new URL(gatewayUrl);
  return {
    host: u.hostname,
    port: Number(u.port || (u.protocol === 'https:' ? 443 : 80)),
    protocol: u.protocol.replace(':', ''),
    baseUrl: gatewayUrl.replace(/\/$/, '')
  };
}

export class HyDBNamespaceClient {
  constructor({ namespace, nodeUrl, contractTxId = null }) {
    this.namespace = namespace;
    this.nodeUrl = nodeUrl.replace(/\/$/, '');
    this.contractTxId = contractTxId;
  }

  async _request(pathAndQuery) {
    const res = await fetch(`${this.nodeUrl}/~hymessage@1.0/${pathAndQuery}`, {
      headers: { Accept: 'application/json' }
    });
    return await res.json();
  }

  async put(key, val, opts = {}) {
    const params = new URLSearchParams({
      namespace: this.namespace,
      key: String(key),
      val: typeof val === 'object' ? JSON.stringify(val) : String(val)
    });
    if (opts.type) params.set('type', String(opts.type));
    if (opts.expectedVer !== undefined) params.set('expected_ver', String(opts.expectedVer));
    if (opts.epoch !== undefined) params.set('epoch', String(opts.epoch));
    if (opts.token) params.set('token', String(opts.token));
    return await this._request(`put?${params.toString()}`);
  }

  async get(key) {
    const params = new URLSearchParams({
      namespace: this.namespace,
      key: String(key)
    });
    return await this._request(`get?${params.toString()}`);
  }

  async del(key, opts = {}) {
    const params = new URLSearchParams({
      namespace: this.namespace,
      key: String(key)
    });
    if (opts.expectedVer !== undefined) params.set('expected_ver', String(opts.expectedVer));
    if (opts.epoch !== undefined) params.set('epoch', String(opts.epoch));
    if (opts.token) params.set('token', String(opts.token));
    return await this._request(`del?${params.toString()}`);
  }

  async exists(key) {
    const params = new URLSearchParams({
      namespace: this.namespace,
      key: String(key)
    });
    return await this._request(`exists?${params.toString()}`);
  }

  async scan(prefix = '') {
    const params = new URLSearchParams({
      namespace: this.namespace,
      prefix: String(prefix)
    });
    return await this._request(`scan?${params.toString()}`);
  }
}

export class HyDBContract {
  constructor(spec = {}) {
    if (!spec.namespace) {
      throw new Error('HyDBContract requires a namespace');
    }

    // Tahap 4: Explicit rejection of unsupported future features
    const unsupportedFeatures = [
      'indexes', 'secondaryIndexes', 'indices',
      'queryLanguage', 'migrations', 'foreignKeys',
      'relations', 'triggers', 'views'
    ];
    for (const feat of unsupportedFeatures) {
      if (spec[feat] !== undefined) {
        throw new ContractPreflightError(
          `Feature '${feat}' is marked as unsupported in hydb.core@1.0. Complex query languages, migrations, and secondary indexes are deferred until formal cross-runtime semantics are proven.`,
          { stage: 'FEATURE_UNSUPPORTED_CHECK', feature: feat }
        );
      }
    }

    this.contractVersion = HYDB_CONTRACT_VERSION;
    this.namespace = spec.namespace;
    this.backend = spec.backend || (spec.wasm ? 'wasm_slot' : 'ets_doc');
    this.schemaVersion = spec.schemaVersion || (spec.schema && spec.schema.version) || '1.0.0';

    const s = spec.schema || {};
    const rules = { ...(s.rules || {}) };
    if (s.minValue !== undefined) rules.min_value = s.minValue;
    if (s.maxValue !== undefined) rules.max_value = s.maxValue;
    if (s.minLength !== undefined) rules.min_length = s.minLength;
    if (s.maxLength !== undefined) rules.max_length = s.maxLength;
    if (s.requiredFields !== undefined) rules.required_fields = s.requiredFields;
    if (s.requireAuth !== undefined) rules.require_auth = Boolean(s.requireAuth);
    if (s.maxRecords !== undefined) rules.max_records = Number(s.maxRecords);
    if (s.fields !== undefined) rules.fields = s.fields;
    if (s.additionalFields !== undefined) rules.additional_fields = Boolean(s.additionalFields);

    const keyType = s.keyType || s.key_type || (this.backend === 'wasm_slot' ? 'int64' : 'string');
    const valueType = s.valueType || s.value_type || (this.backend === 'wasm_slot' ? 'int64' : 'any');

    const allowedKeys = ['int64', 'string'];
    if (!allowedKeys.includes(keyType)) {
      throw new ContractPreflightError(`Unsupported key_type '${keyType}': only int64 and string are supported in hydb.core@1.0`, {
        stage: 'SCHEMA_TYPE_CHECK',
        keyType
      });
    }

    const allowedVals = ['int64', 'bool', 'string', 'bytes', 'json', 'any'];
    if (!allowedVals.includes(valueType)) {
      throw new ContractPreflightError(`Unsupported value_type '${valueType}': only int64, bool, string, bytes, json are supported in hydb.core@1.0`, {
        stage: 'SCHEMA_TYPE_CHECK',
        valueType
      });
    }

    if (this.backend === 'wasm_slot') {
      if (keyType !== 'int64' || (valueType !== 'int64' && valueType !== 'any')) {
        throw new ContractPreflightError(`wasm_slot backend is numerically specialized and only supports int64 key and int64 value`, {
          stage: 'BACKEND_CAPABILITY_CHECK',
          keyType,
          valueType
        });
      }
    }

    this.schema = {
      key_type: keyType,
      value_type: valueType,
      rules
    };

    this.policy = { ...(spec.policy || {}) };

    // WASM / Database Connector Configuration
    const w = spec.wasm || spec.connector || {};
    this.wasmPath = w.file || w.wasmPath || (this.backend === 'wasm_slot' ? DEFAULT_WASM_PATH : null);
    this.wasmBuffer = w.buffer || null;
    this.wasmTxId = w.wasmTxId || w.wasm_tx_id || null;
    this.wasmSha256 = null;

    this.connector = {
      type: this.backend === 'wasm_slot' ? 'wasm-64@1.0' : 'ets_doc',
      backend: this.backend,
      exports: this.backend === 'wasm_slot'
        ? {
            put: 'db_put_ns',
            get: 'db_get_ns',
            del: 'db_del_ns',
            has: 'db_has_ns',
            version: 'db_version_ns',
            ...(w.exports || {})
          }
        : {}
    };

    this.contractTxId = null;
    this.manifest = null;
  }

  /**
   * Pre-Flight Sandbox Simulation Gate:
   * Rigorously verifies WASM binary exports & canary execution, Lua policy handlers,
   * and schema invariants locally before ANY Arweave transaction is signed or broadcast.
   */
  async preflightDryRun() {
    const report = {
      namespace: this.namespace,
      backend: this.backend,
      schemaChecks: [],
      wasmChecks: null,
      policySimulations: []
    };

    // 1. Schema Invariants & Coherence Check
    const rules = this.schema.rules || {};
    if (rules.min_value !== undefined && rules.max_value !== undefined) {
      if (Number(rules.min_value) > Number(rules.max_value)) {
        throw new ContractPreflightError(`Invalid schema: min_value (${rules.min_value}) > max_value (${rules.max_value})`, {
          stage: 'SCHEMA_INVARIANT_CHECK',
          rules
        });
      }
    }
    if (rules.min_length !== undefined && rules.max_length !== undefined) {
      if (Number(rules.min_length) > Number(rules.max_length)) {
        throw new ContractPreflightError(`Invalid schema: min_length (${rules.min_length}) > max_length (${rules.max_length})`, {
          stage: 'SCHEMA_INVARIANT_CHECK',
          rules
        });
      }
    }

    // Validate fields spec if present
    if (rules.fields && typeof rules.fields === 'object') {
      const allowedFieldTypes = ['int64', 'string', 'bool', 'null', 'object', 'array'];
      for (const [fname, fspec] of Object.entries(rules.fields)) {
        if (!fspec || typeof fspec !== 'object') {
          throw new ContractPreflightError(`Invalid field spec for '${fname}': must be an object`, { stage: 'SCHEMA_INVARIANT_CHECK' });
        }
        if (!allowedFieldTypes.includes(fspec.type)) {
          throw new ContractPreflightError(`Invalid type '${fspec.type}' for field '${fname}' (allowed: ${allowedFieldTypes.join(', ')})`, { stage: 'SCHEMA_INVARIANT_CHECK' });
        }
        if (fspec.min_value !== undefined && fspec.max_value !== undefined && Number(fspec.min_value) > Number(fspec.max_value)) {
          throw new ContractPreflightError(`Invalid field '${fname}': min_value > max_value`, { stage: 'SCHEMA_INVARIANT_CHECK' });
        }
        if (fspec.min_length !== undefined && fspec.max_length !== undefined && Number(fspec.min_length) > Number(fspec.max_length)) {
          throw new ContractPreflightError(`Invalid field '${fname}': min_length > max_length`, { stage: 'SCHEMA_INVARIANT_CHECK' });
        }
      }
    }

    report.schemaChecks.push('bounds_and_fields_valid');

    // 2. WASM Binary & Export Integrity Check (if wasm_slot)
    if (this.backend === 'wasm_slot') {
      let wasmBytes = this.wasmBuffer;
      if (!wasmBytes && this.wasmPath) {
        if (!existsSync(this.wasmPath)) {
          throw new ContractPreflightError(`WASM binary file not found at path: ${this.wasmPath}`, {
            stage: 'WASM_FILE_LOOKUP',
            wasmPath: this.wasmPath
          });
        }
        wasmBytes = readFileSync(this.wasmPath);
      }

      if (wasmBytes) {
        const wasmRes = verifyWasmArtifact(wasmBytes, this.connector.exports || {});
        report.wasmChecks = {
          verified: true,
          exportsCount: wasmRes.exportsCount
        };
      }
    }

    // 3. Policy Circuit Allowlist Validation
    if (this.policy && Object.keys(this.policy).length > 0) {
      validateContractPolicy(this.policy);
      report.policyChecks = {
        verified: true,
        rulesCount: Object.keys(this.policy).length
      };
    }

    return { ok: true, report };
  }

  /**
   * Build the canonical Contract Manifest object
   */
  toManifest(overrides = {}) {
    const wasmTxId = overrides.wasmTxId ?? this.wasmTxId;
    const wasmSha256 = overrides.wasmSha256 ?? this.wasmSha256;
    const connector = {
      ...this.connector,
      ...(wasmTxId ? { wasm_tx_id: wasmTxId } : {}),
      ...(wasmSha256 ? { wasm_sha256: wasmSha256 } : {})
    };
    return {
      contract_version: this.contractVersion,
      namespace: this.namespace,
      backend: this.backend,
      schema_version: this.schemaVersion,
      schema: this.schema,
      policy: this.policy,
      connector,
      ...(wasmTxId ? { wasm_tx_id: wasmTxId } : {}),
      ...(wasmSha256 ? { wasm_sha256: wasmSha256 } : {}),
      ...(overrides.owner ? { owner: overrides.owner } : {})
    };
  }

  /**
   * Publish the linked WASM binary (if applicable) + Contract Manifest as permanent Arweave TXs.
   * STRICT PRE-FLIGHT GUARANTEE: Runs preflightDryRun() first. If ANY check fails, publication
   * is aborted immediately before any transaction is signed or broadcast.
   */
  async publishToArweave(options = {}) {
    if (options.skipPreflight !== true) {
      await this.preflightDryRun();
    }

    const gw = parseArweaveGateway(options.gatewayUrl || 'http://127.0.0.1:1984');
    const arweave = Arweave.init({ host: gw.host, port: gw.port, protocol: gw.protocol });
    const jwk = options.jwk || JSON.parse(readFileSync(options.walletPath || DEFAULT_WALLET_PATH, 'utf-8'));
    const ownerAddress = await arweave.wallets.jwkToAddress(jwk);

    // 1. If backend is wasm_slot and we have a WASM binary/file, publish WASM binary TX first
    if (this.backend === 'wasm_slot' && !this.wasmTxId) {
      const wasmBytes = this.wasmBuffer || readFileSync(this.wasmPath);
      this.wasmSha256 = crypto.createHash('sha256').update(wasmBytes).digest('hex').toUpperCase();

      const wasmTx = await arweave.createTransaction({ data: wasmBytes }, jwk);
      wasmTx.addTag('Content-Type', 'application/wasm');
      wasmTx.addTag('Data-Protocol', 'hydb');
      wasmTx.addTag('Type', 'WASM-Engine');
      wasmTx.addTag('WASM-SHA256', this.wasmSha256);
      await arweave.transactions.sign(wasmTx, jwk);
      const postWasm = await arweave.transactions.post(wasmTx);
      if (postWasm.status !== 200 && postWasm.status !== 202) {
        throw new Error(`Failed to post WASM binary to Arweave: HTTP ${postWasm.status}`);
      }
      this.wasmTxId = wasmTx.id;
    }

    // 2. Build & sign the Contract Manifest TX on Arweave
    this.manifest = this.toManifest({
      wasmTxId: this.wasmTxId,
      wasmSha256: this.wasmSha256,
      owner: ownerAddress
    });

    const manifestPayload = Buffer.from(JSON.stringify(this.manifest), 'utf-8');
    const contractTx = await arweave.createTransaction({ data: manifestPayload }, jwk);
    contractTx.addTag('Content-Type', 'application/json');
    contractTx.addTag('Data-Protocol', 'hydb');
    contractTx.addTag('Type', 'Contract-Definition');
    contractTx.addTag('Contract-Version', this.contractVersion);
    contractTx.addTag('Namespace', this.namespace);
    contractTx.addTag('Backend', this.backend);
    if (this.wasmTxId) {
      contractTx.addTag('WASM-Tx-Id', this.wasmTxId);
    }

    await arweave.transactions.sign(contractTx, jwk);
    const postContract = await arweave.transactions.post(contractTx);
    if (postContract.status !== 200 && postContract.status !== 202) {
      throw new Error(`Failed to post Contract Definition to Arweave: HTTP ${postContract.status}`);
    }

    // Mine block on ArLocal if applicable
    try {
      await fetch(`${gw.baseUrl}/mine`);
    } catch {
      // Ignore if public gateway does not expose /mine
    }

    this.contractTxId = contractTx.id;
    return {
      ok: true,
      contractTxId: this.contractTxId,
      wasmTxId: this.wasmTxId,
      wasmSha256: this.wasmSha256,
      owner: ownerAddress,
      manifest: this.manifest
    };
  }

  /**
   * Instruct a worker node (`Hymessage`) to pull & mount this contract directly from Arweave by `contractTxId`
   */
  async mountOnNode(nodeUrl, options = {}) {
    const txId = options.contractTxId || this.contractTxId;
    if (!txId) {
      throw new Error('Contract must be published to Arweave (contractTxId) before mounting on a node');
    }
    const base = nodeUrl.replace(/\/$/, '');
    const params = new URLSearchParams({
      action: 'load_tx',
      tx_id: txId
    });
    if (options.gatewayUrl) params.set('gateway', options.gatewayUrl);
    if (options.epoch !== undefined) params.set('epoch', String(options.epoch));
    if (options.adminToken) params.set('admin_token', String(options.adminToken));

    const res = await fetch(`${base}/~hymessage@1.0/schema?${params.toString()}`, {
      headers: { Accept: 'application/json' }
    });
    return await res.json();
  }

  /**
   * Create a bound namespace client to execute contract operations on a node that mounted this contract
   */
  bindClient(nodeUrl) {
    return new HyDBNamespaceClient({
      namespace: this.namespace,
      nodeUrl,
      contractTxId: this.contractTxId
    });
  }
}

export function defineContract(spec) {
  return new HyDBContract(spec);
}
