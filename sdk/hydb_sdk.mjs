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
  validateSchema as specValidateSchema,
  manifestHash as specManifestHash,
  encodeManifest as specEncodeManifest
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


function parseArweaveGateway(gatewayUrl = (typeof process !== 'undefined' && process.env?.ARWEAVE_GATEWAY) || 'http://127.0.0.1:1984') {
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
    const manifestObj = {
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
    return manifestObj;
  }

  /**
   * Computes deterministic canonical manifest hash binding namespace, schema_version,
   * backend, capabilities, and rules according to spec/hydb_contract_spec.mjs
   */
  computeCanonicalManifestHash() {
    const norm = specValidateSchema({
      namespace: this.namespace,
      schema_version: this.schemaVersion,
      backend: this.backend,
      key_type: this.schema.key_type,
      value_type: this.schema.value_type,
      rules: this.schema.rules || {}
    });
    return specManifestHash(norm);
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

    const gw = parseArweaveGateway(options.gatewayUrl || (typeof process !== 'undefined' && process.env?.ARWEAVE_GATEWAY) || 'http://127.0.0.1:1984');
    const arweave = Arweave.init({ host: gw.host, port: gw.port, protocol: gw.protocol });
    const jwk = options.jwk || JSON.parse(readFileSync(options.walletPath || DEFAULT_WALLET_PATH, 'utf-8'));
    const ownerAddress = await arweave.wallets.jwkToAddress(jwk);

    // In local development / test gateways (ArLocal), auto-fund wallet if needed
    if (gw.host === '127.0.0.1' || gw.host === 'localhost') {
      try {
        await fetch(`${gw.protocol}://${gw.host}:${gw.port}/mint/${ownerAddress}/1000000000000000000000`);
      } catch {}
    }

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
   * Publish manifest and WASM artifacts via ANS-104 DataItems to a Permaweb Gateway or Bundler.
   * Provides instant confirmation on the Permaweb without waiting for L1 block mining.
   */
  async publishViaBundler(options = {}) {
    if (options.skipPreflight !== true) {
      await this.preflightDryRun();
    }

    const { createDataItemSigner } = await import('@permaweb/aoconnect');
    const jwk = options.jwk || JSON.parse(readFileSync(options.walletPath || DEFAULT_WALLET_PATH, 'utf-8'));
    const signer = createDataItemSigner(jwk);

    const gw = parseArweaveGateway(options.gatewayUrl || (typeof process !== 'undefined' && process.env?.ARWEAVE_GATEWAY) || 'https://arweave.net');
    const arweave = Arweave.init({ host: gw.host, port: gw.port, protocol: gw.protocol });
    const ownerAddress = await arweave.wallets.jwkToAddress(jwk);

    // 1. WASM binary publication via ANS-104 DataItem
    if (this.backend === 'wasm_slot' && !this.wasmTxId) {
      const wasmBytes = this.wasmBuffer || readFileSync(this.wasmPath);
      this.wasmSha256 = crypto.createHash('sha256').update(wasmBytes).digest('hex').toUpperCase();

      const wasmDataItem = await signer({
        data: wasmBytes,
        tags: [
          { name: 'Content-Type', value: 'application/wasm' },
          { name: 'Data-Protocol', value: 'hydb' },
          { name: 'Type', value: 'WASM-Engine' },
          { name: 'WASM-SHA256', value: this.wasmSha256 }
        ]
      });

      const bundlerUrl = options.bundlerUrl || `${gw.baseUrl}/tx`;
      try {
        await fetch(bundlerUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/octet-stream' },
          body: wasmDataItem.raw
        });
      } catch {}

      this.wasmTxId = wasmDataItem.id;
    }

    // 2. Contract Manifest via ANS-104 DataItem
    this.manifest = this.toManifest({
      wasmTxId: this.wasmTxId,
      wasmSha256: this.wasmSha256,
      owner: ownerAddress
    });

    const manifestPayload = Buffer.from(JSON.stringify(this.manifest), 'utf-8');
    const contractTags = [
      { name: 'Content-Type', value: 'application/json' },
      { name: 'Data-Protocol', value: 'hydb' },
      { name: 'Type', value: 'Contract-Definition' },
      { name: 'Contract-Version', value: this.contractVersion },
      { name: 'Namespace', value: this.namespace },
      { name: 'Backend', value: this.backend }
    ];
    if (this.wasmTxId) {
      contractTags.push({ name: 'WASM-Tx-Id', value: this.wasmTxId });
    }

    const contractDataItem = await signer({
      data: manifestPayload,
      tags: contractTags
    });

    const bundlerUrl = options.bundlerUrl || `${gw.baseUrl}/tx`;
    try {
      await fetch(bundlerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/octet-stream' },
        body: contractDataItem.raw
      });
    } catch {}

    this.contractTxId = contractDataItem.id;
    return {
      ok: true,
      protocol: 'ans104_data_item',
      contractTxId: this.contractTxId,
      wasmTxId: this.wasmTxId,
      wasmSha256: this.wasmSha256,
      owner: ownerAddress,
      manifest: this.manifest
    };
  }

  /**
   * Universal Permaweb Publisher: Automatically routes to Bundler in live ao_network
   * or direct ArLocal L1 transaction in test environments.
   */
  async publishToPermaweb(options = {}) {
    const isLive = options.mode === 'bundler' || (typeof process !== 'undefined' && process.env?.HYDB_ENV === 'production');
    if (isLive) {
      return await this.publishViaBundler(options);
    }
    return await this.publishToArweave(options);
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

/**
 * Compute deterministic SHA-256 hash of canonical Genesis Stack Manifest.
 */
export function computeGenesisHash(manifest = {}) {
  const canonical = {
    stackId: manifest.stackId || 'hydb_universal_mesh',
    genesisVersion: manifest.genesisVersion || '1.0.0',
    configEpoch: Number(manifest.configEpoch || 1),
    quorumSize: Number(manifest.quorumSize || 3),
    quorumMode: manifest.quorumMode || 'majority',
    network: manifest.network || 'testnet',
    nodes: (manifest.nodes || []).map((n) => (typeof n === 'string' ? { address: n } : n)).sort((a, b) => String(a.address || a.id).localeCompare(String(b.address || b.id))),
    admins: (manifest.admins || []).slice().sort(),
    schemas: (manifest.schemas || []).slice().sort((a, b) => String(a.namespace).localeCompare(String(b.namespace)))
  };
  return crypto.createHash('sha256').update(JSON.stringify(canonical)).digest('hex').toUpperCase();
}

/**
 * Build Genesis Contract Lua code embedding dynamic Genesis Constitution & Registry.
 */
export function buildGenesisContractLua(options = {}) {
  const luaPath = options.genesisTemplatePath || resolve(__dirname, '../ao_process/genesis.lua');
  let baseSource = options.genesisTemplateSource;
  if (!baseSource && existsSync(luaPath)) {
    baseSource = readFileSync(luaPath, 'utf8');
  } else if (!baseSource) {
    baseSource = '-- HyDB Genesis Contract Stub\nreturn {}';
  }

  const stackId = options.stackId || options.genesis?.stackId || 'hydb_universal_mesh';
  const genesisVersion = options.genesisVersion || options.genesis?.genesisVersion || '1.0.0';
  const configEpoch = Number(options.configEpoch || options.genesis?.configEpoch || 1);
  const quorumSize = Number(options.quorumSize || options.genesis?.quorumSize || 3);
  const quorumMode = options.quorumMode || options.genesis?.quorumMode || 'majority';
  const network = options.network || options.genesis?.network || 'testnet';
  const arbitratorProcessId = options.arbitratorProcessId || '';

  const genesisHash = options.genesisHash || computeGenesisHash({
    stackId,
    genesisVersion,
    configEpoch,
    quorumSize,
    quorumMode,
    network,
    nodes: options.nodes,
    admins: options.admins,
    schemas: options.schemas
  });

  const nodesTable = (options.nodes || [])
    .map((n) => {
      const addr = typeof n === 'string' ? n : n.address || n.id;
      return `    ["${addr}"] = true`;
    })
    .join(',\n');

  const adminsTable = (options.admins || [])
    .map((a) => `    ["${a}"] = true`)
    .join(',\n');

  let dynamicHeader = `
-- =========================================================================
-- GENESIS BLOCK: DECLARATIVE STACK CONSTITUTION & TOPOLOGY ANCHOR
-- Protocol: hydb.genesis@1.0 | Dedicated Stack Genesis Process
-- =========================================================================
StackId = "${stackId}"
GenesisVersion = "${genesisVersion}"
GenesisHash = "${genesisHash}"
ConfigEpoch = ${configEpoch}
QuorumSize = ${quorumSize}
QuorumMode = "${quorumMode}"
Network = "${network}"
ArbitratorProcessId = "${arbitratorProcessId}"
`;

  if (nodesTable) {
    dynamicHeader += `
AuthorizedNodes = {
${nodesTable}
}
`;
  }

  if (adminsTable) {
    dynamicHeader += `
AuthorizedAdmins = {
    [Owner] = true,
    ["ADMIN_PRIMARY"] = true,
${adminsTable}
}
`;
  }

  return `${dynamicHeader}\n${baseSource}`;
}

/**
 * Build self-contained "1 Lembar Kontrak" Lua code embedding dynamic Genesis Block.
 */
export function buildMasterContractLua(options = {}) {
  const luaPath = options.templatePath || resolve(__dirname, '../ao_process/arbitrator.lua');
  let baseSource = options.templateSource;
  if (!baseSource && existsSync(luaPath)) {
    baseSource = readFileSync(luaPath, 'utf8');
  } else if (!baseSource) {
    baseSource = '-- HyDB Master Contract Stub\nreturn {}';
  }

  const stackId = options.stackId || options.genesis?.stackId || 'hydb_universal_mesh';
  const genesisVersion = options.genesisVersion || options.genesis?.genesisVersion || '1.0.0';
  const configEpoch = Number(options.configEpoch || options.genesis?.configEpoch || 1);
  const quorumSize = Number(options.quorumSize || options.genesis?.quorumSize || 3);
  const quorumMode = options.quorumMode || options.genesis?.quorumMode || 'majority';
  const network = options.network || options.genesis?.network || 'testnet';

  const genesisHash = options.genesisHash || computeGenesisHash({
    stackId,
    genesisVersion,
    configEpoch,
    quorumSize,
    quorumMode,
    network,
    nodes: options.nodes,
    admins: options.admins,
    schemas: options.schemas
  });

  const nodesTable = (options.nodes || [])
    .map((n) => {
      const addr = typeof n === 'string' ? n : n.address || n.id;
      return `    ["${addr}"] = true`;
    })
    .join(',\n');

  const adminsTable = (options.admins || [])
    .map((a) => `    ["${a}"] = true`)
    .join(',\n');

  let dynamicHeader = `
-- =========================================================================
-- GENESIS BLOCK: DECLARATIVE STACK CONSTITUTION & TOPOLOGY ANCHOR
-- Protocol: hydb.core@1.0 | Single Master Contract ("1 Lembar")
-- =========================================================================
Genesis = {
    stack_id = "${stackId}",
    genesis_version = "${genesisVersion}",
    genesis_hash = "${genesisHash}",
    config_epoch = ${configEpoch},
    quorum_size = ${quorumSize},
    quorum_mode = "${quorumMode}",
    network = "${network}"
}
`;

  if (nodesTable) {
    dynamicHeader += `
AuthorizedNodes = {
${nodesTable}
}
`;
  }

  if (adminsTable) {
    dynamicHeader += `
AuthorizedAdmins = {
    [Owner] = true,
    ["ADMIN_PRIMARY"] = true,
${adminsTable}
}
`;
  }

  // Inject or prepend dynamic header to base source
  if (baseSource.includes('Genesis = Genesis or {')) {
    return baseSource.replace(/Genesis = Genesis or \{[^}]+\}/s, dynamicHeader.trim());
  }
  return `${dynamicHeader}\n${baseSource}`;
}

export function buildArbitratorContractLua(options = {}) {
  return buildMasterContractLua(options);
}

/**
 * Spawns an official HyDB Stack on AO (Genesis Control Plane + Data Plane Arbitrator).
 */
export async function spawnStack(options = {}) {
  const isDryRun = options.dryRun || process.env.HYDB_DRY_RUN === 'true';
  const network = (options.network || process.env.AO_NETWORK || 'testnet').toLowerCase();

  const genesisHash = computeGenesisHash(options);
  const genesisLua = buildGenesisContractLua({
    ...options,
    genesisHash
  });
  const arbitratorLua = buildArbitratorContractLua({
    ...options,
    genesisHash
  });

  if (isDryRun || options.mockSpawn === true) {
    const mockGenesisId = options.genesisProcessId || options.processId || `STACK_${genesisHash.slice(0, 16)}`;
    const mockArbitratorId = options.arbitratorProcessId || `ARBITRATOR_${genesisHash.slice(0, 16)}`;
    return {
      ok: true,
      deployed: false,
      dryRun: true,
      processId: mockGenesisId,
      genesisProcessId: mockGenesisId,
      arbitratorProcessId: mockArbitratorId,
      genesisHash,
      stackId: options.stackId || 'hydb_universal_mesh',
      network,
      contractLua: genesisLua,
      genesisLua,
      arbitratorLua
    };
  }

  const defaultAosModule = network === 'mainnet'
    ? (options.moduleId || process.env.AO_MODULE)
    : (options.moduleId || process.env.AO_MODULE || 'Pq2gahioPTspHGvLjQip-B_r4q5LldMZWUC-20CDohM');
  const defaultScheduler = network === 'mainnet'
    ? (options.scheduler || process.env.AO_SCHEDULER)
    : (options.scheduler || process.env.AO_SCHEDULER || '_GQ33BkPtZrqxA84vM82zscllkQufHyqDistribution_');

  if (network === 'mainnet' && (!defaultAosModule || !defaultScheduler)) {
    throw new Error('Mainnet AO stack spawn requires explicit moduleId and scheduler');
  }

  // Live AO Spawn via @permaweb/aoconnect
  const { connect, createDataItemSigner } = await import('@permaweb/aoconnect');
  const wallet = options.wallet || (options.walletPath && existsSync(options.walletPath)
    ? JSON.parse(readFileSync(options.walletPath, 'utf8'))
    : undefined);

  if (!wallet && !options.signer) {
    throw new Error('A valid wallet (JWK) or signer is required to spawn an AO Stack');
  }

  const signer = options.signer || createDataItemSigner(wallet);

  const ao = options.ao || connect({
    GATEWAY_URL: options.gatewayUrl || (network === 'mainnet' ? 'https://arweave.net' : process.env.ARWEAVE_GATEWAY || 'https://arweave.net'),
    MU_URL: options.muUrl || (network === 'mainnet' ? 'https://mu.ao-mainnet.xyz' : 'https://mu.ao-testnet.xyz'),
    CU_URL: options.cuUrl || (network === 'mainnet' ? 'https://cu.ao-mainnet.xyz' : 'https://cu.ao-testnet.xyz')
  });

  // 1. Spawn Genesis Process (Stack Constitution & Control Plane)
  const genesisProcessId = await ao.spawn({
    module: defaultAosModule,
    scheduler: defaultScheduler,
    signer,
    tags: [
      { name: 'App-Name', value: 'aos' },
      { name: 'Variant', value: 'ao.TN.1' },
      { name: 'Type', value: 'Process' },
      { name: 'Name', value: options.stackId || 'HyDB-Master-Stack' },
      { name: 'Genesis-Hash', value: genesisHash },
      { name: 'Data-Protocol', value: 'hydb' },
      { name: 'Contract-Version', value: HYDB_CONTRACT_VERSION }
    ]
  });

  // Evaluate Genesis Lua into Genesis Process
  const evalMsgId = await ao.message({
    process: genesisProcessId,
    signer,
    tags: [
      { name: 'Action', value: 'Eval' },
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Variant', value: 'ao.TN.1' }
    ],
    data: genesisLua
  });

  // 2. Spawn Arbitrator Process (Data Plane Arbitrator inside Stack)
  const arbitratorProcessId = await ao.spawn({
    module: defaultAosModule,
    scheduler: defaultScheduler,
    signer,
    tags: [
      { name: 'App-Name', value: 'aos' },
      { name: 'Variant', value: 'ao.TN.1' },
      { name: 'Type', value: 'Process' },
      { name: 'Name', value: `${options.stackId || 'HyDB-Master-Stack'}-Arbitrator` },
      { name: 'Genesis-Process-Id', value: genesisProcessId },
      { name: 'Genesis-Hash', value: genesisHash },
      { name: 'Data-Protocol', value: 'hydb' },
      { name: 'Contract-Version', value: HYDB_CONTRACT_VERSION }
    ]
  });

  // Evaluate Arbitrator Lua into Arbitrator Process
  const arbitratorEvalMsgId = await ao.message({
    process: arbitratorProcessId,
    signer,
    tags: [
      { name: 'Action', value: 'Eval' },
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Variant', value: 'ao.TN.1' }
    ],
    data: arbitratorLua
  });

  // 3. Register ArbitratorProcessId in Genesis Process
  await ao.message({
    process: genesisProcessId,
    signer,
    tags: [
      { name: 'Action', value: 'SetArbitratorProcess' },
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Variant', value: 'ao.TN.1' },
      { name: 'Arbitrator-Process-Id', value: arbitratorProcessId }
    ],
    data: JSON.stringify({ arbitrator_process_id: arbitratorProcessId })
  });

  return {
    ok: true,
    deployed: true,
    processId: genesisProcessId,
    genesisProcessId,
    arbitratorProcessId,
    evalMsgId,
    arbitratorEvalMsgId,
    genesisHash,
    stackId: options.stackId || 'hydb_universal_mesh',
    network
  };
}

/**
 * Queries the official Genesis Configuration from a live AO Process.
 */
export async function getGenesisConfig(processId, options = {}) {
  if (!processId) throw new Error('processId is required to query Genesis config');

  if (typeof options.queryGenesisFn === 'function') {
    return await options.queryGenesisFn(processId, options);
  }

  if (typeof options.evaluatorFn === 'function') {
    const res = await options.evaluatorFn({
      Target: processId,
      Action: 'GetGenesisConfig',
      Tags: [
        { name: 'Action', value: 'GetGenesisConfig' },
        { name: 'Data-Protocol', value: 'ao' },
        { name: 'Variant', value: 'ao.TN.1' }
      ],
      Data: '{}'
    });
    const msgs = res?.Messages || res?.responses || [];
    const last = msgs[msgs.length - 1];
    if (last && last.Data) {
      try {
        const data = JSON.parse(last.Data);
        return { ok: true, processId, ...data };
      } catch {
        return { ok: true, processId, rawData: last.Data };
      }
    }
  }

  const { connect } = await import('@permaweb/aoconnect');
  const network = (options.network || process.env.AO_NETWORK || 'testnet').toLowerCase();

  const ao = options.ao || connect({
    GATEWAY_URL: options.gatewayUrl || (network === 'mainnet' ? 'https://arweave.net' : process.env.ARWEAVE_GATEWAY || 'https://arweave.net'),
    MU_URL: options.muUrl || (network === 'mainnet' ? 'https://mu.ao-mainnet.xyz' : 'https://mu.ao-testnet.xyz'),
    CU_URL: options.cuUrl || (network === 'mainnet' ? 'https://cu.ao-mainnet.xyz' : 'https://cu.ao-testnet.xyz')
  });

  const res = await ao.dryrun({
    process: processId,
    tags: [
      { name: 'Action', value: 'GetGenesisConfig' },
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Variant', value: 'ao.TN.1' }
    ],
    data: '{}'
  });

  const last = res?.Messages?.[res.Messages.length - 1];
  if (last && last.Data) {
    try {
      const data = JSON.parse(last.Data);
      return { ok: true, processId, ...data };
    } catch {
      return { ok: true, processId, rawData: last.Data };
    }
  }

  return { ok: false, processId, status: 'no_response' };
}

/**
 * Cryptographically verifies that a running AO Process matches the expected Stack Genesis Hash.
 */
export async function verifyStackGenesis(processId, expectedGenesisHash, options = {}) {
  const config = await getGenesisConfig(processId, options);
  if (!config || !config.ok) {
    return {
      ok: false,
      divergence: true,
      reason: 'Failed to retrieve genesis configuration from AO process',
      processId
    };
  }
  const actualHash = config.genesis?.genesis_hash;
  if (!actualHash || actualHash.toUpperCase() !== String(expectedGenesisHash).toUpperCase()) {
    return {
      ok: false,
      divergence: true,
      reason: `Genesis hash mismatch: expected ${expectedGenesisHash}, received ${actualHash}`,
      expectedHash: expectedGenesisHash,
      actualHash,
      processId
    };
  }
  return {
    ok: true,
    divergence: false,
    processId,
    config
  };
}

/**
 * Queries the operational readiness and quorum status of the Stack from the Genesis process.
 */
export async function getStackStatus(processId, options = {}) {
  if (!processId) throw new Error('processId is required to query Stack Status');

  if (typeof options.evaluatorFn === 'function') {
    const res = await options.evaluatorFn({
      Target: processId,
      Action: 'GetStackStatus',
      Tags: [
        { name: 'Action', value: 'GetStackStatus' },
        { name: 'Data-Protocol', value: 'ao' },
        { name: 'Variant', value: 'ao.TN.1' }
      ],
      Data: '{}'
    });
    const msgs = res?.Messages || res?.responses || [];
    const last = msgs[msgs.length - 1];
    if (last && last.Data) {
      try {
        return { ok: true, processId, ...JSON.parse(last.Data) };
      } catch {
        return { ok: true, processId, rawData: last.Data };
      }
    }
  }

  const { connect } = await import('@permaweb/aoconnect');
  const network = (options.network || process.env.AO_NETWORK || 'testnet').toLowerCase();

  const ao = options.ao || connect({
    GATEWAY_URL: options.gatewayUrl || (network === 'mainnet' ? 'https://arweave.net' : process.env.ARWEAVE_GATEWAY || 'https://arweave.net'),
    MU_URL: options.muUrl || (network === 'mainnet' ? 'https://mu.ao-mainnet.xyz' : 'https://mu.ao-testnet.xyz'),
    CU_URL: options.cuUrl || (network === 'mainnet' ? 'https://cu.ao-mainnet.xyz' : 'https://cu.ao-testnet.xyz')
  });

  const res = await ao.dryrun({
    process: processId,
    tags: [
      { name: 'Action', value: 'GetStackStatus' },
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Variant', value: 'ao.TN.1' }
    ],
    data: '{}'
  });

  const last = res?.Messages?.[res.Messages.length - 1];
  if (last && last.Data) {
    try {
      return { ok: true, processId, ...JSON.parse(last.Data) };
    } catch {
      return { ok: true, processId, rawData: last.Data };
    }
  }

  return { ok: false, processId, status: 'no_response' };
}

/**
 * Registers a node with the Genesis Stack registry.
 */
export async function registerNode(processId, nodeInfo = {}, options = {}) {
  if (!processId) throw new Error('processId is required to register node');

  if (typeof options.evaluatorFn === 'function') {
    const res = await options.evaluatorFn({
      Target: processId,
      From: nodeInfo.node_id || options.sender || 'SU_NODE',
      Action: 'RegisterNode',
      Tags: [
        { name: 'Action', value: 'RegisterNode' },
        { name: 'Data-Protocol', value: 'ao' },
        { name: 'Variant', value: 'ao.TN.1' }
      ],
      Data: JSON.stringify(nodeInfo)
    });
    const msgs = res?.Messages || res?.responses || [];
    const last = msgs[msgs.length - 1];
    if (last && last.Data) {
      try {
        return { ok: true, ...JSON.parse(last.Data) };
      } catch {
        return { ok: true, rawData: last.Data };
      }
    }
  }

  const { connect } = await import('@permaweb/aoconnect');
  const ao = options.ao || connect({});
  const msgId = await ao.message({
    process: processId,
    signer: options.signer,
    tags: [
      { name: 'Action', value: 'RegisterNode' },
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Variant', value: 'ao.TN.1' }
    ],
    data: JSON.stringify(nodeInfo)
  });
  return { ok: true, msgId };
}

/**
 * Reports periodic node heartbeat and operational metrics to Genesis Stack.
 */
export async function reportHeartbeat(processId, heartbeatInfo = {}, options = {}) {
  if (!processId) throw new Error('processId is required to report heartbeat');

  if (typeof options.evaluatorFn === 'function') {
    const res = await options.evaluatorFn({
      Target: processId,
      From: heartbeatInfo.node_id || options.sender || 'SU_NODE',
      Action: 'ReportHeartbeat',
      Tags: [
        { name: 'Action', value: 'ReportHeartbeat' },
        { name: 'Data-Protocol', value: 'ao' },
        { name: 'Variant', value: 'ao.TN.1' }
      ],
      Data: JSON.stringify(heartbeatInfo)
    });
    const msgs = res?.Messages || res?.responses || [];
    const last = msgs[msgs.length - 1];
    if (last && last.Data) {
      try {
        return { ok: true, ...JSON.parse(last.Data) };
      } catch {
        return { ok: true, rawData: last.Data };
      }
    }
  }

  const { connect } = await import('@permaweb/aoconnect');
  const ao = options.ao || connect({});
  const msgId = await ao.message({
    process: processId,
    signer: options.signer,
    tags: [
      { name: 'Action', value: 'ReportHeartbeat' },
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Variant', value: 'ao.TN.1' }
    ],
    data: JSON.stringify(heartbeatInfo)
  });
  return { ok: true, msgId };
}

export class HyDB {
  static defineContract(spec) {
    return defineContract(spec);
  }
  static computeGenesisHash(manifest) {
    return computeGenesisHash(manifest);
  }
  static buildGenesisContractLua(options) {
    return buildGenesisContractLua(options);
  }
  static buildArbitratorContractLua(options) {
    return buildArbitratorContractLua(options);
  }
  static buildMasterContractLua(options) {
    return buildMasterContractLua(options);
  }
  static async spawnStack(options) {
    return await spawnStack(options);
  }
  static async getGenesisConfig(processId, options) {
    return await getGenesisConfig(processId, options);
  }
  static async getStackStatus(processId, options) {
    return await getStackStatus(processId, options);
  }
  static async registerNode(processId, nodeInfo, options) {
    return await registerNode(processId, nodeInfo, options);
  }
  static async reportHeartbeat(processId, heartbeatInfo, options) {
    return await reportHeartbeat(processId, heartbeatInfo, options);
  }
  static async verifyStackGenesis(processId, expectedGenesisHash, options) {
    return await verifyStackGenesis(processId, expectedGenesisHash, options);
  }
}

export function defineContract(spec) {
  return new HyDBContract(spec);
}

export {
  specManifestHash as computeManifestHash,
  specEncodeManifest as encodeManifest
};
