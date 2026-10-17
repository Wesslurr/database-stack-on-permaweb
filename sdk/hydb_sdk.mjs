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
import { execFileSync } from 'node:child_process';
import crypto from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

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
 * Sandbox Lua Policy Handler Simulator:
 * Injects mock incident context $X$ into developer's Lua handler $Y$,
 * validates syntax, verifies non-hanging timeout (<1.5s), and ensures valid return action.
 */
export function simulateLuaPolicyHandler(luaCode, eventName, namespace = 'vault.test') {
  if (typeof luaCode !== 'string' || !luaCode.trim()) {
    throw new ContractPreflightError('Lua policy handler must be a non-empty string', {
      stage: 'LUA_POLICY_CHECK',
      eventName
    });
  }

  const mockEvent = {
    verdict: eventName,
    seq: 100,
    epoch: 1,
    namespace: namespace,
    sub_verdict: eventName,
    timestamp: Date.now()
  };

  const runnerScript = `
    local event_json = ${JSON.stringify(JSON.stringify(mockEvent))}
    local event = {}
    for k, v in string.gmatch(event_json, '"([^"]+)":([%w_%-%.]+)') do
      event[k] = v
    end
    for k, v in string.gmatch(event_json, '"([^"]+)":"([^"]+)"') do
      event[k] = v
    end

    local code = ${JSON.stringify(luaCode)}
    local loader, err = load(code)
    if not loader then
      io.stderr:write("LUA_SYNTAX_ERROR: " .. tostring(err))
      os.exit(101)
    end

    local ok_load, handler = pcall(loader)
    if not ok_load then
      io.stderr:write("LUA_EVAL_ERROR: " .. tostring(handler))
      os.exit(102)
    end

    if type(handler) ~= "function" then
      io.stderr:write("LUA_TYPE_ERROR: Script did not return a function (got " .. type(handler) .. ")")
      os.exit(103)
    end

    local ok_exec, res = pcall(handler, event)
    if not ok_exec then
      io.stderr:write("LUA_RUNTIME_PANIC: " .. tostring(res))
      os.exit(104)
    end

    if res == nil or tostring(res) == "" then
      io.stderr:write("LUA_EMPTY_RETURN: Handler returned nil or empty string")
      os.exit(105)
    end

    io.write(tostring(res))
  `;

  try {
    const verdictOut = execFileSync('lua', ['-e', runnerScript], {
      timeout: 1500,
      encoding: 'utf-8'
    }).trim();
    return { ok: true, verdict: verdictOut };
  } catch (err) {
    let reason = 'Lua execution failed';
    if (err.code === 'ETIMEDOUT' || err.signal === 'SIGTERM') {
      reason = 'Handler timed out (>1500ms) - detected potential infinite loop (while true do end)';
    } else if (err.stderr) {
      reason = err.stderr.trim();
    } else if (err.message) {
      reason = err.message;
    }
    throw new ContractPreflightError(`Lua Policy Simulation failed for event [${eventName}]: ${reason}`, {
      stage: 'LUA_POLICY_SIMULATION',
      eventName,
      reason
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
    this.contractVersion = HYDB_CONTRACT_VERSION;
    this.namespace = spec.namespace;
    this.backend = spec.backend || (spec.wasm ? 'wasm_slot' : 'ets_doc');
    this.schemaVersion = spec.schemaVersion || (spec.schema && spec.schema.version) || '1.0.0';

    const s = spec.schema || {};
    const rules = { ...(s.rules || {}) };
    if (s.minValue !== undefined) rules.min_value = s.minValue;
    if (s.maxValue !== undefined) rules.max_value = s.maxValue;
    if (s.minLength !== undefined) rules.min_length = s.minLength;
    if (s.requiredFields !== undefined) rules.required_fields = s.requiredFields;
    if (s.requireAuth !== undefined) rules.require_auth = Boolean(s.requireAuth);
    if (s.maxRecords !== undefined) rules.max_records = Number(s.maxRecords);

    this.schema = {
      key_type: s.keyType || s.key_type || (this.backend === 'wasm_slot' ? 'int64' : 'string'),
      value_type: s.valueType || s.value_type || (this.backend === 'wasm_slot' ? 'int64' : 'any'),
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

    // 1. Schema Invariants Check
    const rules = this.schema.rules || {};
    if (rules.min_value !== undefined && rules.max_value !== undefined) {
      if (Number(rules.min_value) > Number(rules.max_value)) {
        throw new ContractPreflightError(`Invalid schema: min_value (${rules.min_value}) > max_value (${rules.max_value})`, {
          stage: 'SCHEMA_INVARIANT_CHECK',
          rules
        });
      }
      report.schemaChecks.push('min_max_bounds_valid');
    }

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

    // 3. Lua Policy Handler Sandbox Simulation
    for (const [eventName, handlerSpec] of Object.entries(this.policy)) {
      if (typeof handlerSpec === 'string' && (handlerSpec.includes('function(') || handlerSpec.includes('return '))) {
        const sim = simulateLuaPolicyHandler(handlerSpec, eventName, this.namespace);
        report.policySimulations.push({
          event: eventName,
          passed: true,
          verdict: sim.verdict
        });
      }
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
