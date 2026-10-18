/**
 * HyDB Canonical Contract Specification — hydb.spec/1
 * ===================================================
 * NORMATIVE. This module is the single source of truth for the meaning of a
 * HyDB contract. Every runtime (Erlang Hymessage/SU, Rust WASM, JavaScript
 * evaluator, AO Lua arbitrator) MUST produce byte-identical canonical encodings,
 * identical event hashes, identical record versions and identical state
 * commitments for the same inputs. `tests/contract_conformance.mjs` enforces it.
 *
 * The functions here are a *reference implementation*: simple and pure, not fast.
 *
 * ---------------------------------------------------------------------------
 * 1. VALUE MODEL (HValue)
 * ---------------------------------------------------------------------------
 *   { t: 'int64',  v: bigint }                 -2^63 .. 2^63-1
 *   { t: 'bool',   v: boolean }
 *   { t: 'string', v: string }                 well-formed Unicode, UTF-8 on the wire
 *   { t: 'bytes',  v: Uint8Array }
 *   { t: 'null' }                              only inside json values
 *   { t: 'array',  v: HValue[] }               only inside json values
 *   { t: 'object', v: [string, HValue][] }     entries sorted by UTF-8 key bytes, unique
 *   Floats are NOT supported in hydb.spec/1 (rejected with ERR_TYPE_MISMATCH).
 *
 * ---------------------------------------------------------------------------
 * 2. CANONICAL TLV ENCODING (all integers big-endian)
 * ---------------------------------------------------------------------------
 *   0x01 int64   : 8 bytes two's complement
 *   0x02 bool    : 1 byte (0x00 | 0x01)
 *   0x03 string  : u32 byte length + UTF-8 bytes
 *   0x04 bytes   : u32 length + raw bytes
 *   0x05 null    : (no payload)
 *   0x06 array   : u32 count + elements
 *   0x07 object  : u32 count + entries; entry = u32 keylen + key UTF-8 + value
 *                  (entries sorted bytewise by key UTF-8, duplicates forbidden)
 *   0x08 absent  : (no payload) — "no key" / "no value" (delete, register)
 *   Every field is length-delimited, so no delimiter/concatenation ambiguity exists.
 *
 * ---------------------------------------------------------------------------
 * 3. IDENTITY
 * ---------------------------------------------------------------------------
 *   Record identity = (namespace, key) as two separate canonical fields.
 *   Namespaces and keys are NEVER merged via delimiter, XOR or truncated hash in
 *   any normative structure. Backends MAY use internal hashes (e.g. wasm_slot's
 *   64-bit slot hash) but MUST detect collisions and reject, never overwrite.
 *
 * ---------------------------------------------------------------------------
 * 4. EVENT  (frame: "HYDB" 'E' 0x01, then fields in this exact order)
 * ---------------------------------------------------------------------------
 *   contract_version string | prev_hash bytes(32) | epoch int64 | seq int64 |
 *   op string ('put'|'del'|'register') | namespace string | schema_version string |
 *   key (TLV|absent) | value_type string | value (TLV|absent) | ver int64
 *   event_hash = uppercase hex SHA-256(frame). Genesis prev_hash = 32 zero bytes.
 *   `ver` is the resulting record version (or registration counter for 'register').
 *   `expected_ver` (CAS) is a precondition checked before sequencing; it is not
 *   part of the event because the resulting `ver` already binds the effect.
 *
 * ---------------------------------------------------------------------------
 * 5. STATE COMMITMENT (frame: "HYDB" 'S' 0x01)
 * ---------------------------------------------------------------------------
 *   u32 schema_count, per schema sorted by namespace bytes:
 *       string namespace | string schema_version | int64 reg_ver | bytes(32) manifest_hash
 *   u32 record_count, per record (tombstones INCLUDED) sorted by
 *   (namespace UTF-8 bytes, then canonical key TLV bytes), bytewise:
 *       string namespace | key TLV | int64 ver | value TLV (absent = tombstone)
 *   commitment = uppercase hex SHA-256(frame).
 *   The 64-bit Merkle-XOR fingerprint used by wasm_slot is a NON-NORMATIVE
 *   optimization and MUST NOT be used as an integrity proof.
 *
 * ---------------------------------------------------------------------------
 * 6. RECORD VERSION & CAS SEMANTICS
 * ---------------------------------------------------------------------------
 *   put on never-seen key       -> ver = 1
 *   put on active record        -> ver = prev + 1
 *   del on active record        -> tombstone, ver = prev + 1 (value absent)
 *   del on missing/tombstone    -> ERR_NOT_FOUND (rejected before sequencing)
 *   put on tombstone            -> ver = tombstone ver + 1   (versions never reset)
 *   get / exists on tombstone   -> not found; `ver` still reports tombstone ver
 *   expected_ver omitted        -> unconditional
 *   expected_ver = 0            -> record must NOT be active (missing or tombstone)
 *   expected_ver = n > 0        -> record must be active with ver == n
 *   mismatch                    -> ERR_VERSION_CONFLICT
 *
 * ---------------------------------------------------------------------------
 * 7. VALIDATION ORDER & SEQUENCING RULE
 * ---------------------------------------------------------------------------
 *   Everything that can be checked deterministically (namespace registered,
 *   capability, key/value type, rules, auth, CAS, capacity) MUST be checked
 *   BEFORE a sequence number is allocated. Invalid input never consumes a seq.
 *   Operational failure after allocation (backend crash, replication failure)
 *   MUST NOT be reported as success; see ACK_LEVELS and Tahap 5.
 */

import crypto from 'node:crypto';

// ============================================================================
// Versions & constants
// ============================================================================
export const SPEC_VERSION = 'hydb.spec/1';
export const CONTRACT_VERSION = 'hydb.core@1.0';
export const ENCODING_VERSION = 1;
export const GENESIS_HASH = '0'.repeat(64);

export const TAG = Object.freeze({
  INT64: 0x01, BOOL: 0x02, STRING: 0x03, BYTES: 0x04,
  NULL: 0x05, ARRAY: 0x06, OBJECT: 0x07, ABSENT: 0x08
});

const MAGIC = Buffer.from('HYDB', 'ascii');
const FRAME = Object.freeze({ EVENT: 0x45, STATE: 0x53, MANIFEST: 0x4d });

export const LIMITS = Object.freeze({
  MAX_KEY_BYTES: 1024,
  MAX_STRING_BYTES: 1024 * 1024,
  MAX_JSON_DEPTH: 32,
  MAX_CONTAINER_ENTRIES: 10000,
  MAX_NAMESPACE_BYTES: 64
});

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;
const NAMESPACE_RE = /^[a-z0-9][a-z0-9._-]{0,63}$/;
const SEMVER_RE = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;

export const KEY_TYPES = Object.freeze(['int64', 'string']);
export const VALUE_TYPES = Object.freeze(['int64', 'bool', 'string', 'bytes', 'json']);
export const JSON_FIELD_TYPES = Object.freeze(['int64', 'string', 'bool', 'null', 'object', 'array']);
export const OPS = Object.freeze(['put', 'del', 'register']);

// ============================================================================
// Error catalog
// ============================================================================
export const ERRORS = Object.freeze({
  ERR_ENCODING: 'malformed or non-canonical input',
  ERR_INVALID_KEY: 'key missing, empty, too long or wrong type',
  ERR_TYPE_MISMATCH: 'value does not match declared type',
  ERR_SCHEMA_VIOLATION: 'value violates schema rules',
  ERR_INVALID_SCHEMA: 'schema definition is invalid or unsupported',
  ERR_CAPABILITY_UNSUPPORTED: 'backend lacks a capability required by the contract',
  ERR_NAMESPACE_UNKNOWN: 'namespace is not registered',
  ERR_NOT_FOUND: 'record does not exist or is deleted',
  ERR_VERSION_CONFLICT: 'expected_ver does not match',
  ERR_CAPACITY_EXCEEDED: 'namespace record limit reached',
  ERR_UNAUTHORIZED: 'operation requires authorization',
  ERR_STALE_EPOCH: 'request epoch is older than current epoch',
  ERR_CONTRACT_MISMATCH: 'contract or schema version mismatch',
  ERR_POLICY_INVALID: 'policy circuit configuration is invalid',
  ERR_BACKEND_UNAVAILABLE: 'required backend is not available'
});

export class SpecError extends Error {
  constructor(code, detail = '') {
    super(`${code}: ${ERRORS[code] || 'unknown error'}${detail ? ` (${detail})` : ''}`);
    this.name = 'SpecError';
    this.code = code;
    this.detail = detail;
  }
}

const fail = (code, detail) => { throw new SpecError(code, detail); };

// ============================================================================
// Backend capability registry
// ============================================================================
export const BACKENDS = Object.freeze({
  wasm_slot: Object.freeze({
    key_types: ['int64'],
    value_types: ['int64'],
    capabilities: ['get', 'put', 'del', 'exists', 'cas'],
    rules: ['min_value', 'max_value', 'max_records'],
    notes: 'Fixed-capacity numeric reference backend. Internal 64-bit slot hash MUST reject collisions.'
  }),
  ets_doc: Object.freeze({
    key_types: ['int64', 'string'],
    value_types: ['int64', 'bool', 'string', 'bytes', 'json'],
    capabilities: ['get', 'put', 'del', 'exists', 'cas', 'prefix_scan'],
    rules: ['min_value', 'max_value', 'min_length', 'max_length', 'max_records', 'fields', 'additional_fields'],
    notes: 'Document backend. prefix_scan only for string keys.'
  })
});

// ============================================================================
// Verdicts, Policy Circuit allowlist, ACK levels
// ============================================================================
export const VERDICTS = Object.freeze([
  'COMMITTED',
  'DUPLICATE_BATCH_IDEMPOTENT',
  'REJECTED_SEQUENCE_GAP',
  'REJECTED_OUT_OF_ORDER_BATCH',
  'REJECTED_SCHEMA_VIOLATION',
  'REJECTED_CONTRACT_MISMATCH',
  'REJECTED_HASH_CHAIN_BROKEN',
  'REJECTED_EVENT_HASH_MISMATCH',
  'REJECTED_STATE_ROOT_MISMATCH',
  'REJECTED_ENCODING',
  'EQUIVOCATION_PROVEN',
  'FENCED_STALE_EPOCH',
  'REJECTED_UNAUTHORIZED_SENDER'
]);

/**
 * For each verdict: the ONLY operational actions a contract may choose.
 * The first entry is the safe default. No list for a rejecting verdict contains
 * anything that commits state — the verdict itself is never changed by policy.
 */
export const POLICY_ALLOWLIST = Object.freeze({
  COMMITTED: ['COMMIT_CHECKPOINT_TO_LEDGER'],
  DUPLICATE_BATCH_IDEMPOTENT: ['RECORD_SU_ATTESTATION_IDEMPOTENT', 'IGNORE_DUPLICATE'],
  REJECTED_SEQUENCE_GAP: ['REQUEST_CATCHUP_FROM_SU_POOL', 'HALT_PROCESSING', 'ALERT_OPERATOR'],
  REJECTED_OUT_OF_ORDER_BATCH: ['REJECT_MALFORMED_BATCH', 'HALT_PROCESSING', 'ALERT_OPERATOR'],
  REJECTED_SCHEMA_VIOLATION: ['CUT_BRANCH_AND_REJECT_INVALID_SCHEMA', 'FREEZE_NAMESPACE', 'HALT_PROCESSING', 'ALERT_OPERATOR'],
  REJECTED_CONTRACT_MISMATCH: ['REJECT_INCOMPATIBLE_CONTRACT_VERSION', 'FREEZE_NAMESPACE', 'HALT_PROCESSING'],
  REJECTED_HASH_CHAIN_BROKEN: ['CUT_BRANCH_AND_FREEZE_EPOCH', 'HALT_PROCESSING'],
  REJECTED_EVENT_HASH_MISMATCH: ['CUT_BRANCH_AND_FREEZE_EPOCH', 'HALT_PROCESSING'],
  REJECTED_STATE_ROOT_MISMATCH: ['CUT_BRANCH_AND_SLASH_PROPOSER', 'CUT_BRANCH_AND_FREEZE_EPOCH', 'HALT_PROCESSING'],
  REJECTED_ENCODING: ['REJECT_MALFORMED_BATCH', 'HALT_PROCESSING'],
  EQUIVOCATION_PROVEN: ['FREEZE_EPOCH_AND_PROMOTE_STANDBY_HOT', 'HALT_PROCESSING'],
  FENCED_STALE_EPOCH: ['FENCE_AND_DROP_ZOMBIE_HOT', 'ALERT_OPERATOR'],
  REJECTED_UNAUTHORIZED_SENDER: ['DROP_UNAUTHORIZED_PACKET', 'ALERT_OPERATOR']
});

export const DEFAULT_POLICY = Object.freeze(
  Object.fromEntries(Object.entries(POLICY_ALLOWLIST).map(([v, acts]) => [v, acts[0]]))
);

/** Durability meaning of an ACK. Runtimes MUST report the level they actually reached. */
export const ACK_LEVELS = Object.freeze([
  'ACCEPTED',      // validated + sequenced in Hot memory only (provisional)
  'WAL_WRITTEN',   // written to the SU WAL file (OS page cache, not crash-safe)
  'WAL_SYNCED',    // WAL fsync'ed on the SU that was addressed (survives machine crash)
  'QUORUM',        // WAL_SYNCED on the configured quorum of distinct SUs
  'AO_COMMITTED'   // verified and committed by the AO Arbitrator (official state)
]);

/** Validate a policy map. Throws ERR_POLICY_INVALID. Returns a full, normalized policy. */
export function validatePolicy(policy = {}) {
  if (policy === null || typeof policy !== 'object' || Array.isArray(policy)) {
    fail('ERR_POLICY_INVALID', 'policy must be an object');
  }
  const out = { ...DEFAULT_POLICY };
  for (const [verdict, action] of Object.entries(policy)) {
    const allowed = POLICY_ALLOWLIST[verdict];
    if (!allowed) fail('ERR_POLICY_INVALID', `unknown verdict ${verdict}`);
    if (typeof action !== 'string' || !allowed.includes(action)) {
      fail('ERR_POLICY_INVALID', `action ${String(action).slice(0, 64)} not allowed for ${verdict}`);
    }
    out[verdict] = action;
  }
  return out;
}

/**
 * Runtime resolution. Never throws: anything outside the allowlist falls back to
 * the safe default and is flagged (guardrail_triggered).
 */
export function resolvePolicy(policy, verdict) {
  const allowed = POLICY_ALLOWLIST[verdict];
  if (!allowed) return { action: 'HALT_PROCESSING', guardrail_triggered: true, rejected_action: null };
  const chosen = policy && Object.prototype.hasOwnProperty.call(policy, verdict) ? policy[verdict] : allowed[0];
  if (typeof chosen === 'string' && allowed.includes(chosen)) {
    return { action: chosen, guardrail_triggered: false, rejected_action: null };
  }
  return { action: allowed[0], guardrail_triggered: true, rejected_action: chosen ?? null };
}

// ============================================================================
// Value normalization
// ============================================================================
function toInt64(input) {
  let b;
  if (typeof input === 'bigint') b = input;
  else if (typeof input === 'number') {
    if (!Number.isSafeInteger(input)) fail('ERR_TYPE_MISMATCH', 'number is not a safe integer');
    b = BigInt(input);
  } else if (typeof input === 'string' && /^-?(0|[1-9]\d*)$/.test(input)) {
    b = BigInt(input);
  } else {
    fail('ERR_TYPE_MISMATCH', 'expected int64');
  }
  if (b < INT64_MIN || b > INT64_MAX) fail('ERR_TYPE_MISMATCH', 'int64 out of range');
  return b;
}

function checkString(s, maxBytes, code = 'ERR_TYPE_MISMATCH') {
  if (typeof s !== 'string') fail(code, 'expected string');
  if (!s.isWellFormed()) fail('ERR_ENCODING', 'string contains lone surrogates');
  if (Buffer.byteLength(s, 'utf8') > maxBytes) fail(code, 'string too long');
  return s;
}

function cmpBytes(a, b) {
  return Buffer.compare(Buffer.from(a), Buffer.from(b));
}

function sortObjectEntries(entries) {
  const sorted = entries.slice().sort((x, y) => cmpBytes(Buffer.from(x[0], 'utf8'), Buffer.from(y[0], 'utf8')));
  for (let i = 1; i < sorted.length; i++) {
    if (sorted[i][0] === sorted[i - 1][0]) fail('ERR_ENCODING', `duplicate object key ${sorted[i][0]}`);
  }
  return sorted;
}

function jsonToH(v, depth) {
  if (depth > LIMITS.MAX_JSON_DEPTH) fail('ERR_TYPE_MISMATCH', 'json nesting too deep');
  if (v === null) return { t: 'null' };
  if (typeof v === 'boolean') return { t: 'bool', v };
  if (typeof v === 'string') return { t: 'string', v: checkString(v, LIMITS.MAX_STRING_BYTES) };
  if (typeof v === 'number' || typeof v === 'bigint') return { t: 'int64', v: toInt64(v) };
  if (Array.isArray(v)) {
    if (v.length > LIMITS.MAX_CONTAINER_ENTRIES) fail('ERR_TYPE_MISMATCH', 'array too large');
    return { t: 'array', v: v.map(x => jsonToH(x, depth + 1)) };
  }
  if (typeof v === 'object' && Object.getPrototypeOf(v) === Object.prototype) {
    const entries = Object.entries(v);
    if (entries.length > LIMITS.MAX_CONTAINER_ENTRIES) fail('ERR_TYPE_MISMATCH', 'object too large');
    return { t: 'object', v: sortObjectEntries(entries.map(([k, x]) => [checkString(k, LIMITS.MAX_KEY_BYTES), jsonToH(x, depth + 1)])) };
  }
  fail('ERR_TYPE_MISMATCH', `unsupported json value of type ${typeof v}`);
}

/** Convert a plain JS value into a canonical HValue for the given declared value_type. */
export function normalizeValue(valueType, input) {
  switch (valueType) {
    case 'int64': return { t: 'int64', v: toInt64(input) };
    case 'bool':
      if (typeof input !== 'boolean') fail('ERR_TYPE_MISMATCH', 'expected boolean');
      return { t: 'bool', v: input };
    case 'string': return { t: 'string', v: checkString(input, LIMITS.MAX_STRING_BYTES) };
    case 'bytes':
      if (!(input instanceof Uint8Array)) fail('ERR_TYPE_MISMATCH', 'expected Uint8Array');
      if (input.length > LIMITS.MAX_STRING_BYTES) fail('ERR_TYPE_MISMATCH', 'bytes too long');
      return { t: 'bytes', v: Uint8Array.from(input) };
    case 'json': {
      if (input === null || typeof input !== 'object' || Array.isArray(input)) {
        fail('ERR_TYPE_MISMATCH', 'json value must be an object at the root');
      }
      return jsonToH(input, 1);
    }
    default: fail('ERR_INVALID_SCHEMA', `unknown value_type ${valueType}`);
  }
}

/** Convert a plain key into a canonical HValue key. */
export function normalizeKey(keyType, input) {
  if (keyType === 'int64') {
    try { return { t: 'int64', v: toInt64(input) }; } catch { fail('ERR_INVALID_KEY', 'expected int64 key'); }
  }
  if (keyType === 'string') {
    if (typeof input !== 'string' || input.length === 0) fail('ERR_INVALID_KEY', 'expected non-empty string key');
    return { t: 'string', v: checkString(input, LIMITS.MAX_KEY_BYTES, 'ERR_INVALID_KEY') };
  }
  fail('ERR_INVALID_SCHEMA', `unknown key_type ${keyType}`);
}

/** Convert an HValue back to a plain JS value (int64 -> bigint). */
export function hToPlain(h) {
  switch (h.t) {
    case 'int64': case 'bool': case 'string': return h.v;
    case 'bytes': return Uint8Array.from(h.v);
    case 'null': return null;
    case 'array': return h.v.map(hToPlain);
    case 'object': return Object.fromEntries(h.v.map(([k, x]) => [k, hToPlain(x)]));
    default: fail('ERR_ENCODING', `unknown tag ${h.t}`);
  }
}

// ============================================================================
// Canonical TLV encoding
// ============================================================================
function u32(n) {
  const b = Buffer.alloc(4);
  b.writeUInt32BE(n);
  return b;
}

function rawString(s) {
  const bytes = Buffer.from(s, 'utf8');
  return Buffer.concat([u32(bytes.length), bytes]);
}

/** Canonical bytes of an HValue (or null/undefined => ABSENT). */
export function encodeValue(h) {
  if (h === null || h === undefined) return Buffer.from([TAG.ABSENT]);
  switch (h.t) {
    case 'int64': {
      const b = Buffer.alloc(9);
      b[0] = TAG.INT64;
      b.writeBigInt64BE(h.v, 1);
      return b;
    }
    case 'bool': return Buffer.from([TAG.BOOL, h.v ? 1 : 0]);
    case 'string': return Buffer.concat([Buffer.from([TAG.STRING]), rawString(h.v)]);
    case 'bytes': return Buffer.concat([Buffer.from([TAG.BYTES]), u32(h.v.length), Buffer.from(h.v)]);
    case 'null': return Buffer.from([TAG.NULL]);
    case 'array': return Buffer.concat([Buffer.from([TAG.ARRAY]), u32(h.v.length), ...h.v.map(encodeValue)]);
    case 'object': {
      const sorted = sortObjectEntries(h.v);
      const parts = sorted.flatMap(([k, x]) => [rawString(k), encodeValue(x)]);
      return Buffer.concat([Buffer.from([TAG.OBJECT]), u32(sorted.length), ...parts]);
    }
    default: fail('ERR_ENCODING', `unknown tag ${h && h.t}`);
  }
}

const S = s => ({ t: 'string', v: s });
const I = n => ({ t: 'int64', v: BigInt(n) });

function frameHeader(kind) {
  return Buffer.concat([MAGIC, Buffer.from([kind, ENCODING_VERSION])]);
}

function sha256Hex(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex').toUpperCase();
}

function hexToBytes32(hex) {
  if (typeof hex !== 'string' || !/^[0-9A-Fa-f]{64}$/.test(hex)) fail('ERR_ENCODING', 'hash must be 64 hex chars');
  return { t: 'bytes', v: Buffer.from(hex, 'hex') };
}

/**
 * Canonical event bytes.
 * @param {object} ev { contract_version, prev_hash, epoch, seq, op, namespace,
 *                      schema_version, key: HValue|null, value_type, value: HValue|null, ver }
 */
export function encodeEvent(ev) {
  if (!OPS.includes(ev.op)) fail('ERR_ENCODING', `unknown op ${ev.op}`);
  return Buffer.concat([
    frameHeader(FRAME.EVENT),
    encodeValue(S(ev.contract_version)),
    encodeValue(hexToBytes32(ev.prev_hash)),
    encodeValue(I(ev.epoch)),
    encodeValue(I(ev.seq)),
    encodeValue(S(ev.op)),
    encodeValue(S(ev.namespace)),
    encodeValue(S(ev.schema_version)),
    encodeValue(ev.key ?? null),
    encodeValue(S(ev.value_type)),
    encodeValue(ev.value ?? null),
    encodeValue(I(ev.ver))
  ]);
}

export function eventHash(ev) {
  return sha256Hex(encodeEvent(ev));
}

// ============================================================================
// Schema / manifest
// ============================================================================
function checkInt64Rule(rules, name) {
  if (rules[name] === undefined) return undefined;
  try { return toInt64(rules[name]); } catch { fail('ERR_INVALID_SCHEMA', `${name} must be int64`); }
}

function checkNonNegRule(rules, name) {
  const v = checkInt64Rule(rules, name);
  if (v !== undefined && v < 0n) fail('ERR_INVALID_SCHEMA', `${name} must be >= 0`);
  return v;
}

function normalizeFieldSpec(name, spec) {
  if (!spec || typeof spec !== 'object') fail('ERR_INVALID_SCHEMA', `field ${name} spec must be an object`);
  const allowed = new Set(['type', 'required', 'min_value', 'max_value', 'min_length', 'max_length']);
  for (const k of Object.keys(spec)) if (!allowed.has(k)) fail('ERR_INVALID_SCHEMA', `field ${name}: unknown property ${k}`);
  if (!JSON_FIELD_TYPES.includes(spec.type)) fail('ERR_INVALID_SCHEMA', `field ${name}: unsupported type ${spec.type}`);
  const out = { type: spec.type, required: spec.required === undefined ? false : spec.required };
  if (typeof out.required !== 'boolean') fail('ERR_INVALID_SCHEMA', `field ${name}: required must be boolean`);
  const numeric = spec.type === 'int64';
  const lengthy = spec.type === 'string';
  for (const r of ['min_value', 'max_value']) {
    if (spec[r] !== undefined) {
      if (!numeric) fail('ERR_INVALID_SCHEMA', `field ${name}: ${r} only valid for int64`);
      out[r] = checkInt64Rule(spec, r);
    }
  }
  for (const r of ['min_length', 'max_length']) {
    if (spec[r] !== undefined) {
      if (!lengthy) fail('ERR_INVALID_SCHEMA', `field ${name}: ${r} only valid for string`);
      out[r] = checkNonNegRule(spec, r);
    }
  }
  if (out.min_value !== undefined && out.max_value !== undefined && out.min_value > out.max_value) {
    fail('ERR_INVALID_SCHEMA', `field ${name}: min_value > max_value`);
  }
  if (out.min_length !== undefined && out.max_length !== undefined && out.min_length > out.max_length) {
    fail('ERR_INVALID_SCHEMA', `field ${name}: min_length > max_length`);
  }
  return out;
}

/**
 * Validate and normalize a schema definition. Strict: unknown properties, unknown
 * types, unsupported backend/type combinations and incoherent rules are rejected.
 * Returns a normalized schema object with only canonical fields.
 */
export function validateSchema(schema) {
  if (!schema || typeof schema !== 'object') fail('ERR_INVALID_SCHEMA', 'schema must be an object');
  const top = new Set(['namespace', 'schema_version', 'backend', 'key_type', 'value_type', 'require_auth', 'requires', 'rules']);
  for (const k of Object.keys(schema)) if (!top.has(k)) fail('ERR_INVALID_SCHEMA', `unknown schema property ${k}`);

  const { namespace, schema_version, backend, key_type, value_type } = schema;
  if (typeof namespace !== 'string' || !NAMESPACE_RE.test(namespace)) fail('ERR_INVALID_SCHEMA', 'invalid namespace');
  if (typeof schema_version !== 'string' || !SEMVER_RE.test(schema_version)) fail('ERR_INVALID_SCHEMA', 'schema_version must be MAJOR.MINOR.PATCH');
  const be = BACKENDS[backend];
  if (!be) fail('ERR_INVALID_SCHEMA', `unknown backend ${backend}`);
  if (!KEY_TYPES.includes(key_type)) fail('ERR_INVALID_SCHEMA', `unknown key_type ${key_type}`);
  if (!VALUE_TYPES.includes(value_type)) fail('ERR_INVALID_SCHEMA', `unknown value_type ${value_type}`);
  if (!be.key_types.includes(key_type)) fail('ERR_CAPABILITY_UNSUPPORTED', `${backend} does not support key_type ${key_type}`);
  if (!be.value_types.includes(value_type)) fail('ERR_CAPABILITY_UNSUPPORTED', `${backend} does not support value_type ${value_type}`);

  const require_auth = schema.require_auth === undefined ? false : schema.require_auth;
  if (typeof require_auth !== 'boolean') fail('ERR_INVALID_SCHEMA', 'require_auth must be boolean');

  const requires = schema.requires === undefined ? [] : schema.requires;
  if (!Array.isArray(requires) || requires.some(r => typeof r !== 'string')) fail('ERR_INVALID_SCHEMA', 'requires must be string[]');
  for (const cap of requires) {
    if (!be.capabilities.includes(cap)) fail('ERR_CAPABILITY_UNSUPPORTED', `${backend} lacks capability ${cap}`);
    if (cap === 'prefix_scan' && key_type !== 'string') fail('ERR_CAPABILITY_UNSUPPORTED', 'prefix_scan requires string keys');
  }
  const reqSorted = [...new Set(requires)].sort();

  const rulesIn = schema.rules === undefined ? {} : schema.rules;
  if (!rulesIn || typeof rulesIn !== 'object' || Array.isArray(rulesIn)) fail('ERR_INVALID_SCHEMA', 'rules must be an object');
  const byType = {
    int64: ['min_value', 'max_value', 'max_records'],
    bool: ['max_records'],
    string: ['min_length', 'max_length', 'max_records'],
    bytes: ['min_length', 'max_length', 'max_records'],
    json: ['fields', 'additional_fields', 'max_records']
  }[value_type];
  const rules = {};
  for (const k of Object.keys(rulesIn)) {
    if (!byType.includes(k)) fail('ERR_INVALID_SCHEMA', `rule ${k} not valid for value_type ${value_type}`);
    if (!be.rules.includes(k)) fail('ERR_CAPABILITY_UNSUPPORTED', `${backend} cannot enforce rule ${k}`);
  }
  for (const r of ['min_value', 'max_value']) { const v = checkInt64Rule(rulesIn, r); if (v !== undefined) rules[r] = v; }
  for (const r of ['min_length', 'max_length', 'max_records']) { const v = checkNonNegRule(rulesIn, r); if (v !== undefined) rules[r] = v; }
  if (rules.min_value !== undefined && rules.max_value !== undefined && rules.min_value > rules.max_value) {
    fail('ERR_INVALID_SCHEMA', 'min_value > max_value');
  }
  if (rules.min_length !== undefined && rules.max_length !== undefined && rules.min_length > rules.max_length) {
    fail('ERR_INVALID_SCHEMA', 'min_length > max_length');
  }
  if (value_type === 'json') {
    const fields = rulesIn.fields === undefined ? {} : rulesIn.fields;
    if (!fields || typeof fields !== 'object' || Array.isArray(fields)) fail('ERR_INVALID_SCHEMA', 'fields must be an object');
    const nf = {};
    for (const [name, spec] of Object.entries(fields)) {
      checkString(name, LIMITS.MAX_KEY_BYTES, 'ERR_INVALID_SCHEMA');
      nf[name] = normalizeFieldSpec(name, spec);
    }
    rules.fields = nf;
    const af = rulesIn.additional_fields === undefined ? true : rulesIn.additional_fields;
    if (typeof af !== 'boolean') fail('ERR_INVALID_SCHEMA', 'additional_fields must be boolean');
    rules.additional_fields = af;
  }

  return { namespace, schema_version, backend, key_type, value_type, require_auth, requires: reqSorted, rules };
}

/** HValue form of a normalized schema (deterministic). */
export function schemaToH(n) {
  const rulesEntries = [];
  for (const [k, v] of Object.entries(n.rules)) {
    if (k === 'fields') {
      rulesEntries.push([k, {
        t: 'object',
        v: Object.entries(v).map(([fname, f]) => [fname, {
          t: 'object',
          v: Object.entries(f).map(([fk, fv]) => [fk, typeof fv === 'bigint' ? I(fv) : typeof fv === 'boolean' ? { t: 'bool', v: fv } : S(fv)])
        }])
      }]);
    } else if (typeof v === 'boolean') rulesEntries.push([k, { t: 'bool', v }]);
    else rulesEntries.push([k, I(v)]);
  }
  return {
    t: 'object',
    v: sortObjectEntries([
      ['backend', S(n.backend)],
      ['key_type', S(n.key_type)],
      ['namespace', S(n.namespace)],
      ['require_auth', { t: 'bool', v: n.require_auth }],
      ['requires', { t: 'array', v: n.requires.map(S) }],
      ['rules', { t: 'object', v: sortObjectEntries(rulesEntries) }],
      ['schema_version', S(n.schema_version)],
      ['value_type', S(n.value_type)]
    ])
  };
}

export function encodeManifest(normalizedSchema) {
  return Buffer.concat([frameHeader(FRAME.MANIFEST), encodeValue(schemaToH(normalizedSchema))]);
}

export function manifestHash(normalizedSchema) {
  return sha256Hex(encodeManifest(normalizedSchema));
}

function semverCmp(a, b) {
  const pa = a.split('.').map(Number);
  const pb = b.split('.').map(Number);
  for (let i = 0; i < 3; i++) if (pa[i] !== pb[i]) return pa[i] < pb[i] ? -1 : 1;
  return 0;
}

// ============================================================================
// Value validation against rules
// ============================================================================
function utf8Len(s) { return BigInt(Buffer.byteLength(s, 'utf8')); }

function jsonFieldTypeOf(h) {
  return h.t; // 'int64'|'string'|'bool'|'null'|'object'|'array'
}

/** Throws ERR_SCHEMA_VIOLATION if value breaks schema rules. Value must already be normalized. */
export function checkRules(schema, h) {
  const r = schema.rules;
  if (schema.value_type === 'int64') {
    if (r.min_value !== undefined && h.v < r.min_value) fail('ERR_SCHEMA_VIOLATION', 'below min_value');
    if (r.max_value !== undefined && h.v > r.max_value) fail('ERR_SCHEMA_VIOLATION', 'above max_value');
  } else if (schema.value_type === 'string' || schema.value_type === 'bytes') {
    const len = schema.value_type === 'string' ? utf8Len(h.v) : BigInt(h.v.length);
    if (r.min_length !== undefined && len < r.min_length) fail('ERR_SCHEMA_VIOLATION', 'shorter than min_length');
    if (r.max_length !== undefined && len > r.max_length) fail('ERR_SCHEMA_VIOLATION', 'longer than max_length');
  } else if (schema.value_type === 'json') {
    const present = new Map(h.v);
    for (const [name, f] of Object.entries(r.fields)) {
      const fv = present.get(name);
      if (fv === undefined) {
        if (f.required) fail('ERR_SCHEMA_VIOLATION', `missing required field ${name}`);
        continue;
      }
      if (jsonFieldTypeOf(fv) !== f.type) fail('ERR_SCHEMA_VIOLATION', `field ${name} must be ${f.type}`);
      if (f.min_value !== undefined && fv.v < f.min_value) fail('ERR_SCHEMA_VIOLATION', `field ${name} below min_value`);
      if (f.max_value !== undefined && fv.v > f.max_value) fail('ERR_SCHEMA_VIOLATION', `field ${name} above max_value`);
      if (f.min_length !== undefined && utf8Len(fv.v) < f.min_length) fail('ERR_SCHEMA_VIOLATION', `field ${name} shorter than min_length`);
      if (f.max_length !== undefined && utf8Len(fv.v) > f.max_length) fail('ERR_SCHEMA_VIOLATION', `field ${name} longer than max_length`);
    }
    if (r.additional_fields === false) {
      for (const name of present.keys()) {
        if (!Object.prototype.hasOwnProperty.call(r.fields, name)) fail('ERR_SCHEMA_VIOLATION', `unexpected field ${name}`);
      }
    }
  }
}

/** Type check of an already-built HValue against value_type (used when verifying events). */
function checkValueType(valueType, h) {
  const ok = {
    int64: h && h.t === 'int64' && h.v >= INT64_MIN && h.v <= INT64_MAX,
    bool: h && h.t === 'bool',
    string: h && h.t === 'string',
    bytes: h && h.t === 'bytes',
    json: h && h.t === 'object'
  }[valueType];
  if (!ok) fail('ERR_TYPE_MISMATCH', `event value is not ${valueType}`);
}

function checkKeyType(keyType, h) {
  if (!h || h.t !== keyType) fail('ERR_INVALID_KEY', `event key is not ${keyType}`);
  if (keyType === 'string' && (h.v.length === 0 || utf8Len(h.v) > BigInt(LIMITS.MAX_KEY_BYTES))) fail('ERR_INVALID_KEY');
}

// ============================================================================
// Reference state machine
// ============================================================================
export function recordId(namespace, keyH) {
  return Buffer.concat([encodeValue(S(namespace)), encodeValue(keyH)]).toString('hex');
}

export function createState() {
  return { schemas: new Map(), records: new Map() };
  // schemas: ns -> { schema (normalized), reg_ver: bigint, manifest_hash }
  // records: recordId -> { namespace, key: HValue, ver: bigint, value: HValue|null }
}

export function cloneState(st) {
  return {
    schemas: new Map([...st.schemas].map(([k, v]) => [k, { ...v }])),
    records: new Map([...st.records].map(([k, v]) => [k, { ...v }]))
  };
}

/** get/exists semantics. */
export function readRecord(st, namespace, keyH) {
  const rec = st.records.get(recordId(namespace, keyH));
  if (!rec) return { found: false, ver: 0n, value: null };
  return { found: rec.value !== null, ver: rec.ver, value: rec.value };
}

function activeCount(st, namespace) {
  let n = 0n;
  for (const r of st.records.values()) if (r.namespace === namespace && r.value !== null) n++;
  return n;
}

/**
 * Pre-sequence validation. Returns an event draft (without contract_version,
 * prev_hash, epoch, seq) or throws a SpecError. Never mutates state.
 *
 * req = { op:'put'|'del', namespace, key, value?, expected_ver?, authorized? }
 *     | { op:'register', schema }
 */
export function prepareOp(st, req) {
  if (req.op === 'register') {
    const n = validateSchema(req.schema);
    const prev = st.schemas.get(n.namespace);
    if (prev) {
      const p = prev.schema;
      if (p.backend !== n.backend || p.key_type !== n.key_type || p.value_type !== n.value_type) {
        fail('ERR_INVALID_SCHEMA', 'changing backend/key_type/value_type requires migration (unsupported in hydb.spec/1)');
      }
      if (semverCmp(n.schema_version, p.schema_version) <= 0) {
        fail('ERR_CONTRACT_MISMATCH', 'schema_version must increase');
      }
    }
    return {
      op: 'register', namespace: n.namespace, schema_version: n.schema_version,
      key: null, value_type: 'schema', value: schemaToH(n), ver: (prev ? prev.reg_ver : 0n) + 1n
    };
  }

  if (req.op !== 'put' && req.op !== 'del') fail('ERR_ENCODING', `unknown op ${req.op}`);
  const entry = st.schemas.get(req.namespace);
  if (!entry) fail('ERR_NAMESPACE_UNKNOWN', String(req.namespace));
  const schema = entry.schema;
  if (schema.require_auth && req.authorized !== true) fail('ERR_UNAUTHORIZED');

  const keyH = normalizeKey(schema.key_type, req.key);
  const cur = readRecord(st, req.namespace, keyH);

  if (req.expected_ver !== undefined && req.expected_ver !== null) {
    let ev;
    try { ev = toInt64(req.expected_ver); } catch { fail('ERR_VERSION_CONFLICT', 'expected_ver must be int64'); }
    if (ev < 0n) fail('ERR_VERSION_CONFLICT', 'expected_ver must be >= 0');
    if (ev === 0n ? cur.found : (!cur.found || cur.ver !== ev)) fail('ERR_VERSION_CONFLICT', `current ver ${cur.ver}, active=${cur.found}`);
  }

  if (req.op === 'del') {
    if (!cur.found) fail('ERR_NOT_FOUND');
    return {
      op: 'del', namespace: req.namespace, schema_version: schema.schema_version,
      key: keyH, value_type: schema.value_type, value: null, ver: cur.ver + 1n
    };
  }

  const valH = normalizeValue(schema.value_type, req.value);
  checkRules(schema, valH);
  if (!cur.found && schema.rules.max_records !== undefined && activeCount(st, req.namespace) >= schema.rules.max_records) {
    fail('ERR_CAPACITY_EXCEEDED');
  }
  return {
    op: 'put', namespace: req.namespace, schema_version: schema.schema_version,
    key: keyH, value_type: schema.value_type, value: valH, ver: cur.ver + 1n
  };
}

/**
 * Independent verification + application of one event (what the Arbitrator does).
 * Mutates `st`. Throws SpecError whose code maps to a verdict via verdictForError().
 * Authorization of require_auth writes is enforced at the sequencer; the
 * Arbitrator verifies sender/batch authenticity separately (Tahap 6).
 */
export function applyEvent(st, ev) {
  if (ev.contract_version !== CONTRACT_VERSION) fail('ERR_CONTRACT_MISMATCH', `contract_version ${ev.contract_version}`);
  if (ev.op === 'register') {
    if (ev.key !== null && ev.key !== undefined) fail('ERR_ENCODING', 'register event must not carry a key');
    if (ev.value_type !== 'schema') fail('ERR_ENCODING', 'register event value_type must be schema');
    const plainSchema = schemaFromH(ev.value);
    const draft = prepareOp(st, { op: 'register', schema: plainSchema });
    if (draft.namespace !== ev.namespace || draft.schema_version !== ev.schema_version || draft.ver !== BigInt(ev.ver)) {
      fail('ERR_CONTRACT_MISMATCH', 'register event fields inconsistent with manifest');
    }
    if (!encodeValue(draft.value).equals(encodeValue(ev.value))) fail('ERR_ENCODING', 'manifest not canonical');
    const n = validateSchema(plainSchema);
    st.schemas.set(n.namespace, { schema: n, reg_ver: draft.ver, manifest_hash: manifestHash(n) });
    return;
  }

  const entry = st.schemas.get(ev.namespace);
  if (!entry) fail('ERR_NAMESPACE_UNKNOWN', ev.namespace);
  const schema = entry.schema;
  if (ev.schema_version !== schema.schema_version) fail('ERR_CONTRACT_MISMATCH', 'schema_version mismatch');
  if (ev.value_type !== schema.value_type) fail('ERR_CONTRACT_MISMATCH', 'value_type mismatch');
  checkKeyType(schema.key_type, ev.key);
  const id = recordId(ev.namespace, ev.key);
  const cur = readRecord(st, ev.namespace, ev.key);
  const ver = BigInt(ev.ver);
  if (ver !== cur.ver + 1n) fail('ERR_VERSION_CONFLICT', `event ver ${ver}, expected ${cur.ver + 1n}`);

  if (ev.op === 'del') {
    if (ev.value !== null && ev.value !== undefined) fail('ERR_ENCODING', 'del event must not carry a value');
    if (!cur.found) fail('ERR_NOT_FOUND');
    st.records.set(id, { namespace: ev.namespace, key: ev.key, ver, value: null });
    return;
  }
  if (ev.op !== 'put') fail('ERR_ENCODING', `unknown op ${ev.op}`);
  checkValueType(schema.value_type, ev.value);
  if (schema.value_type === 'json') normalizeValue('json', hToPlain(ev.value)); // depth/limits/float checks
  checkRules(schema, ev.value);
  if (!cur.found && schema.rules.max_records !== undefined && activeCount(st, ev.namespace) >= schema.rules.max_records) {
    fail('ERR_CAPACITY_EXCEEDED');
  }
  st.records.set(id, { namespace: ev.namespace, key: ev.key, ver, value: ev.value });
}

/** Rebuild a plain schema object from its canonical HValue form. */
export function schemaFromH(h) {
  if (!h || h.t !== 'object') fail('ERR_ENCODING', 'schema must be an object');
  const plain = hToPlain(h);
  const rules = { ...(plain.rules || {}) };
  if (rules.fields) {
    for (const f of Object.values(rules.fields)) {
      for (const k of Object.keys(f)) if (typeof f[k] === 'bigint') f[k] = f[k].toString();
    }
  }
  for (const k of Object.keys(rules)) if (typeof rules[k] === 'bigint') rules[k] = rules[k].toString();
  return { ...plain, rules };
}

/** Map a SpecError code raised by applyEvent to an Arbitrator verdict. */
export function verdictForError(code) {
  switch (code) {
    case 'ERR_CONTRACT_MISMATCH': case 'ERR_NAMESPACE_UNKNOWN': case 'ERR_INVALID_SCHEMA':
    case 'ERR_CAPABILITY_UNSUPPORTED':
      return 'REJECTED_CONTRACT_MISMATCH';
    case 'ERR_ENCODING':
      return 'REJECTED_ENCODING';
    default:
      return 'REJECTED_SCHEMA_VIOLATION';
  }
}

// ============================================================================
// State commitment
// ============================================================================
export function encodeState(st) {
  const schemas = [...st.schemas.entries()].sort((a, b) => cmpBytes(Buffer.from(a[0], 'utf8'), Buffer.from(b[0], 'utf8')));
  const records = [...st.records.values()].sort((a, b) => {
    const c = cmpBytes(Buffer.from(a.namespace, 'utf8'), Buffer.from(b.namespace, 'utf8'));
    return c !== 0 ? c : cmpBytes(encodeValue(a.key), encodeValue(b.key));
  });
  const parts = [frameHeader(FRAME.STATE), u32(schemas.length)];
  for (const [ns, e] of schemas) {
    parts.push(encodeValue(S(ns)), encodeValue(S(e.schema.schema_version)), encodeValue(I(e.reg_ver)), encodeValue(hexToBytes32(e.manifest_hash)));
  }
  parts.push(u32(records.length));
  for (const r of records) {
    parts.push(encodeValue(S(r.namespace)), encodeValue(r.key), encodeValue(I(r.ver)), encodeValue(r.value));
  }
  return Buffer.concat(parts);
}

export function stateCommitment(st) {
  return sha256Hex(encodeState(st));
}

// ============================================================================
// Batch verification (sequence, chain, hash, epoch) — atomic
// ============================================================================
/**
 * Verify a batch of fully-formed events (each with `hash`) against a chain tip.
 * Atomic: on any failure the input state is untouched.
 * @returns {{ok:true, state, tip:{seq,hash}, commitment}} | {{ok:false, verdict, index, code?}}
 */
export function verifyBatch(st, tip, events, { epoch }) {
  if (!Array.isArray(events) || events.length === 0) return { ok: false, verdict: 'REJECTED_ENCODING', index: -1 };
  const staged = cloneState(st);
  let seq = BigInt(tip.seq);
  let prev = tip.hash;
  for (let i = 0; i < events.length; i++) {
    const ev = events[i];
    try {
      if (BigInt(ev.epoch) < BigInt(epoch)) return { ok: false, verdict: 'FENCED_STALE_EPOCH', index: i };
      const s = BigInt(ev.seq);
      if (s <= seq) return { ok: false, verdict: 'REJECTED_OUT_OF_ORDER_BATCH', index: i };
      if (s !== seq + 1n) return { ok: false, verdict: 'REJECTED_SEQUENCE_GAP', index: i };
      if (String(ev.prev_hash).toUpperCase() !== String(prev).toUpperCase()) return { ok: false, verdict: 'REJECTED_HASH_CHAIN_BROKEN', index: i };
      const h = eventHash(ev);
      if (String(ev.hash).toUpperCase() !== h) return { ok: false, verdict: 'REJECTED_EVENT_HASH_MISMATCH', index: i };
      applyEvent(staged, ev);
      seq = s;
      prev = h;
    } catch (err) {
      if (err instanceof SpecError) return { ok: false, verdict: verdictForError(err.code), index: i, code: err.code };
      return { ok: false, verdict: 'REJECTED_ENCODING', index: i, code: 'ERR_ENCODING' };
    }
  }
  return { ok: true, state: staged, tip: { seq, hash: prev }, commitment: stateCommitment(staged) };
}

/** Sequencer helper: turn an event draft into a full event with hash. */
export function sealEvent(draft, { prev_hash, epoch, seq }) {
  const ev = { contract_version: CONTRACT_VERSION, prev_hash, epoch: BigInt(epoch), seq: BigInt(seq), ...draft };
  return { ...ev, hash: eventHash(ev) };
}

// ============================================================================
// Typed JSON (golden vector interchange format)
// ============================================================================
/** HValue -> JSON-safe typed form: {t, v}; int64 as decimal string, bytes as hex. */
export function hToTyped(h) {
  if (h === null || h === undefined) return null;
  switch (h.t) {
    case 'int64': return { t: 'int64', v: h.v.toString() };
    case 'bool': return { t: 'bool', v: h.v };
    case 'string': return { t: 'string', v: h.v };
    case 'bytes': return { t: 'bytes', v: Buffer.from(h.v).toString('hex') };
    case 'null': return { t: 'null' };
    case 'array': return { t: 'array', v: h.v.map(hToTyped) };
    case 'object': return { t: 'object', v: h.v.map(([k, x]) => [k, hToTyped(x)]) };
    default: fail('ERR_ENCODING', `unknown tag ${h.t}`);
  }
}

export function typedToH(x) {
  if (x === null || x === undefined) return null;
  switch (x.t) {
    case 'int64': return { t: 'int64', v: BigInt(x.v) };
    case 'bool': return { t: 'bool', v: x.v };
    case 'string': return { t: 'string', v: x.v };
    case 'bytes': return { t: 'bytes', v: Buffer.from(x.v, 'hex') };
    case 'null': return { t: 'null' };
    case 'array': return { t: 'array', v: x.v.map(typedToH) };
    case 'object': return { t: 'object', v: x.v.map(([k, y]) => [k, typedToH(y)]) };
    default: fail('ERR_ENCODING', `unknown typed tag ${x.t}`);
  }
}

export function eventToTyped(ev) {
  const out = {
    contract_version: ev.contract_version, prev_hash: ev.prev_hash, epoch: String(ev.epoch), seq: String(ev.seq),
    op: ev.op, namespace: ev.namespace, schema_version: ev.schema_version, key: hToTyped(ev.key),
    value_type: ev.value_type, value: hToTyped(ev.value), ver: String(ev.ver)
  };
  if (ev.hash) out.hash = ev.hash;
  return out;
}

export function typedToEvent(t) {
  const ev = {
    contract_version: t.contract_version, prev_hash: t.prev_hash, epoch: BigInt(t.epoch), seq: BigInt(t.seq),
    op: t.op, namespace: t.namespace, schema_version: t.schema_version, key: typedToH(t.key),
    value_type: t.value_type, value: typedToH(t.value), ver: BigInt(t.ver)
  };
  if (t.hash) ev.hash = t.hash;
  return ev;
}
