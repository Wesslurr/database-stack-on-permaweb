#!/usr/bin/env node
/**
 * Deterministic Golden Vector Generator & Provenance Verifier
 * ===========================================================
 * Generates or verifies `spec/golden_vectors.json` directly from the normative
 * `spec/hydb_contract_spec.mjs` specification.
 */

import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import * as spec from './hydb_contract_spec.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const GOLDEN_PATH = join(__dirname, 'golden_vectors.json');

export function buildGoldenVectors() {
  const validSchema = {
    namespace: 'finance.balances',
    schema_version: '1.0.0',
    backend: 'wasm_slot',
    key_type: 'int64',
    value_type: 'int64',
    require_auth: true,
    requires: ['cas'],
    rules: { min_value: 0n, max_records: 1000n }
  };

  const docSchema = {
    namespace: 'app.docs',
    schema_version: '1.0.0',
    backend: 'ets_doc',
    key_type: 'string',
    value_type: 'json',
    require_auth: false,
    requires: ['prefix_scan'],
    rules: {
      fields: {
        title: { type: 'string', required: true, min_length: 3n },
        views: { type: 'int64', required: false, min_value: 0n }
      },
      additional_fields: true
    }
  };

  const st = spec.createState();
  const ev1Draft = spec.prepareOp(st, { op: 'register', schema: validSchema });
  const ev1 = spec.sealEvent(ev1Draft, { prev_hash: spec.GENESIS_HASH, epoch: 1n, seq: 1n });
  spec.applyEvent(st, ev1);

  const ev2Draft = spec.prepareOp(st, {
    op: 'put',
    namespace: 'finance.balances',
    key: 1001,
    value: 50000,
    authorized: true
  });
  const ev2 = spec.sealEvent(ev2Draft, { prev_hash: ev1.hash, epoch: 1n, seq: 2n });
  spec.applyEvent(st, ev2);

  const ev3Draft = spec.prepareOp(st, {
    op: 'del',
    namespace: 'finance.balances',
    key: 1001,
    authorized: true
  });
  const ev3 = spec.sealEvent(ev3Draft, { prev_hash: ev2.hash, epoch: 1n, seq: 3n });
  spec.applyEvent(st, ev3);

  const ev4Draft = spec.prepareOp(st, {
    op: 'put',
    namespace: 'finance.balances',
    key: 1001,
    value: 75000,
    authorized: true
  });
  const ev4 = spec.sealEvent(ev4Draft, { prev_hash: ev3.hash, epoch: 1n, seq: 4n });
  spec.applyEvent(st, ev4);

  const docRegDraft = spec.prepareOp(st, { op: 'register', schema: docSchema });
  const docRegEv = spec.sealEvent(docRegDraft, { prev_hash: ev4.hash, epoch: 1n, seq: 5n });
  spec.applyEvent(st, docRegEv);

  const ev5Draft = spec.prepareOp(st, {
    op: 'put',
    namespace: 'finance.balances',
    key: 1002,
    value: 25000,
    authorized: true
  });
  const ev5 = spec.sealEvent(ev5Draft, { prev_hash: docRegEv.hash, epoch: 1n, seq: 6n });
  spec.applyEvent(st, ev5);

  const ev6Draft = spec.prepareOp(st, {
    op: 'put',
    namespace: 'app.docs',
    key: 'doc:readme',
    value: { title: 'HyDB Guide', views: 10 }
  });
  const ev6 = spec.sealEvent(ev6Draft, { prev_hash: ev5.hash, epoch: 1n, seq: 7n });
  spec.applyEvent(st, ev6);

  return {
    spec_version: spec.SPEC_VERSION,
    contract_version: spec.CONTRACT_VERSION,
    values: {
      int64_42: spec.encodeValue({ t: 'int64', v: 42n }).toString('hex').toUpperCase(),
      int64_min: spec.encodeValue({ t: 'int64', v: -(2n ** 63n) }).toString('hex').toUpperCase(),
      int64_max: spec.encodeValue({ t: 'int64', v: 2n ** 63n - 1n }).toString('hex').toUpperCase(),
      bool_true: spec.encodeValue({ t: 'bool', v: true }).toString('hex').toUpperCase(),
      bool_false: spec.encodeValue({ t: 'bool', v: false }).toString('hex').toUpperCase(),
      string_simple: spec.encodeValue({ t: 'string', v: 'hello world' }).toString('hex').toUpperCase(),
      string_unicode: spec.encodeValue({ t: 'string', v: 'Käse 🚀 日本語' }).toString('hex').toUpperCase(),
      bytes_raw: spec.encodeValue({ t: 'bytes', v: new Uint8Array([1, 2, 3, 255]) }).toString('hex').toUpperCase(),
      absent: spec.encodeValue(null).toString('hex').toUpperCase(),
      null: spec.encodeValue({ t: 'null' }).toString('hex').toUpperCase(),
      sorted_json: spec
        .encodeValue({
          t: 'object',
          v: [
            ['zeta', { t: 'int64', v: 100n }],
            ['alpha', { t: 'string', v: 'first' }],
            ['mid', { t: 'array', v: [{ t: 'int64', v: 1n }, { t: 'int64', v: 2n }] }]
          ]
        })
        .toString('hex')
        .toUpperCase()
    },
    manifests: {
      finance_balances_hash: spec.manifestHash(spec.validateSchema(validSchema)),
      app_docs_hash: spec.manifestHash(spec.validateSchema(docSchema))
    },
    events: {
      ev1_register_hash: ev1.hash,
      ev2_put_hash: ev2.hash,
      ev3_del_hash: ev3.hash,
      ev4_reput_hash: ev4.hash
    },
    state_commitment_256: spec.stateCommitment(st),
    errors: {
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
    },
    policy_allowlist: {
      COMMITTED: ['COMMIT_CHECKPOINT_TO_LEDGER'],
      DUPLICATE_BATCH_IDEMPOTENT: ['RECORD_SU_ATTESTATION_IDEMPOTENT', 'IGNORE_DUPLICATE'],
      REJECTED_SEQUENCE_GAP: ['REQUEST_CATCHUP_FROM_SU_POOL', 'HALT_PROCESSING', 'ALERT_OPERATOR'],
      REJECTED_OUT_OF_ORDER_BATCH: ['REJECT_MALFORMED_BATCH', 'HALT_PROCESSING', 'ALERT_OPERATOR'],
      REJECTED_SCHEMA_VIOLATION: [
        'CUT_BRANCH_AND_REJECT_INVALID_SCHEMA',
        'FREEZE_NAMESPACE',
        'HALT_PROCESSING',
        'ALERT_OPERATOR'
      ],
      REJECTED_CONTRACT_MISMATCH: [
        'REJECT_INCOMPATIBLE_CONTRACT_VERSION',
        'FREEZE_NAMESPACE',
        'HALT_PROCESSING'
      ],
      REJECTED_HASH_CHAIN_BROKEN: ['CUT_BRANCH_AND_FREEZE_EPOCH', 'HALT_PROCESSING'],
      REJECTED_EVENT_HASH_MISMATCH: ['CUT_BRANCH_AND_FREEZE_EPOCH', 'HALT_PROCESSING'],
      REJECTED_STATE_ROOT_MISMATCH: [
        'CUT_BRANCH_AND_SLASH_PROPOSER',
        'CUT_BRANCH_AND_FREEZE_EPOCH',
        'HALT_PROCESSING'
      ],
      REJECTED_ENCODING: ['REJECT_MALFORMED_BATCH', 'HALT_PROCESSING'],
      EQUIVOCATION_PROVEN: ['FREEZE_EPOCH_AND_PROMOTE_STANDBY_HOT', 'HALT_PROCESSING'],
      FENCED_STALE_EPOCH: ['FENCE_AND_DROP_ZOMBIE_HOT', 'ALERT_OPERATOR'],
      REJECTED_UNAUTHORIZED_SENDER: ['DROP_UNAUTHORIZED_PACKET', 'ALERT_OPERATOR']
    }
  };
}

if (process.argv[1] && process.argv[1].endsWith('generate_golden_vectors.mjs')) {
  const generated = JSON.stringify(buildGoldenVectors(), null, 2) + '\n';
  if (process.argv.includes('--check')) {
    const existing = readFileSync(GOLDEN_PATH, 'utf-8').replace(/\r\n/g, '\n');
    if (existing !== generated) {
      console.error('❌ FAIL: spec/golden_vectors.json drifted from spec/hydb_contract_spec.mjs!');
      process.exit(1);
    }
    console.log('✅ PASS: spec/golden_vectors.json is 100% reproducible from spec/hydb_contract_spec.mjs');
  } else {
    writeFileSync(GOLDEN_PATH, generated, 'utf-8');
    console.log('✅ Generated spec/golden_vectors.json');
  }
}
