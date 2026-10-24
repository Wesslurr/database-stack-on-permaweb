import * as spec from '../spec/hydb_contract_spec.mjs';
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));

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
const ev1 = {
  contract_version: spec.CONTRACT_VERSION,
  prev_hash: spec.GENESIS_HASH,
  epoch: 1n,
  seq: 1n,
  ...ev1Draft
};
ev1.hash = spec.eventHash(ev1);
spec.applyEvent(st, ev1);

const ev2Draft = spec.prepareOp(st, { op: 'put', namespace: 'finance.balances', key: 1001, value: 50000, authorized: true });
const ev2 = {
  contract_version: spec.CONTRACT_VERSION,
  prev_hash: ev1.hash,
  epoch: 1n,
  seq: 2n,
  ...ev2Draft
};
ev2.hash = spec.eventHash(ev2);
spec.applyEvent(st, ev2);

const ev3Draft = spec.prepareOp(st, { op: 'del', namespace: 'finance.balances', key: 1001, authorized: true });
const ev3 = {
  contract_version: spec.CONTRACT_VERSION,
  prev_hash: ev2.hash,
  epoch: 1n,
  seq: 3n,
  ...ev3Draft
};
ev3.hash = spec.eventHash(ev3);
spec.applyEvent(st, ev3);

const ev4Draft = spec.prepareOp(st, { op: 'put', namespace: 'finance.balances', key: 1001, value: 75000, authorized: true });
const ev4 = {
  contract_version: spec.CONTRACT_VERSION,
  prev_hash: ev3.hash,
  epoch: 1n,
  seq: 4n,
  ...ev4Draft
};
ev4.hash = spec.eventHash(ev4);
spec.applyEvent(st, ev4);

const commitment256 = spec.stateCommitment(st);

const goldenVectors = {
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
    sorted_json: spec.encodeValue({
      t: 'object',
      v: [
        ['zeta', { t: 'int64', v: 100n }],
        ['alpha', { t: 'string', v: 'first' }],
        ['mid', { t: 'array', v: [{ t: 'int64', v: 1n }, { t: 'int64', v: 2n }] }]
      ]
    }).toString('hex').toUpperCase()
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
  state_commitment_256: commitment256,
  errors: spec.ERRORS,
  policy_allowlist: spec.POLICY_ALLOWLIST
};

const outPath = join(__dirname, '../spec/golden_vectors.json');
writeFileSync(outPath, JSON.stringify(goldenVectors, null, 2) + '\n');
console.log(`Golden vectors generated at ${outPath}`);
