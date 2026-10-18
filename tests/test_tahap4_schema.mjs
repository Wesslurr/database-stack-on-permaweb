/**
 * HyDB Tahap 4: Minimal, Strict Schema & Capability Enforcement Verification Suite
 *
 * Verifies:
 * 1. Minimal core schema definition: only supported primitive types and JSON objects.
 * 2. Explicit rejection of unimplemented complex features (indexes, query language, migrations).
 * 3. Numerical specialization of wasm_slot (strictly int64 key/value, rejects string/json).
 * 4. Document specialization of ets_doc (structured doc, string/int64 key, string/bytes/bool/int64/json value).
 * 5. String and Bytes bounds enforcement (min_length, max_length).
 * 6. Strict JSON field specifications (types, bounds, required fields, additional_fields toggle).
 * 7. End-to-end enforcement through SDK and Hot Node HTTP API.
 */

import { HyDBContract, ContractPreflightError } from '../sdk/hydb_sdk.mjs';

const HOT_URL = 'http://127.0.0.1:8734/~hymessage@1.0';

const isTrue = (v) => v === true || String(v).toLowerCase() === 'true';
const isFalse = (v) => v === false || String(v).toLowerCase() === 'false';

async function httpGet(url) {
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  const text = await res.text();
  try {
    return { status: res.status, body: JSON.parse(text) };
  } catch {
    return { status: res.status, body: text };
  }
}

async function httpPost(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body)
  });
  const text = await res.text();
  try {
    return { status: res.status, body: JSON.parse(text) };
  } catch {
    return { status: res.status, body: text };
  }
}

async function main() {
  console.log('========================================================================');
  console.log(' HyDB Tahap 4: Schema & Capability Enforcement Test Suite');
  console.log('========================================================================\n');

  let passed = 0;
  let failed = 0;

  function assert(condition, message) {
    if (condition) {
      console.log(`  ✅ PASS: ${message}`);
      passed++;
    } else {
      console.error(`  ❌ FAIL: ${message}`);
      failed++;
    }
  }

  // --------------------------------------------------------------------------
  // TEST 1: SDK Preflight Rejection of Unsupported Features & Invalid Types
  // --------------------------------------------------------------------------
  console.log('[Test 1] SDK Preflight: Rejection of Unsupported Features & Types:');

  // 1a. Unsupported features
  for (const feat of ['indexes', 'secondaryIndexes', 'queryLanguage', 'migrations', 'relations']) {
    let rejected = false;
    try {
      new HyDBContract({
        namespace: 'test.unsupported',
        backend: 'ets_doc',
        [feat]: { some: 'config' }
      });
    } catch (err) {
      if (err instanceof ContractPreflightError && err.stage === 'FEATURE_UNSUPPORTED_CHECK') {
        rejected = true;
      }
    }
    assert(rejected, `SDK constructor rejects '${feat}' with FEATURE_UNSUPPORTED_CHECK`);
  }

  // 1b. Unsupported key types
  let badKeyRejected = false;
  try {
    new HyDBContract({
      namespace: 'test.badkey',
      backend: 'ets_doc',
      schema: { keyType: 'boolean' }
    });
  } catch (err) {
    if (err instanceof ContractPreflightError && err.stage === 'SCHEMA_TYPE_CHECK') {
      badKeyRejected = true;
    }
  }
  assert(badKeyRejected, 'SDK constructor rejects unsupported key_type (boolean)');

  // 1c. Unsupported value types
  let badValRejected = false;
  try {
    new HyDBContract({
      namespace: 'test.badval',
      backend: 'ets_doc',
      schema: { valueType: 'float64' }
    });
  } catch (err) {
    if (err instanceof ContractPreflightError && err.stage === 'SCHEMA_TYPE_CHECK') {
      badValRejected = true;
    }
  }
  assert(badValRejected, 'SDK constructor rejects unsupported value_type (float64)');

  // 1d. wasm_slot numeric specialization
  let wasmStringRejected = false;
  try {
    new HyDBContract({
      namespace: 'test.wasmslot',
      backend: 'wasm_slot',
      schema: { keyType: 'string', valueType: 'int64' }
    });
  } catch (err) {
    if (err instanceof ContractPreflightError && err.stage === 'BACKEND_CAPABILITY_CHECK') {
      wasmStringRejected = true;
    }
  }
  assert(wasmStringRejected, 'SDK constructor rejects string key on wasm_slot');

  // 1e. Incoherent bounds in preflight
  let incoherentLenRejected = false;
  try {
    const c = new HyDBContract({
      namespace: 'test.incoherent',
      backend: 'ets_doc',
      schema: { valueType: 'string', minLength: 50, maxLength: 10 }
    });
    await c.preflightDryRun();
  } catch (err) {
    if (err instanceof ContractPreflightError && err.stage === 'SCHEMA_INVARIANT_CHECK') {
      incoherentLenRejected = true;
    }
  }
  assert(incoherentLenRejected, 'SDK preflightDryRun rejects min_length > max_length');

  // 1f. Incoherent JSON field rule in preflight
  let badFieldSpecRejected = false;
  try {
    const c = new HyDBContract({
      namespace: 'test.badfield',
      backend: 'ets_doc',
      schema: {
        valueType: 'json',
        fields: {
          age: { type: 'int64', min_value: 100, max_value: 10 }
        }
      }
    });
    await c.preflightDryRun();
  } catch (err) {
    if (err instanceof ContractPreflightError && err.stage === 'SCHEMA_INVARIANT_CHECK') {
      badFieldSpecRejected = true;
    }
  }
  assert(badFieldSpecRejected, 'SDK preflightDryRun rejects incoherent JSON field min_value > max_value');

  // --------------------------------------------------------------------------
  // TEST 2: Erlang Node Rejection of Unsupported Features & Incoherent Rules
  // --------------------------------------------------------------------------
  console.log('\n[Test 2] Erlang Node Rejection of Unsupported Features (HTTP 501 / 422):');

  // 2a. Attempt registering with indexes
  const resIndexes = await httpPost(`${HOT_URL}/schema?action=register`, {
    namespace: 'test.node.indexes',
    backend: 'ets_doc',
    schema_version: '1.0.0',
    key_type: 'string',
    value_type: 'json',
    indexes: ['idx_created_at']
  });
  assert(resIndexes.status === 501 || resIndexes.body?.status === 501, 'Node rejects registration with indexes (HTTP 501)');
  assert(resIndexes.body?.error_code === 'ERR_UNSUPPORTED_FEATURE', `Returns ERR_UNSUPPORTED_FEATURE (got ${resIndexes.body?.error_code})`);

  // 2b. Attempt registering with query_language
  const resQL = await httpPost(`${HOT_URL}/schema?action=register`, {
    namespace: 'test.node.ql',
    backend: 'ets_doc',
    schema_version: '1.0.0',
    key_type: 'string',
    value_type: 'json',
    query_language: 'sql_lite'
  });
  assert(resQL.status === 501 || resQL.body?.status === 501, 'Node rejects registration with query_language (HTTP 501)');
  assert(resQL.body?.error_code === 'ERR_UNSUPPORTED_FEATURE', `Returns ERR_UNSUPPORTED_FEATURE (got ${resQL.body?.error_code})`);

  // 2c. Attempt registering with incoherent string lengths
  const resIncohStr = await httpPost(`${HOT_URL}/schema?action=register`, {
    namespace: 'test.node.incohstr',
    backend: 'ets_doc',
    schema_version: '1.0.0',
    key_type: 'string',
    value_type: 'string',
    rules: { min_length: 100, max_length: 5 }
  });
  assert(resIncohStr.status === 422 || resIncohStr.body?.status === 422, 'Node rejects min_length > max_length (HTTP 422)');
  assert(resIncohStr.body?.error_code === 'ERR_INVALID_SCHEMA', `Returns ERR_INVALID_SCHEMA (got ${resIncohStr.body?.error_code})`);

  // 2d. Attempt registering with invalid JSON field type
  const resBadFType = await httpPost(`${HOT_URL}/schema?action=register`, {
    namespace: 'test.node.badftype',
    backend: 'ets_doc',
    schema_version: '1.0.0',
    key_type: 'string',
    value_type: 'json',
    rules: {
      fields: {
        score: { type: 'complex_decimal' }
      }
    }
  });
  assert(resBadFType.status === 422 || resBadFType.body?.status === 422, 'Node rejects invalid JSON field type (HTTP 422)');
  assert(resBadFType.body?.error_code === 'ERR_INVALID_SCHEMA', `Returns ERR_INVALID_SCHEMA (got ${resBadFType.body?.error_code})`);

  // --------------------------------------------------------------------------
  // TEST 3: String & Bytes Bounds Enforcement in Active Namespaces
  // --------------------------------------------------------------------------
  console.log('\n[Test 3] String & Bytes Bounds Enforcement:');

  const strNs = `test.strbounds.${Date.now()}`;
  const regStr = await httpPost(`${HOT_URL}/schema?action=register`, {
    namespace: strNs,
    backend: 'ets_doc',
    schema_version: '1.0.0',
    key_type: 'string',
    value_type: 'string',
    rules: { min_length: 5, max_length: 15 }
  });
  assert(isTrue(regStr.body?.ok), `Registered string namespace '${strNs}'`);

  // 3a. String too short
  const putTooShort = await httpPost(`${HOT_URL}/put`, {
    namespace: strNs,
    key: 'k1',
    val: 'abc' // 3 bytes < 5
  });
  assert(putTooShort.status === 422 || putTooShort.body?.status === 422, 'Put string shorter than min_length rejected (HTTP 422)');
  assert(putTooShort.body?.error_code === 'ERR_SCHEMA_VIOLATION', `Returns ERR_SCHEMA_VIOLATION (got ${putTooShort.body?.error_code})`);

  // 3b. String too long
  const putTooLong = await httpPost(`${HOT_URL}/put`, {
    namespace: strNs,
    key: 'k1',
    val: 'this string is much too long for the schema limit'
  });
  assert(putTooLong.status === 422 || putTooLong.body?.status === 422, 'Put string longer than max_length rejected (HTTP 422)');
  assert(putTooLong.body?.error_code === 'ERR_SCHEMA_VIOLATION', `Returns ERR_SCHEMA_VIOLATION (got ${putTooLong.body?.error_code})`);

  // 3c. Valid string
  const putValidStr = await httpPost(`${HOT_URL}/put`, {
    namespace: strNs,
    key: 'k1',
    val: 'valid_string'
  });
  assert(isTrue(putValidStr.body?.ok), 'Put valid string accepted (HTTP 200)');

  // Verify read back
  const getStr = await httpGet(`${HOT_URL}/get?namespace=${strNs}&key=k1`);
  assert(isTrue(getStr.body?.ok) && getStr.body?.val === 'valid_string', 'Read back valid stored string');

  // --------------------------------------------------------------------------
  // TEST 4: Strict JSON Field Specification & additional_fields: false Enforcement
  // --------------------------------------------------------------------------
  console.log('\n[Test 4] Strict JSON Field Specification & additional_fields Enforcement:');

  const jsonNs = `test.strictjson.${Date.now()}`;
  const regJson = await httpPost(`${HOT_URL}/schema?action=register`, {
    namespace: jsonNs,
    backend: 'ets_doc',
    schema_version: '1.0.0',
    key_type: 'string',
    value_type: 'json',
    rules: {
      required_fields: ['id', 'username'],
      additional_fields: false,
      fields: {
        id: { type: 'int64', min_value: 1, max_value: 1000000 },
        username: { type: 'string', min_length: 3, max_length: 20 },
        active: { type: 'bool' },
        metadata: { type: 'object' }
      }
    }
  });
  assert(isTrue(regJson.body?.ok), `Registered strict JSON namespace '${jsonNs}'`);

  // 4a. Missing required field 'username'
  const putMissingField = await httpPost(`${HOT_URL}/put`, {
    namespace: jsonNs,
    key: 'user:1',
    val: { id: 1 }
  });
  assert(putMissingField.status === 422 || putMissingField.body?.status === 422, 'Missing required JSON field rejected (HTTP 422)');
  assert(putMissingField.body?.error_code === 'ERR_SCHEMA_VIOLATION', `Returns ERR_SCHEMA_VIOLATION (got ${putMissingField.body?.error_code})`);

  // 4b. Field type mismatch ('id' must be int64, passing string)
  const putBadType = await httpPost(`${HOT_URL}/put`, {
    namespace: jsonNs,
    key: 'user:1',
    val: { id: 'not_an_int', username: 'alice' }
  });
  assert(putBadType.status === 422 || putBadType.body?.status === 422, 'Field type mismatch rejected (HTTP 422)');
  assert(putBadType.body?.error_code === 'ERR_SCHEMA_VIOLATION', `Returns ERR_SCHEMA_VIOLATION (got ${putBadType.body?.error_code})`);

  // 4c. Field numeric bounds violation ('id' < min_value 1)
  const putMinVal = await httpPost(`${HOT_URL}/put`, {
    namespace: jsonNs,
    key: 'user:1',
    val: { id: 0, username: 'alice' }
  });
  assert(putMinVal.status === 422 || putMinVal.body?.status === 422, 'Field below min_value rejected (HTTP 422)');
  assert(putMinVal.body?.error_code === 'ERR_SCHEMA_VIOLATION', `Returns ERR_SCHEMA_VIOLATION (got ${putMinVal.body?.error_code})`);

  // 4d. Field string bounds violation ('username' shorter than min_length 3)
  const putMinLen = await httpPost(`${HOT_URL}/put`, {
    namespace: jsonNs,
    key: 'user:1',
    val: { id: 1, username: 'ab' }
  });
  assert(putMinLen.status === 422 || putMinLen.body?.status === 422, 'Field string shorter than min_length rejected (HTTP 422)');
  assert(putMinLen.body?.error_code === 'ERR_SCHEMA_VIOLATION', `Returns ERR_SCHEMA_VIOLATION (got ${putMinLen.body?.error_code})`);

  // 4e. Undeclared field rejected when additional_fields: false
  const putUndeclared = await httpPost(`${HOT_URL}/put`, {
    namespace: jsonNs,
    key: 'user:1',
    val: { id: 1, username: 'alice', extra_field: 'unauthorized_payload' }
  });
  assert(putUndeclared.status === 422 || putUndeclared.body?.status === 422, 'Undeclared field rejected when additional_fields=false (HTTP 422)');
  assert(putUndeclared.body?.error_code === 'ERR_SCHEMA_VIOLATION', `Returns ERR_SCHEMA_VIOLATION (got ${putUndeclared.body?.error_code})`);

  // 4f. Valid JSON payload matching all rules exactly
  const putValidJson = await httpPost(`${HOT_URL}/put`, {
    namespace: jsonNs,
    key: 'user:1',
    val: {
      id: 1,
      username: 'alice',
      active: true,
      metadata: { role: 'admin' }
    }
  });
  assert(isTrue(putValidJson.body?.ok), 'Valid JSON payload matching strict rules accepted (HTTP 200)');

  // Verify read back
  const getJson = await httpGet(`${HOT_URL}/get?namespace=${jsonNs}&key=user:1`);
  const parsedVal = typeof getJson.body?.val === 'string' ? JSON.parse(getJson.body.val) : getJson.body?.val;
  assert(isTrue(getJson.body?.ok) && parsedVal?.username === 'alice', 'Read back stored JSON object intact');

  // --------------------------------------------------------------------------
  // Summary
  // --------------------------------------------------------------------------
  console.log('\n========================================================================');
  console.log(` Tahap 4 Verification Summary: ${passed} PASSED, ${failed} FAILED`);
  console.log('========================================================================\n');

  if (failed > 0) {
    process.exit(1);
  }
}

main().catch(err => {
  console.error('Fatal error in Tahap 4 test suite:', err);
  process.exit(1);
});
