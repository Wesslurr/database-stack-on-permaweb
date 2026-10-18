-- Lua Conformance Vector Runner for HyDB hydb.spec/1

local DB_DIR = "/home/weslur/database"
package.path = DB_DIR .. "/ao_process/?.lua;" .. package.path

local hydb_spec = require("hydb_spec")

local function to_hex(bin_str)
    local hex = {}
    for i = 1, #bin_str do
        table.insert(hex, string.format("%02X", string.byte(bin_str, i)))
    end
    return table.concat(hex)
end

-- 1. Value Encodings
local int64_42 = to_hex(hydb_spec.encode_value({ t = "int64", v = 42 }))
local int64_min = to_hex(hydb_spec.encode_value({ t = "int64", v = -9223372036854775808 }))
local int64_max = to_hex(hydb_spec.encode_value({ t = "int64", v = 9223372036854775807 }))
local bool_true = to_hex(hydb_spec.encode_value({ t = "bool", v = true }))
local bool_false = to_hex(hydb_spec.encode_value({ t = "bool", v = false }))
local str_simple = to_hex(hydb_spec.encode_value({ t = "string", v = "hello world" }))
local str_unicode = to_hex(hydb_spec.encode_value({ t = "string", v = "Käse 🚀 日本語" }))
local bytes_val = to_hex(hydb_spec.encode_value({ t = "bytes", v = string.char(1, 2, 3, 255) }))
local absent_val = to_hex(hydb_spec.encode_value(nil))
local null_val = to_hex(hydb_spec.encode_value({ t = "null" }))

local json_val = to_hex(hydb_spec.encode_value({
    t = "object",
    v = {
        { "zeta", { t = "int64", v = 100 } },
        { "alpha", { t = "string", v = "first" } },
        { "mid", { t = "array", v = { { t = "int64", v = 1 }, { t = "int64", v = 2 } } } }
    }
}))

-- 2. Schemas and Manifests
local valid_schema_h = {
    t = "object",
    v = {
        { "backend", { t = "string", v = "wasm_slot" } },
        { "key_type", { t = "string", v = "int64" } },
        { "namespace", { t = "string", v = "finance.balances" } },
        { "require_auth", { t = "bool", v = true } },
        { "requires", { t = "array", v = { { t = "string", v = "cas" } } } },
        { "rules", { t = "object", v = {
            { "max_records", { t = "int64", v = 1000 } },
            { "min_value", { t = "int64", v = 0 } }
        } } },
        { "schema_version", { t = "string", v = "1.0.0" } },
        { "value_type", { t = "string", v = "int64" } }
    }
}
local valid_manifest_hash = hydb_spec.manifest_hash(valid_schema_h)

local doc_schema_h = {
    t = "object",
    v = {
        { "backend", { t = "string", v = "ets_doc" } },
        { "key_type", { t = "string", v = "string" } },
        { "namespace", { t = "string", v = "app.docs" } },
        { "require_auth", { t = "bool", v = false } },
        { "requires", { t = "array", v = { { t = "string", v = "prefix_scan" } } } },
        { "rules", { t = "object", v = {
            { "additional_fields", { t = "bool", v = true } },
            { "fields", { t = "object", v = {
                { "title", { t = "object", v = {
                    { "min_length", { t = "int64", v = 3 } },
                    { "required", { t = "bool", v = true } },
                    { "type", { t = "string", v = "string" } }
                } } },
                { "views", { t = "object", v = {
                    { "min_value", { t = "int64", v = 0 } },
                    { "required", { t = "bool", v = false } },
                    { "type", { t = "string", v = "int64" } }
                } } }
            } } }
        } } },
        { "schema_version", { t = "string", v = "1.0.0" } },
        { "value_type", { t = "string", v = "json" } }
    }
}
local doc_manifest_hash = hydb_spec.manifest_hash(doc_schema_h)

-- 3. Events and Hash Chain
local ev1 = {
    contract_version = "hydb.core@1.0",
    prev_hash = string.rep("0", 64),
    epoch = 1,
    seq = 1,
    op = "register",
    namespace = "finance.balances",
    schema_version = "1.0.0",
    key = nil,
    value_type = "schema",
    value = valid_schema_h,
    ver = 1
}
local ev1_hash = hydb_spec.event_hash(ev1)

local ev2 = {
    contract_version = "hydb.core@1.0",
    prev_hash = ev1_hash,
    epoch = 1,
    seq = 2,
    op = "put",
    namespace = "finance.balances",
    schema_version = "1.0.0",
    key = { t = "int64", v = 1001 },
    value_type = "int64",
    value = { t = "int64", v = 50000 },
    ver = 1
}
local ev2_hash = hydb_spec.event_hash(ev2)

local ev3 = {
    contract_version = "hydb.core@1.0",
    prev_hash = ev2_hash,
    epoch = 1,
    seq = 3,
    op = "del",
    namespace = "finance.balances",
    schema_version = "1.0.0",
    key = { t = "int64", v = 1001 },
    value_type = "int64",
    value = nil,
    ver = 2
}
local ev3_hash = hydb_spec.event_hash(ev3)

local ev4 = {
    contract_version = "hydb.core@1.0",
    prev_hash = ev3_hash,
    epoch = 1,
    seq = 4,
    op = "put",
    namespace = "finance.balances",
    schema_version = "1.0.0",
    key = { t = "int64", v = 1001 },
    value_type = "int64",
    value = { t = "int64", v = 75000 },
    ver = 3
}
local ev4_hash = hydb_spec.event_hash(ev4)

-- 4. State Commitment
local schemas_state = {
    { ns = "finance.balances", schema_vsn = "1.0.0", reg_ver = 1, manifest_hash = valid_manifest_hash },
    { ns = "app.docs", schema_vsn = "1.0.0", reg_ver = 1, manifest_hash = doc_manifest_hash }
}
local records_state = {
    { ns = "finance.balances", key_h = { t = "int64", v = 1001 }, ver = 3, val_h = { t = "int64", v = 75000 } },
    { ns = "finance.balances", key_h = { t = "int64", v = 1002 }, ver = 1, val_h = { t = "int64", v = 25000 } },
    { ns = "app.docs", key_h = { t = "string", v = "doc:readme" }, ver = 1, val_h = {
        t = "object",
        v = {
            { "title", { t = "string", v = "HyDB Guide" } },
            { "views", { t = "int64", v = 10 } }
        }
    } }
}
local commitment_hex = hydb_spec.state_commitment(schemas_state, records_state)

local output = string.format(
    '{"int64_42":"%s","int64_min":"%s","int64_max":"%s",' ..
    '"bool_true":"%s","bool_false":"%s","str_simple":"%s","str_unicode":"%s",' ..
    '"bytes":"%s","absent":"%s","null":"%s","json":"%s",' ..
    '"valid_manifest_hash":"%s","doc_manifest_hash":"%s",' ..
    '"ev1_hash":"%s","ev2_hash":"%s","ev3_hash":"%s","ev4_hash":"%s",' ..
    '"state_commitment":"%s"}',
    int64_42, int64_min, int64_max,
    bool_true, bool_false, str_simple, str_unicode,
    bytes_val, absent_val, null_val, json_val,
    valid_manifest_hash, doc_manifest_hash,
    ev1_hash, ev2_hash, ev3_hash, ev4_hash,
    commitment_hex
)

print(output)
