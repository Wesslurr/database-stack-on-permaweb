--[[
========================================================================
HyDB Canonical Contract Specification Reference — hydb.spec/1 in Lua
========================================================================
Byte-for-byte canonical TLV encoding, SHA-256 event hashing, manifest hashing,
schema validation, record version semantics, and state commitment.
Matches spec/hydb_contract_spec.mjs and hymessage/src/hydb_spec.erl.
--]]

local M = {}

-- ============================================================================
-- SHA-256 (Pure Lua 5.3 bitwise implementation with crypto fallback)
-- ============================================================================
local K = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
}

local function ror32(x, n)
    x = x & 0xffffffff
    return ((x >> n) | (x << (32 - n))) & 0xffffffff
end

function M.sha256(msg)
    local len = #msg
    local bit_len = len * 8
    local pad_len = ((56 - (len + 1) % 64) % 64)
    local padded = msg .. "\128" .. string.rep("\0", pad_len) ..
                   string.char(
                       (bit_len >> 56) & 0xff, (bit_len >> 48) & 0xff,
                       (bit_len >> 40) & 0xff, (bit_len >> 32) & 0xff,
                       (bit_len >> 24) & 0xff, (bit_len >> 16) & 0xff,
                       (bit_len >> 8) & 0xff, bit_len & 0xff
                   )

    local h0, h1, h2, h3, h4, h5, h6, h7 =
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19

    local w = {}
    for chunk = 1, #padded, 64 do
        for i = 1, 16 do
            local idx = chunk + (i - 1) * 4
            local b1, b2, b3, b4 = string.byte(padded, idx, idx + 3)
            w[i] = ((b1 << 24) | (b2 << 16) | (b3 << 8) | b4) & 0xffffffff
        end
        for i = 17, 64 do
            local s0 = (ror32(w[i-15], 7) ~ ror32(w[i-15], 18) ~ (w[i-15] >> 3)) & 0xffffffff
            local s1 = (ror32(w[i-2], 17) ~ ror32(w[i-2], 19) ~ (w[i-2] >> 10)) & 0xffffffff
            w[i] = (w[i-16] + s0 + w[i-7] + s1) & 0xffffffff
        end

        local a, b, c, d, e, f, g, h = h0, h1, h2, h3, h4, h5, h6, h7
        for i = 1, 64 do
            local S1 = (ror32(e, 6) ~ ror32(e, 11) ~ ror32(e, 25)) & 0xffffffff
            local ch = ((e & f) ~ ((~e) & g)) & 0xffffffff
            local temp1 = (h + S1 + ch + K[i] + w[i]) & 0xffffffff
            local S0 = (ror32(a, 2) ~ ror32(a, 13) ~ ror32(a, 22)) & 0xffffffff
            local maj = ((a & b) ~ (a & c) ~ (b & c)) & 0xffffffff
            local temp2 = (S0 + maj) & 0xffffffff

            h = g
            g = f
            f = e
            e = (d + temp1) & 0xffffffff
            d = c
            c = b
            b = a
            a = (temp1 + temp2) & 0xffffffff
        end

        h0 = (h0 + a) & 0xffffffff
        h1 = (h1 + b) & 0xffffffff
        h2 = (h2 + c) & 0xffffffff
        h3 = (h3 + d) & 0xffffffff
        h4 = (h4 + e) & 0xffffffff
        h5 = (h5 + f) & 0xffffffff
        h6 = (h6 + g) & 0xffffffff
        h7 = (h7 + h) & 0xffffffff
    end

    return string.format("%08X%08X%08X%08X%08X%08X%08X%08X", h0, h1, h2, h3, h4, h5, h6, h7)
end

-- ============================================================================
-- Canonical TLV Encoding
-- ============================================================================
local TAG = { INT64 = 1, BOOL = 2, STRING = 3, BYTES = 4, NULL = 5, ARRAY = 6, OBJECT = 7, ABSENT = 8 }
M.TAG = TAG

local function u32(n)
    return string.char((n >> 24) & 0xff, (n >> 16) & 0xff, (n >> 8) & 0xff, n & 0xff)
end

local function i64(n)
    local bytes = {}
    for i = 7, 0, -1 do
        table.insert(bytes, string.char((n >> (i * 8)) & 0xff))
    end
    return table.concat(bytes)
end

local function hex_to_bin(hex)
    local bytes = {}
    for i = 1, #hex, 2 do
        table.insert(bytes, string.char(tonumber(hex:sub(i, i + 1), 16)))
    end
    return table.concat(bytes)
end

function M.encode_value(h)
    if h == nil or h.t == "absent" then
        return string.char(TAG.ABSENT)
    elseif h.t == "null" then
        return string.char(TAG.NULL)
    elseif h.t == "int64" then
        return string.char(TAG.INT64) .. i64(math.floor(h.v))
    elseif h.t == "bool" then
        return string.char(TAG.BOOL, h.v and 1 or 0)
    elseif h.t == "string" then
        local s = tostring(h.v)
        return string.char(TAG.STRING) .. u32(#s) .. s
    elseif h.t == "bytes" then
        local b = h.v
        return string.char(TAG.BYTES) .. u32(#b) .. b
    elseif h.t == "array" then
        local parts = { string.char(TAG.ARRAY), u32(#h.v) }
        for _, elem in ipairs(h.v) do
            table.insert(parts, M.encode_value(elem))
        end
        return table.concat(parts)
    elseif h.t == "object" then
        local entries = {}
        for _, entry in ipairs(h.v) do
            table.insert(entries, { k = tostring(entry[1]), v = entry[2] })
        end
        table.sort(entries, function(a, b) return a.k < b.k end)
        local parts = { string.char(TAG.OBJECT), u32(#entries) }
        for _, entry in ipairs(entries) do
            table.insert(parts, u32(#entry.k))
            table.insert(parts, entry.k)
            table.insert(parts, M.encode_value(entry.v))
        end
        return table.concat(parts)
    else
        error("unknown tag " .. tostring(h.t))
    end
end

function M.encode_event(ev)
    local parts = {
        "HYDBE\1",
        M.encode_value({ t = "string", v = ev.contract_version or "hydb.core@1.0" }),
        M.encode_value({ t = "bytes", v = hex_to_bin(ev.prev_hash or string.rep("0", 64)) }),
        M.encode_value({ t = "int64", v = ev.epoch }),
        M.encode_value({ t = "int64", v = ev.seq }),
        M.encode_value({ t = "string", v = ev.op }),
        M.encode_value({ t = "string", v = ev.namespace }),
        M.encode_value({ t = "string", v = ev.schema_version }),
        M.encode_value(ev.key),
        M.encode_value({ t = "string", v = ev.value_type }),
        M.encode_value(ev.value),
        M.encode_value({ t = "int64", v = ev.ver })
    }
    return table.concat(parts)
end

function M.event_hash(ev)
    return M.sha256(M.encode_event(ev))
end

function M.encode_manifest(h_schema)
    return "HYDBM\1" .. M.encode_value(h_schema)
end

function M.manifest_hash(h_schema)
    return M.sha256(M.encode_manifest(h_schema))
end

function M.encode_state(schemas, records)
    table.sort(schemas, function(a, b) return a.ns < b.ns end)
    table.sort(records, function(a, b)
        if a.ns == b.ns then
            return M.encode_value(a.key_h) < M.encode_value(b.key_h)
        end
        return a.ns < b.ns
    end)

    local parts = { "HYDBS\1", u32(#schemas) }
    for _, s in ipairs(schemas) do
        table.insert(parts, M.encode_value({ t = "string", v = s.ns }))
        table.insert(parts, M.encode_value({ t = "string", v = s.schema_vsn }))
        table.insert(parts, M.encode_value({ t = "int64", v = s.reg_ver }))
        table.insert(parts, M.encode_value({ t = "bytes", v = hex_to_bin(s.manifest_hash) }))
    end
    table.insert(parts, u32(#records))
    for _, r in ipairs(records) do
        table.insert(parts, M.encode_value({ t = "string", v = r.ns }))
        table.insert(parts, M.encode_value(r.key_h))
        table.insert(parts, M.encode_value({ t = "int64", v = r.ver }))
        table.insert(parts, M.encode_value(r.val_h))
    end
    return table.concat(parts)
end

function M.state_commitment(schemas, records)
    return M.sha256(M.encode_state(schemas, records))
end

return M
