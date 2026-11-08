-- ==============================================================================
-- Primary Process (Batched Ingestion, Honest Quorum, Canonical State Root)
-- ==============================================================================
-- Contract (see ROADMAP.md §3):
--   * ACK is only `replicated` (HTTP 200) after a real SD response has been
--     parsed and verified (batch_id echo + state_root equality + tick).
--   * Inputs enter a batch queue and are answered with HTTP 202 (`accepted`).
--     A 202 NEVER claims replication.
--   * If no replica verifies the batch, the response is HTTP 503 and the
--     Primary state (Points, Tick, StateRoot) is left untouched.
--   * The state root is canonical: sorted entity IDs + fixed-point numbers.
--
-- The topology is resolved from Genesis at runtime (zero hardcoded peers).

Points = Points or {}           -- id -> "x|y|tick|owner" (compact, bounded)
Tick = Tick or 0
StateRoot = StateRoot or "0x0000000000000000000000000000000000000000000000000000000000000000"
GenesisUrl = GenesisUrl or nil
Topology = Topology or nil
NodeSigner = NodeSigner or nil
Hydrated = Hydrated or false

-- Batched ingress ------------------------------------------------------------
Queue = Queue or {}             -- pending inputs for the open batch
NextSeq = NextSeq or 0
BatchOpenedTs = BatchOpenedTs or nil
OpenBatchId = OpenBatchId or nil
LastBatch = LastBatch or nil
CommittedBatches = CommittedBatches or {}   -- batch_id -> result body
CommittedOrder = CommittedOrder or {}       -- bounded FIFO of batch ids
SeenInputs = SeenInputs or {}               -- idempotency key -> disposition
SeenInputOrder = SeenInputOrder or {}

-- Honest quorum ---------------------------------------------------------------
-- A Commit is CAST to every replica. State moves only when a replica casts an
-- `Ack` back that echoes the exact batch_id, tick and root the primary asked
-- for. Synchronous cross-node calls are never made from this process: a dead
-- peer must never be able to turn an ACK into a false claim.
PendingBatches = PendingBatches or {}       -- batch_id -> in-flight batch
MaxPendingBatches = MaxPendingBatches or 32
-- Confirmed batches that not every replica has acknowledged yet. They are
-- re-cast (rate limited) so a replica that was offline can replay the deltas.
RepairBatches = RepairBatches or {}
MaxRepairBatches = MaxRepairBatches or 32
RepairInterval = RepairInterval or 4
-- The Lua sandbox is deterministic (`os.time` is unavailable), so the batch
-- clock is the primary's own request counter. Expiry only affects what a
-- status request reports: state still moves only on a verified Ack.
RequestClock = RequestClock or 0
AckDeadlineRequests = AckDeadlineRequests or 16
QuorumAcks = QuorumAcks or 1                -- verified replicas needed to commit

MaxBatchInputs = MaxBatchInputs or 256
MinBatchInputs = MinBatchInputs or 8
BatchWindowMs = BatchWindowMs or 33
TicksPerBatch = TicksPerBatch or 2
MaxTicksPerBatch = MaxTicksPerBatch or 60
MaxQueue = MaxQueue or 4096
MaxLagInputs = MaxLagInputs or 256
MaxCommittedHistory = MaxCommittedHistory or 256
MaxSeenInputs = MaxSeenInputs or 4096

-- Dual-watermark, Anti-rubberband & Compaction state -------------------------
EntityLastTick = EntityLastTick or {}
HeadTick = HeadTick or Tick
HeadRoot = HeadRoot or StateRoot
HeadPoints = HeadPoints or {}
CompactedCount = CompactedCount or 0
OutOfOrderDropped = OutOfOrderDropped or 0

-- Guard rails ----------------------------------------------------------------
MaxEntities = MaxEntities or 1000
MaxPayloadBytes = MaxPayloadBytes or 65536
MaxIdLength = MaxIdLength or 128
MaxAuditRing = MaxAuditRing or 50
TickAuditRing = TickAuditRing or {}
ActiveEntityCount = ActiveEntityCount or nil
-- Monotonic count of inputs that reached the canonical root through a verified
-- quorum Ack. Exposed by Read-All so a load test can measure confirmed
-- throughput exactly instead of inferring it from polled batch bodies.
AppliedInputs = AppliedInputs or 0

local function json_safe(value)
    if value == nil then return "" end
    local s = tostring(value)
    s = string.gsub(s, "\\", "\\\\")
    s = string.gsub(s, '"', '\\"')
    s = string.gsub(s, "[\r\n\t\b\f]", " ")
    return s
end

-- ==============================================================================
-- Pure Lua 5.3 FIPS 180-4 Standard SHA-256 Cryptographic Engine
-- ==============================================================================
local K_SHA256 = {
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
    return (((x >> n) | (x << (32 - n))) & 0xffffffff)
end

local function sha256_hex_lua(str)
    str = str or ""
    local len = #str
    local bit_len = len * 8
    local pad_len = (56 - (len + 1) % 64) % 64
    local padded = str .. string.char(0x80) .. string.rep(string.char(0), pad_len)
    for i = 7, 0, -1 do
        padded = padded .. string.char((bit_len >> (i * 8)) & 0xff)
    end

    local h0, h1, h2, h3 = 0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a
    local h4, h5, h6, h7 = 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19

    local w = {}
    for chunk = 1, #padded, 64 do
        for i = 1, 16 do
            local idx = chunk + (i - 1) * 4
            local b1, b2, b3, b4 = string.byte(padded, idx, idx + 3)
            w[i] = ((b1 << 24) | (b2 << 16) | (b3 << 8) | b4) & 0xffffffff
        end
        for i = 17, 64 do
            local s0 = ror32(w[i - 15], 7) ~ ror32(w[i - 15], 18) ~ (w[i - 15] >> 3)
            local s1 = ror32(w[i - 2], 17) ~ ror32(w[i - 2], 19) ~ (w[i - 2] >> 10)
            w[i] = (w[i - 16] + s0 + w[i - 7] + s1) & 0xffffffff
        end

        local a, b, c, d, e, f, g, h = h0, h1, h2, h3, h4, h5, h6, h7
        for i = 1, 64 do
            local S1 = ror32(e, 6) ~ ror32(e, 11) ~ ror32(e, 25)
            local ch = (e & f) ~ ((~e) & g)
            local temp1 = (h + S1 + ch + K_SHA256[i] + w[i]) & 0xffffffff
            local S0 = ror32(a, 2) ~ ror32(a, 13) ~ ror32(a, 22)
            local maj = (a & b) ~ (a & c) ~ (b & c)
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

    return string.format("0x%08x%08x%08x%08x%08x%08x%08x%08x", h0, h1, h2, h3, h4, h5, h6, h7)
end

local function sha256_hex(str)
    if ao and ao.sha256 then
        return ao.sha256(str or "")
    end
    return sha256_hex_lua(str)
end

-- ==============================================================================
-- Compact entity encoding + canonical (deterministic) state root
-- ==============================================================================
-- Luerl represents every Lua number as an Erlang float, so `tostring(2)` yields
-- "2.0" while the same tick decoded from JSON yields "2". Root hashes must use a
-- single canonical integer rendering or Primary and replica disagree forever.
local function int_str(value)
    local n = tonumber(value) or 0
    return string.format("%d", math.floor(n))
end

local function pack_entity(x, y, tick, owner)
    return string.format("%.6f|%.6f|%d|%s", x, y, tick, json_safe(owner or "anonymous"))
end

local function unpack_entity(packed)
    if type(packed) ~= "string" then return nil end
    local x, y, tick, owner = string.match(packed, "^([%d%.%-]+)|([%d%.%-]+)|(%d+)|(.*)$")
    if not x then return nil end
    return { x = tonumber(x), y = tonumber(y), tick = tonumber(tick), owner = owner }
end

local function sorted_ids(map)
    local ids = {}
    for id in pairs(map) do ids[#ids + 1] = id end
    table.sort(ids)
    return ids
end

-- Canonical root: identical bytes on Primary and every SD for the same state.
-- Sorted IDs + fixed-point coordinates (no locale/pairs()/float-format drift).

-- ------------------------------------------------------------------------------
-- Incremental canonical root (delta, not full-state, hashing)
-- ------------------------------------------------------------------------------
-- Hashing every entity on every commit is O(state) and dominated the commit
-- latency once a few hundred entities existed (~0.65 ms/entity in luerl).
-- The canonical root is now a hash over fixed shards: a commit only re-hashes
-- the shards its delta actually touches. The function is byte-for-byte
-- identical on Primary and replicas.
ShardCount = 32

local function id_shard(id)
    local h = 2166136261
    for i = 1, #id do
        h = (h ~ string.byte(id, i)) & 0xffffffff
        h = (h * 16777619) & 0xffffffff
    end
    return (h % ShardCount) + 1
end

local function shard_digest(map)
    local ids = {}
    for id in pairs(map) do ids[#ids + 1] = id end
    table.sort(ids)
    local parts = {}
    for i = 1, #ids do parts[i] = ids[i] .. "=" .. map[ids[i]] end
    return sha256_hex(table.concat(parts, "\n"))
end

local function shard_root(hashes, tick)
    local parts = {}
    for i = 1, ShardCount do parts[i] = int_str(i) .. ":" .. (hashes[i] or "") end
    return sha256_hex("HS2|tick=" .. int_str(tick) .. "|shards\n" .. table.concat(parts, "\n"))
end

local function build_shards(points)
    local shards, hashes = {}, {}
    for i = 1, ShardCount do shards[i] = {} end
    for id, packed in pairs(points or {}) do
        shards[id_shard(id)][id] = packed
    end
    for i = 1, ShardCount do hashes[i] = shard_digest(shards[i]) end
    return shards, hashes
end

-- Root of (current shards + delta) WITHOUT mutating anything. Used to verify a
-- claimed root before applying it (verify-don't-trust).
local function candidate_root(shards, hashes, delta, tick)
    local touched = {}
    for id in pairs(delta or {}) do touched[id_shard(id)] = true end
    local next_hashes = {}
    for i = 1, ShardCount do next_hashes[i] = hashes[i] end
    for si in pairs(touched) do
        local tmp = {}
        for id, packed in pairs(shards[si]) do tmp[id] = packed end
        for id, packed in pairs(delta) do
            if id_shard(id) == si then tmp[id] = packed end
        end
        next_hashes[si] = shard_digest(tmp)
    end
    return shard_root(next_hashes, tick)
end

-- Mutate the shards with an accepted delta and return the new canonical root.
local function commit_shards(shards, hashes, delta, tick)
    local touched = {}
    for id, packed in pairs(delta or {}) do
        local si = id_shard(id)
        shards[si][id] = packed
        touched[si] = true
    end
    for si in pairs(touched) do hashes[si] = shard_digest(shards[si]) end
    return shard_root(hashes, tick)
end

Shards, ShardHashes = build_shards(Points)
StateRoot = shard_root(ShardHashes, Tick)

local function count_entities()
    local n = 0
    for _ in pairs(Points) do n = n + 1 end
    return n
end

local function contains_newlines(value)
    return type(value) == "string" and string.find(value, "[\r\n]") ~= nil
end

-- ==============================================================================
-- Payload guard: checks string AND table bodies (roadmap T10)
-- ==============================================================================
local function estimate_size(value, depth)
    depth = depth or 0
    if depth > 6 then return 16 end
    local t = type(value)
    if t == "string" then return #value end
    if t == "number" or t == "boolean" then return 12 end
    if t == "nil" then return 0 end
    if t == "table" then
        local n = 2
        for k, v in pairs(value) do
            n = n + estimate_size(k, depth + 1) + estimate_size(v, depth + 1) + 4
            if n > MaxPayloadBytes then return n end
        end
        return n
    end
    return 8
end

local function message_too_large(message)
    if type(message) ~= "table" then return false end
    local total = 0
    if type(message.body) == "string" then
        total = #message.body
    elseif type(message.body) == "table" then
        total = estimate_size(message.body, 0)
    end
    -- Also guard top-level keys when the body is not a table.
    if total <= MaxPayloadBytes then
        total = total + estimate_size(message.query or {}, 0)
    end
    return total > MaxPayloadBytes
end

local function append_audit(tick, id, x, y)
    if #TickAuditRing >= MaxAuditRing then
        table.remove(TickAuditRing, 1)
    end
    table.insert(TickAuditRing, { tick = tick, id = id, x = x, y = y })
end

local function extract_first_signer(process_or_msg)
    if type(process_or_msg) ~= "table" then return nil end
    if type(process_or_msg.authority) == "table" and #process_or_msg.authority > 0 then
        return tostring(process_or_msg.authority[1])
    elseif type(process_or_msg.authority) == "string" and #process_or_msg.authority > 0 then
        return process_or_msg.authority
    end
    if process_or_msg.commitments ~= nil and ao and ao.signers then
        local ok, list = pcall(function() return ao.signers(process_or_msg) end)
        if ok and type(list) == "table" and #list > 0 then
            return tostring(list[1])
        end
    end
    return nil
end

local function extract_msg_id(msg_table)
    if type(msg_table) == "table" then
        if msg_table.id then return tostring(msg_table.id) end
        if msg_table["hash-chain"] then return tostring(msg_table["hash-chain"]) end
    end
    return "slot-" .. tostring(RequestClock or 0)
end

local function encode_audit_ring()
    local items = {}
    for _, entry in ipairs(TickAuditRing) do
        table.insert(items, string.format(
            [[{"tick":%d,"id":"%s","x":%s,"y":%s}]],
            entry.tick, json_safe(entry.id), tostring(entry.x), tostring(entry.y)
        ))
    end
    return "[" .. table.concat(items, ",") .. "]"
end

local function encode_string_list(list)
    if not list or #list == 0 then return "[]" end
    local items = {}
    for _, v in ipairs(list) do
        table.insert(items, string.format([["%s"]], json_safe(v)))
    end
    return "[" .. table.concat(items, ",") .. "]"
end

local function encode_sd_urls(topo)
    if not topo or not topo.sd_targets then return "[]" end
    local urls = {}
    for _, target in ipairs(topo.sd_targets) do
        table.insert(urls, target.url)
    end
    return encode_string_list(urls)
end

local function encode_points(points)
    local ids = sorted_ids(points)
    local items = {}
    for i = 1, #ids do
        local id = ids[i]
        local e = unpack_entity(points[id])
        if e then
            items[i] = string.format(
                '["%s",%.6f,%.6f,%d,"%s"]',
                json_safe(id), e.x, e.y, e.tick, json_safe(e.owner)
            )
        end
    end
    return "[" .. table.concat(items, ",") .. "]"
end

local function remember_batch(batch_id, body)
    if CommittedBatches[batch_id] then return end
    CommittedBatches[batch_id] = body
    table.insert(CommittedOrder, batch_id)
    while #CommittedOrder > MaxCommittedHistory do
        local old = table.remove(CommittedOrder, 1)
        CommittedBatches[old] = nil
    end
end

local function remember_input(key, disposition)
    if not key then return end
    if SeenInputs[key] then return end
    SeenInputs[key] = disposition
    table.insert(SeenInputOrder, key)
    while #SeenInputOrder > MaxSeenInputs do
        local old = table.remove(SeenInputOrder, 1)
        SeenInputs[old] = nil
    end
end

-- ==============================================================================
-- Topology discovery + cold-start hydration (fixed parser: no field ordering)
-- ==============================================================================
local function parse_point_array(raw)
    local points = {}
    for id, x, y, tick, owner in string.gmatch(
        raw,
        '%[%s*"([^"]+)"%s*,%s*([%d%.%-]+)%s*,%s*([%d%.%-]+)%s*,%s*(%d+)%s*,%s*"([^"]*)"%s*%]'
    ) do
        points[id] = pack_entity(tonumber(x) or 0, tonumber(y) or 0, tonumber(tick) or 0, owner)
    end
    return points
end

local function parse_body(res)
    if type(res) == "table" then
        if res.body then return tostring(res.body) end
        if res.output and type(res.output) == "table" and res.output.body then
            return tostring(res.output.body)
        end
    elseif type(res) == "string" or type(res) == "number" then
        return tostring(res)
    end
    return nil
end

local function relay_call(url, method, payload)
    local ok, status, res = pcall(function()
        return ao.resolve({
            path = "/~relay@1.0/call",
            ["relay-path"] = url,
            ["relay-method"] = method,
            ["relay-body"] = payload,
            ["content-type"] = "application/json",
            ["cache-control"] = "no-store"
        })
    end)
    if not ok or (status ~= "ok" and status ~= true) then return nil, status end
    return parse_body(res), status
end

local function relay_cast(url, payload)
    pcall(function()
        ao.resolve({
            path = "/~relay@1.0/cast",
            ["relay-path"] = url,
            ["relay-method"] = "POST",
            ["relay-body"] = payload,
            ["content-type"] = "application/json",
            ["cache-control"] = "no-store"
        })
    end)
end

local function sd_endpoint(target, key)
    return target.url .. "/~node-process@1.0/" .. target.process .. "/" .. key
end

-- Pull the authoritative state from one replica. Returns true when the pull
-- succeeded. Used for cold start and for recovery when a replica reports that
-- it is ahead of us (a replica must never be overwritten by a stale primary).
local function hydrate_from(target)
    if not target then return false end
    local raw = relay_call(sd_endpoint(target, "push"), "POST", '{"action":"Hydrate"}')
    if not raw then return false end
    local tick_str = string.match(raw, '"last_tick"%s*:%s*(%d+)')
    local root_str = string.match(raw, '"state_root"%s*:%s*"(0x%x+)"')
    if not tick_str then return false end
    local points = {}
    local init = 1
    while true do
        local s, e, id, x, y, tick, owner = string.find(
            raw,
            '%[%s*"([^"]+)"%s*,%s*([%d%.%-]+)%s*,%s*([%d%.%-]+)%s*,%s*(%d+)%s*,%s*"([^"]*)"%s*%]',
            init
        )
        if not s then break end
        points[id] = pack_entity(tonumber(x) or 0, tonumber(y) or 0, tonumber(tick) or 0, owner)
        init = e + 1
    end
    Tick = tonumber(tick_str) or 0
    Points = points
    ActiveEntityCount = count_entities()
    Shards, ShardHashes = build_shards(Points)
    local computed = shard_root(ShardHashes, Tick)
    StateRoot = computed
    if root_str and root_str ~= computed and (Tick > 0 or next(points) ~= nil) then
        -- Heal a replica that has drifted from the canonical root.
        local payload = string.format(
            '{"action":"Sync-State","last_tick":%d,"state_root":"%s","points":%s}',
            Tick, computed, encode_points(Points)
        )
        relay_call(sd_endpoint(target, "push"), "POST", payload)
    end
    return true
end

-- Pull the authoritative state from whichever replica (SD1 or duplicate SD2)
-- reports the HIGHEST confirmed tick. Returns true if hydration succeeded.
local function hydrate_from_latest_replica()
    if not (Topology and Topology.sd_targets and #Topology.sd_targets > 0) then return false end
    local best_target = nil
    local highest_tick = -1
    for _, target in ipairs(Topology.sd_targets) do
        local raw = relay_call(sd_endpoint(target, "push"), "POST", '{"action":"Hydrate"}')
        if raw then
            local tick_str = string.match(raw, '"last_tick"%s*:%s*(%d+)')
            local t = tonumber(tick_str) or 0
            if t > highest_tick then
                highest_tick = t
                best_target = target
            end
        end
    end
    if best_target then
        return hydrate_from(best_target)
    end
    return hydrate_from(Topology.sd_targets[1])
end

local function ensure_topology(process, msg)
    GenesisUrl = GenesisUrl or process["genesis-url"] or (msg and msg["genesis-url"])
    if not NodeSigner then
        NodeSigner = extract_first_signer(process) or "primary_authority"
    end
    if not Topology and GenesisUrl then
        local raw = relay_call(
            GenesisUrl .. "/~node-process@1.0/genesis/now/results~message@1.0/output/body",
            "GET", nil
        )
        if raw then
            local sd1_url = string.match(raw, '"sd1_url"%s*:%s*"([^"]+)"')
            local sd1_proc = string.match(raw, '"sd1_process"%s*:%s*"([^"]+)"') or "sd1"
            local sd2_url = string.match(raw, '"sd2_url"%s*:%s*"([^"]+)"')
            local sd2_proc = string.match(raw, '"sd2_process"%s*:%s*"([^"]+)"') or "sd2"
            local arbit_url = string.match(raw, '"arbit_url"%s*:%s*"([^"]+)"')
            local epoch_str = string.match(raw, '"epoch"%s*:%s*(%d+)') or "1"
            local max_entities_str = string.match(raw, '"primary_max_entities"%s*:%s*(%d+)')
            local audit_ring_str = string.match(raw, '"primary_audit_ring"%s*:%s*(%d+)')

            if max_entities_str then MaxEntities = tonumber(max_entities_str) or MaxEntities end
            if audit_ring_str then MaxAuditRing = tonumber(audit_ring_str) or MaxAuditRing end

            if sd1_url then
                local my_role = tostring(process and process["node-role"] or "primary")
                local worker_suffix = string.match(my_role, "_(%d+)$")
                local target_sd1_proc = worker_suffix and ("sd1_" .. worker_suffix) or sd1_proc
                local target_sd2_proc = worker_suffix and ("sd2_" .. worker_suffix) or sd2_proc
                local role_sd1 = worker_suffix and ("sd1_" .. worker_suffix) or "sd1"
                local role_sd2 = worker_suffix and ("sd2_" .. worker_suffix) or "sd2"
                Topology = {
                    genesis_url = GenesisUrl,
                    epoch = tonumber(epoch_str) or 1,
                    arbit_url = arbit_url,
                    sd_targets = { { url = sd1_url, process = target_sd1_proc, role = role_sd1 } }
                }
                if sd2_url then
                    table.insert(Topology.sd_targets, { url = sd2_url, process = target_sd2_proc, role = role_sd2 })
                end
            end
        end
    end

    -- Cold start: recover the authoritative state from whichever replica (SD1 or duplicate SD2)
    -- reports the highest confirmed tick before accepting input.
    if not Hydrated and Topology and Topology.sd_targets and #Topology.sd_targets > 0 and Tick == 0 then
        Hydrated = true
        hydrate_from_latest_replica()
    end
    return Topology
end

-- ==============================================================================
-- Commit path: verify a REAL replica response, never a local guess
-- ==============================================================================
local function verify_sd_response(raw, batch_id, expected_root, expected_tick)
    if not raw then return false, nil, "no_response" end
    local status = string.match(raw, '"status"%s*:%s*"([^"]+)"')
    local echo_batch = string.match(raw, '"batch_id"%s*:%s*"([^"]*)"')
    local root = string.match(raw, '"state_root"%s*:%s*"(0x%x+)"')
    local tick = tonumber(string.match(raw, '"last_tick"%s*:%s*(%d+)') or string.match(raw, '"tick"%s*:%s*(%d+)'))
    local signer = string.match(raw, '"sd_signer"%s*:%s*"([^"]+)"')
    local ok = (status == "committed")
        and (echo_batch == batch_id)
        and (root == expected_root)
        and (tick ~= nil and tick == expected_tick)
    return ok, { tick = tick, root = root, signer = signer, status = status, raw = raw }
end

local function make_batch_id(seq_from, seq_to, tick_from, digest)
    return string.format("b%d-%d-%d-%s", tick_from, seq_from, seq_to, string.sub(digest, 3, 18))
end

-- A batch gets its id when the queue opens, not when it closes, so the first
-- `202 accepted` already carries the id the client can poll while the window
-- is still filling. `start_batch` reuses it (OpenBatchId).
local function make_open_batch_id(seq)
    local digest = sha256_hex("open|" .. int_str(Tick) .. "|" .. int_str(seq) .. "|" .. int_str(RequestClock))
    return string.format("b%d-%d-%s", Tick, seq, string.sub(digest, 3, 18))
end

-- Canonical out-of-order tick sorter (Pilar 2):
-- Orders incoming multi-tick / multi-client inputs deterministically by
-- (target_tick, client_ts, entity_id, arrival_seq).
local function sort_queue_canonical(queue)
    local sorted = {}
    for i = 1, #queue do sorted[i] = queue[i] end
    table.sort(sorted, function(a, b)
        local ta = tonumber(a.target_tick) or 0
        local tb = tonumber(b.target_tick) or 0
        if ta ~= tb then return ta < tb end
        local tsa = tonumber(a.ts) or 0
        local tsb = tonumber(b.ts) or 0
        if tsa ~= tsb then return tsa < tsb end
        local ida = tostring(a.id or "")
        local idb = tostring(b.id or "")
        if ida ~= idb then return ida < idb end
        return (tonumber(a.seq) or 0) < (tonumber(b.seq) or 0)
    end)
    return sorted
end

-- Build the delta map (last write wins per entity + monotonic entity tick guard)
-- from the open queue. Stale out-of-order ticks for an entity that already has
-- a newer target_tick are ignored so entities never rubberband backward.
local function build_delta(queue, tick_to, commit_monotonic)
    local sorted = sort_queue_canonical(queue)
    local delta, order = {}, {}
    local temp_last_tick = {}
    for _, item in ipairs(sorted) do
        local id = item.id
        local item_tick = tonumber(item.target_tick) or 0
        local min_allowed = temp_last_tick[id] or EntityLastTick[id] or 0
        if item_tick > 0 and item_tick < min_allowed then
            if commit_monotonic then
                OutOfOrderDropped = (OutOfOrderDropped or 0) + 1
            end
        else
            if item_tick > min_allowed then
                temp_last_tick[id] = item_tick
            end
            if delta[id] == nil then
                table.insert(order, id)
            end
            delta[id] = pack_entity(item.x, item.y, tick_to, item.owner)
        end
    end
    -- Fallback if all items were filtered out: preserve existing entity coords so stale ticks never rubberband
    if #order == 0 and #sorted > 0 then
        local last = sorted[#sorted]
        local existing_e = Points[last.id] and unpack_entity(Points[last.id])
        table.insert(order, last.id)
        if existing_e then
            delta[last.id] = pack_entity(existing_e.x, existing_e.y, tick_to, existing_e.owner)
        else
            delta[last.id] = pack_entity(last.x, last.y, tick_to, last.owner)
        end
    end
    if commit_monotonic then
        for id, t in pairs(temp_last_tick) do
            EntityLastTick[id] = t
        end
    end
    return delta, order
end

local function encode_delta(delta, order, include_tick)
    local items = {}
    for i, id in ipairs(order) do
        local e = unpack_entity(delta[id])
        if include_tick then
            items[i] = string.format(
                '["%s",%.6f,%.6f,"%s",%d]',
                json_safe(id), e.x, e.y, json_safe(e.owner), e.tick
            )
        else
            items[i] = string.format(
                '["%s",%.6f,%.6f,"%s"]',
                json_safe(id), e.x, e.y, json_safe(e.owner)
            )
        end
    end
    return "[" .. table.concat(items, ",") .. "]"
end

-- Sync committed delta to native BEAM C-ETS (hyperstack@1.0) for <0.01ms lookups
local function sync_ets_delta(delta, order, tick_to)
    if ao and ao.ets_put then
        for _, id in ipairs(order) do
            local e = unpack_entity(delta[id])
            if e then
                ao.ets_put("primary", id, e.x, e.y, e.tick, e.owner or "anonymous")
            end
        end
    end
end

local function pending_count()
    local n = 0
    for _ in pairs(PendingBatches) do n = n + 1 end
    return n
end

local function first_pending()
    for _, p in pairs(PendingBatches) do
        if not p.done then return p end
    end
    return nil
end

-- Recompute optimistic Zero-Wait Local Head (HeadTick, HeadRoot)
-- without mutating the verified Replicated Watermark (Tick, StateRoot, Points).
-- Lightweight: skips expensive shard-merkle recomputation and table cloning on hot path.
local function recompute_head()
    local in_flight = first_pending()
    local base_tick = Tick
    if in_flight and not in_flight.done then
        base_tick = in_flight.tick_to
    end
    if #Queue > 0 then
        local span = TicksPerBatch
        HeadTick = base_tick + span
        HeadRoot = in_flight and in_flight.root or StateRoot
    elseif in_flight and not in_flight.done then
        HeadTick = in_flight.tick_to
        HeadRoot = in_flight.root
    else
        HeadTick = Tick
        HeadRoot = StateRoot
    end
end

-- Lazy sync of HeadPoints only when serialized inspection is needed
local function sync_head_points()
    HeadPoints = {}
    for id, packed in pairs(Points) do HeadPoints[id] = packed end
    local in_flight = first_pending()
    if in_flight and not in_flight.done then
        for id, packed in pairs(in_flight.delta or {}) do
            HeadPoints[id] = packed
        end
    end
    if #Queue > 0 then
        local q_delta = build_delta(Queue, HeadTick or Tick, false)
        for id, packed in pairs(q_delta) do
            HeadPoints[id] = packed
        end
    end
end

local function commit_payload(rec)
    return string.format(
        '{"action":"Commit","batch_id":"%s","tick_from":%d,"tick_to":%d,"state_root":"%s","delta":%s,"epoch":%d}',
        json_safe(rec.batch_id), rec.tick_from, rec.tick_to, rec.root,
        encode_delta(rec.delta, rec.order, rec.compacted), (Topology and Topology.epoch) or 1
    )
end

local function cast_commit_to(rec, targets)
    if not targets or #targets == 0 then return end
    local payload = commit_payload(rec)
    for _, target in ipairs(targets) do
        relay_cast(sd_endpoint(target, "push"), payload)
    end
end

local function cast_commit(rec)
    cast_commit_to(rec, Topology and Topology.sd_targets)
end

-- Apply a batch that a real replica has confirmed. Called exactly once.
local function acknowledge_batch(p, role)
    for id, packed in pairs(p.delta) do
        if Points[id] == nil then
            ActiveEntityCount = (ActiveEntityCount or 0) + 1
        end
        Points[id] = packed
    end
    Tick = p.tick_to
    StateRoot = commit_shards(Shards, ShardHashes, p.delta, p.tick_to)
    sync_ets_delta(p.delta, p.order, p.tick_to)
    for id in pairs(p.delta) do
        local e = unpack_entity(p.delta[id])
        if e then append_audit(p.tick_to, id, e.x, e.y) end
    end
    p.done = true
    p.status = "confirmed"
    AppliedInputs = (AppliedInputs or 0) + (p.inputs or 0)
    p.acked_by = p.acked_by or {}
    table.insert(p.acked_by, role or "replica")
    LastBatch = { batch_id = p.batch_id, tick = p.tick_to, root = p.root, inputs = p.inputs }

    -- Keep the confirmed batch around until every configured replica has
    -- confirmed it too, so a replica that was down catches up from the delta
    -- log instead of needing a full state re-sync.
    local rec = {
        batch_id = p.batch_id, tick_from = p.tick_from, tick_to = p.tick_to,
        root = p.root, delta = p.delta, order = p.order,
        acked = {}, cast_at = RequestClock, attempts = 0
    }
    rec.acked[role or "replica"] = true
    table.insert(RepairBatches, rec)
    while #RepairBatches > MaxRepairBatches do
        table.remove(RepairBatches, 1)
    end

    -- Hot backup: the replica that confirmed the batch (SD1 in the 2-of-3
    -- quorum) is not the only copy. Push the same delta to every replica that
    -- has not acked yet (and wasn't already cast by pump_pending), immediately,
    -- so a hot standby converges in one hop without sending duplicate Commits.
    if Topology and Topology.sd_targets and (p.cast_stage or 0) < #Topology.sd_targets then
        local lagging = {}
        for _, t in ipairs(Topology.sd_targets) do
            local key = t.role or t.process
            if key ~= (role or "replica") and not rec.acked[key] then
                lagging[#lagging + 1] = t
            end
        end
        if #lagging > 0 then
            cast_commit_to(rec, lagging)
            rec.cast_at = RequestClock
        end
    end

    recompute_head()
    local lag = math.max(0, (HeadTick or Tick) - Tick)
    local n_ent = count_entities()
    local points_ext = ""
    if n_ent <= 250 or (p and p.include_points) then
        sync_head_points()
        points_ext = string.format(',"points":%s,"head_points":%s', encode_points(Points), encode_points(HeadPoints))
    end
    local body = string.format(
        '{"status":"ok","ack":"replicated","batch_id":"%s","tick":%d,"head_tick":%d,"replicated_tick":%d,"replication_lag":%d,"state_root":"%s","head_root":"%s","replicated_root":"%s","replicas":%s,"sd_ack":true,"sd_acked_nodes":%s,"sd_confirmed_tick":%d,"sd_persistence":"lmdb_disk","active_entity_count":%d,"queued":%d,"batch_inputs":%d,"compacted_total":%d,"out_of_order_dropped":%d,"audit_ring_size":%d%s%s}',
        json_safe(p.batch_id), Tick, HeadTick or Tick, Tick, lag, StateRoot, HeadRoot or StateRoot, StateRoot,
        encode_string_list(p.acked_by), encode_string_list(p.acked_by),
        p.tick_to, n_ent, #Queue, p.inputs, CompactedCount or 0, OutOfOrderDropped or 0, #TickAuditRing,
        points_ext, p.suffix or ""
    )
    remember_batch(p.batch_id, body)
    for _, entry in ipairs(p.entries) do
        if entry.key and SeenInputs[entry.key] then
            SeenInputs[entry.key].batch_id = p.batch_id
        end
    end
    return body
end

-- Re-cast an unresolved batch (lost Ack, or a replica that came back online).
-- `cast` is fire-and-forget: a dead peer can never turn this into a failure.
local function resend_batch(p, rearm, now_ts)
    -- A client retry re-arms the reporting window (the batch was just
    -- re-cast); the background pump does not, so a dead quorum keeps being
    -- reported as 503 while it is still being retried in the background.
    if rearm then
        p.expired = false
        p.deadline = RequestClock + AckDeadlineRequests
        p.created_ts = now_ts or p.created_ts
    end
    if not (Topology and Topology.sd_targets) then return end
    -- An explicit client retry is a signal to re-send now, even if the
    -- background pump sent this batch a few requests ago; the background pump
    -- itself stays rate limited so a dead quorum (or busy SD) is not hammered.
    if not rearm then
        if p.last_sent and (RequestClock - p.last_sent) < 8 then return end
        if now_ts and p.last_sent_ts and now_ts > 1000000000000 and p.last_sent_ts > 1000000000000 then
            if (now_ts - p.last_sent_ts) < 900 then return end
        end
    end
    p.last_sent = RequestClock
    p.last_sent_ts = now_ts or p.last_sent_ts
    p.cast_stage = #Topology.sd_targets
    cast_commit(p)
end

local function expire_pending(now_ts)
    for batch_id, p in pairs(PendingBatches) do
        if p.done then
            PendingBatches[batch_id] = nil
        elseif not p.expired and RequestClock >= (p.deadline or 0) then
            local ts_ok = true
            if now_ts and p.created_ts and now_ts > 1000000000000 and p.created_ts > 1000000000000 then
                ts_ok = (now_ts - p.created_ts) >= 850
            end
            if ts_ok then
                -- No verified Ack yet. Do NOT discard the batch and do NOT apply
                -- it: it stays the only batch in flight until a replica confirms
                -- it (possibly after a client retry re-casts the same batch_id).
                p.expired = true
            end
        end
    end
end

-- Rate-limited retry pump: a poll or a new write also re-casts an unresolved
-- batch, so a replica that comes back online heals without client action.
local function pump_pending(now_ts)
    if not (Topology and Topology.sd_targets) then return end
    local targets = Topology.sd_targets
    for _, p in pairs(PendingBatches) do
        if not p.done then
            if p.expired then
                resend_batch(p, false, now_ts)
            elseif (p.cast_stage or 0) < #targets and RequestClock >= (p.next_cast or 0) then
                -- First replica was cast on the write path; the remaining
                -- replicas are cast here so the client's 202 stays cheap.
                local rest = {}
                for i = (p.cast_stage or 0) + 1, #targets do
                    rest[#rest + 1] = targets[i]
                end
                cast_commit_to(p, rest)
                p.cast_stage = #targets
            end
        end
    end
end

-- Every configured replica must confirm every committed batch. When one is
-- missing (it was down), replay the delta log to it, oldest first, so it can
-- catch up without a full state transfer.
local function missing_replicas(rec)
    if not (Topology and Topology.sd_targets) then return 0 end
    local n = 0
    for _, t in ipairs(Topology.sd_targets) do
        if not rec.acked[t.role or t.process] then n = n + 1 end
    end
    return n
end

local function missing_signature(rec)
    if not (Topology and Topology.sd_targets) then return "" end
    local parts = {}
    for _, t in ipairs(Topology.sd_targets) do
        local k = t.role or t.process
        if not rec.acked[k] then parts[#parts + 1] = k end
    end
    return table.concat(parts, ",")
end

-- Delta Compaction for lagging replicas (Pilar 4):
-- When multiple consecutive RepairBatches are waiting for the same lagging
-- replica(s), coalesce them into 1 Super-Delta (tick_from -> tick_to) so the
-- replica catches up in 1 single RTT instead of replaying batch by batch.
local function compact_repair_batches()
    if #RepairBatches <= 1 then return end
    local compacted = {}
    for i = 1, #RepairBatches do
        local rec = RepairBatches[i]
        if rec and missing_replicas(rec) > 0 then
            local prev = compacted[#compacted]
            if prev and prev.tick_to == rec.tick_from and missing_signature(prev) == missing_signature(rec) then
                for _, id in ipairs(rec.order) do
                    if prev.delta[id] == nil then
                        table.insert(prev.order, id)
                    end
                    prev.delta[id] = rec.delta[id]
                end
                prev.tick_to = rec.tick_to
                prev.root = rec.root
                prev.batch_id = rec.batch_id
                prev.compacted = true
                CompactedCount = (CompactedCount or 0) + 1
            else
                compacted[#compacted + 1] = rec
            end
        end
    end
    RepairBatches = compacted
end

local function pump_repair()
    if not (Topology and Topology.sd_targets) then return end
    compact_repair_batches()
    local sent = false
    for i = 1, #RepairBatches do
        local rec = RepairBatches[i]
        local missing = 0
        for _, t in ipairs(Topology.sd_targets) do
            if not rec.acked[t.role or t.process] then missing = missing + 1 end
        end
        if missing == 0 then
            RepairBatches[i] = false
        elseif not sent and rec.attempts < 64
            and (RequestClock - rec.cast_at) >= RepairInterval then
            -- One repair round per request at most, and only to the replicas
            -- that have not confirmed: a cast costs real CPU inside luerl.
            rec.cast_at = RequestClock
            rec.attempts = rec.attempts + 1
            local payload = commit_payload(rec)
            for _, t in ipairs(Topology.sd_targets) do
                if not rec.acked[t.role or t.process] then
                    relay_cast(sd_endpoint(t, "push"), payload)
                end
            end
            sent = true
        end
    end
    local kept = {}
    for _, rec in ipairs(RepairBatches) do
        if rec then kept[#kept + 1] = rec end
    end
    RepairBatches = kept
end

-- Body that reports an unresolved batch without ever claiming replication.
-- Note: never include the substring "replicated" in a 202 body (contract test #1).
local function pending_body(p, suffix, status)
    recompute_head()
    local lag = math.max(0, (HeadTick or Tick) - Tick)
    return string.format(
        '{"status":"%s","ack":"accepted","batch_id":"%s","tick":%d,"head_tick":%d,"confirmed_tick":%d,"lag_ticks":%d,"tick_from":%d,"tick_to":%d,"state_root":"%s","head_root":"%s","target_root":"%s","queued":%d,"compacted_total":%d,"retryable":true%s}',
        status or "pending", json_safe(p.batch_id), Tick, HeadTick or p.tick_to, Tick, lag,
        p.tick_from, p.tick_to, StateRoot, HeadRoot or p.root, p.root, #Queue, CompactedCount or 0, suffix or ""
    )
end

-- Open a batch: compute the candidate root, cast it to every replica, and tell
-- the caller it is accepted but NOT yet replicated.
-- Includes Delta Compaction: if multiple ticks/writes piled up in Queue while
-- the previous batch was in flight, all queued updates are coalesced into 1 batch.
local function start_batch(process, suffix)
    local in_flight = first_pending()

    if #Queue == 0 then
        if in_flight then
            -- A batch is still unresolved (replicas were down). Never answer
            -- 200 here: re-cast the SAME batch_id and report it as retrying.
            resend_batch(in_flight, true)
            return { http = 202, body = pending_body(in_flight, suffix, "retrying") }
        end
        recompute_head()
        return {
            http = 200,
            body = string.format(
                '{"status":"ok","ack":"replicated","batch_id":null,"tick":%d,"head_tick":%d,"replicated_tick":%d,"replication_lag":0,"state_root":"%s","head_root":"%s","replicated_root":"%s","active_entity_count":%d,"queued":0,"noop":true%s}',
                Tick, Tick, Tick, StateRoot, StateRoot, StateRoot, count_entities(), suffix or ""
            )
        }
    end

    if in_flight then
        recompute_head()
        local lag = math.max(0, (HeadTick or Tick) - Tick)
        local q_batch_id = OpenBatchId or in_flight.batch_id
        return {
            http = 202,
            body = string.format(
                '{"status":"queued","ack":"accepted","batch_id":"%s","pending_batch":"%s","queued":%d,"tick":%d,"head_tick":%d,"tick_to":%d,"confirmed_tick":%d,"lag_ticks":%d,"state_root":"%s","head_root":"%s"%s}',
                json_safe(q_batch_id), json_safe(in_flight.batch_id), #Queue, Tick,
                HeadTick or Tick, HeadTick or Tick, Tick, lag, StateRoot, HeadRoot or StateRoot, suffix or ""
            )
        }
    end

    local tick_from = Tick
    local batch_entries = Queue
    local span = TicksPerBatch
    local tick_to = Tick + span
    local seq_from = batch_entries[1].seq
    local seq_to = batch_entries[#batch_entries].seq
    if #batch_entries > 1 then
        CompactedCount = (CompactedCount or 0) + (#batch_entries - 1)
    end
    local delta, order = build_delta(batch_entries, tick_to, true)
    local digest = sha256_hex(
        table.concat(order, ",") .. "|" .. int_str(seq_from) .. "|" .. int_str(seq_to) .. "|" .. int_str(tick_from)
    )
    local batch_id = OpenBatchId or make_batch_id(seq_from, seq_to, tick_from, digest)
    local new_root = candidate_root(Shards, ShardHashes, delta, tick_to)

    local batch = {
        batch_id = batch_id,
        tick_from = tick_from,
        tick_to = tick_to,
        root = new_root,
        delta = delta,
        order = order,
        entries = batch_entries,
        inputs = seq_to - seq_from + 1,
        created = RequestClock,
        created_ts = BatchOpenedTs,
        deadline = RequestClock + AckDeadlineRequests,
        last_sent = RequestClock,
        last_sent_ts = BatchOpenedTs,
        done = false,
        status = "pending",
        suffix = suffix or ""
    }
    PendingBatches[batch_id] = batch
    -- Bind the idempotency keys to the batch as soon as it exists, not when it
    -- is confirmed: a retry must be able to find this batch while it is still
    -- in flight (or expired).
    for _, entry in ipairs(batch_entries) do
        if entry.key and SeenInputs[entry.key] then
            SeenInputs[entry.key].batch_id = batch_id
        end
    end
    while pending_count() > MaxPendingBatches do
        local oldest_id, oldest = nil, nil
        for id, p in pairs(PendingBatches) do
            if not oldest or (p.created or 0) < (oldest.created or 0) then
                oldest_id, oldest = id, p
            end
        end
        if oldest_id then PendingBatches[oldest_id] = nil end
    end

    -- Cast the first replica on the write path (the client latency path), and
    -- the remaining replicas on the next request: a relay/cast costs real CPU
    -- inside luerl, so paying for both here would double the 202 latency.
    batch.cast_stage = 0
    batch.next_cast = RequestClock + 2
    if Topology and Topology.sd_targets and #Topology.sd_targets > 0 then
        cast_commit_to(batch, { Topology.sd_targets[1] })
        batch.cast_stage = 1
    end

    Queue = {}
    OpenBatchId = nil
    BatchOpenedTs = nil
    recompute_head()
    local lag = math.max(0, (HeadTick or tick_to) - Tick)

    return {
        http = 202,
        body = string.format(
            '{"status":"committing","ack":"accepted","batch_id":"%s","tick":%d,"head_tick":%d,"confirmed_tick":%d,"lag_ticks":%d,"tick_from":%d,"tick_to":%d,"state_root":"%s","head_root":"%s","target_root":"%s","delta_count":%d,"compacted_total":%d,"queued":0,"replicas_expected":%d%s}',
            json_safe(batch_id), Tick, HeadTick or tick_to, Tick, lag, tick_from, tick_to,
            StateRoot, HeadRoot or new_root, new_root, #order, CompactedCount or 0,
            (Topology and Topology.sd_targets and #Topology.sd_targets) or 0, suffix or ""
        )
    }
end

-- Verify a replica's Ack against the batch it claims to confirm.
local function handle_ack(msg, suffix)
    local batch_id = msg.batch_id and tostring(msg.batch_id) or ""
    local p = PendingBatches[batch_id]
    local claimed_root = msg.state_root or (msg.query and msg.query.state_root)
    local claimed_tick = tonumber(msg.tick or (msg.query and msg.query.tick))
    local role = msg.role or msg.sd_signer or "replica"

    -- A late Ack for an already-committed batch still counts for repair.
    for _, rec in ipairs(RepairBatches) do
        if rec.batch_id == batch_id then
            rec.acked[role] = true
            break
        end
    end

    if not p then
        local cached = CommittedBatches[batch_id]
        if cached then
            return 200, cached
        end
        return 200, string.format(
            '{"status":"ignored","reason":"unknown_batch","batch_id":"%s","tick":%d,"state_root":"%s"}',
            json_safe(batch_id), Tick, StateRoot
        )
    end
    if p.done then
        return 200, string.format(
            '{"status":"duplicate_ack","batch_id":"%s","tick":%d,"state_root":"%s"}',
            json_safe(batch_id), Tick, StateRoot
        )
    end
    if claimed_root ~= p.root or claimed_tick ~= p.tick_to then
        return 409, string.format(
            '{"status":"ack_mismatch","batch_id":"%s","claimed_root":"%s","expected_root":"%s","claimed_tick":%s,"expected_tick":%d,"tick":%d,"state_root":"%s"}',
            json_safe(batch_id), json_safe(claimed_root), p.root,
            claimed_tick and int_str(claimed_tick) or "null", p.tick_to, Tick, StateRoot
        )
    end
    -- Epoch fencing: an Ack from a different epoch is not part of this quorum
    -- set, so it can never complete a batch (split-brain guard). Older replicas
    -- that omit the field are still accepted; the real SD always sends it.
    local claimed_epoch = tonumber(msg.epoch or (msg.query and msg.query.epoch))
    local expected_epoch = Topology and tonumber(Topology.epoch) or nil
    if claimed_epoch ~= nil and expected_epoch ~= nil and claimed_epoch ~= expected_epoch then
        return 409, string.format(
            '{"status":"epoch_mismatch","batch_id":"%s","claimed_epoch":%d,"expected_epoch":%d,"tick":%d,"state_root":"%s"}',
            json_safe(batch_id), claimed_epoch, expected_epoch, Tick, StateRoot
        )
    end

    acknowledge_batch(p, role)
    PendingBatches[batch_id] = nil
    -- Keep the pipeline moving: a queued batch may start as soon as the
    -- previous one is durable on a replica.
    local next_http = 0
    if #Queue > 0 then
        local next_result = start_batch(nil, suffix)
        next_http = next_result.http or 0
    end
    recompute_head()
    local lag = math.max(0, (HeadTick or Tick) - Tick)
    local n_ent = count_entities()
    local points_ext = ""
    if n_ent <= 250 or (p and p.include_points) then
        sync_head_points()
        points_ext = string.format(',"points":%s,"head_points":%s', encode_points(Points), encode_points(HeadPoints))
    end
    local res = string.format(
        '{"status":"acked","ack":"replicated","batch_id":"%s","tick":%d,"head_tick":%d,"replicated_tick":%d,"replication_lag":%d,"state_root":"%s","head_root":"%s","replicated_root":"%s","active_entity_count":%d,"compacted_total":%d,"out_of_order_dropped":%d,"next_http":%d,"pending":%d%s%s}',
        json_safe(batch_id), Tick, HeadTick or Tick, Tick, lag, StateRoot, HeadRoot or StateRoot, StateRoot,
        n_ent, CompactedCount or 0, OutOfOrderDropped or 0, next_http, #Queue, points_ext, suffix or ""
    )
    return 200, res
end


local function should_close_batch(msg, engine_ts)
    if #Queue >= MaxBatchInputs then return true end
    if type(msg) == "table" and (msg.flush == true or msg.flush == "true" or msg.advance == true) then
        return true
    end
    -- The frame window is measured on the scheduler's clock (the assignment
    -- timestamp), never on a client-supplied value: inputs from clients with
    -- different clocks land in the same frame purely by arrival order.
    if engine_ts and BatchOpenedTs and (engine_ts - BatchOpenedTs) >= BatchWindowMs then
        return true
    end
    return false
end

-- Scheduler-assigned time of the current slot (ms). It is part of the signed
-- assignment, so a replay of the schedule sees the exact same value.
local function engine_clock(message)
    if type(message) ~= "table" then return nil end
    return tonumber(message.timestamp)
end

-- ==============================================================================
local function enqueue_or_coalesce(entry, engine_ts, explicit_batch_id)
    if #Queue == 0 then
        BatchOpenedTs = engine_ts or tonumber(entry.ts)
        if explicit_batch_id then
            OpenBatchId = tostring(explicit_batch_id)
        else
            OpenBatchId = make_open_batch_id(entry.seq)
        end
    end
    if entry.id then
        for i = #Queue, 1, -1 do
            local q = Queue[i]
            if q.id == entry.id then
                local qt = tonumber(q.target_tick) or 0
                local et = tonumber(entry.target_tick) or 0
                if et > 0 and qt > 0 and et < qt then
                    -- Out-of-order older tick: keep both so build_delta drops it
                    -- and increments OutOfOrderDropped (Pilar 2 contract).
                    table.insert(Queue, entry)
                    return
                end
                -- Newer or equal tick for the same entity in the open Queue:
                -- coalesce in-place so Queue never overflows during Swarm!
                Queue[i] = entry
                CompactedCount = (CompactedCount or 0) + 1
                return
            end
        end
    end
    table.insert(Queue, entry)
end

local function circuit_breaker_tripped(in_flight, msg, now_ts)
    if not (in_flight and in_flight.expired) then return false end
    if msg and msg.circuit_breaker == true then return true end
    if #Queue < MaxLagInputs then return false end
    if now_ts and in_flight.created_ts and now_ts > 1000000000000 and in_flight.created_ts > 1000000000000 then
        if (now_ts - in_flight.created_ts) < 2000 then
            return false
        end
    end
    return true
end

-- ==============================================================================
-- Main entrypoint
-- ==============================================================================
function compute(process, message, opts)
    local msg = message.body or message
    if type(msg) ~= "table" then msg = {} end
    local action = msg.action or msg.Action or (msg.query and msg.query.action) or "Read-All"
    RequestClock = RequestClock + 1
    local engine_ts = engine_clock(message) or tonumber(msg.ts)

    if action == "Sync-Topology" then
        Topology = nil
        Hydrated = false
    end
    local topo = ensure_topology(process, msg)
    -- Lazily resolve batches whose Ack deadline has passed. No timer is
    -- available inside the sandbox, so any request acts as the clock tick.
    expire_pending(engine_ts)
    pump_pending(engine_ts)
    pump_repair()
    local slot_signer = NodeSigner or extract_first_signer(process) or "primary_authority"
    local slot_id = extract_msg_id(message)
    local genesis_src = (topo and topo.genesis_url) or tostring(GenesisUrl or "")

    local http_status = 200
    local res = ""

    local function status_suffix()
        return string.format(
            ',"tier":"primary_ram","primary_signer":"%s","assignment_id":"%s","topology_source":"%s","sd_targets":%s,"max_entities":%d,"max_payload_bytes":%d',
            json_safe(slot_signer), json_safe(slot_id), json_safe(genesis_src),
            encode_sd_urls(topo), MaxEntities, MaxPayloadBytes
        )
    end

    if action == "Input" or action == "Write-Point" or action == "Write-Batch" or action == "Advance" then
        local force = (action == "Advance") or msg.flush == true or msg.flush == "true" or (action == "Write-Batch" and msg.flush ~= false and msg.flush ~= "false")
        local blocked = false
        local duplicate = false

        if action == "Write-Batch" or type(msg.packed_inputs) == "string" or (type(msg.inputs) == "table" and #msg.inputs > 0) then
            local batch_list = msg.inputs or (msg.query and msg.query.inputs) or {}
            local packed_str = msg.packed_inputs or msg["packed-inputs"] or (msg.query and (msg.query.packed_inputs or msg.query["packed-inputs"]))
            if type(packed_str) == "string" and #packed_str > 0 then
                batch_list = {}
                local p_init = 1
                while true do
                    local s, e, pid, px, py, ptick, pts, powner = string.find(
                        packed_str,
                        "([^|;]+)|([%d%.%-]+)|([%d%.%-]+)|([%d%.%-]+)|([%d%.%-]+)|([^;|]+)",
                        p_init
                    )
                    if not s then break end
                    batch_list[#batch_list + 1] = {
                        id = pid,
                        x = tonumber(px),
                        y = tonumber(py),
                        target_tick = tonumber(ptick),
                        ts = tonumber(pts),
                        owner = powner
                    }
                    p_init = e + 1
                end
            end
            local in_flight = first_pending()
            if message_too_large(message) then
                http_status = 413
                res = string.format(
                    '{"status":"error_payload_too_large","ack":"none","tick":%d,"state_root":"%s"%s}',
                    Tick, StateRoot, status_suffix()
                )
                blocked = true
            elseif circuit_breaker_tripped(in_flight, msg, engine_ts) then
                http_status = 503
                res = string.format(
                    '{"status":"unavailable","ack":"none","error":"quorum_unavailable","reason":"max_lag_circuit_breaker","tick":%d,"head_tick":%d,"lag_ticks":%d,"state_root":"%s"%s}',
                    Tick, HeadTick or Tick, math.max(0, (HeadTick or Tick) - Tick), StateRoot, status_suffix()
                )
                blocked = true
            elseif #Queue + #batch_list > MaxQueue then
                http_status = 503
                res = string.format(
                    '{"status":"error_queue_full","ack":"none","tick":%d,"state_root":"%s"%s}',
                    Tick, StateRoot, status_suffix()
                )
                blocked = true
            else
                if ActiveEntityCount == nil then ActiveEntityCount = count_entities() end
                local default_signer = extract_first_signer(msg) or msg.client_signer or "anonymous"
                for _, item in ipairs(batch_list) do
                    if type(item) == "table" then
                        local iid = item.id and tostring(item.id) or nil
                        local ix = tonumber(item.x)
                        local iy = tonumber(item.y)
                        local itick = tonumber(item.target_tick or item.tick)
                        local its = tonumber(item.ts or msg.ts)
                        local iowner = tostring(item.owner or item.client_signer or default_signer)
                        local existing = iid and Points[iid]
                        local existing_entity = existing and unpack_entity(existing)
                        local owner_ok = not (existing_entity and existing_entity.owner and existing_entity.owner ~= "anonymous" and existing_entity.owner ~= iowner)
                        if iid and #iid > 0 and #iid <= MaxIdLength and not contains_newlines(iid)
                            and ix and iy and math.abs(ix) <= 1000 and math.abs(iy) <= 1000 and owner_ok then
                            NextSeq = NextSeq + 1
                            local seq = NextSeq
                            enqueue_or_coalesce({
                                seq = seq,
                                id = iid,
                                x = ix,
                                y = iy,
                                target_tick = itick,
                                owner = iowner,
                                ts = its
                            }, engine_ts, msg.batch_id)
                        end
                    end
                end
            end
        elseif action ~= "Advance" then
            local id = msg.id or (msg.query and msg.query.id)
            local raw_x = msg.x or (msg.query and msg.query.x) or 0
            local raw_y = msg.y or (msg.query and msg.query.y) or 0
            local x = tonumber(raw_x)
            local y = tonumber(raw_y)
            local target_tick = tonumber(msg.target_tick or msg.client_tick or (msg.query and msg.query.target_tick))
            local id_str = id and tostring(id) or nil

            if ActiveEntityCount == nil then
                ActiveEntityCount = count_entities()
            end

            -- Cryptographic signer first; body string is only a local-dev fallback.
            local client_signer = extract_first_signer(msg) or msg.client_signer or (msg.query and msg.query.client_signer)
            local existing = id_str and Points[id_str]
            local existing_entity = existing and unpack_entity(existing)
            local candidate_owner = client_signer or (existing_entity and existing_entity.owner) or "anonymous"

            local guard_error, guard_http = nil, 400
            if message_too_large(message) then
                guard_error = "payload_too_large"
                guard_http = 413
            elseif id_str and (#id_str == 0 or #id_str > MaxIdLength or contains_newlines(id_str)) then
                guard_error = "invalid_entity_id"
            elseif x == nil or y == nil or x ~= x or y ~= y or math.abs(x) > 1000 or math.abs(y) > 1000 then
                guard_error = "invalid_coordinates"
            elseif existing_entity and existing_entity.owner and existing_entity.owner ~= "anonymous" and existing_entity.owner ~= client_signer then
                guard_error = "unauthorized_owner"
                guard_http = 403
            elseif (id_str == nil or Points[id_str] == nil) and ActiveEntityCount >= MaxEntities then
                guard_error = "entity_limit_reached"
                guard_http = 409
            elseif #Queue >= MaxQueue then
                guard_error = "queue_full"
                guard_http = 503
            end

            -- A retried write re-sends the same client timestamp. Without an
            -- explicit key we derive one from the write itself, so pressing the
            -- same button twice (same ts) can never double-apply, while a real
            -- later write (new ts) is still accepted.
            local input_key = msg.idempotency_key or msg.input_id
            if not input_key and msg.ts ~= nil and id_str then
                input_key = id_str .. "|" .. int_str(x) .. "|" .. int_str(y) .. "|"
                    .. int_str(msg.ts) .. "|" .. tostring(candidate_owner)
            end
            local disposition = input_key and SeenInputs[tostring(input_key)]

            -- Retry of an already committed batch: hand back the original ACK.
            if not guard_error and msg.batch_id and CommittedBatches[tostring(msg.batch_id)] then
                http_status = 200
                res = CommittedBatches[tostring(msg.batch_id)]
                blocked = true
            end

            -- Max-Lag Circuit Breaker: if quorum is expired and lag exceeds MaxLagInputs
            -- on a brand new (non-retry) write, reject with 503 so Primary cannot drift unbounded.
            local in_flight = first_pending()
            if not blocked and not disposition and not guard_error
                and circuit_breaker_tripped(in_flight, msg, engine_ts) then
                http_status = 503
                res = string.format(
                    '{"status":"unavailable","ack":"none","error":"quorum_unavailable","reason":"max_lag_circuit_breaker","tick":%d,"head_tick":%d,"lag_ticks":%d,"state_root":"%s"%s}',
                    Tick, HeadTick or Tick, math.max(0, (HeadTick or Tick) - Tick), StateRoot, status_suffix()
                )
                blocked = true
            end

            if guard_error then
                http_status = guard_http
                res = string.format(
                    '{"status":"error_%s","ack":"none","tick":%d,"state_root":"%s"%s}',
                    guard_error, Tick, StateRoot, status_suffix()
                )
                blocked = true
            elseif not blocked and disposition then
                -- Already seen: never enqueue twice. If that batch already
                -- committed, replay its ACK; if it is still in flight, answer
                -- with that batch_id and re-cast it when it has expired.
                duplicate = true
                local target = nil
                if disposition.batch_id then
                    if CommittedBatches[disposition.batch_id] then
                        http_status = 200
                        res = CommittedBatches[disposition.batch_id]
                        blocked = true
                    else
                        target = PendingBatches[disposition.batch_id]
                    end
                end
                if not blocked and not target then
                    -- Legacy disposition recorded before the batch opened: adopt
                    -- the single in-flight batch (there is only ever one).
                    local sole, count = nil, 0
                    for _, p in pairs(PendingBatches) do
                        if not p.done then
                            count = count + 1
                            sole = p
                        end
                    end
                    if count == 1 then
                        target = sole
                        disposition.batch_id = sole.batch_id
                    end
                end
                if not blocked and target then
                    local retrying = target.expired
                    if retrying then resend_batch(target, true) end
                    http_status = 202
                    res = pending_body(target, status_suffix(), retrying and "retrying" or "pending")
                    blocked = true
                elseif not blocked then
                    -- The batch that accepted this input no longer exists and
                    -- was never applied (e.g. after a resync): let it through.
                    SeenInputs[tostring(input_key)] = nil
                    disposition = nil
                end
            end

            if not blocked and not disposition and not guard_error then
                NextSeq = NextSeq + 1
                local seq = NextSeq
                enqueue_or_coalesce({
                    seq = seq,
                    id = id_str or ("pt_" .. tostring(seq)),
                    x = x,
                    y = y,
                    target_tick = target_tick,
                    owner = candidate_owner,
                    ts = tonumber(msg.ts),
                    key = input_key and tostring(input_key) or nil
                }, engine_ts, msg.batch_id)
                if input_key then
                    remember_input(tostring(input_key), { seq = seq, batch_id = OpenBatchId })
                end
            end
        end

        if not blocked then
            if should_close_batch(msg, engine_ts) or force then
                local committed = start_batch(process, status_suffix())
                http_status = committed.http
                res = committed.body
            elseif duplicate or #Queue > 0 then
                recompute_head()
                local lag = math.max(0, (HeadTick or Tick) - Tick)
                http_status = 202
                res = string.format(
                    '{"status":"accepted","ack":"accepted","batch_id":"%s","batch_seq":%d,"queued":%d,"tick":%d,"head_tick":%d,"confirmed_tick":%d,"lag_ticks":%d,"state_root":"%s","head_root":"%s"%s%s}',
                    json_safe(OpenBatchId or ""), #Queue > 0 and Queue[#Queue].seq or 0, #Queue,
                    Tick, HeadTick or Tick, Tick, lag, StateRoot, HeadRoot or StateRoot,
                    duplicate and ',"duplicate":true' or "", status_suffix()
                )
            end
        end
    elseif action == "Reject" then
        -- A replica says it is ahead of us (or our root is wrong). Never
        -- overwrite it: pull its state and discard the superseded batch.
        local peer_tick = tonumber(msg.last_tick or (msg.query and msg.query.last_tick))
        local peer_signer = msg.sd_signer or (msg.query and msg.query.sd_signer)
        local target = nil
        if Topology and Topology.sd_targets then
            for _, t in ipairs(Topology.sd_targets) do
                if peer_signer and (peer_signer == t.role or peer_signer == t.process) then
                    target = t
                end
            end
            target = target or Topology.sd_targets[1]
        end
        if peer_tick and peer_tick > Tick then
            local hydrated = false
            if target then
                hydrated = hydrate_from(target)
            end
            if not hydrated then
                hydrated = hydrate_from_latest_replica()
            end
            if hydrated then
                PendingBatches = {}
                Queue = {}
                OpenBatchId = nil
                BatchOpenedTs = nil
                recompute_head()
                http_status = 200
                res = string.format(
                    '{"status":"resynced","ack":"replicated","batch_id":"%s","tick":%d,"head_tick":%d,"replicated_tick":%d,"replication_lag":0,"state_root":"%s","from":"%s"%s}',
                    json_safe(msg.batch_id or ""), Tick, Tick, Tick, StateRoot,
                    json_safe(peer_signer or ""), status_suffix()
                )
            else
                http_status = 409
                res = string.format(
                    '{"status":"stale","ack":"none","batch_id":"%s","reason":"%s","tick":%d,"state_root":"%s"%s}',
                    json_safe(msg.batch_id or ""), json_safe(msg.reason or "replica_ahead"),
                    Tick, StateRoot, status_suffix()
                )
            end
        else
            http_status = 409
            res = string.format(
                '{"status":"stale","ack":"none","batch_id":"%s","reason":"%s","tick":%d,"state_root":"%s"%s}',
                json_safe(msg.batch_id or ""), json_safe(msg.reason or "replica_ahead"),
                Tick, StateRoot, status_suffix()
            )
        end
    elseif action == "Ack" then
        http_status, res = handle_ack(msg, status_suffix())
    elseif action == "Batch-Status" then
        recompute_head()
        local lag = math.max(0, (HeadTick or Tick) - Tick)
        local batch_id = msg.batch_id or (msg.query and msg.query.batch_id)
        if batch_id then
            batch_id = tostring(batch_id)
            local p = PendingBatches[batch_id]
            if p and not p.done then
                if p.expired then
                    http_status = 503
                    res = string.format(
                        '{"status":"unavailable","ack":"none","error":"quorum_unavailable","batch_id":"%s","tick":%d,"head_tick":%d,"replicated_tick":%d,"replication_lag":%d,"tick_to":%d,"state_root":"%s","expected_root":"%s","retryable":true%s}',
                        json_safe(batch_id), Tick, HeadTick or p.tick_to, Tick, lag, p.tick_to, StateRoot, p.root, status_suffix()
                    )
                else
                    http_status = 202
                    res = string.format(
                        '{"status":"pending","ack":"pending","batch_id":"%s","tick":%d,"head_tick":%d,"confirmed_tick":%d,"lag_ticks":%d,"state_root":"%s","target_tick":%d,"target_root":"%s"%s}',
                        json_safe(batch_id), Tick, HeadTick or p.tick_to, Tick, lag, StateRoot, p.tick_to, p.root, status_suffix()
                    )
                end
            elseif CommittedBatches[batch_id] then
                http_status = 200
                res = CommittedBatches[batch_id]
            elseif OpenBatchId == batch_id and #Queue > 0 then
                local in_f = first_pending()
                if in_f and in_f.expired then
                    http_status = 503
                    res = string.format(
                        '{"status":"unavailable","ack":"none","error":"quorum_unavailable","batch_id":"%s","tick":%d,"head_tick":%d,"replicated_tick":%d,"replication_lag":%d,"tick_to":%d,"state_root":"%s","expected_root":"%s","retryable":true%s}',
                        json_safe(batch_id), Tick, HeadTick or Tick, Tick, lag, HeadTick or Tick, StateRoot, HeadRoot or StateRoot, status_suffix()
                    )
                else
                    http_status = 202
                    res = string.format(
                        '{"status":"pending","ack":"pending","batch_id":"%s","tick":%d,"head_tick":%d,"confirmed_tick":%d,"lag_ticks":%d,"state_root":"%s","target_tick":%d,"target_root":"%s"%s}',
                        json_safe(batch_id), Tick, HeadTick or Tick, Tick, lag, StateRoot, HeadTick or Tick, HeadRoot or StateRoot, status_suffix()
                    )
                end
            else
                http_status = 404
                res = string.format(
                    '{"status":"unknown_batch","ack":"none","batch_id":"%s","tick":%d,"state_root":"%s"%s}',
                    json_safe(batch_id), Tick, StateRoot, status_suffix()
                )
            end
        else
            res = string.format(
                '{"status":"ok","ack":"replicated","tick":%d,"head_tick":%d,"replicated_tick":%d,"replication_lag":%d,"state_root":"%s","head_root":"%s","replicated_root":"%s","queued":%d,"batch_id":"%s","pending_batches":%d,"repair_batches":%d,"compacted_total":%d,"last_batch":%s,"active_entity_count":%d%s}',
                Tick, HeadTick or Tick, Tick, lag, StateRoot, HeadRoot or StateRoot, StateRoot, #Queue, json_safe(OpenBatchId or ""), pending_count(), #RepairBatches, CompactedCount or 0,
                LastBatch and string.format('{"batch_id":"%s","tick":%d,"root":"%s"}',
                    json_safe(LastBatch.batch_id), LastBatch.tick, json_safe(LastBatch.root)) or "null",
                count_entities(), status_suffix()
            )
        end
    elseif action == "Probe-SD" then
        -- Diagnostic: measure the real relay/call round trip to one replica.
        local which = tonumber(msg.sd or (msg.query and msg.query.sd)) or 1
        local target = Topology and Topology.sd_targets and Topology.sd_targets[which]
        if not target then
            http_status = 503
            res = '{"status":"error_no_replica","ack":"none"}'
        else
            local raw = relay_call(sd_endpoint(target, "push"), "POST", '{"action":"Hydrate"}')
            local tick_str = raw and string.match(raw, '"last_tick"%s*:%s*(%d+)')
            http_status = raw and 200 or 503
            res = string.format(
                '{"status":"%s","ack":"none","sd":%d,"role":"%s","bytes":%d,"last_tick":%d,"state_root":"%s"%s}',
                raw and "probed" or "error_unreachable", which,
                json_safe(target.role or target.process or ""), raw and #raw or 0,
                tonumber(tick_str) or 0, StateRoot, status_suffix()
            )
        end
    elseif action == "Read-Nearby" then
        local qx = tonumber(msg.x or (msg.query and msg.query.x)) or 0
        local qy = tonumber(msg.y or (msg.query and msg.query.y)) or 0
        local radius = tonumber(msg.radius or (msg.query and msg.query.radius)) or 25
        local r2 = radius * radius
        local nearby_items = {}
        for id, packed in pairs(Points) do
            local e = unpack_entity(packed)
            if e then
                local dx = e.x - qx
                local dy = e.y - qy
                if (dx * dx + dy * dy) <= r2 then
                    nearby_items[#nearby_items + 1] = string.format(
                        '{"id":"%s","x":%.6f,"y":%.6f,"tick":%d,"owner":"%s"}',
                        json_safe(id), e.x, e.y, e.tick, json_safe(e.owner)
                    )
                end
            end
        end
        recompute_head()
        local lag = math.max(0, (HeadTick or Tick) - Tick)
        local n_ent = count_entities()
        local points_ext = ""
        if n_ent <= 250 then
            sync_head_points()
            points_ext = string.format(',"points":%s,"head_points":%s', encode_points(Points), encode_points(HeadPoints))
        end
        res = string.format(
            '{"status":"ok","tick":%d,"head_tick":%d,"replicated_tick":%d,"replication_lag":%d,"state_root":"%s","count":%d,"active_entity_count":%d,"compacted_total":%d,"out_of_order_dropped":%d,"source":"%s","nearby":[%s]%s%s}',
            Tick, HeadTick or Tick, Tick, lag, StateRoot, #nearby_items, n_ent, CompactedCount or 0, OutOfOrderDropped or 0,
            used_ets and "beam_c_ets" or "lua_table",
            table.concat(nearby_items, ","), points_ext, status_suffix()
        )
    elseif action == "Snapshot" then
        -- Full state export. Used by clients for initial load and by benches.
        recompute_head()
        local lag = math.max(0, (HeadTick or Tick) - Tick)
        res = string.format(
            '{"status":"ok","ack":"replicated","tick":%d,"head_tick":%d,"replicated_tick":%d,"replication_lag":%d,"state_root":"%s","head_root":"%s","replicated_root":"%s","total":%d,"active_entity_count":%d,"points":%s%s}',
            Tick, HeadTick or Tick, Tick, lag, StateRoot, HeadRoot or StateRoot, StateRoot,
            count_entities(), count_entities(), encode_points(Points), status_suffix()
        )
    elseif action == "Read-Point" then
        local id = msg.id or (msg.query and msg.query.id)
        local packed = id and Points[tostring(id)]
        local e = packed and unpack_entity(packed)
        if e then
            res = string.format(
                '{"status":"ok","tick":%d,"state_root":"%s","point":{"id":"%s","x":%.6f,"y":%.6f,"tick":%d,"owner":"%s"}%s}',
                Tick, StateRoot, json_safe(id), e.x, e.y, e.tick, json_safe(e.owner), status_suffix()
            )
        else
            res = string.format(
                '{"status":"not_found","tick":%d,"state_root":"%s","id":"%s"%s}',
                Tick, StateRoot, json_safe(id), status_suffix()
            )
        end
    else
        -- Read-All and anything else: includes Dual-Watermark (head_tick vs replicated_tick)
        -- and arena points when entity count <= 250 so zero-slot GET /now serves the 60fps UI.
        recompute_head()
        local lag = math.max(0, (HeadTick or Tick) - Tick)
        local missing = 0
        for _, rec in ipairs(RepairBatches) do missing = missing + missing_replicas(rec) end
        local n_ent = count_entities()
        local points_ext = ""
        if n_ent <= 250 then
            sync_head_points()
            points_ext = string.format(',"points":%s,"head_points":%s', encode_points(Points), encode_points(HeadPoints))
        end
        res = string.format(
            '{"status":"ok","ack":"replicated","tick":%d,"head_tick":%d,"replicated_tick":%d,"replication_lag":%d,"state_root":"%s","head_root":"%s","replicated_root":"%s","total":%d,"active_entity_count":%d,"queued":%d,"pending_batches":%d,"repair_batches":%d,"repair_missing":%d,"applied_inputs":%d,"compacted_total":%d,"out_of_order_dropped":%d,"audit_ring":%s%s%s}',
            Tick, HeadTick or Tick, Tick, lag, StateRoot, HeadRoot or StateRoot, StateRoot,
            n_ent, n_ent, #Queue, pending_count(),
            #RepairBatches, missing, AppliedInputs or 0, CompactedCount or 0, OutOfOrderDropped or 0,
            encode_audit_ring(), points_ext, status_suffix()
        )
    end

    process.results = {
        output = {
            status = http_status,
            body = res,
            tick = int_str(Tick),
            state_root = StateRoot,
            active_entity_count = tostring(ActiveEntityCount or 0),
            queued = tostring(#Queue),
            primary_signer = slot_signer
        }
    }
    return process
end
