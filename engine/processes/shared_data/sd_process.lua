-- ==============================================================================
-- Shared Data Process (Verified Replica, Canonical Root, Batch Idempotency)
-- ==============================================================================
-- Zero hardcoded peer URLs: the Arbit endpoint is resolved from Genesis.
--
-- Contract (see ROADMAP.md §3/§4):
--   * `Commit` applies a delta ONLY when `tick_from == LastTick` and the
--     recomputed canonical root equals the root claimed by the Primary.
--   * Duplicate `batch_id`s are applied exactly once (idempotent ACK).
--   * `Hydrate` returns the full state so a cold Primary can recover.
--   * `Sync-State` atomically replaces the replica with the canonical state.

ReplicaStore = ReplicaStore or {}       -- id -> "x|y|tick|owner"
LastTick = LastTick or 0
LastBatchSentTick = LastBatchSentTick or 0
LastRelayedBySigner = LastRelayedBySigner or "unverified"
GenesisUrl = GenesisUrl or nil
Topology = Topology or nil
CurrentEpoch = CurrentEpoch or 1
StateRoot = StateRoot or "0x0000000000000000000000000000000000000000000000000000000000000000"

RecentDiffs = RecentDiffs or {}
DiffSeq = DiffSeq or 0
LastCommittedTick = LastCommittedTick or 0
LastL1TxId = LastL1TxId or ""
LastPrunedTick = LastPrunedTick or 0
PrunedDiffCount = PrunedDiffCount or 0
MaxDiffSlots = MaxDiffSlots or 512

CommittedBatches = CommittedBatches or {}
CommittedOrder = CommittedOrder or {}
PendingBatches = PendingBatches or {}
MaxCommittedHistory = MaxCommittedHistory or 512
MaxPendingBatches = MaxPendingBatches or 64

MaxEntities = MaxEntities or 1000
MaxBatchInputs = MaxBatchInputs or 256
MaxPayloadBytes = MaxPayloadBytes or 65536

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
-- Entity encoding + canonical root (must be byte-identical to Primary)
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

Shards, ShardHashes = build_shards(ReplicaStore)
StateRoot = shard_root(ShardHashes, LastTick)

local function encode_points(points)
    local ids = sorted_ids(points or ReplicaStore)
    local items = {}
    for i = 1, #ids do
        local id = ids[i]
        local e = unpack_entity((points or ReplicaStore)[id])
        if e then
            items[i] = string.format(
                '["%s",%.6f,%.6f,%d,"%s"]',
                json_safe(id), e.x, e.y, e.tick, json_safe(e.owner)
            )
        end
    end
    return "[" .. table.concat(items, ",") .. "]"
end

local function count_replicas()
    local n = 0
    for _ in pairs(ReplicaStore) do n = n + 1 end
    return n
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

local function extract_msg_id(msg_obj)
    if type(msg_obj) == "table" then
        if msg_obj.id then return tostring(msg_obj.id) end
        if msg_obj["hash-chain"] then return tostring(msg_obj["hash-chain"]) end
    end
    return "sd-slot-" .. tostring(LastTick or 0)
end

local function fetch_genesis_topology_once(genesis_url)
    if not genesis_url then return nil end
    local ok, status, res = pcall(function()
        return ao.resolve({
            path = "/~relay@1.0/call",
            ["relay-method"] = "GET",
            ["relay-path"] = genesis_url .. "/~node-process@1.0/genesis/now/results~message@1.0/output/body",
            ["cache-control"] = "no-store"
        })
    end)
    if ok and status == "ok" then
        local body_str = (type(res) == "table" and res.body) and tostring(res.body) or tostring(res or "")
        local arbit_url = string.match(body_str, '"arbit_url"%s*:%s*"([^"]+)"')
        local arbit_proc = string.match(body_str, '"arbit_process"%s*:%s*"([^"]+)"') or "arbit"
        local primary_url = string.match(body_str, '"primary_url"%s*:%s*"([^"]+)"')
        local primary_proc = string.match(body_str, '"primary_process"%s*:%s*"([^"]+)"') or "primary"
        local epoch_str = string.match(body_str, '"epoch"%s*:%s*(%d+)') or "1"
        local batch_str = string.match(body_str, '"arbit_batch_interval"%s*:%s*(%d+)') or "5"
        local diff_slots_str = string.match(body_str, '"sd_diff_slots"%s*:%s*(%d+)')
        local max_entities_str = string.match(body_str, '"primary_max_entities"%s*:%s*(%d+)')
        if diff_slots_str then MaxDiffSlots = tonumber(diff_slots_str) or MaxDiffSlots end
        if max_entities_str then MaxEntities = tonumber(max_entities_str) or MaxEntities end
        if arbit_url then
            return {
                genesis_url = genesis_url,
                epoch = tonumber(epoch_str) or 1,
                arbit_url = arbit_url,
                arbit_process = arbit_proc,
                arbit_batch_interval = tonumber(batch_str) or 5,
                primary_url = primary_url,
                primary_process = primary_proc,
                sd_diff_slots = MaxDiffSlots
            }
        end
    end
    return nil
end

local function ensure_topology(process, msg)
    GenesisUrl = GenesisUrl or process["genesis-url"] or (msg and msg["genesis-url"])
    if not Topology and GenesisUrl then
        Topology = fetch_genesis_topology_once(GenesisUrl)
    end
    return Topology
end

local function encode_string_list(list)
    if not list or #list == 0 then return "[]" end
    local items = {}
    for _, v in ipairs(list) do
        items[#items + 1] = string.format([["%s"]], json_safe(v))
    end
    return "[" .. table.concat(items, ",") .. "]"
end

local function parse_delta(raw)
    local delta, order = {}, {}
    local function add(id, x, y, owner, etick)
        id = tostring(id)
        if delta[id] == nil then table.insert(order, id) end
        delta[id] = {
            x = tonumber(x) or 0,
            y = tonumber(y) or 0,
            owner = tostring(owner or "anonymous"),
            tick = etick and tonumber(etick) or nil
        }
    end
    if type(raw) == "table" then
        -- HyperBEAM decodes the relayed JSON body into a Lua table.
        local list = raw
        if raw.delta ~= nil then list = raw.delta end
        if type(list) == "table" then
            for _, item in pairs(list) do
                if type(item) == "table" then
                    local id = item[1] or item.id
                    local x = item[2] or item.x
                    local y = item[3] or item.y
                    local owner = item[4] or item.owner
                    local etick = item[5] or item.tick
                    if id ~= nil then add(id, x, y, owner, etick) end
                end
            end
        end
        return delta, order
    end
    raw = tostring(raw or "")
    local init = 1
    while true do
        local s, e, id, x, y, owner, rest = string.find(
            raw,
            '%[%s*"([^"]+)"%s*,%s*([%d%.%-]+)%s*,%s*([%d%.%-]+)%s*,%s*"([^"]*)"%s*([^%]]*)%]',
            init
        )
        if not s then break end
        local etick = rest and string.match(rest, ',%s*(%d+)')
        add(id, x, y, owner, etick)
        init = e + 1
    end
    return delta, order
end

local function parse_points(raw, max)
    local points, n = {}, 0
    local function add(id, x, y, tick, owner)
        n = n + 1
        if n > max then return end
        points[tostring(id)] = pack_entity(tonumber(x) or 0, tonumber(y) or 0, tonumber(tick) or 0, tostring(owner or "anonymous"))
    end
    if type(raw) == "table" then
        local list = raw
        if raw.points ~= nil then list = raw.points end
        if type(list) == "table" then
            for _, item in pairs(list) do
                if type(item) == "table" then
                    local id = item[1] or item.id
                    local x = item[2] or item.x
                    local y = item[3] or item.y
                    local tick = item[4] or item.tick
                    local owner = item[5] or item.owner
                    if id ~= nil then add(id, x, y, tick, owner) end
                end
            end
        end
        return points, n
    end
    raw = tostring(raw or "")
    local init = 1
    while true do
        local s, e, id, x, y, tick, owner = string.find(
            raw,
            '%[%s*"([^"]+)"%s*,%s*([%d%.%-]+)%s*,%s*([%d%.%-]+)%s*,%s*(%d+)%s*,%s*"([^"]*)"%s*%]',
            init
        )
        if not s then break end
        add(id, x, y, tick, owner)
        init = e + 1
    end
    return points, n
end

local function remember_batch(batch_id, result_body)
    if CommittedBatches[batch_id] then return end
    CommittedBatches[batch_id] = result_body
    table.insert(CommittedOrder, batch_id)
    while #CommittedOrder > MaxCommittedHistory do
        local old = table.remove(CommittedOrder, 1)
        CommittedBatches[old] = nil
    end
end

-- Cast a verified ACK back to the Primary. `cast` never waits and never
-- raises on a dead peer, so the replica can never block on the primary.
local function cast_ack(topo, batch_id, tick, root, sd_signer, node_role)
    if not (topo and topo.primary_url) then return end
    local worker_suffix = node_role and string.match(tostring(node_role), "_(%d+)$")
    local target_primary = worker_suffix and ("primary_" .. worker_suffix) or topo.primary_process
    pcall(function()
        ao.resolve({
            path = "/~relay@1.0/cast",
            ["relay-path"] = topo.primary_url .. "/~node-process@1.0/" .. target_primary .. "/push",
            ["relay-method"] = "POST",
            ["relay-body"] = string.format(
                '{"action":"Ack","batch_id":"%s","tick":%d,"state_root":"%s","sd_signer":"%s","role":"%s","epoch":%d}',
                json_safe(batch_id), tick, json_safe(root), json_safe(sd_signer), json_safe(node_role),
                tonumber(CurrentEpoch) or 1
            ),
            ["content-type"] = "application/json",
            ["cache-control"] = "no-store"
        })
    end)
end

-- Tell the Primary that this replica is ahead (or that the claimed root is
-- wrong) so it can re-hydrate instead of retrying forever. Cast only.
local function cast_reject(topo, batch_id, reason, sd_signer, node_role)
    if not (topo and topo.primary_url) then return end
    local worker_suffix = node_role and string.match(tostring(node_role), "_(%d+)$")
    local target_primary = worker_suffix and ("primary_" .. worker_suffix) or topo.primary_process
    pcall(function()
        ao.resolve({
            path = "/~relay@1.0/cast",
            ["relay-path"] = topo.primary_url .. "/~node-process@1.0/" .. target_primary .. "/push",
            ["relay-method"] = "POST",
            ["relay-body"] = string.format(
                '{"action":"Reject","batch_id":"%s","reason":"%s","last_tick":%d,"state_root":"%s","sd_signer":"%s","role":"%s"}',
                json_safe(batch_id), json_safe(reason), LastTick, json_safe(StateRoot),
                json_safe(sd_signer), json_safe(node_role)
            ),
            ["content-type"] = "application/json",
            ["cache-control"] = "no-store"
        })
    end)
end

local function maybe_checkpoint(topo, batch_interval, node_role, sd_signer, count)
    local uncommitted_count = #RecentDiffs
    local count_trigger = (LastTick % batch_interval == 0) or (LastTick >= 5 and LastBatchSentTick == 0)
    local guard_trigger = (uncommitted_count >= MaxDiffSlots)
    local is_checkpoint_sender = (node_role == "sd1") or (node_role == "sd2" and (LastTick - LastBatchSentTick >= batch_interval * 2))
    if topo and topo.arbit_url and is_checkpoint_sender and LastTick > 0
        and (count_trigger or guard_trigger) and LastTick > LastBatchSentTick then
        LastBatchSentTick = LastTick
        local payload = string.format(
            '{"action":"Commit-Checkpoint","last_tick":%d,"point_count":%d,"state_root":"%s","checkpoint_signer":"%s","sd_node":"%s","diff_seq":%d,"epoch":%d}',
            LastTick, count, json_safe(StateRoot), json_safe(sd_signer), json_safe(node_role), DiffSeq, CurrentEpoch
        )
        pcall(function()
            ao.resolve({
                path = "/~relay@1.0/cast",
                ["relay-path"] = topo.arbit_url .. "/~node-process@1.0/" .. topo.arbit_process .. "/schedule",
                ["relay-method"] = "POST",
                ["relay-body"] = payload,
                ["content-type"] = "application/json",
                ["cache-control"] = "no-store"
            })
        end)
    end
end

-- Apply one already-verified delta. Returns a result body string.
local function apply_delta(batch_id, tick_from, tick_to, delta, order, sd_signer)
    local packed = {}
    for _, id in ipairs(order) do
        local d = delta[id]
        packed[id] = pack_entity(d.x, d.y, d.tick or tick_to, d.owner)
        ReplicaStore[id] = packed[id]
    end
    LastTick = tick_to
    StateRoot = commit_shards(Shards, ShardHashes, packed, tick_to)
    DiffSeq = DiffSeq + 1
    table.insert(RecentDiffs, { seq = DiffSeq, tick = tick_to, batch_id = batch_id, inputs = #order })
    while #RecentDiffs > MaxDiffSlots do
        table.remove(RecentDiffs, 1)
    end
    local body = string.format(
        '{"status":"committed","tier":"shared_data_replica","batch_id":"%s","last_tick":%d,"tick":%d,"state_root":"%s","count":%d,"sd_signer":"%s","diff_seq":%d,"persistence":"lmdb_disk"}',
        json_safe(batch_id), LastTick, LastTick, StateRoot, count_replicas(), json_safe(sd_signer), DiffSeq
    )
    remember_batch(batch_id, body)
    return body
end

local function drain_pending(sd_signer, topo, node_role)
    local progress = true
    while progress do
        progress = false
        for batch_id, pending in pairs(PendingBatches) do
            if pending.tick_from == LastTick then
                PendingBatches[batch_id] = nil
                apply_delta(batch_id, pending.tick_from, pending.tick_to, pending.delta, pending.order, sd_signer)
                cast_ack(topo, batch_id, LastTick, StateRoot, sd_signer, node_role)
                progress = true
            end
        end
    end
end

function compute(process, message, opts)
    local msg = message.body or message
    if type(msg) ~= "table" then msg = {} end
    local action = msg.action or msg.Action or (msg.query and msg.query.action) or "Get-Replica-State"

    if action == "Sync-Topology" then
        Topology = nil
    end
    local topo = ensure_topology(process, msg)
    local genesis_src = (topo and topo.genesis_url) or tostring(GenesisUrl or "")
    local arbit_target = (topo and topo.arbit_url) or "unresolved"
    local batch_interval = (topo and topo.arbit_batch_interval) or 5
    local node_role = tostring(process["node-role"] or "sd1")

    if not SdSigner then
        SdSigner = extract_first_signer(process) or "sd_authority"
    end
    local sd_signer = SdSigner
    local assignment_id = extract_msg_id(message)
    local incoming_signer = msg.relayed_by_signer or extract_first_signer(msg)
    if incoming_signer then
        LastRelayedBySigner = incoming_signer
    end

    -- Drain contiguous pending batches so reads, snapshots, and hydrations
    -- always see the freshest tick confirmed on this replica.
    drain_pending(sd_signer, topo, node_role)

    local msg_epoch = tonumber(msg.epoch or (msg.query and msg.query.epoch))
    if msg_epoch then
        if msg_epoch < CurrentEpoch then
            local err = string.format(
                '{"status":"error_stale_epoch","tier":"shared_data_replica","epoch":%d,"current_epoch":%d,"last_tick":%d}',
                msg_epoch, CurrentEpoch, LastTick
            )
            process.results = { output = { status = 409, body = err, last_tick = tostring(LastTick), sd_signer = sd_signer } }
            return process
        elseif msg_epoch > CurrentEpoch then
            CurrentEpoch = msg_epoch
        end
    end

    local http_status = 200
    local res = ""

    if action == "Commit" then
        local batch_id = msg.batch_id and tostring(msg.batch_id) or ""
        local tick_from = tonumber(msg.tick_from or (msg.query and msg.query.tick_from))
        local tick_to = tonumber(msg.tick_to or (msg.query and msg.query.tick_to))
        local claimed_root = msg.state_root or (msg.query and msg.query.state_root)
        local delta, order = parse_delta(msg.delta or (msg.query and msg.query.delta))

        if batch_id == "" or tick_from == nil or tick_to == nil or not claimed_root then
            http_status = 400
            res = string.format(
                '{"status":"error_invalid_commit","tier":"shared_data_replica","sd_signer":"%s","last_tick":%d}',
                json_safe(sd_signer), LastTick
            )
        elseif CommittedBatches[batch_id] then
            -- Idempotent retry: report the original ACK, never apply twice,
            -- and re-cast it in case the first one was lost in transit.
            http_status = 200
            res = CommittedBatches[batch_id]
            local ct = tonumber(string.match(res, '"last_tick"%s*:%s*(%d+)'))
            local cr = string.match(res, '"state_root"%s*:%s*"(0x%x+)"')
            cast_ack(topo, batch_id, ct or LastTick, cr or StateRoot, sd_signer, node_role)
        elseif #order > MaxBatchInputs then
            http_status = 413
            res = string.format(
                '{"status":"error_batch_too_large","batch_id":"%s","inputs":%d,"max_inputs":%d,"sd_signer":"%s","last_tick":%d}',
                json_safe(batch_id), #order, MaxBatchInputs, json_safe(sd_signer), LastTick
            )
        elseif tick_from > LastTick then
            -- Out-of-order (async hot backup): buffer, do not drop.
            if #order > 0 and not PendingBatches[batch_id] then
                if count_replicas() + #order > MaxEntities then
                    http_status = 409
                    res = string.format(
                        '{"status":"error_entity_limit","batch_id":"%s","sd_signer":"%s","last_tick":%d}',
                        json_safe(batch_id), json_safe(sd_signer), LastTick
                    )
                else
                    local pending_keys = 0
                    for _ in pairs(PendingBatches) do pending_keys = pending_keys + 1 end
                    if pending_keys < MaxPendingBatches then
                        PendingBatches[batch_id] = {
                            tick_from = tick_from, tick_to = tick_to,
                            delta = delta, order = order,
                        }
                    end
                end
            end
            http_status = 202
            res = string.format(
                '{"status":"deferred","batch_id":"%s","tick_from":%d,"last_tick":%d,"sd_signer":"%s"}',
                json_safe(batch_id), tick_from, LastTick, json_safe(sd_signer)
            )
        elseif tick_from < LastTick then
            http_status = 409
            res = string.format(
                '{"status":"error_tick_mismatch","batch_id":"%s","tick_from":%d,"expected_tick_from":%d,"last_tick":%d,"state_root":"%s","sd_signer":"%s"}',
                json_safe(batch_id), tick_from, LastTick, LastTick, StateRoot, json_safe(sd_signer)
            )
            cast_reject(topo, batch_id, "replica_ahead", sd_signer, node_role)
        else
            -- Verify the claimed root BEFORE mutating (verify-don't-trust).
            local packed = {}
            for id, d in pairs(delta) do
                packed[id] = pack_entity(d.x, d.y, d.tick or tick_to, d.owner)
            end
            local computed_root = candidate_root(Shards, ShardHashes, packed, tick_to)
            if computed_root ~= claimed_root then
                http_status = 409
                res = string.format(
                    '{"status":"diverged","batch_id":"%s","expected_root":"%s","computed_root":"%s","last_tick":%d,"sd_signer":"%s","count":%d}',
                    json_safe(batch_id), json_safe(claimed_root), json_safe(computed_root),
                    LastTick, json_safe(sd_signer), count_replicas()
                )
                cast_reject(topo, batch_id, "root_divergence", sd_signer, node_role)
            else
                res = apply_delta(batch_id, tick_from, tick_to, delta, order, sd_signer)
                cast_ack(topo, batch_id, LastTick, StateRoot, sd_signer, node_role)
                drain_pending(sd_signer, topo, node_role)
                LastPrunedTick = LastPrunedTick
                maybe_checkpoint(topo, batch_interval, node_role, sd_signer, count_replicas())
                http_status = 200
            end
        end
    elseif action == "Sync-State" then
        local tick = tonumber(msg.last_tick or (msg.query and msg.query.last_tick))
        local claimed = msg.state_root or (msg.query and msg.query.state_root)
        local points, n = parse_points(msg.points or (msg.query and msg.query.points), MaxEntities)
        if tick == nil or n == 0 then
            http_status = 400
            res = string.format('{"status":"error_invalid_sync","sd_signer":"%s","last_tick":%d}', json_safe(sd_signer), LastTick)
        else
            ReplicaStore = points
            LastTick = tick
            Shards, ShardHashes = build_shards(ReplicaStore)
            StateRoot = shard_root(ShardHashes, tick)
            PendingBatches = {}
            http_status = 200
            res = string.format(
                '{"status":"synced","last_tick":%d,"tick":%d,"state_root":"%s","claimed_root":"%s","root_match":%s,"count":%d,"sd_signer":"%s"}',
                LastTick, LastTick, StateRoot, json_safe(claimed),
                (StateRoot == claimed) and "true" or "false", n, json_safe(sd_signer)
            )
        end
    elseif action == "Hydrate" then
        http_status = 200
        res = string.format(
            '{"status":"ok","tier":"shared_data_replica","last_tick":%d,"tick":%d,"state_root":"%s","count":%d,"points":%s,"sd_signer":"%s","node_role":"%s"}',
            LastTick, LastTick, StateRoot, count_replicas(), encode_points(ReplicaStore),
            json_safe(sd_signer), json_safe(node_role)
        )
    elseif action == "Prune-Committed-Slots" then
        local committed_tick = tonumber(msg.committed_tick or (msg.query and msg.query.committed_tick) or 0)
        local l1_tx_id = msg.l1_tx_id or (msg.query and msg.query.l1_tx_id) or ""
        if committed_tick > LastTick or l1_tx_id == "" then
            http_status = 409
            res = string.format(
                '{"status":"error_invalid_prune_request","last_tick":%d,"requested_tick":%d,"sd_signer":"%s"}',
                LastTick, committed_tick, json_safe(sd_signer)
            )
        else
            if committed_tick > LastCommittedTick then LastCommittedTick = committed_tick end
            if l1_tx_id ~= "" then LastL1TxId = tostring(l1_tx_id) end
            LastPrunedTick = math.max(LastPrunedTick, committed_tick)
            local removed = 0
            while #RecentDiffs > 0 and RecentDiffs[1].tick <= committed_tick do
                table.remove(RecentDiffs, 1)
                removed = removed + 1
            end
            PrunedDiffCount = PrunedDiffCount + removed
            res = string.format(
                '{"status":"pruned","committed_tick":%d,"last_pruned_tick":%d,"pruned_now":%d,"pruned_diff_count":%d,"recent_diff_count":%d,"last_tick":%d,"sd_signer":"%s"}',
                committed_tick, LastPrunedTick, removed, PrunedDiffCount, #RecentDiffs, LastTick, json_safe(sd_signer)
            )
        end
    elseif action == "Read-Point" then
        local id = msg.id or (msg.query and msg.query.id)
        local packed = id and ReplicaStore[tostring(id)]
        local e = packed and unpack_entity(packed)
        if e then
            res = string.format(
                '{"status":"ok","tier":"shared_data_replica","last_tick":%d,"state_root":"%s","point":{"id":"%s","x":%.6f,"y":%.6f,"tick":%d,"owner":"%s"},"sd_signer":"%s"}',
                LastTick, StateRoot, json_safe(id), e.x, e.y, e.tick, json_safe(e.owner), json_safe(sd_signer)
            )
        else
            res = string.format(
                '{"status":"not_found","last_tick":%d,"state_root":"%s","id":"%s","sd_signer":"%s"}',
                LastTick, StateRoot, json_safe(id), json_safe(sd_signer)
            )
        end
    elseif action == "Snapshot" then
        res = string.format(
            '{"status":"ok","tier":"shared_data_replica","last_tick":%d,"state_root":"%s","count":%d,"points":%s,"sd_signer":"%s"}',
            LastTick, StateRoot, count_replicas(), encode_points(ReplicaStore), json_safe(sd_signer)
        )
    else
        -- Compact summary; never echo the full replica set into a stored slot.
        res = string.format(
            '{"status":"ok","tier":"shared_data_replica","persistence":"lmdb_disk","requires_sign":true,"sd_signer":"%s","relayed_by_signer":"%s","assignment_id":"%s","topology_source":"%s","arbit_target":"%s","arbit_batch_interval":%d,"last_batch_sent_tick":%d,"last_tick":%d,"tick":%d,"count":%d,"state_root":"%s","last_committed_tick":%d,"last_l1_tx_id":"%s","last_pruned_tick":%d,"pruned_diff_count":%d,"recent_diff_count":%d,"max_diff_slots":%d,"node_role":"%s"}',
            json_safe(sd_signer), json_safe(LastRelayedBySigner), json_safe(assignment_id),
            json_safe(genesis_src), json_safe(arbit_target), batch_interval, LastBatchSentTick,
            LastTick, LastTick, count_replicas(), json_safe(StateRoot), LastCommittedTick,
            json_safe(LastL1TxId), LastPrunedTick, PrunedDiffCount, #RecentDiffs, MaxDiffSlots,
            json_safe(node_role)
        )
    end

    process.results = {
        output = {
            status = http_status,
            body = res,
            last_tick = tostring(LastTick),
            state_root = StateRoot,
            sd_signer = sd_signer,
            count = tostring(count_replicas())
        }
    }
    return process
end
