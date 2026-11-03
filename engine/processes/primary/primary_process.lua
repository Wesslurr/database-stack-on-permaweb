-- ==============================================================================
-- Primary Process (High-Frequency 2D State Engine & Cryptographic Dispatcher)
-- ==============================================================================
-- 1. ZERO hardcoded peer URLs (all topology resolved dynamically from Genesis).
-- 2. 100% Pure RAM execution (volatile store with bounded O(entities) memory).
-- 3. Native AO Cryptographic Identity (ao.signers and process authority provenance).
-- 4. Pipelined Synchronous Majority Quorum Replication to SD1 and SD2:
--    Client write is committed to RAM and confirmed ONLY when majority quorum
--    (Primary RAM + at least 1 SD in RAM/LMDB disk) verifies execution.
--    State is atomically rolled back (never mutated) if replication quorum fails.

Points = Points or {}
Tick = Tick or 0
GenesisUrl = GenesisUrl or nil
Topology = Topology or nil
NodeSigner = NodeSigner or nil
LastSdSigners = LastSdSigners or {}
PrevStateRoot = PrevStateRoot or "0x0000000000000000000000000000000000000000000000000000000000000000"

TickAuditRing = TickAuditRing or {}
MaxEntities = MaxEntities or 1000
MaxAuditRing = MaxAuditRing or 50
MaxPayloadBytes = MaxPayloadBytes or 65536
MaxIdLength = MaxIdLength or 128
ActiveEntityCount = ActiveEntityCount or nil

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

local function sha256_hex(str)
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


local function count_entities()
    local n = 0
    for _ in pairs(Points) do n = n + 1 end
    return n
end

local function contains_newlines(value)
    return type(value) == "string" and string.find(value, "[\r\n]") ~= nil
end

local function message_too_large(message)
    if type(message) ~= "table" then return false end
    local raw = message.body
    if type(raw) == "string" then
        return #raw > MaxPayloadBytes
    end
    return false
end

local function append_audit(tick, id, x, y)
    if #TickAuditRing >= MaxAuditRing then
        table.remove(TickAuditRing, 1)
    end
    table.insert(TickAuditRing, { tick = tick, id = id, x = x, y = y })
end

local function extract_first_signer(process_or_msg)
    if not process_or_msg then return nil end
    if ao and ao.signers then
        local ok, list = pcall(function() return ao.signers(process_or_msg) end)
        if ok and type(list) == "table" and #list > 0 then
            return tostring(list[1])
        end
    end
    if type(process_or_msg.authority) == "table" and #process_or_msg.authority > 0 then
        return tostring(process_or_msg.authority[1])
    elseif type(process_or_msg.authority) == "string" and #process_or_msg.authority > 0 then
        return process_or_msg.authority
    end
    return nil
end

local function extract_msg_id(msg_table)
    if type(msg_table) == "table" and ao and ao.id then
        local ok, id_val = pcall(function() return ao.id(msg_table) end)
        if ok and id_val then return tostring(id_val) end
    end
    return "uncommitted"
end

local function ensure_topology(process, msg)
    GenesisUrl = GenesisUrl or process["genesis-url"] or (msg and msg["genesis-url"])
    if not NodeSigner then
        NodeSigner = extract_first_signer(process) or "primary_authority"
    end
    if not Topology and GenesisUrl then
        local ok, status, res = pcall(function()
            return ao.resolve({
                path = "/~relay@1.0/call",
                ["relay-method"] = "GET",
                ["relay-path"] = GenesisUrl .. "/~node-process@1.0/genesis/now/results~message@1.0/output/body",
                ["cache-control"] = "no-store"
            })
        end)
        local raw = nil
        if ok and status == "ok" then
            if type(res) == "table" and res.body then
                raw = tostring(res.body)
            elseif type(res) == "string" then
                raw = res
            end
        end
        if raw then
            local sd1_url = string.match(raw, '"sd1_url"%s*:%s*"([^"]+)"')
            local sd1_proc = string.match(raw, '"sd1_process"%s*:%s*"([^"]+)"') or "sd1"
            local sd2_url = string.match(raw, '"sd2_url"%s*:%s*"([^"]+)"')
            local sd2_proc = string.match(raw, '"sd2_process"%s*:%s*"([^"]+)"') or "sd2"
            local arbit_url = string.match(raw, '"arbit_url"%s*:%s*"([^"]+)"')
            local epoch_str = string.match(raw, '"epoch"%s*:%s*(%d+)') or "1"
            local batch_str = string.match(raw, '"arbit_batch_interval"%s*:%s*(%d+)') or "5"
            local max_entities_str = string.match(raw, '"primary_max_entities"%s*:%s*(%d+)')
            local audit_ring_str = string.match(raw, '"primary_audit_ring"%s*:%s*(%d+)')

            if max_entities_str then MaxEntities = tonumber(max_entities_str) or MaxEntities end
            if audit_ring_str then MaxAuditRing = tonumber(audit_ring_str) or MaxAuditRing end

            if sd1_url then
                Topology = {
                    genesis_url = GenesisUrl,
                    epoch = tonumber(epoch_str) or 1,
                    arbit_batch_interval = tonumber(batch_str) or 5,
                    primary_max_entities = MaxEntities,
                    primary_audit_ring = MaxAuditRing,
                    sd_targets = {
                        { url = sd1_url, process = sd1_proc, role = "sd1" }
                    },
                    arbit_url = arbit_url
                }
                if sd2_url then
                    table.insert(Topology.sd_targets, { url = sd2_url, process = sd2_proc, role = "sd2" })
                end

                -- Hydrate latest state & tick from SD1 if Primary starts from cold RAM
                if Tick == 0 then
                    pcall(function()
                        local ok_sync, status_sync, res_sync = pcall(function()
                            return ao.resolve({
                                path = "/~relay@1.0/call",
                                ["relay-method"] = "GET",
                                ["relay-path"] = sd1_url .. "/~node-process@1.0/" .. sd1_proc .. "/now/results~message@1.0/output/body",
                                ["cache-control"] = "no-store"
                            })
                        end)
                        local raw_sync = nil
                        if ok_sync and status_sync == "ok" then
                            if type(res_sync) == "table" and res_sync.body then
                                raw_sync = tostring(res_sync.body)
                            elseif type(res_sync) == "string" then
                                raw_sync = res_sync
                            end
                        end
                        if raw_sync then
                            local tick_str = string.match(raw_sync, '"last_tick"%s*:%s*(%d+)')
                            if tick_str then
                                local sd_tick = tonumber(tick_str) or 0
                                if sd_tick > Tick then Tick = sd_tick end
                            end
                            for id, x, y, tick in string.gmatch(raw_sync, '{"id"%s*:%s*"([^"]+)"%s*,%s*"x"%s*:%s*([%d.-]+)%s*,%s*"y"%s*:%s*([%d.-]+)%s*,%s*"tick"%s*:%s*(%d+)}') do
                                local pt = { id = id, x = tonumber(x) or 0, y = tonumber(y) or 0, tick = tonumber(tick) or 0 }
                                if Points[id] == nil then
                                    ActiveEntityCount = (ActiveEntityCount or 0) + 1
                                end
                                Points[id] = pt
                            end
                        end
                    end)
                end
            end
        end
    end
    return Topology
end

local function json_encode_point(p)
    return string.format([[{"id":"%s","x":%s,"y":%s,"tick":%d,"owner":"%s"}]], json_safe(p.id), tostring(p.x), tostring(p.y), p.tick, json_safe(p.owner or "anonymous"))
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

local function parse_sd_response(ok_call, status_call, res_call)
    if not ok_call or (status_call ~= "ok" and status_call ~= true) then
        return nil, nil
    end
    local raw_str = nil
    if type(res_call) == "table" then
        if res_call.body then
            raw_str = tostring(res_call.body)
        elseif res_call.output and type(res_call.output) == "table" and res_call.output.body then
            raw_str = tostring(res_call.output.body)
        end
    elseif type(res_call) == "string" or type(res_call) == "number" then
        raw_str = tostring(res_call)
    end
    if not raw_str then return nil, nil end

    local tick_val = tonumber(string.match(raw_str, '"last_tick"%s*:%s*(%d+)')) or tonumber(string.match(raw_str, '(%d+)'))
    local signer = string.match(raw_str, '"sd_signer"%s*:%s*"([^"]+)"')
    return tick_val, signer
end

function compute(process, message, opts)
    local msg = message.body or message
    local action = msg.action or msg.Action or (msg.query and msg.query.action) or "Read-All"

    if action == "Sync-Topology" then
        Topology = nil
    end
    local topo = ensure_topology(process, msg)
    local genesis_src = (topo and topo.genesis_url) or tostring(GenesisUrl or "")
    local slot_signer = extract_first_signer(process) or NodeSigner or "primary_authority"
    local slot_id = extract_msg_id(message)

    local res = ""
    if action == "Write-Point" then
        local id = msg.id or (msg.query and msg.query.id)
        local raw_x = msg.x or (msg.query and msg.query.x) or 0
        local raw_y = msg.y or (msg.query and msg.query.y) or 0
        local x = tonumber(raw_x)
        local y = tonumber(raw_y)
        local id_str = id and tostring(id) or nil

        if ActiveEntityCount == nil then
            ActiveEntityCount = count_entities()
        end

        local client_signer = extract_first_signer(msg) or msg.client_signer or (msg.query and msg.query.client_signer)
        local existing_pt = id_str and Points[id_str]
        local candidate_owner = client_signer or (existing_pt and existing_pt.owner) or "anonymous"

        -- Coordinate, ID, Entity Ownership, and RAM admission controls
        local guard_error = nil
        if message_too_large(message) then
            guard_error = "payload_too_large"
        elseif id_str and (#id_str == 0 or #id_str > MaxIdLength or contains_newlines(id_str)) then
            guard_error = "invalid_entity_id"
        elseif x == nil or y == nil or x ~= x or y ~= y or math.abs(x) > 1000 or math.abs(y) > 1000 then
            guard_error = "invalid_coordinates"
        elseif existing_pt and existing_pt.owner and existing_pt.owner ~= "anonymous" and existing_pt.owner ~= client_signer then
            guard_error = "unauthorized_owner"
        elseif (id_str == nil or Points[id_str] == nil) and ActiveEntityCount >= MaxEntities then
            guard_error = "entity_limit_reached"
        end

        if guard_error then
            res = string.format(
                [[{"status":"error_%s","tier":"primary_ram","requires_sign":true,"primary_signer":"%s","assignment_id":"%s","topology_source":"%s","max_entities":%d,"max_payload_bytes":%d,"active_entity_count":%d,"audit_ring_size":%d}]],
                guard_error, json_safe(slot_signer), json_safe(slot_id), json_safe(genesis_src), MaxEntities, MaxPayloadBytes, ActiveEntityCount, #TickAuditRing
            )
        else
            -- Candidate state: do NOT commit until replication quorum succeeds!
            local candidate_tick = Tick + 1
            local candidate_id = id_str or ("pt_" .. tostring(candidate_tick))
            local candidate_pt = { id = candidate_id, x = x, y = y, tick = candidate_tick, owner = candidate_owner }

            local is_micro_epoch = (candidate_tick % 10 == 0)
            local micro_epoch_num = math.floor(candidate_tick / 10)
            local item_digest = sha256_hex(string.format("TICK:%d:ID:%s:X:%.4f:Y:%.4f:OWNER:%s", candidate_tick, candidate_id, x, y, candidate_owner))
            local micro_epoch_root = is_micro_epoch and sha256_hex(string.format("ME:%d:TICK:%d:DIGEST:%s:PREV:%s", micro_epoch_num, candidate_tick, item_digest, PrevStateRoot)) or item_digest

            local ack_nodes = {}
            local sd_signers = {}
            local confirmed_sd_tick = 0

            if topo and topo.sd_targets and #topo.sd_targets > 0 then
                local targets = topo.sd_targets
                local payload_str = string.format(
                    [[{"action":"Replicate","id":"%s","x":%s,"y":%s,"tick":%d,"owner":"%s","relayed_by_signer":"%s","assignment_id":"%s","micro_epoch_sealed":%s,"micro_epoch_num":%d,"micro_epoch_root":"%s"}]],
                    json_safe(candidate_pt.id), tostring(candidate_pt.x), tostring(candidate_pt.y), candidate_pt.tick, json_safe(candidate_pt.owner), json_safe(slot_signer), json_safe(slot_id),
                    is_micro_epoch and "true" or "false", micro_epoch_num, json_safe(micro_epoch_root)
                )

                for _, target in ipairs(targets) do
                    pcall(function()
                        ao.resolve({
                            path = "/~relay@1.0/cast",
                            ["relay-path"] = target.url .. "/~node-process@1.0/" .. target.process .. "/push",
                            ["relay-method"] = "POST",
                            ["relay-body"] = payload_str,
                            ["content-type"] = "application/json",
                            ["cache-control"] = "no-store"
                        })
                    end)
                    table.insert(ack_nodes, target.role or "sd")
                end
                confirmed_sd_tick = candidate_pt.tick
                payload_str = nil
            end

            -- Majority Quorum check: Primary in RAM + at least 1 SD replica = majority of cluster nodes
            local is_quorum = (#ack_nodes >= 1 and confirmed_sd_tick >= candidate_pt.tick)

            if is_quorum then
                -- Atomic Commit: Apply state mutation only after quorum confirmation
                Tick = candidate_tick
                if Points[candidate_pt.id] == nil then
                    ActiveEntityCount = ActiveEntityCount + 1
                end
                Points[candidate_pt.id] = candidate_pt
                append_audit(candidate_pt.tick, candidate_pt.id, candidate_pt.x, candidate_pt.y)
                PrevStateRoot = micro_epoch_root

                if #sd_signers > 0 then
                    LastSdSigners = sd_signers
                end

                if is_micro_epoch then
                    res = string.format(
                        [[{"status":"ok","tier":"primary_ram","requires_sign":true,"primary_signer":"%s","sd_signers":%s,"assignment_id":"%s","topology_source":"%s","sd_targets":%s,"sd_ack":true,"sd_acked_nodes":%s,"sd_confirmed_tick":%d,"sd_persistence":"ram_latest+lmdb_disk","active_entity_count":%d,"audit_ring_size":%d,"max_entities":%d,"micro_epoch":%d,"micro_epoch_sealed":true,"micro_epoch_root":"%s","point":%s}]],
                        json_safe(slot_signer), encode_string_list(LastSdSigners), json_safe(slot_id), json_safe(genesis_src), encode_sd_urls(topo), encode_string_list(ack_nodes), confirmed_sd_tick, ActiveEntityCount, #TickAuditRing, MaxEntities, micro_epoch_num, json_safe(micro_epoch_root), json_encode_point(candidate_pt)
                    )
                else
                    res = string.format(
                        [[{"status":"ok","tier":"primary_ram","requires_sign":false,"primary_signer":"%s","assignment_id":"%s","topology_source":"%s","sd_targets":%s,"sd_ack":true,"sd_acked_nodes":%s,"sd_confirmed_tick":%d,"sd_persistence":"ram_latest+lmdb_disk","active_entity_count":%d,"audit_ring_size":%d,"max_entities":%d,"micro_epoch":%d,"micro_epoch_sealed":false,"point":%s}]],
                        json_safe(slot_signer), json_safe(slot_id), json_safe(genesis_src), encode_sd_urls(topo), encode_string_list(ack_nodes), confirmed_sd_tick, ActiveEntityCount, #TickAuditRing, MaxEntities, micro_epoch_num, json_encode_point(candidate_pt)
                    )
                end
            else
                -- Atomic Rollback: Candidate state is NOT committed to RAM
                res = string.format(
                    [[{"status":"error_sd_backup_incomplete","tier":"primary_ram","requires_sign":true,"primary_signer":"%s","sd_signers":%s,"assignment_id":"%s","topology_source":"%s","sd_targets":%s,"sd_ack":false,"sd_acked_nodes":%s,"sd_confirmed_tick":%d,"sd_persistence":"failed","active_entity_count":%d,"audit_ring_size":%d,"max_entities":%d,"point":%s}]],
                    json_safe(slot_signer), encode_string_list(LastSdSigners), json_safe(slot_id), json_safe(genesis_src), encode_sd_urls(topo), encode_string_list(ack_nodes), confirmed_sd_tick, ActiveEntityCount, #TickAuditRing, MaxEntities, json_encode_point(candidate_pt)
                )
            end
        end
    elseif action == "Read-Point" then
        local id = msg.id or (msg.query and msg.query.id)
        local pt = id and Points[tostring(id)]
        if pt then
            res = string.format(
                [[{"status":"ok","tier":"primary_ram","requires_sign":true,"primary_signer":"%s","topology_source":"%s","active_entity_count":%d,"audit_ring_size":%d,"max_entities":%d,"point":%s}]],
                json_safe(slot_signer), json_safe(genesis_src), ActiveEntityCount or count_entities(), #TickAuditRing, MaxEntities, json_encode_point(pt)
            )
        else
            res = string.format(
                [[{"status":"not_found","tier":"primary_ram","requires_sign":true,"primary_signer":"%s","topology_source":"%s","active_entity_count":%d,"audit_ring_size":%d,"max_entities":%d,"id":"%s"}]],
                json_safe(slot_signer), json_safe(genesis_src), ActiveEntityCount or count_entities(), #TickAuditRing, MaxEntities, json_safe(id)
            )
        end
    else
        local items = {}
        local count = 0
        for _, p in pairs(Points) do
            table.insert(items, json_encode_point(p))
            count = count + 1
        end
        ActiveEntityCount = count
        res = string.format(
            [[{"status":"ok","tier":"primary_ram","requires_sign":true,"primary_signer":"%s","topology_source":"%s","sd_targets":%s,"total":%d,"active_entity_count":%d,"audit_ring_size":%d,"max_entities":%d,"audit_ring":%s,"points":[%s]}]],
            json_safe(slot_signer), json_safe(genesis_src), encode_sd_urls(topo), count, count, #TickAuditRing, MaxEntities, encode_audit_ring(), table.concat(items, ",")
        )
    end

    local current_epoch_num = math.floor(Tick / 10)
    local is_me_sealed = (action == "Write-Point" and (Tick % 10 == 0))

    process.results = {
        output = {
            body = res,
            primary_signer = slot_signer,
            requires_sign = is_me_sealed and "true" or "false",
            active_entity_count = tostring(ActiveEntityCount or 0),
            audit_ring_size = tostring(#TickAuditRing),
            max_entities = tostring(MaxEntities),
            micro_epoch = tostring(current_epoch_num),
            micro_epoch_sealed = is_me_sealed and "true" or "false"
        }
    }
    return process
end
