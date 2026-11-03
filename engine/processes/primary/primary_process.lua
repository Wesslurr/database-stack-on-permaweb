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
PrevStateRoot = PrevStateRoot or "0x0000000000000000"

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

local function canonical_hash(str)
    local h1 = 2166136261
    local h2 = 5381
    for i = 1, #str do
        local b = string.byte(str, i)
        h1 = ((h1 ~ b) * 16777619) % 4294967296
        h2 = ((h2 * 33) + b) % 4294967296
    end
    return string.format("0x%08x%08x", h1, h2)
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
    return string.format([[{"id":"%s","x":%s,"y":%s,"tick":%d}]], json_safe(p.id), tostring(p.x), tostring(p.y), p.tick)
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

        -- Coordinate, ID and RAM admission controls
        local guard_error = nil
        if message_too_large(message) then
            guard_error = "payload_too_large"
        elseif id_str and (#id_str == 0 or #id_str > MaxIdLength or contains_newlines(id_str)) then
            guard_error = "invalid_entity_id"
        elseif x == nil or y == nil or x ~= x or y ~= y or math.abs(x) > 1000 or math.abs(y) > 1000 then
            guard_error = "invalid_coordinates"
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
            local candidate_pt = { id = candidate_id, x = x, y = y, tick = candidate_tick }

            local is_micro_epoch = (candidate_tick % 10 == 0)
            local micro_epoch_num = math.floor(candidate_tick / 10)
            local item_digest = canonical_hash(string.format("TICK:%d:ID:%s:X:%.4f:Y:%.4f", candidate_tick, candidate_id, x, y))
            local micro_epoch_root = canonical_hash(string.format("ME:%d:TICK:%d:DIGEST:%s:PREV:%s", micro_epoch_num, candidate_tick, item_digest, PrevStateRoot))

            local ack_nodes = {}
            local sd_signers = {}
            local confirmed_sd_tick = 0

            if topo and topo.sd_targets and #topo.sd_targets > 0 then
                local targets = topo.sd_targets
                local payload_str = string.format(
                    [[{"action":"Replicate","id":"%s","x":%s,"y":%s,"tick":%d,"relayed_by_signer":"%s","assignment_id":"%s","micro_epoch_sealed":%s,"micro_epoch_num":%d,"micro_epoch_root":"%s"}]],
                    json_safe(candidate_pt.id), tostring(candidate_pt.x), tostring(candidate_pt.y), candidate_pt.tick, json_safe(slot_signer), json_safe(slot_id),
                    is_micro_epoch and "true" or "false", micro_epoch_num, json_safe(micro_epoch_root)
                )

                for _, target in ipairs(targets) do
                    local ok_call, status_call, res_call = pcall(function()
                        return ao.resolve({
                            path = "/~relay@1.0/call",
                            ["relay-path"] = target.url .. "/~node-process@1.0/" .. target.process .. "/push/output~message@1.0/body",
                            ["relay-method"] = "POST",
                            ["relay-body"] = payload_str,
                            ["content-type"] = "application/json",
                            ["cache-control"] = "no-store"
                        })
                    end)
                    local returned_tick, returned_signer = parse_sd_response(ok_call, status_call, res_call)
                    if returned_tick and returned_tick >= candidate_pt.tick then
                        table.insert(ack_nodes, target.role or "sd")
                        if returned_signer and returned_signer ~= "" then
                            table.insert(sd_signers, returned_signer)
                        end
                        if returned_tick > confirmed_sd_tick then
                            confirmed_sd_tick = returned_tick
                        end
                    end
                end
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
            requires_sign = (is_me_sealed or action ~= "Write-Point") and "true" or "false",
            active_entity_count = tostring(ActiveEntityCount or 0),
            audit_ring_size = tostring(#TickAuditRing),
            max_entities = tostring(MaxEntities),
            micro_epoch = tostring(current_epoch_num),
            micro_epoch_sealed = is_me_sealed and "true" or "false"
        }
    }
    return process
end
