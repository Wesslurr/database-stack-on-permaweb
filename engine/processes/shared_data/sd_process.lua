-- ==============================================================================
-- Shared Data Process (Hot Backup Replica & ANS-104 Checkpoint Bundler)
-- ==============================================================================
-- ZERO hardcoded peer URLs.
-- Resolves downstream Arbit endpoint dynamically from Genesis Process.
-- Exposes and records native AO Cryptographic Signers (ao.signers, process authority).

ReplicaStore = ReplicaStore or {}
LastTick = LastTick or 0
LastBatchSentTick = LastBatchSentTick or 0
LastRelayedBySigner = LastRelayedBySigner or "unverified"
GenesisUrl = GenesisUrl or nil
Topology = Topology or nil
CurrentEpoch = CurrentEpoch or 1
PrevStateRoot = PrevStateRoot or "0x0000000000000000"

RecentDiffs = RecentDiffs or {}
DiffSeq = DiffSeq or 0
LastCommittedTick = LastCommittedTick or 0
LastL1TxId = LastL1TxId or ""
LastPrunedTick = LastPrunedTick or 0
PrunedDiffCount = PrunedDiffCount or 0
MaxDiffSlots = MaxDiffSlots or 256

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

local function prune_diffs_through(tick)
    local removed = 0
    while #RecentDiffs > 0 and RecentDiffs[1].tick <= tick do
        table.remove(RecentDiffs, 1)
        removed = removed + 1
    end
    if removed > 0 then
        PrunedDiffCount = PrunedDiffCount + removed
        LastPrunedTick = math.max(LastPrunedTick, tick)
    end
    return removed
end

local function encode_diff_tail(limit)
    local items = {}
    local start = math.max(1, #RecentDiffs - limit + 1)
    for i = start, #RecentDiffs do
        local d = RecentDiffs[i]
        table.insert(items, string.format(
            [[{"seq":%d,"tick":%d,"id":"%s","x":%s,"y":%s}]],
            d.seq, d.tick, json_safe(d.id), tostring(d.x), tostring(d.y)
        ))
    end
    return "[" .. table.concat(items, ",") .. "]"
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

local function extract_msg_id(msg_obj)
    if not msg_obj or not ao or not ao.id then return "uncommitted" end
    local ok, mid = pcall(function() return ao.id(msg_obj) end)
    if ok and mid then return tostring(mid) end
    return "uncommitted"
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
        local epoch_str = string.match(body_str, '"epoch"%s*:%s*(%d+)') or "1"
        local batch_str = string.match(body_str, '"arbit_batch_interval"%s*:%s*(%d+)') or "5"
        local diff_slots_str = string.match(body_str, '"sd_diff_slots"%s*:%s*(%d+)')
        if diff_slots_str then MaxDiffSlots = tonumber(diff_slots_str) or MaxDiffSlots end
        if arbit_url then
            return {
                genesis_url = genesis_url,
                epoch = tonumber(epoch_str) or 1,
                arbit_url = arbit_url,
                arbit_process = arbit_proc,
                primary_url = primary_url,
                arbit_batch_interval = tonumber(batch_str) or 5,
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

local function format_replica_points()
    local items = {}
    for _, p in pairs(ReplicaStore) do
        table.insert(items, string.format([[{"id":"%s","x":%s,"y":%s,"tick":%d}]], json_safe(p.id), tostring(p.x), tostring(p.y), p.tick))
    end
    return "[" .. table.concat(items, ",") .. "]"
end

function compute(process, message, opts)
    local msg = message.body or message
    local action = msg.action or msg.Action or (msg.query and msg.query.action) or "Get-Replica-State"

    if action == "Sync-Topology" then
        Topology = nil
    end
    local topo = ensure_topology(process, msg)
    local genesis_src = (topo and topo.genesis_url) or tostring(GenesisUrl or "")
    local arbit_target = (topo and topo.arbit_url) or "unresolved"
    local batch_interval = (topo and topo.arbit_batch_interval) or 5
    local node_role = tostring(process["node-role"] or "sd1")

    local sd_signer = extract_first_signer(process) or "sd_authority"
    local assignment_id = extract_msg_id(message)
    local incoming_signer = extract_first_signer(msg) or msg.relayed_by_signer
    if incoming_signer then
        LastRelayedBySigner = incoming_signer
    end

    -- Epoch fencing: check incoming epoch if provided
    local msg_epoch = tonumber(msg.epoch or (msg.query and msg.query.epoch))
    if msg_epoch then
        if msg_epoch < CurrentEpoch then
            local err_res = string.format([[{"status":"error_stale_epoch","tier":"shared_data_replica","epoch":%d,"current_epoch":%d}]], msg_epoch, CurrentEpoch)
            process.results = { output = { body = err_res, last_tick = tostring(LastTick), sd_signer = sd_signer } }
            return process
        elseif msg_epoch > CurrentEpoch then
            CurrentEpoch = msg_epoch
        end
    end

    local res = ""
    if action == "Replicate" then
        local id = msg.id or (msg.query and msg.query.id)
        local raw_x = msg.x or (msg.query and msg.query.x) or 0
        local raw_y = msg.y or (msg.query and msg.query.y) or 0
        local x = tonumber(raw_x) or 0
        local y = tonumber(raw_y) or 0
        local tick = tonumber(msg.tick or (msg.query and msg.query.tick) or 0)

        local is_stale_update = false
        if id then
            local existing = ReplicaStore[id]
            -- Monotonic tick check per entity: reject or ignore stale out-of-order ticks
            if existing and tick <= existing.tick then
                is_stale_update = true
            else
                ReplicaStore[id] = { id = id, x = x, y = y, tick = tick }
                if tick > LastTick then LastTick = tick end

                DiffSeq = DiffSeq + 1
                table.insert(RecentDiffs, { seq = DiffSeq, id = tostring(id), x = x, y = y, tick = tick })
                while #RecentDiffs > MaxDiffSlots do
                    table.remove(RecentDiffs, 1)
                end
            end
        end

        local count = 0
        for _ in pairs(ReplicaStore) do count = count + 1 end
        local state_root = canonical_hash(string.format("SD_TICK:%d:COUNT:%d:SEQ:%d:PREV:%s", LastTick, count, DiffSeq, PrevStateRoot))
        PrevStateRoot = state_root

        -- Periodic or early threshold batch checkpoint to Arbit
        local uncommitted_count = #RecentDiffs
        local count_trigger = (LastTick % batch_interval == 0) or (LastTick >= 5 and LastBatchSentTick == 0)
        local guard_trigger = (uncommitted_count >= MaxDiffSlots)
        local is_checkpoint_sender = (node_role == "sd1") or (node_role == "sd2" and (LastTick - LastBatchSentTick >= batch_interval * 2))

        local should_batch_arbit = (
            topo and topo.arbit_url and
            is_checkpoint_sender and
            LastTick > 0 and
            (count_trigger or guard_trigger) and
            LastTick > LastBatchSentTick
        )
        if should_batch_arbit then
            LastBatchSentTick = LastTick
            local arbit_payload = string.format(
                [[{"action":"Commit-Checkpoint","last_tick":%d,"point_count":%d,"state_root":"%s","checkpoint_signer":"%s","sd_node":"%s","diff_seq":%d,"epoch":%d}]],
                LastTick, count, json_safe(state_root), json_safe(sd_signer), json_safe(node_role), DiffSeq, CurrentEpoch
            )
            pcall(function()
                ao.resolve({
                    path = "/~relay@1.0/cast",
                    ["relay-path"] = topo.arbit_url .. "/~node-process@1.0/" .. topo.arbit_process .. "/schedule",
                    ["relay-method"] = "POST",
                    ["relay-body"] = arbit_payload,
                    ["content-type"] = "application/json",
                    ["cache-control"] = "no-store"
                })
            end)
            arbit_payload = nil
        end

        local is_me_sealed = (msg.micro_epoch_sealed == true or msg.micro_epoch_sealed == "true" or (tick % 10 == 0))
        local me_num = tonumber(msg.micro_epoch_num) or math.floor(tick / 10)
        local me_root = msg.micro_epoch_root or state_root

        local status_str = is_stale_update and "stale_tick_ignored" or "replicated"

        res = string.format(
            [[{"status":"%s","tier":"shared_data_replica","persistence":"ram_latest+lmdb_disk","requires_sign":true,"sd_signer":"%s","relayed_by_signer":"%s","assignment_id":"%s","topology_source":"%s","arbit_target":"%s","arbit_batch_interval":%d,"last_batch_sent_tick":%d,"point_id":"%s","last_tick":%d,"count":%d,"state_root":"%s","last_committed_tick":%d,"last_l1_tx_id":"%s","last_pruned_tick":%d,"pruned_diff_count":%d,"recent_diff_count":%d,"max_diff_slots":%d,"micro_epoch":%d,"micro_epoch_sealed":%s,"micro_epoch_root":"%s","recent_diffs":%s,"points":%s}]],
            status_str, json_safe(sd_signer), json_safe(LastRelayedBySigner), json_safe(assignment_id), json_safe(genesis_src), json_safe(arbit_target), batch_interval, LastBatchSentTick, json_safe(id), LastTick, count, json_safe(state_root), LastCommittedTick, json_safe(LastL1TxId), LastPrunedTick, PrunedDiffCount, #RecentDiffs, MaxDiffSlots, me_num, is_me_sealed and "true" or "false", json_safe(me_root), encode_diff_tail(5), format_replica_points()
        )
    elseif action == "Prune-Committed-Slots" then
        local committed_tick = tonumber(msg.committed_tick or (msg.query and msg.query.committed_tick) or 0)
        local l1_tx_id = msg.l1_tx_id or (msg.query and msg.query.l1_tx_id) or ""

        -- Prune authentication & safety check: cannot prune future or uncommitted ticks
        if committed_tick > LastTick or l1_tx_id == "" then
            local err_res = string.format([[{"status":"error_invalid_prune_request","tier":"shared_data_replica","last_tick":%d,"requested_tick":%d}]], LastTick, committed_tick)
            process.results = { output = { body = err_res, last_tick = tostring(LastTick), sd_signer = sd_signer } }
            return process
        end

        if committed_tick > LastCommittedTick then
            LastCommittedTick = committed_tick
        end
        if l1_tx_id ~= "" then
            LastL1TxId = tostring(l1_tx_id)
        end
        LastPrunedTick = math.max(LastPrunedTick, committed_tick)
        local removed = prune_diffs_through(committed_tick)

        local count = 0
        for _ in pairs(ReplicaStore) do count = count + 1 end
        res = string.format(
            [[{"status":"pruned","tier":"shared_data_replica","persistence":"ram_latest+lmdb_disk","requires_sign":true,"sd_signer":"%s","assignment_id":"%s","topology_source":"%s","committed_tick":%d,"last_committed_tick":%d,"last_l1_tx_id":"%s","last_pruned_tick":%d,"pruned_now":%d,"pruned_diff_count":%d,"recent_diff_count":%d,"max_diff_slots":%d,"count":%d,"last_tick":%d}]],
            json_safe(sd_signer), json_safe(assignment_id), json_safe(genesis_src), committed_tick, LastCommittedTick, json_safe(LastL1TxId), LastPrunedTick, removed, PrunedDiffCount, #RecentDiffs, MaxDiffSlots, count, LastTick
        )
    elseif action == "Read-Point" then
        local id = msg.id or (msg.query and msg.query.id)
        local p = id and ReplicaStore[tostring(id)]
        if p then
            res = string.format(
                [[{"status":"ok","tier":"shared_data_replica","persistence":"ram_latest+lmdb_disk","requires_sign":true,"sd_signer":"%s","point":{"id":"%s","x":%s,"y":%s,"tick":%d}}]],
                json_safe(sd_signer), json_safe(p.id), tostring(p.x), tostring(p.y), p.tick
            )
        else
            res = string.format(
                [[{"status":"not_found","tier":"shared_data_replica","persistence":"ram_latest+lmdb_disk","requires_sign":true,"sd_signer":"%s","id":"%s"}]],
                json_safe(sd_signer), json_safe(id)
            )
        end
    else
        local count = 0
        for _ in pairs(ReplicaStore) do count = count + 1 end
        local state_root = canonical_hash(string.format("SD_TICK:%d:COUNT:%d:SEQ:%d:PREV:%s", LastTick, count, DiffSeq, PrevStateRoot))
        res = string.format(
            [[{"status":"ok","tier":"shared_data_replica","persistence":"ram_latest+lmdb_disk","requires_sign":true,"sd_signer":"%s","relayed_by_signer":"%s","assignment_id":"%s","topology_source":"%s","arbit_target":"%s","arbit_batch_interval":%d,"last_batch_sent_tick":%d,"last_tick":%d,"count":%d,"state_root":"%s","last_committed_tick":%d,"last_l1_tx_id":"%s","last_pruned_tick":%d,"pruned_diff_count":%d,"recent_diff_count":%d,"max_diff_slots":%d,"recent_diffs":%s,"points":%s}]],
            json_safe(sd_signer), json_safe(LastRelayedBySigner), json_safe(assignment_id), json_safe(genesis_src), json_safe(arbit_target), batch_interval, LastBatchSentTick, LastTick, count, json_safe(state_root), LastCommittedTick, json_safe(LastL1TxId), LastPrunedTick, PrunedDiffCount, #RecentDiffs, MaxDiffSlots, encode_diff_tail(5), format_replica_points()
        )
    end

    process.results = {
        output = {
            body = res,
            last_tick = tostring(LastTick),
            sd_signer = sd_signer,
            last_committed_tick = tostring(LastCommittedTick),
            last_l1_tx_id = LastL1TxId,
            last_pruned_tick = tostring(LastPrunedTick),
            recent_diff_count = tostring(#RecentDiffs),
            max_diff_slots = tostring(MaxDiffSlots),
            micro_epoch = tostring(math.floor(LastTick / 10)),
            micro_epoch_sealed = (LastTick % 10 == 0) and "true" or "false"
        }
    }
    return process
end
