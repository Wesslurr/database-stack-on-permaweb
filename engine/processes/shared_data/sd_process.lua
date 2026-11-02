-- ==============================================================================
-- Shared Data Process (Hot Backup Replica & ANS-104 Checkpoint Bundler)
-- ==============================================================================
-- ZERO hardcoded peer URLs.
-- Resolves downstream Arbit endpoint (`arbit_url`, `arbit_process`, and
-- `arbit_batch_interval`) dynamically from the Genesis Process specified by
-- `genesis-url` at process spawn.
-- Exposes and records native AO Cryptographic Signers (`ao.signers`, `process.authority`).

ReplicaStore = ReplicaStore or {}
LastTick = LastTick or 0
LastBatchSentTick = LastBatchSentTick or 0
LastRelayedBySigner = LastRelayedBySigner or "unverified"
GenesisUrl = GenesisUrl or nil
Topology = Topology or nil

-- Cascade-of-Relief: `ReplicaStore` keeps the latest value per entity, while
-- `RecentDiffs` holds a bounded rolling window of uncommitted diffs. Once Arbit
-- reports L1 finality, `Prune-Committed-Slots` releases everything at or below
-- the committed tick. Memory is therefore O(entities) + O(MaxDiffSlots).
RecentDiffs = RecentDiffs or {}
DiffSeq = DiffSeq or 0
LastCommittedTick = LastCommittedTick or 0
LastL1TxId = LastL1TxId or ""
LastPrunedTick = LastPrunedTick or 0
PrunedDiffCount = PrunedDiffCount or 0
MaxDiffSlots = MaxDiffSlots or 256

local function json_safe(value)
    local s = tostring(value)
    s = string.gsub(s, "\\", "\\\\")
    s = string.gsub(s, '"', '\\"')
    s = string.gsub(s, "[\r\n\t]", " ")
    return s
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

local function replica_state_root(count)
    return string.format("0xSD_TICK_%d_PTS_%d_SEQ_%d", LastTick, count, DiffSeq)
end

local function extract_first_signer(process_or_msg)
    if not process_or_msg then return nil end
    if type(process_or_msg.authority) == "table" and #process_or_msg.authority > 0 then
        return tostring(process_or_msg.authority[1])
    elseif type(process_or_msg.authority) == "string" and #process_or_msg.authority > 0 then
        return process_or_msg.authority
    end
    if ao and ao.signers then
        local ok, list = pcall(function() return ao.signers(process_or_msg) end)
        if ok and type(list) == "table" and #list > 0 then
            return tostring(list[1])
        end
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
    local genesis_src = (topo and topo.genesis_url) or tostring(GenesisUrl)
    local arbit_target = (topo and topo.arbit_url) or "unresolved"
    local batch_interval = (topo and topo.arbit_batch_interval) or 5
    local node_role = tostring(process["node-role"] or "sd1")

    local sd_signer = extract_first_signer(process) or "sd_authority"
    local assignment_id = extract_msg_id(message)
    local incoming_signer = msg.relayed_by_signer or extract_first_signer(msg)
    if incoming_signer then
        LastRelayedBySigner = incoming_signer
    end

    local res = ""
    if action == "Replicate" then
        local id = msg.id or (msg.query and msg.query.id)
        local x = tonumber(msg.x or (msg.query and msg.query.x) or 0)
        local y = tonumber(msg.y or (msg.query and msg.query.y) or 0)
        local tick = tonumber(msg.tick or (msg.query and msg.query.tick) or 0)

        if id then
            ReplicaStore[id] = { id = id, x = x, y = y, tick = tick }
            if tick > LastTick then LastTick = tick end

            -- Keep a bounded rolling diff window for failover/replay. Committed
            -- slots are released by the Arbit prune cascade, not by unbounded
            -- growth of this table.
            DiffSeq = DiffSeq + 1
            table.insert(RecentDiffs, { seq = DiffSeq, id = tostring(id), x = x, y = y, tick = tick })
            while #RecentDiffs > MaxDiffSlots do
                table.remove(RecentDiffs, 1)
            end
        end

        local count = 0
        for _ in pairs(ReplicaStore) do count = count + 1 end
        local state_root = replica_state_root(count)
        
        -- Decoupled Batch Checkpoint to Arbit Process:
        -- SD updates in REAL-TIME on every single tick (1, 2, 3...) in RAM + LMDB Disk,
        -- while Arbit receives periodic batch checkpoints every `batch_interval` ticks (e.g. 100 ticks)
        -- or early when uncommitted diffs approach MaxDiffSlots capacity (emergency sweet spot guard),
        -- dispatched asynchronously by SD1.
        local uncommitted_count = #RecentDiffs
        local count_trigger = (LastTick % batch_interval == 0)
        local guard_trigger = (uncommitted_count >= MaxDiffSlots)
        local should_batch_arbit = (
            topo and topo.arbit_url and
            node_role == "sd1" and
            LastTick > 0 and
            (count_trigger or guard_trigger) and
            LastTick > LastBatchSentTick
        )
        if should_batch_arbit then
            LastBatchSentTick = LastTick
            local arbit_payload = string.format(
                [[{"action":"Commit-Checkpoint","last_tick":%d,"point_count":%d,"state_root":"%s","checkpoint_signer":"%s","sd_node":"%s","diff_seq":%d}]],
                LastTick, count, json_safe(state_root), json_safe(sd_signer), json_safe(node_role), DiffSeq
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

        res = string.format(
            [[{"status":"replicated","tier":"shared_data_replica","persistence":"ram_latest+lmdb_disk","requires_sign":true,"sd_signer":"%s","relayed_by_signer":"%s","assignment_id":"%s","topology_source":"%s","arbit_target":"%s","arbit_batch_interval":%d,"last_batch_sent_tick":%d,"point_id":"%s","last_tick":%d,"count":%d,"state_root":"%s","last_committed_tick":%d,"last_l1_tx_id":"%s","last_pruned_tick":%d,"pruned_diff_count":%d,"recent_diff_count":%d,"max_diff_slots":%d,"recent_diffs":%s,"points":%s}]],
            sd_signer, LastRelayedBySigner, assignment_id, genesis_src, arbit_target, batch_interval, LastBatchSentTick, json_safe(id), LastTick, count, json_safe(state_root), LastCommittedTick, json_safe(LastL1TxId), LastPrunedTick, PrunedDiffCount, #RecentDiffs, MaxDiffSlots, encode_diff_tail(5), format_replica_points()
        )
    elseif action == "Prune-Committed-Slots" then
        local committed_tick = tonumber(msg.committed_tick or (msg.query and msg.query.committed_tick) or 0)
        local l1_tx_id = msg.l1_tx_id or (msg.query and msg.query.l1_tx_id) or ""
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
            sd_signer, assignment_id, genesis_src, committed_tick, LastCommittedTick, json_safe(LastL1TxId), LastPrunedTick, removed, PrunedDiffCount, #RecentDiffs, MaxDiffSlots, count, LastTick
        )
    else
        local count = 0
        for _ in pairs(ReplicaStore) do count = count + 1 end
        res = string.format(
            [[{"status":"ok","tier":"shared_data_replica","persistence":"ram_latest+lmdb_disk","requires_sign":true,"sd_signer":"%s","relayed_by_signer":"%s","assignment_id":"%s","topology_source":"%s","arbit_target":"%s","arbit_batch_interval":%d,"last_batch_sent_tick":%d,"last_tick":%d,"count":%d,"state_root":"%s","last_committed_tick":%d,"last_l1_tx_id":"%s","last_pruned_tick":%d,"pruned_diff_count":%d,"recent_diff_count":%d,"max_diff_slots":%d,"recent_diffs":%s,"points":%s}]],
            sd_signer, LastRelayedBySigner, assignment_id, genesis_src, arbit_target, batch_interval, LastBatchSentTick, LastTick, count, json_safe(replica_state_root(count)), LastCommittedTick, json_safe(LastL1TxId), LastPrunedTick, PrunedDiffCount, #RecentDiffs, MaxDiffSlots, encode_diff_tail(5), format_replica_points()
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
            max_diff_slots = tostring(MaxDiffSlots)
        }
    }
    return process
end
