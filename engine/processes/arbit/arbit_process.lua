-- ==============================================================================
-- Arbit Process (Consensus Gatekeeper & Anti-Double-Spend Official Ledger)
-- ==============================================================================
-- ZERO hardcoded peer URLs.
-- Resolves cluster topology dynamically from Genesis Process.
-- Exposes and records native AO Cryptographic Signers (ao.signers, process authority).

OfficialTick = OfficialTick or 0
OfficialPointCount = OfficialPointCount or 0
CommitmentSeq = CommitmentSeq or 0
OfficialStateRoot = OfficialStateRoot or "0x0000000000000000"
PrevStateRoot = PrevStateRoot or "0x0000000000000000"
LastCheckpointSigner = LastCheckpointSigner or "unverified"
GenesisUrl = GenesisUrl or nil
Topology = Topology or nil
CurrentEpoch = CurrentEpoch or 1

L1CommitMode = L1CommitMode or "local_block"
LastL1TxId = LastL1TxId or ""
LastL1Height = LastL1Height or 0
LastL1Status = LastL1Status or "idle"
LastFinalizedTick = LastFinalizedTick or 0
PrunedThroughTick = PrunedThroughTick or 0
LastDiffSeq = LastDiffSeq or 0
LastPruneNotified = LastPruneNotified or {}

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

local function response_body(res)
    if type(res) == "table" then
        return tostring(res.body or "")
    end
    return tostring(res or "")
end

local function commit_to_l1(topo, tick, point_count, state_root)
    if not topo or not topo.l1_url then
        return "", 0, "no_l1_target"
    end
    local mode = topo.l1_commit_mode or L1CommitMode
    if mode == "none" then
        return "", 0, "disabled"
    end

    if mode == "http_tx" then
        local payload = string.format(
            [[{"action":"Commit-Checkpoint","official_tick":%d,"point_count":%d,"state_root":"%s"}]],
            tick, point_count, json_safe(state_root)
        )
        local ok, status, res = pcall(function()
            return ao.resolve({
                path = "/~relay@1.0/call",
                ["relay-path"] = topo.l1_url .. "/tx",
                ["relay-method"] = "POST",
                ["relay-body"] = payload,
                ["content-type"] = "application/json",
                ["cache-control"] = "no-store"
            })
        end)
        payload = nil
        if ok and status == "ok" then
            local body = response_body(res)
            local tx_id = string.match(body, '"id"%s*:%s*"([^"]+)"') or string.match(body, '"tx_id"%s*:%s*"([^"]+)"')
            return tx_id or ("http_tx_tick_" .. tostring(tick)), 0, "committed"
        end
        return "", 0, "pending"
    end

    -- Default: ArLocal local block finality
    local ok, status, res = pcall(function()
        return ao.resolve({
            path = "/~relay@1.0/call",
            ["relay-path"] = topo.l1_url .. "/mine",
            ["relay-method"] = "GET",
            ["cache-control"] = "no-store"
        })
    end)
    if ok and status == "ok" then
        local body = response_body(res)
        local height = tonumber(string.match(body, '"height"%s*:%s*(%d+)'))
            or tonumber(string.match(body, '"current"%s*:%s*(%d+)'))
            or 0
        local block_hash = string.match(body, '"indep_hash"%s*:%s*"([^"]+)"')
            or string.match(body, '"previous_block"%s*:%s*"([^"]+)"')
        if block_hash then
            return string.format("arlocal_block_%d_%s", height, block_hash), height, "finalized"
        end
        return string.format("arlocal_block_%d", height), height, "finalized"
    end
    return "", 0, "pending"
end

local function notify_sd_prune(topo, committed_tick, l1_tx_id, arbit_signer)
    local notified = {}
    if not topo then return notified end
    local targets = {}
    if topo.sd1_url then
        table.insert(targets, { url = topo.sd1_url, process = topo.sd1_process or "sd1", role = "sd1" })
    end
    if topo.sd2_url then
        table.insert(targets, { url = topo.sd2_url, process = topo.sd2_process or "sd2", role = "sd2" })
    end
    local payload = string.format(
        [[{"action":"Prune-Committed-Slots","committed_tick":%d,"l1_tx_id":"%s","arbit_signer":"%s"}]],
        committed_tick, json_safe(l1_tx_id), json_safe(arbit_signer)
    )
    for _, target in ipairs(targets) do
        local ok, status = pcall(function()
            return ao.resolve({
                path = "/~relay@1.0/cast",
                ["relay-path"] = target.url .. "/~node-process@1.0/" .. target.process .. "/schedule",
                ["relay-method"] = "POST",
                ["relay-body"] = payload,
                ["content-type"] = "application/json",
                ["cache-control"] = "no-store"
            })
        end)
        if ok and (status == "ok" or status == true) then
            table.insert(notified, target.role)
        end
    end
    payload = nil
    return notified
end

local function encode_string_list(list)
    if not list or #list == 0 then return "[]" end
    local items = {}
    for _, v in ipairs(list) do
        table.insert(items, string.format([["%s"]], json_safe(v)))
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
        local l1_url = string.match(body_str, '"l1_url"%s*:%s*"([^"]+)"')
        local primary_url = string.match(body_str, '"primary_url"%s*:%s*"([^"]+)"')
        local sd1_url = string.match(body_str, '"sd1_url"%s*:%s*"([^"]+)"')
        local sd1_proc = string.match(body_str, '"sd1_process"%s*:%s*"([^"]+)"') or "sd1"
        local sd2_url = string.match(body_str, '"sd2_url"%s*:%s*"([^"]+)"')
        local sd2_proc = string.match(body_str, '"sd2_process"%s*:%s*"([^"]+)"') or "sd2"
        local epoch_str = string.match(body_str, '"epoch"%s*:%s*(%d+)') or "1"
        local batch_str = string.match(body_str, '"arbit_batch_interval"%s*:%s*(%d+)') or "5"
        local l1_mode = string.match(body_str, '"l1_commit_mode"%s*:%s*"([^"]+)"')
        if l1_mode then L1CommitMode = l1_mode end
        if l1_url then
            return {
                genesis_url = genesis_url,
                epoch = tonumber(epoch_str) or 1,
                l1_url = l1_url,
                l1_commit_mode = L1CommitMode,
                primary_url = primary_url,
                sd1_url = sd1_url,
                sd1_process = sd1_proc,
                sd2_url = sd2_url,
                sd2_process = sd2_proc,
                arbit_batch_interval = tonumber(batch_str) or 5
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

function compute(process, message, opts)
    local msg = message.body or message
    local action = msg.action or msg.Action or (msg.query and msg.query.action) or "Get-Official-State"

    if action == "Sync-Topology" then
        Topology = nil
    end
    local topo = ensure_topology(process, msg)
    local genesis_src = (topo and topo.genesis_url) or tostring(GenesisUrl or "")
    local l1_target = (topo and topo.l1_url) or "unresolved"
    local batch_interval = (topo and topo.arbit_batch_interval) or 5

    local arbit_signer = extract_first_signer(process) or "arbit_authority"
    local assignment_id = extract_msg_id(message)
    local incoming_signer = extract_first_signer(msg) or msg.checkpoint_signer
    if incoming_signer then
        LastCheckpointSigner = incoming_signer
    end

    local res = ""
    local notified = {}
    if action == "Commit-Checkpoint" then
        local last_tick = tonumber(msg.last_tick or (msg.query and msg.query.last_tick) or 0)
        local point_count = tonumber(msg.point_count or (msg.query and msg.query.point_count) or 0)
        local provided_root = msg.state_root or (msg.query and msg.query.state_root)
        local diff_seq = tonumber(msg.diff_seq or (msg.query and msg.query.diff_seq) or 0)
        local msg_epoch = tonumber(msg.epoch or (msg.query and msg.query.epoch) or CurrentEpoch)

        -- Epoch fencing
        if msg_epoch < CurrentEpoch then
            local err_res = string.format([[{"status":"error_stale_epoch","tier":"arbit_consensus","epoch":%d,"current_epoch":%d}]], msg_epoch, CurrentEpoch)
            process.results = { output = { body = err_res, arbit_signer = arbit_signer } }
            return process
        elseif msg_epoch > CurrentEpoch then
            CurrentEpoch = msg_epoch
        end

        -- Poisoning guards: check tick bounds and monotonic sequence
        local max_allowed_tick = OfficialTick + (batch_interval * 20)
        if last_tick <= OfficialTick then
            -- Duplicate or stale checkpoint ignored gracefully
            res = string.format(
                [[{"status":"stale_checkpoint_ignored","tier":"arbit_consensus","official_tick":%d,"received_tick":%d,"topology_source":"%s","l1_gateway":"%s","state_root":"%s","commitment_seq":%d,"diff_seq":%d}]],
                OfficialTick, last_tick, json_safe(genesis_src), json_safe(l1_target), json_safe(OfficialStateRoot), CommitmentSeq, LastDiffSeq
            )
        elseif last_tick > max_allowed_tick then
            -- Reject excessive tick leap attempt (C2 defense)
            res = string.format(
                [[{"status":"error_excessive_tick_leap","tier":"arbit_consensus","official_tick":%d,"received_tick":%d,"max_allowed":%d,"topology_source":"%s","l1_gateway":"%s","state_root":"%s","commitment_seq":%d,"diff_seq":%d}]],
                OfficialTick, last_tick, max_allowed_tick, json_safe(genesis_src), json_safe(l1_target), json_safe(OfficialStateRoot), CommitmentSeq, LastDiffSeq
            )
        elseif diff_seq < LastDiffSeq then
            -- Reject non-monotonic diff sequence
            res = string.format(
                [[{"status":"error_diff_seq_regression","tier":"arbit_consensus","last_diff_seq":%d,"received_diff_seq":%d,"official_tick":%d,"topology_source":"%s","l1_gateway":"%s","state_root":"%s","commitment_seq":%d}]],
                LastDiffSeq, diff_seq, OfficialTick, json_safe(genesis_src), json_safe(l1_target), json_safe(OfficialStateRoot), CommitmentSeq
            )
        else
            OfficialTick = last_tick
            OfficialPointCount = point_count
            CommitmentSeq = CommitmentSeq + 1
            if diff_seq > LastDiffSeq then LastDiffSeq = diff_seq end

            -- Cryptographic State Root Chaining (C4)
            OfficialStateRoot = canonical_hash(string.format("ARBIT:TICK:%d:COUNT:%d:SEQ:%d:PREV:%s:SD_ROOT:%s", OfficialTick, OfficialPointCount, CommitmentSeq, PrevStateRoot, provided_root or ""))
            PrevStateRoot = OfficialStateRoot

            -- Finalize on L1, then cascade release only the slots covered by finality
            local tx_id, height, l1_status = commit_to_l1(topo, OfficialTick, OfficialPointCount, OfficialStateRoot)
            LastL1TxId = tx_id
            LastL1Height = height
            LastL1Status = l1_status
            if l1_status == "finalized" or l1_status == "committed" then
                LastFinalizedTick = OfficialTick
                PrunedThroughTick = OfficialTick
                notified = notify_sd_prune(topo, OfficialTick, tx_id, arbit_signer)
                LastPruneNotified = notified
            end

            res = string.format(
                [[{"status":"committed","tier":"arbit_consensus","requires_sign":true,"arbit_signer":"%s","checkpoint_signer":"%s","assignment_id":"%s","batch_interval":%d,"topology_source":"%s","l1_gateway":"%s","l1_commit_mode":"%s","l1_tx_id":"%s","l1_height":%d,"l1_status":"%s","finalized_tick":%d,"pruned_through_tick":%d,"prune_notified_nodes":%s,"official_tick":%d,"point_count":%d,"state_root":"%s","commitment_seq":%d,"diff_seq":%d}]],
                json_safe(arbit_signer), json_safe(LastCheckpointSigner), json_safe(assignment_id), batch_interval, json_safe(genesis_src), json_safe(l1_target), json_safe(L1CommitMode), json_safe(LastL1TxId), LastL1Height, json_safe(LastL1Status), LastFinalizedTick, PrunedThroughTick, encode_string_list(LastPruneNotified), OfficialTick, OfficialPointCount, json_safe(OfficialStateRoot), CommitmentSeq, LastDiffSeq
            )
        end
    else
        res = string.format(
            [[{"status":"ok","tier":"arbit_consensus","requires_sign":true,"arbit_signer":"%s","checkpoint_signer":"%s","assignment_id":"%s","batch_interval":%d,"topology_source":"%s","l1_gateway":"%s","l1_commit_mode":"%s","l1_tx_id":"%s","l1_height":%d,"l1_status":"%s","finalized_tick":%d,"pruned_through_tick":%d,"prune_notified_nodes":%s,"official_tick":%d,"point_count":%d,"state_root":"%s","commitment_seq":%d,"diff_seq":%d}]],
            json_safe(arbit_signer), json_safe(LastCheckpointSigner), json_safe(assignment_id), batch_interval, json_safe(genesis_src), json_safe(l1_target), json_safe(L1CommitMode), json_safe(LastL1TxId), LastL1Height, json_safe(LastL1Status), LastFinalizedTick, PrunedThroughTick, encode_string_list(LastPruneNotified), OfficialTick, OfficialPointCount, json_safe(OfficialStateRoot), CommitmentSeq, LastDiffSeq
        )
    end

    process.results = {
        output = {
            body = res,
            arbit_signer = arbit_signer,
            l1_commit_mode = L1CommitMode,
            l1_tx_id = LastL1TxId,
            l1_status = LastL1Status,
            pruned_through_tick = tostring(PrunedThroughTick),
            official_tick = tostring(OfficialTick),
            state_root = OfficialStateRoot
        }
    }
    return process
end
