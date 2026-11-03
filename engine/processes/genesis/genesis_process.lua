-- ==============================================================================
-- Genesis Process (Master Cluster Registry, Node Selector & Discovery Authority)
-- ==============================================================================
-- Single Source of Truth for all node endpoints and batch policy in the cluster.
-- Worker processes (Primary, SD1, SD2, Arbit) contain ZERO hardcoded peer URLs;
-- they discover and sync all peer endpoints exclusively from this Genesis Process.

ClusterNodePool = ClusterNodePool or {
    primary_candidates = {
        { url = "http://127.0.0.1:8734", process = "primary" }
    },
    sd_candidates = {
        { url = "http://127.0.0.1:8735", process = "sd1" },
        { url = "http://127.0.0.1:8736", process = "sd2" }
    },
    arbit_candidates = {
        { url = "http://127.0.0.1:8737", process = "arbit" }
    },
    l1_gateway = "http://127.0.0.1:1984",
    arbit_batch_interval = 100,
    l1_commit_mode = "local_block",
    primary_max_entities = 1000,
    primary_audit_ring = 50,
    sd_snapshot_slots = 20,
    sd_diff_slots = 512
}

CurrentEpoch = CurrentEpoch or 1
ArbitBatchInterval = ArbitBatchInterval or ClusterNodePool.arbit_batch_interval or 100
L1CommitMode = L1CommitMode or ClusterNodePool.l1_commit_mode or "local_block"
PrimaryMaxEntities = PrimaryMaxEntities or ClusterNodePool.primary_max_entities or 1000
PrimaryAuditRing = PrimaryAuditRing or ClusterNodePool.primary_audit_ring or 50
SdSnapshotSlots = SdSnapshotSlots or ClusterNodePool.sd_snapshot_slots or 20
SdDiffSlots = SdDiffSlots or ClusterNodePool.sd_diff_slots or 512

-- Active topology selected by Genesis from the candidate pool
SelectedPrimary = SelectedPrimary or ClusterNodePool.primary_candidates[1]
SelectedSd1     = SelectedSd1     or ClusterNodePool.sd_candidates[1]
SelectedSd2     = SelectedSd2     or ClusterNodePool.sd_candidates[2]
SelectedArbit   = SelectedArbit   or ClusterNodePool.arbit_candidates[1]
SelectedL1Url   = SelectedL1Url   or ClusterNodePool.l1_gateway
AdminKey        = AdminKey        or "hyperstack_admin_genesis_key"
AdminSigner     = AdminSigner     or "genesis_admin_signer"

local function json_safe(value)
    if value == nil then return "" end
    local s = tostring(value)
    s = string.gsub(s, "\\", "\\\\")
    s = string.gsub(s, '"', '\\"')
    s = string.gsub(s, "[\r\n\t\b\f]", " ")
    return s
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

local function is_valid_url(url)
    if type(url) ~= "string" or #url == 0 or #url > 256 then return false end
    -- Whitelist http and https URLs with valid hostname/port, no quotes or newlines
    local match = string.match(url, "^https?://[%w%.%-%:]+$")
    return match ~= nil
end

local function is_valid_process_name(name)
    if type(name) ~= "string" or #name == 0 or #name > 64 then return false end
    local match = string.match(name, "^[%w%_%-]+$")
    return match ~= nil
end

local function encode_topology_json(genesis_signer)
    return string.format(
        [[{"status":"ok","requires_sign":true,"epoch":%d,"genesis_signer":"%s","arbit_batch_interval":%d,"primary_url":"%s","primary_process":"%s","sd1_url":"%s","sd1_process":"%s","sd2_url":"%s","sd2_process":"%s","arbit_url":"%s","arbit_process":"%s","l1_url":"%s","l1_commit_mode":"%s","primary_max_entities":%d,"primary_audit_ring":%d,"sd_snapshot_slots":%d,"sd_diff_slots":%d}]],
        CurrentEpoch,
        json_safe(genesis_signer),
        ArbitBatchInterval,
        json_safe(SelectedPrimary.url), json_safe(SelectedPrimary.process),
        json_safe(SelectedSd1.url), json_safe(SelectedSd1.process),
        json_safe(SelectedSd2.url), json_safe(SelectedSd2.process),
        json_safe(SelectedArbit.url), json_safe(SelectedArbit.process),
        json_safe(SelectedL1Url),
        json_safe(L1CommitMode), PrimaryMaxEntities, PrimaryAuditRing, SdSnapshotSlots, SdDiffSlots
    )
end

function compute(process, message, opts)
    local msg = message.body or message
    local action = msg.action or msg.Action or (msg.query and msg.query.action) or "Get-Topology"
    local genesis_signer = extract_first_signer(process) or "genesis_authority"

    if action == "Register-Node" then
        local caller_key = msg.admin_key or (msg.query and msg.query.admin_key)
        local caller_signer = msg.caller_signer or (msg.query and msg.query.caller_signer)
        local is_authorized = false

        if AdminKey and caller_key == AdminKey then
            is_authorized = true
        elseif AdminSigner and caller_signer == AdminSigner then
            is_authorized = true
        end

        if not is_authorized then
            local err_res = string.format([[{"status":"error_unauthorized","tier":"genesis_registry","reason":"unauthorized_caller","epoch":%d,"primary_url":"%s"}]], CurrentEpoch, json_safe(SelectedPrimary.url))
            process.results = {
                output = {
                    body = err_res,
                    epoch = tostring(CurrentEpoch),
                    genesis_signer = genesis_signer,
                    primary_url = SelectedPrimary.url
                }
            }
            return process
        end

        local role = msg.role or (msg.query and msg.query.role)
        local url = msg.url or (msg.query and msg.query.url)
        local proc_name = msg.process_name or (msg.query and msg.query.process_name)

        if not is_valid_url(url) then
            local err_res = string.format([[{"status":"error_invalid_url","tier":"genesis_registry","epoch":%d,"primary_url":"%s"}]], CurrentEpoch, json_safe(SelectedPrimary.url))
            process.results = { output = { body = err_res, epoch = tostring(CurrentEpoch), primary_url = SelectedPrimary.url } }
            return process
        end

        if proc_name and not is_valid_process_name(proc_name) then
            local err_res = string.format([[{"status":"error_invalid_process_name","tier":"genesis_registry","epoch":%d,"primary_url":"%s"}]], CurrentEpoch, json_safe(SelectedPrimary.url))
            process.results = { output = { body = err_res, epoch = tostring(CurrentEpoch), primary_url = SelectedPrimary.url } }
            return process
        end

        local updated = false
        if role == "primary" then
            SelectedPrimary = { url = url, process = proc_name or "primary" }
            updated = true
        elseif role == "sd1" then
            SelectedSd1 = { url = url, process = proc_name or "sd1" }
            updated = true
        elseif role == "sd2" then
            SelectedSd2 = { url = url, process = proc_name or "sd2" }
            updated = true
        elseif role == "arbit" then
            SelectedArbit = { url = url, process = proc_name or "arbit" }
            updated = true
        end

        if updated then
            CurrentEpoch = CurrentEpoch + 1
        end
    end

    local res = encode_topology_json(genesis_signer)

    process.results = {
        output = {
            body = res,
            epoch = tostring(CurrentEpoch),
            genesis_signer = genesis_signer,
            requires_sign = "true",
            arbit_batch_interval = tostring(ArbitBatchInterval),
            primary_url = SelectedPrimary.url,
            primary_process = SelectedPrimary.process,
            sd1_url = SelectedSd1.url,
            sd1_process = SelectedSd1.process,
            sd2_url = SelectedSd2.url,
            sd2_process = SelectedSd2.process,
            arbit_url = SelectedArbit.url,
            arbit_process = SelectedArbit.process,
            l1_url = SelectedL1Url,
            l1_commit_mode = L1CommitMode,
            primary_max_entities = tostring(PrimaryMaxEntities),
            primary_audit_ring = tostring(PrimaryAuditRing),
            sd_snapshot_slots = tostring(SdSnapshotSlots),
            sd_diff_slots = tostring(SdDiffSlots)
        }
    }
    return process
end
