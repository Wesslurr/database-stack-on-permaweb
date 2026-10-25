--[[
========================================================================
AO Process: Stack Genesis, Registry, & Lifecycle Control Plane
========================================================================
Protocol: hydb.genesis@1.0
Responsibilities:
1. Root of trust & canonical stack constitution (StackId, GenesisHash, ConfigEpoch).
2. Stack-level configuration and topology registry (AuthorizedNodes, AuthorizedAdmins).
3. Configuration and binding of internal stack AO processes (ArbitratorProcessId).
4. Cold-start node discovery, registration (RegisterNode), and heartbeat tracking (ReportHeartbeat).
5. Observability and quorum readiness state machine (GetStackStatus: BOOTSTRAPPING -> READY -> DEGRADED).
6. Ultra-fast state projection for HyperBEAM ~patch@1.0 (/compute/genesis).
========================================================================
--]]

local ok_json, json = pcall(require, "json")
if not ok_json or not json then
    json = {
        decode = function(str)
            error("json decode module required")
        end,
        encode = function(tbl)
            return "{}"
        end
    }
end

local ok_crypto, crypto = pcall(require, ".crypto")
if not ok_crypto or not crypto then
    crypto = nil
end

if not Handlers then
    Handlers = {
        _list = {},
        utils = {
            hasMatchingTag = function(k, v)
                return function(msg)
                    if not msg or not msg.Tags then return false end
                    for _, t in ipairs(msg.Tags) do
                        if (t.name == k or t[1] == k) and (t.value == v or t[2] == v) then
                            return true
                        end
                    end
                    return false
                end
            end
        },
        add = function(name, pattern, handle)
            table.insert(Handlers._list, { name = name, pattern = pattern, handle = handle })
        end
    }
end

if not ao then
    ao = {
        id = "HYDB_GENESIS",
        _outbox = {},
        send = function(msg)
            table.insert(ao._outbox, msg)
        end
    }
end

-- =========================================================================
-- DECLARATIVE STACK CONSTITUTION & TOPOLOGY
-- =========================================================================
ProcessId = ProcessId or (ao and ao.id) or "HYDB_GENESIS"
Owner = Owner or (ao and ao.env and ao.env.Process and ao.env.Process.Owner) or (ao and ao.id) or "OWNER_PRIMARY"

StackId = StackId or "hydb_universal_mesh"
GenesisVersion = GenesisVersion or "1.0.0"
GenesisHash = GenesisHash or "GENESIS_HASH_PENDING_CALCULATION"
ConfigEpoch = ConfigEpoch or 1
QuorumSize = QuorumSize or 3
QuorumMode = QuorumMode or "majority"
Network = Network or "testnet"

-- Internal Stack AO Processes
ArbitratorProcessId = ArbitratorProcessId or ""

-- Access Control
AuthorizedAdmins = AuthorizedAdmins or {}
if Owner and Owner ~= "" then
    AuthorizedAdmins[Owner] = true
end
AuthorizedAdmins["ADMIN_PRIMARY"] = true

AuthorizedNodes = AuthorizedNodes or {}

-- Operational Registry
NodesRegistry = NodesRegistry or {}
Schemas = Schemas or {}

-- Helper function to extract tag value
local function getTagValue(msg, tagName)
    if not msg or not msg.Tags then return nil end
    for _, t in ipairs(msg.Tags) do
        if (t.name == tagName or t[1] == tagName) then
            return t.value or t[2]
        end
    end
    return msg.Tags[tagName]
end

-- Helper function to extract JSON payload or tags
local function getMessagePayload(msg)
    local data = {}
    if msg.Data and type(msg.Data) == "string" and string.len(msg.Data) > 0 and msg.Data ~= "{}" then
        local ok, parsed = pcall(json.decode, msg.Data)
        if ok and type(parsed) == "table" then
            data = parsed
        end
    end
    return data
end

-- =========================================================================
-- HANDLER: GetGenesisConfig
-- Projections for HyperBEAM ~patch@1.0 & Node Bootstrap
-- =========================================================================
Handlers.add(
    "GetGenesisConfig",
    Handlers.utils.hasMatchingTag("Action", "GetGenesisConfig"),
    function(msg)
        ao.send({
            Target = msg.From,
            Action = "GenesisConfigResponse",
            Data = json.encode({
                ok = true,
                genesis = {
                    stack_id = StackId,
                    genesis_version = GenesisVersion,
                    genesis_hash = GenesisHash,
                    config_epoch = ConfigEpoch,
                    quorum_size = QuorumSize,
                    quorum_mode = QuorumMode,
                    network = Network
                },
                arbitrator_process_id = ArbitratorProcessId,
                authorized_nodes = AuthorizedNodes,
                authorized_admins = AuthorizedAdmins,
                schemas = Schemas
            })
        })
    end
)

-- =========================================================================
-- HANDLER: GetStackStatus
-- Evaluates cluster quorum and operational readiness
-- =========================================================================
Handlers.add(
    "GetStackStatus",
    Handlers.utils.hasMatchingTag("Action", "GetStackStatus"),
    function(msg)
        local registered_count = 0
        local healthy_count = 0
        local now = tonumber(msg.Timestamp) or 0
        local is_ms = now > 100000000000
        local HeartbeatTimeout = is_ms and 60000 or 60

        for node_id, node_info in pairs(NodesRegistry) do
            registered_count = registered_count + 1
            local last_hb = tonumber(node_info.last_heartbeat) or 0
            local is_stale = (now > 0 and last_hb > 0 and (now - last_hb) > HeartbeatTimeout)
            if is_stale then
                node_info.status = "STALE"
            end
            if (node_info.status == "READY" or node_info.status == "ACTIVE") and not is_stale then
                healthy_count = healthy_count + 1
            end
        end

        local required_quorum = QuorumSize
        if QuorumMode == "all" then
            required_quorum = math.max(1, registered_count)
        elseif QuorumMode == "majority" then
            required_quorum = math.floor(QuorumSize / 2) + 1
        end

        local status = "BOOTSTRAPPING"
        if healthy_count >= required_quorum and healthy_count > 0 then
            status = "HEALTHY"
        elseif healthy_count > 0 and healthy_count < required_quorum then
            status = "BOOTSTRAPPING"
        elseif registered_count > 0 and healthy_count == 0 then
            status = "DEGRADED"
        end

        ao.send({
            Target = msg.From,
            Action = "StackStatusResponse",
            Data = json.encode({
                ok = true,
                stack_id = StackId,
                status = status,
                config_epoch = ConfigEpoch,
                quorum_size = QuorumSize,
                quorum_mode = QuorumMode,
                healthy_nodes_count = healthy_count,
                registered_nodes_count = registered_count,
                arbitrator_process_id = ArbitratorProcessId,
                nodes = NodesRegistry
            })
        })
    end
)

-- =========================================================================
-- HANDLER: RegisterNode / RegisterComponent
-- Nodes (Hot Sequencer, Storage Units) register during cold-start
-- =========================================================================
Handlers.add(
    "RegisterNode",
    Handlers.utils.hasMatchingTag("Action", "RegisterNode"),
    function(msg)
        local sender = msg.From
        local payload = getMessagePayload(msg)
        local nodeId = payload.node_id or sender

        -- Enforce node authorization & anti-impersonation
        local isAuthorizedSender = AuthorizedNodes[sender] or AuthorizedAdmins[sender] or (sender == Owner)
        if not isAuthorizedSender then
            ao.send({
                Target = sender,
                Action = "RegisterNodeResponse",
                Data = json.encode({
                    ok = false,
                    status = "REJECTED_UNAUTHORIZED_NODE",
                    error = "Sender address not recognized in AuthorizedNodes list",
                    sender = sender
                })
            })
            return
        end

        local isAdmin = AuthorizedAdmins[sender] or (sender == Owner)
        if not isAdmin and nodeId ~= sender then
            ao.send({
                Target = sender,
                Action = "RegisterNodeResponse",
                Data = json.encode({
                    ok = false,
                    status = "REJECTED_NODE_IMPERSONATION",
                    error = "Sender cannot claim a different node ID without admin authorization",
                    sender = sender,
                    node_id = nodeId
                })
            })
            return
        end

        NodesRegistry[nodeId] = {
            node_id = nodeId,
            role = payload.role or "storage_unit",
            endpoint = payload.endpoint or "",
            wasm_sha256 = payload.wasm_sha256 or "",
            version = payload.version or GenesisVersion,
            status = "READY",
            registered_at = msg.Timestamp or 0,
            last_heartbeat = msg.Timestamp or 0
        }

        ao.send({
            Target = sender,
            Action = "RegisterNodeResponse",
            Data = json.encode({
                ok = true,
                status = "REGISTERED",
                node_id = nodeId,
                stack_id = StackId,
                arbitrator_process_id = ArbitratorProcessId,
                quorum_size = QuorumSize
            })
        })
    end
)

-- =========================================================================
-- HANDLER: ReportHeartbeat
-- Periodic heartbeat, throughput, and sequence reporting from nodes
-- =========================================================================
Handlers.add(
    "ReportHeartbeat",
    Handlers.utils.hasMatchingTag("Action", "ReportHeartbeat"),
    function(msg)
        local sender = msg.From
        local payload = getMessagePayload(msg)
        local nodeId = payload.node_id or sender

        -- Enforce heartbeat authorization & anti-impersonation
        local isAuthorizedSender = AuthorizedNodes[sender] or AuthorizedAdmins[sender] or (sender == Owner)
        if not isAuthorizedSender then
            ao.send({
                Target = sender,
                Action = "ReportHeartbeatResponse",
                Data = json.encode({
                    ok = false,
                    status = "REJECTED_UNAUTHORIZED_NODE",
                    sender = sender
                })
            })
            return
        end

        local isAdmin = AuthorizedAdmins[sender] or (sender == Owner)
        if not isAdmin and nodeId ~= sender then
            ao.send({
                Target = sender,
                Action = "ReportHeartbeatResponse",
                Data = json.encode({
                    ok = false,
                    status = "REJECTED_NODE_IMPERSONATION",
                    sender = sender,
                    node_id = nodeId
                })
            })
            return
        end

        local existing = NodesRegistry[nodeId] or {
            node_id = nodeId,
            role = payload.role or "unknown",
            endpoint = payload.endpoint or "",
            registered_at = msg.Timestamp or 0
        }

        existing.last_heartbeat = msg.Timestamp or 0
        existing.status = payload.health or "ACTIVE"
        existing.throughput = payload.throughput or 0
        existing.last_seq = payload.last_seq or existing.last_seq or 0
        existing.telemetry = {
            throughput = payload.throughput or 0,
            last_seq = payload.last_seq or existing.last_seq or 0,
            self_reported = true
        }
        NodesRegistry[nodeId] = existing

        ao.send({
            Target = sender,
            Action = "ReportHeartbeatResponse",
            Data = json.encode({
                ok = true,
                status = "ACK",
                node_id = nodeId,
                config_epoch = ConfigEpoch
            })
        })
    end
)

-- =========================================================================
-- HANDLER: SetArbitratorProcess
-- Bind or update the authoritative Arbitrator AO Process within this stack
-- =========================================================================
Handlers.add(
    "SetArbitratorProcess",
    Handlers.utils.hasMatchingTag("Action", "SetArbitratorProcess"),
    function(msg)
        local sender = msg.From
        if not AuthorizedAdmins[sender] and sender ~= Owner then
            ao.send({
                Target = sender,
                Action = "SetArbitratorProcessResponse",
                Data = json.encode({
                    ok = false,
                    status = "REJECTED_UNAUTHORIZED_ADMIN",
                    error = "Only AuthorizedAdmins can configure the stack ArbitratorProcessId"
                })
            })
            return
        end

        local payload = getMessagePayload(msg)
        local newArbitratorId = payload.arbitrator_process_id or getTagValue(msg, "Arbitrator-Process-Id")
        if not newArbitratorId or newArbitratorId == "" then
            ao.send({
                Target = sender,
                Action = "SetArbitratorProcessResponse",
                Data = json.encode({
                    ok = false,
                    status = "REJECTED_INVALID_ARBITRATOR_ID",
                    error = "arbitrator_process_id cannot be empty"
                })
            })
            return
        end

        ArbitratorProcessId = newArbitratorId

        ao.send({
            Target = sender,
            Action = "SetArbitratorProcessResponse",
            ArbitratorProcessId = ArbitratorProcessId,
            Data = json.encode({
                ok = true,
                status = "ARBITRATOR_CONFIGURED",
                stack_id = StackId,
                arbitrator_process_id = ArbitratorProcessId
            })
        })
    end
)

-- =========================================================================
-- HANDLER: UpdateStackConfig
-- Authorized admins can adjust topology or quorum
-- =========================================================================
Handlers.add(
    "UpdateStackConfig",
    Handlers.utils.hasMatchingTag("Action", "UpdateStackConfig"),
    function(msg)
        local sender = msg.From
        if not AuthorizedAdmins[sender] and sender ~= Owner then
            ao.send({
                Target = sender,
                Action = "UpdateStackConfigResponse",
                Data = json.encode({
                    ok = false,
                    status = "REJECTED_UNAUTHORIZED_ADMIN"
                })
            })
            return
        end

        local payload = getMessagePayload(msg)
        local has_mutations = (payload.quorum_size ~= nil) or (payload.quorum_mode ~= nil) or (payload.add_node ~= nil) or (payload.remove_node ~= nil) or (payload.add_schema ~= nil)

        if payload.config_epoch then
            local newEpoch = tonumber(payload.config_epoch)
            if newEpoch and newEpoch > ConfigEpoch then
                ConfigEpoch = newEpoch
            else
                ao.send({
                    Target = sender,
                    Action = "UpdateStackConfigResponse",
                    Data = json.encode({
                        ok = false,
                        status = "REJECTED_EPOCH_REGRESSION",
                        error = "ConfigEpoch must strictly advance (newEpoch > ConfigEpoch)",
                        current_epoch = ConfigEpoch
                    })
                })
                return
            end
        elseif has_mutations then
            ConfigEpoch = ConfigEpoch + 1
        end

        if payload.genesis_hash and type(payload.genesis_hash) == "string" and #payload.genesis_hash == 64 then
            GenesisHash = payload.genesis_hash
        end

        if payload.quorum_size then
            local q = tonumber(payload.quorum_size)
            if q and q >= 1 then
                QuorumSize = q
            end
        end
        if payload.quorum_mode then QuorumMode = payload.quorum_mode end

        if payload.add_node then
            AuthorizedNodes[payload.add_node] = true
        end
        if payload.remove_node then
            AuthorizedNodes[payload.remove_node] = nil
        end
        if payload.add_schema and type(payload.add_schema) == "string" then
            Schemas[payload.add_schema] = { namespace = payload.add_schema, active = true }
        end

        ao.send({
            Target = sender,
            Action = "UpdateStackConfigResponse",
            Data = json.encode({
                ok = true,
                status = "CONFIG_UPDATED",
                config_epoch = ConfigEpoch,
                quorum_size = QuorumSize,
                quorum_mode = QuorumMode,
                genesis_hash = GenesisHash
            })
        })
    end
)

-- Direct Dispatcher function for runners / test evaluators
function handle_message(msg)
    if not msg then return { ok = false, error = "nil message" } end
    local handled = false
    local initial_outbox_len = #ao._outbox
    for _, h in ipairs(Handlers._list) do
        if h.pattern(msg) then
            h.handle(msg)
            handled = true
            break
        end
    end
    local responses = {}
    for i = initial_outbox_len + 1, #ao._outbox do
        table.insert(responses, ao._outbox[i])
    end
    return {
        handled = handled,
        responses = responses,
        stack_id = StackId,
        config_epoch = ConfigEpoch,
        arbitrator_process_id = ArbitratorProcessId
    }
end

-- Export module table
return {
    handle = handle_message,
    Handlers = Handlers,
    ao = ao,
    Owner = Owner,
    ProcessId = ProcessId,
    StackId = StackId,
    GenesisVersion = GenesisVersion,
    GenesisHash = GenesisHash,
    ConfigEpoch = ConfigEpoch,
    QuorumSize = QuorumSize,
    QuorumMode = QuorumMode,
    Network = Network,
    ArbitratorProcessId = ArbitratorProcessId,
    AuthorizedNodes = AuthorizedNodes,
    AuthorizedAdmins = AuthorizedAdmins,
    NodesRegistry = NodesRegistry,
    Schemas = Schemas
}
