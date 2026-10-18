--[[
========================================================================
AO Process: Universal Logical Security Domain Arbitrator & Policy Circuit
========================================================================
Responsibilities (Principles B, C, D, E, F):
1. Verify event ordering, SHA-256 hash chain, sender auth, and epoch fencing.
2. Deterministically reconstruct state in an atomic staging area (all-or-nothing)
   and recompute both:
   - 64-bit Fast Fingerprint (Merkle-XOR matching Rust HyDB)
   - 256-bit Cryptographic State Commitment (Canonical SHA-256)
3. Detect invariant violations, tampered state roots, sequence gaps, and out-of-order batches.
4. Handle Multi-SU deduplication, overlapping batches, and prove Equivocation
   when two SUs report conflicting events for the same sequence number.
5. Separate Deterministic Verdict (what is proven) from Developer Policy Circuit
   (what action the application chooses to take).
--]]

local ok_json, json = pcall(require, "json")
if not ok_json or not json then
    -- Minimal fallback JSON decode/encode for standalone test environments
    json = {
        decode = function(str)
            -- If string is a simple primitive or table representation
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

-- Fallback to pure-Lua hydb_spec SHA-256 if crypto is unavailable in sandbox
if not crypto or not crypto.digest then
    local ok_spec, hydb_spec = pcall(require, "hydb_spec")
    if ok_spec and hydb_spec and hydb_spec.sha256 then
        crypto = {
            digest = function(algo, str)
                if string.lower(algo) == "sha256" then
                    return string.lower(hydb_spec.sha256(str))
                end
                error("unsupported hash algo in fallback: " .. tostring(algo))
            end
        }
    end
end

-- Official Persistent Process State
ProcessId = ProcessId or (ao and ao.id) or "HYDB_ARBITRATOR"
Owner = Owner or (ao and ao.env and ao.env.Process and ao.env.Process.Owner) or "OWNER_PRIMARY"
ContractVersion = ContractVersion or "hydb.core@1.0"
CurrentEpoch = CurrentEpoch or 1
OfficialSeq = OfficialSeq or 0
OfficialHash = OfficialHash or "0000000000000000000000000000000000000000000000000000000000000000"
OfficialStateRoot = OfficialStateRoot or "0"
OfficialCommitment256 = OfficialCommitment256 or "0000000000000000000000000000000000000000000000000000000000000000"

Records = Records or {}         -- slot_id -> { namespace, raw_key, val, canonical_val, ver, active }
Schemas = Schemas or {
    ["default"] = { backend = "wasm_slot", schema_version = "1.0.0", key_type = "int64", value_type = "int64", rules = {} },
    ["finance.balances"] = { backend = "wasm_slot", schema_version = "1.0.0", key_type = "int64", value_type = "int64", rules = { min_value = 0 } },
    ["app.docs"] = { backend = "ets_doc", schema_version = "1.0.0", key_type = "string", value_type = "any", rules = {} }
}
EventHistory = EventHistory or {} -- seq_num -> { epoch, seq, namespace, action, key, val, hash, state_root, attestations = {} }
Checkpoints = Checkpoints or {}

-- Explicit Bootstrapping of Trusted Node and Admin Identities (Fail-Closed)
AuthorizedNodes = AuthorizedNodes or {
    ["SU_PRIMARY"] = true,
    ["SU_NODE_1"] = true,
    ["SU_NODE_2"] = true,
    ["hb_su"] = true
}

AuthorizedAdmins = AuthorizedAdmins or {
    [Owner] = true,
    ["ADMIN_PRIMARY"] = true,
    ["SU_PRIMARY"] = true
}

-- Fallback mock for ao and Handlers when loaded outside AOS (e.g. standalone lua5.3 runner)
if not Handlers then
    Handlers = {
        _list = {},
        utils = {
            hasMatchingTag = function(tag, val)
                return function(msg)
                    if not msg or not msg.Tags then return false end
                    for _, t in ipairs(msg.Tags) do
                        if t.name == tag and t.value == val then return true end
                    end
                    return msg[tag] == val
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
        id = "HYDB_ARBITRATOR",
        _outbox = {},
        send = function(msg)
            table.insert(ao._outbox, msg)
        end
    }
end

-- Canonical Policy Catalog and Allowlist matching spec/hydb_contract_spec.mjs
POLICY_ALLOWLIST = {
    ["COMMITTED"] = { "COMMIT_CHECKPOINT_TO_LEDGER" },
    ["DUPLICATE_BATCH_IDEMPOTENT"] = { "RECORD_SU_ATTESTATION_IDEMPOTENT", "IGNORE_DUPLICATE" },
    ["REJECTED_SEQUENCE_GAP"] = { "REQUEST_CATCHUP_FROM_SU_POOL", "HALT_PROCESSING", "ALERT_OPERATOR" },
    ["REJECTED_OUT_OF_ORDER_BATCH"] = { "REJECT_MALFORMED_BATCH", "HALT_PROCESSING", "ALERT_OPERATOR" },
    ["REJECTED_SCHEMA_VIOLATION"] = { "CUT_BRANCH_AND_REJECT_INVALID_SCHEMA", "FREEZE_NAMESPACE", "HALT_PROCESSING", "ALERT_OPERATOR" },
    ["REJECTED_CONTRACT_MISMATCH"] = { "REJECT_INCOMPATIBLE_CONTRACT_VERSION", "FREEZE_NAMESPACE", "HALT_PROCESSING" },
    ["REJECTED_HASH_CHAIN_BROKEN"] = { "CUT_BRANCH_AND_FREEZE_EPOCH", "HALT_PROCESSING" },
    ["REJECTED_EVENT_HASH_MISMATCH"] = { "CUT_BRANCH_AND_FREEZE_EPOCH", "HALT_PROCESSING" },
    ["REJECTED_STATE_ROOT_MISMATCH"] = { "CUT_BRANCH_AND_SLASH_PROPOSER", "CUT_BRANCH_AND_FREEZE_EPOCH", "HALT_PROCESSING" },
    ["REJECTED_STATE_COMMITMENT_MISMATCH"] = { "CUT_BRANCH_AND_SLASH_PROPOSER", "CUT_BRANCH_AND_FREEZE_EPOCH", "HALT_PROCESSING" },
    ["REJECTED_ENCODING"] = { "REJECT_MALFORMED_BATCH", "HALT_PROCESSING" },
    ["EQUIVOCATION_PROVEN"] = { "FREEZE_EPOCH_AND_PROMOTE_STANDBY_HOT", "HALT_PROCESSING" },
    ["FENCED_STALE_EPOCH"] = { "FENCE_AND_DROP_ZOMBIE_HOT", "ALERT_OPERATOR" },
    ["REJECTED_UNAUTHORIZED_SENDER"] = { "DROP_UNAUTHORIZED_PACKET", "ALERT_OPERATOR" }
}

-- Mapping between legacy on_* keys and canonical Verdict strings
local LEGACY_POLICY_MAP = {
    ["on_valid_commit"] = "COMMITTED",
    ["on_duplicate_overlap"] = "DUPLICATE_BATCH_IDEMPOTENT",
    ["on_sequence_gap"] = "REJECTED_SEQUENCE_GAP",
    ["on_out_of_order"] = "REJECTED_OUT_OF_ORDER_BATCH",
    ["on_schema_violation"] = "REJECTED_SCHEMA_VIOLATION",
    ["on_contract_mismatch"] = "REJECTED_CONTRACT_MISMATCH",
    ["on_hash_chain_broken"] = "REJECTED_HASH_CHAIN_BROKEN",
    ["on_event_hash_mismatch"] = "REJECTED_EVENT_HASH_MISMATCH",
    ["on_state_root_mismatch"] = "REJECTED_STATE_ROOT_MISMATCH",
    ["on_state_commitment_mismatch"] = "REJECTED_STATE_COMMITMENT_MISMATCH",
    ["on_equivocation"] = "EQUIVOCATION_PROVEN",
    ["on_stale_epoch"] = "FENCED_STALE_EPOCH",
    ["on_unauthorized_sender"] = "REJECTED_UNAUTHORIZED_SENDER"
}

DEFAULT_POLICY = {
    ["COMMITTED"] = "COMMIT_CHECKPOINT_TO_LEDGER",
    ["DUPLICATE_BATCH_IDEMPOTENT"] = "RECORD_SU_ATTESTATION_IDEMPOTENT",
    ["REJECTED_SEQUENCE_GAP"] = "REQUEST_CATCHUP_FROM_SU_POOL",
    ["REJECTED_OUT_OF_ORDER_BATCH"] = "REJECT_MALFORMED_BATCH",
    ["REJECTED_SCHEMA_VIOLATION"] = "CUT_BRANCH_AND_REJECT_INVALID_SCHEMA",
    ["REJECTED_CONTRACT_MISMATCH"] = "REJECT_INCOMPATIBLE_CONTRACT_VERSION",
    ["REJECTED_HASH_CHAIN_BROKEN"] = "CUT_BRANCH_AND_FREEZE_EPOCH",
    ["REJECTED_EVENT_HASH_MISMATCH"] = "CUT_BRANCH_AND_FREEZE_EPOCH",
    ["REJECTED_STATE_ROOT_MISMATCH"] = "CUT_BRANCH_AND_SLASH_PROPOSER",
    ["REJECTED_STATE_COMMITMENT_MISMATCH"] = "CUT_BRANCH_AND_SLASH_PROPOSER",
    ["REJECTED_ENCODING"] = "REJECT_MALFORMED_BATCH",
    ["EQUIVOCATION_PROVEN"] = "FREEZE_EPOCH_AND_PROMOTE_STANDBY_HOT",
    ["FENCED_STALE_EPOCH"] = "FENCE_AND_DROP_ZOMBIE_HOT",
    ["REJECTED_UNAUTHORIZED_SENDER"] = "DROP_UNAUTHORIZED_PACKET"
}

-- Pluggable Developer Policy Circuit (Configurable per Application Use-Case)
PolicyCircuit = PolicyCircuit or {}
for k, v in pairs(DEFAULT_POLICY) do
    if PolicyCircuit[k] == nil then
        PolicyCircuit[k] = v
    end
end
-- Keep legacy aliases synchronized for backward compatibility
PolicyCircuit.on_valid_commit = PolicyCircuit.on_valid_commit or PolicyCircuit["COMMITTED"]
PolicyCircuit.on_duplicate_overlap = PolicyCircuit.on_duplicate_overlap or PolicyCircuit["DUPLICATE_BATCH_IDEMPOTENT"]
PolicyCircuit.on_sequence_gap = PolicyCircuit.on_sequence_gap or PolicyCircuit["REJECTED_SEQUENCE_GAP"]
PolicyCircuit.on_out_of_order = PolicyCircuit.on_out_of_order or PolicyCircuit["REJECTED_OUT_OF_ORDER_BATCH"]
PolicyCircuit.on_schema_violation = PolicyCircuit.on_schema_violation or PolicyCircuit["REJECTED_SCHEMA_VIOLATION"]
PolicyCircuit.on_hash_chain_broken = PolicyCircuit.on_hash_chain_broken or PolicyCircuit["REJECTED_HASH_CHAIN_BROKEN"]
PolicyCircuit.on_state_root_mismatch = PolicyCircuit.on_state_root_mismatch or PolicyCircuit["REJECTED_STATE_ROOT_MISMATCH"]
PolicyCircuit.on_equivocation = PolicyCircuit.on_equivocation or PolicyCircuit["EQUIVOCATION_PROVEN"]
PolicyCircuit.on_stale_epoch = PolicyCircuit.on_stale_epoch or PolicyCircuit["FENCED_STALE_EPOCH"]
PolicyCircuit.on_unauthorized_sender = PolicyCircuit.on_unauthorized_sender or PolicyCircuit["REJECTED_UNAUTHORIZED_SENDER"]

local FORBIDDEN_POLICY_OVERRIDES = {
    ["COMMIT_CHECKPOINT_TO_LEDGER"] = true,
    ["FORCE_ACCEPT"] = true,
    ["FORCE_COMMIT"] = true,
    ["IGNORE_AND_COMMIT"] = true,
    ["OVERRIDE_VALID"] = true
}

-- Validate policy table before registration or activation
function validate_policy(policy)
    if type(policy) ~= "table" then
        return false, "policy must be a table", "ERR_POLICY_INVALID"
    end
    for k, act in pairs(policy) do
        local canonical_verdict = LEGACY_POLICY_MAP[k] or k
        local allowed = POLICY_ALLOWLIST[canonical_verdict]
        if not allowed then
            return false, "unknown verdict: " .. tostring(k), "ERR_POLICY_INVALID"
        end
        local allowed_map = {}
        for _, a in ipairs(allowed) do allowed_map[a] = true end
        if not allowed_map[act] then
            return false, string.format("action '%s' not allowed for verdict '%s'", tostring(act), tostring(canonical_verdict)), "ERR_POLICY_INVALID"
        end
    end
    return true
end

-- Resolve policy action safely with canonical allowlist and Guardrail enforcement
function resolve_policy(policy, verdict_key)
    local canonical_verdict = LEGACY_POLICY_MAP[verdict_key] or verdict_key
    local allowed = POLICY_ALLOWLIST[canonical_verdict]
    local default_action = (allowed and allowed[1]) or "HALT_PROCESSING"

    local chosen = (policy and (policy[canonical_verdict] or policy[verdict_key])) or default_action
    local chosen_str = tostring(chosen)

    -- Guardrail check: non-committing verdict can NEVER force commit
    if canonical_verdict ~= "COMMITTED" and canonical_verdict ~= "DUPLICATE_BATCH_IDEMPOTENT" then
        if FORBIDDEN_POLICY_OVERRIDES[string.upper(chosen_str)] then
            return {
                action = string.format("GUARDRAIL_BLOCKED_OVERRIDE(%s)->%s", chosen_str, default_action),
                guardrail_triggered = true,
                rejected_action = chosen_str
            }
        end
    end

    -- Allowlist enforcement
    if allowed then
        local allowed_map = {}
        for _, a in ipairs(allowed) do allowed_map[a] = true end
        if allowed_map[chosen_str] then
            return { action = chosen_str, guardrail_triggered = false }
        else
            return {
                action = string.format("GUARDRAIL_BLOCKED_OVERRIDE(%s)->%s", chosen_str, default_action),
                guardrail_triggered = true,
                rejected_action = chosen_str
            }
        end
    end

    return { action = default_action, guardrail_triggered = true, rejected_action = chosen_str }
end

local function safe_policy_action(policy_key, default_action)
    local pol = resolve_policy(PolicyCircuit, policy_key)
    return pol.action
end

-- Convert string to 64-bit integer hash (first 8 bytes big-endian)
local function bytes_to_u64(str)
    local hex = crypto.digest("sha256", tostring(str))
    local b0 = tonumber(hex:sub(1, 2), 16)
    local b1 = tonumber(hex:sub(3, 4), 16)
    local b2 = tonumber(hex:sub(5, 6), 16)
    local b3 = tonumber(hex:sub(7, 8), 16)
    local b4 = tonumber(hex:sub(9, 10), 16)
    local b5 = tonumber(hex:sub(11, 12), 16)
    local b6 = tonumber(hex:sub(13, 14), 16)
    local b7 = tonumber(hex:sub(15, 16), 16)
    return (b0 << 56) | (b1 << 48) | (b2 << 40) | (b3 << 32) | (b4 << 24) | (b5 << 16) | (b6 << 8) | b7
end

-- Match Erlang hydb_contract:namespace_id_64/1
local function namespace_id_64(ns)
    if not ns or ns == "default" then return 0 end
    local hex = crypto.digest("sha256", ns)
    local b0 = tonumber(hex:sub(1, 2), 16)
    local b1 = tonumber(hex:sub(3, 4), 16)
    local b2 = tonumber(hex:sub(5, 6), 16)
    local b3 = tonumber(hex:sub(7, 8), 16)
    local u32 = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3
    return 1000 + (u32 % 100000000)
end

-- Exact 64-bit FNV-1a + SplitMix64 record hash matching Rust HyDB (Lua 5.3 64-bit bitwise)
local function record_hash_64(key, val, ver)
    local h = -3750763034362895579 -- 0xcbf29ce484222325 in signed 64-bit
    local prime = 1099511628211    -- 0x100000001b3
    local m1 = -4658895280553007687 -- 0xbf58476d1ce4e5b9
    local m2 = -7723592293110705685 -- 0x94d049bb133111eb

    h = h ~ key
    h = h * prime
    h = h ~ val
    h = h * prime
    h = h ~ ver
    h = h * prime
    h = h ~ ((h >> 30) & 0x3FFFFFFFF)
    h = h * m1
    h = h ~ ((h >> 27) & 0x1FFFFFFFFF)
    h = h * m2
    h = h ~ ((h >> 31) & 0x1FFFFFFFF)
    return h
end

-- Compute 256-bit Canonical State Commitment over active records
local function compute_commitment_256(rec_table)
    local list = {}
    for slot_id, r in pairs(rec_table) do
        if r.active then
            table.insert(list, {
                slot_id = slot_id,
                ns = r.namespace or "default",
                raw_key = r.raw_key,
                canonical_val = tostring(r.canonical_val or r.val or ""),
                ver = r.ver
            })
        end
    end

    table.sort(list, function(a, b)
        if a.ns ~= b.ns then
            return a.ns < b.ns
        end
        local num_a = tonumber(a.raw_key)
        local num_b = tonumber(b.raw_key)
        if num_a and num_b then
            return num_a < num_b
        end
        return tostring(a.raw_key) < tostring(b.raw_key)
    end)

    local parts = {}
    for _, item in ipairs(list) do
        if item.ns == "default" then
            table.insert(parts, string.format("%s:%s:%d", tostring(item.raw_key), item.canonical_val, item.ver))
        else
            table.insert(parts, string.format("%s:%s:%s:%d", item.ns, tostring(item.raw_key), item.canonical_val, item.ver))
        end
    end
    local canonical = table.concat(parts, "|")
    return string.upper(crypto.digest("sha256", canonical))
end

local function count_active_records(rec_table)
    local c = 0
    for _, r in pairs(rec_table) do
        if r.active then c = c + 1 end
    end
    return c
end

local function clone_records(src)
    local dst = {}
    for k, v in pairs(src) do
        dst[k] = {
            namespace = v.namespace,
            backend = v.backend,
            raw_key = v.raw_key,
            val = v.val,
            canonical_val = v.canonical_val,
            key_hash_64 = v.key_hash_64,
            val_hash_64 = v.val_hash_64,
            ver = v.ver,
            active = v.active
        }
    end
    return dst
end

local function clone_schemas(src)
    local dst = {}
    for k, v in pairs(src) do
        local r_copy = {}
        if v.rules then
            for rk, rv in pairs(v.rules) do r_copy[rk] = rv end
        end
        dst[k] = {
            namespace = v.namespace or k,
            backend = v.backend,
            schema_version = v.schema_version,
            key_type = v.key_type,
            value_type = v.value_type,
            rules = r_copy,
            contract_tx_id = v.contract_tx_id,
            wasm_tx_id = v.wasm_tx_id,
            wasm_sha256 = v.wasm_sha256
        }
    end
    return dst
end

-- Handler: Authorize SU Node (Admin only)
Handlers.add(
    "AuthorizeNode",
    Handlers.utils.hasMatchingTag("Action", "AuthorizeNode"),
    function(msg)
        local caller = msg.From
        if not AuthorizedAdmins[caller] and caller ~= Owner then
            ao.send({
                Target = msg.From,
                Action = "AuthorizeNode-Rejected",
                Status = "REJECTED_UNAUTHORIZED_ADMIN",
                Reason = "Only authorized admin or owner can authorize nodes"
            })
            return
        end
        local data = json.decode(msg.Data or "{}")
        local node = data.node or data.node_id or (msg.Tags and msg.Tags.Node)
        if not node then
            ao.send({ Target = msg.From, Action = "AuthorizeNode-Rejected", Status = "REJECTED_INVALID_INPUT" })
            return
        end
        AuthorizedNodes[node] = true
        ao.send({
            Target = msg.From,
            Action = "Node-Authorized",
            Status = "AUTHORIZED",
            Node = node
        })
    end
)

-- Handler: Revoke SU Node (Admin only)
Handlers.add(
    "RevokeNode",
    Handlers.utils.hasMatchingTag("Action", "RevokeNode"),
    function(msg)
        local caller = msg.From
        if not AuthorizedAdmins[caller] and caller ~= Owner then
            ao.send({
                Target = msg.From,
                Action = "RevokeNode-Rejected",
                Status = "REJECTED_UNAUTHORIZED_ADMIN",
                Reason = "Only authorized admin or owner can revoke nodes"
            })
            return
        end
        local data = json.decode(msg.Data or "{}")
        local node = data.node or data.node_id or (msg.Tags and msg.Tags.Node)
        if node then
            AuthorizedNodes[node] = nil
        end
        ao.send({
            Target = msg.From,
            Action = "Node-Revoked",
            Status = "REVOKED",
            Node = node
        })
    end
)

-- Handler: Authorize Admin (Owner only)
Handlers.add(
    "AuthorizeAdmin",
    Handlers.utils.hasMatchingTag("Action", "AuthorizeAdmin"),
    function(msg)
        local caller = msg.From
        if caller ~= Owner then
            ao.send({
                Target = msg.From,
                Action = "AuthorizeAdmin-Rejected",
                Status = "REJECTED_UNAUTHORIZED_OWNER",
                Reason = "Only process owner can authorize admins"
            })
            return
        end
        local data = json.decode(msg.Data or "{}")
        local admin = data.admin or data.address or (msg.Tags and msg.Tags.Admin)
        if admin then
            AuthorizedAdmins[admin] = true
        end
        ao.send({
            Target = msg.From,
            Action = "Admin-Authorized",
            Status = "AUTHORIZED",
            Admin = admin
        })
    end
)

-- Handler: Set Policy Circuit (Admin or Owner only, strictly validated against allowlist)
Handlers.add(
    "SetPolicy",
    Handlers.utils.hasMatchingTag("Action", "SetPolicy"),
    function(msg)
        local caller = msg.From
        if not AuthorizedAdmins[caller] and caller ~= Owner then
            ao.send({
                Target = msg.From,
                Action = "SetPolicy-Rejected",
                Status = "REJECTED_UNAUTHORIZED_ADMIN",
                Reason = "Only authorized admin or owner can configure policy"
            })
            return
        end
        local data = json.decode(msg.Data or "{}")
        local new_policy = data.policy or data
        if type(new_policy) ~= "table" then
            ao.send({
                Target = msg.From,
                Action = "SetPolicy-Rejected",
                Status = "REJECTED_INVALID_POLICY",
                ErrorCode = "ERR_POLICY_INVALID",
                Reason = "Policy must be a valid object"
            })
            return
        end

        local ok_val, val_err, val_code = validate_policy(new_policy)
        if not ok_val then
            ao.send({
                Target = msg.From,
                Action = "SetPolicy-Rejected",
                Status = "REJECTED_INVALID_POLICY",
                ErrorCode = val_code or "ERR_POLICY_INVALID",
                Reason = val_err
            })
            return
        end

        -- Merge valid policy configuration into active PolicyCircuit
        for k, v in pairs(new_policy) do
            local canonical_v = LEGACY_POLICY_MAP[k] or k
            PolicyCircuit[canonical_v] = v
            PolicyCircuit[k] = v
        end

        ao.send({
            Target = msg.From,
            Action = "Policy-Updated",
            Status = "POLICY_UPDATED",
            UpdatedKeysCount = tostring(#new_policy)
        })
    end
)

-- Handler: Commit / Verify Checkpoint Batch (Single or Multi-SU)
Handlers.add(
    "CommitCheckpoint",
    Handlers.utils.hasMatchingTag("Action", "CommitCheckpoint"),
    function(msg)
        local batch = json.decode(msg.Data)
        local su_id = batch.su_id or batch.sender or msg.From or "SU_PRIMARY"

        -- 1. Sender Node Authorization Check (Fail-Closed)
        if not AuthorizedNodes[su_id] and not AuthorizedNodes[msg.From] then
            ao.send({
                Target = msg.From,
                Action = "Audit-Rejected",
                Status = "AUDIT_REJECTED",
                SubStatus = "REJECTED_UNAUTHORIZED_SENDER",
                ErrorCode = "ERR_UNAUTHORIZED_SENDER",
                PolicyAction = safe_policy_action("on_unauthorized_sender", "DROP_UNAUTHORIZED_PACKET"),
                Reason = string.format("Sender %s is not in AuthorizedNodes list", su_id),
                Sender = su_id
            })
            return
        end

        local batch_epoch = tonumber(batch.epoch) or 1
        local proposed_root = tostring(batch.proposed_state_root or "0")
        local proposed_hash = string.upper(tostring(batch.proposed_hash or ""))
        local proposed_commitment = batch.proposed_state_commitment_256 and string.upper(tostring(batch.proposed_state_commitment_256)) or nil
        local events = batch.events or {}

        -- 2. Check Epoch Fencing (Reject Zombie / Stale Hot Node)
        if batch_epoch < CurrentEpoch then
            ao.send({
                Target = msg.From,
                Action = "Audit-Rejected",
                Status = "FENCED_STALE_EPOCH",
                PolicyAction = safe_policy_action("on_stale_epoch", "FENCE_AND_DROP_ZOMBIE_HOT"),
                Reason = string.format("Batch epoch %d is older than active security epoch %d", batch_epoch, CurrentEpoch),
                CurrentEpoch = tostring(CurrentEpoch),
                OfficialSeq = tostring(OfficialSeq)
            })
            return
        end

        if #events == 0 then
            ao.send({
                Target = msg.From,
                Action = "Audit-Rejected",
                Status = "REJECTED_EMPTY_BATCH",
                Reason = "Batch contains no events"
            })
            return
        end

        -- 3. Verify internal batch ordering (strictly monotonic by +1)
        for i = 1, #events do
            local seq = tonumber(events[i].seq)
            if i > 1 then
                local prev_s = tonumber(events[i - 1].seq)
                if seq ~= (prev_s + 1) then
                    ao.send({
                        Target = msg.From,
                        Action = "Audit-Rejected",
                        Status = "REJECTED_OUT_OF_ORDER_BATCH",
                        PolicyAction = safe_policy_action("on_out_of_order", "REJECT_MALFORMED_BATCH"),
                        Reason = string.format("Internal batch sequence disorder: %d followed by %d", prev_s, seq)
                    })
                    return
                end
            end
        end

        local first_seq = tonumber(events[1].seq)
        local last_seq = tonumber(events[#events].seq)

        -- 4. Check for Sequence Gap before batch starts
        if first_seq > (OfficialSeq + 1) then
            ao.send({
                Target = msg.From,
                Action = "Audit-Rejected",
                Status = "REJECTED_SEQUENCE_GAP",
                PolicyAction = safe_policy_action("on_sequence_gap", "REQUEST_CATCHUP_FROM_SU_POOL"),
                Reason = string.format("Expected sequence <= %d, but batch starts at %d", OfficialSeq + 1, first_seq),
                OfficialSeq = tostring(OfficialSeq),
                MissingFrom = tostring(OfficialSeq + 1),
                MissingTo = tostring(first_seq - 1)
            })
            return
        end

        -- 5. Stage state for Atomic All-or-Nothing Execution
        local staged_records = clone_records(Records)
        local staged_schemas = clone_schemas(Schemas)
        local staged_root = tonumber(OfficialStateRoot) or 0
        local running_hash = (batch.initial_hash and OfficialSeq == 0) and string.upper(tostring(batch.initial_hash)) or OfficialHash
        local staged_seq = OfficialSeq
        local new_events_to_commit = {}
        local deduplicated_count = 0

        for _, ev in ipairs(events) do
            local seq = tonumber(ev.seq)
            local ev_epoch = tonumber(ev.epoch) or batch_epoch
            local namespace = ev.namespace or "default"
            local action = ev.action or ev.op or "put"
            local val_type = ev.val_type or (action == "register_schema" and "schema" or "int64")
            local key = tonumber(ev.key) or tostring(ev.key or "")
            local val = ev.val
            local canonical_val = tostring(ev.canonical_val or ev.val or "")
            local given_hash = string.upper(tostring(ev.hash or ""))

            if ev.contract_version and ev.contract_version ~= ContractVersion then
                ao.send({
                    Target = msg.From,
                    Action = "Audit-Rejected",
                    Status = "FRAUD_DETECTED_AND_CUT",
                    SubStatus = "REJECTED_CONTRACT_MISMATCH",
                    ErrorCode = "ERR_CONTRACT_MISMATCH",
                    Reason = string.format("Incompatible contract_version %s, expected %s", ev.contract_version, ContractVersion)
                })
                return
            end

            if ev_epoch < CurrentEpoch then
                ao.send({
                    Target = msg.From,
                    Action = "Audit-Rejected",
                    Status = "FENCED_STALE_EPOCH",
                    PolicyAction = safe_policy_action("on_stale_epoch", "FENCE_AND_DROP_ZOMBIE_HOT"),
                    Reason = string.format("Event at seq %d carries stale epoch %d < %d", seq, ev_epoch, CurrentEpoch)
                })
                return
            end

            -- Schema checking & Dynamic Registration
            if action == "register_schema" then
                local parsed_schema = (type(val) == "table") and val or (json.decode(canonical_val) or {})
                if parsed_schema.policy and type(parsed_schema.policy) == "table" then
                    local ok_p, err_p, code_p = validate_policy(parsed_schema.policy)
                    if not ok_p then
                        ao.send({
                            Target = msg.From,
                            Action = "Audit-Rejected",
                            Status = "FRAUD_DETECTED_AND_CUT",
                            SubStatus = "REJECTED_CONTRACT_MISMATCH",
                            ErrorCode = code_p or "ERR_POLICY_INVALID",
                            PolicyAction = safe_policy_action("on_contract_mismatch", "REJECT_INCOMPATIBLE_CONTRACT_VERSION"),
                            Reason = string.format("Invalid policy in register_schema at seq %d: %s", seq, tostring(err_p))
                        })
                        return
                    end
                    for pk, pv in pairs(parsed_schema.policy) do
                        local canon_pk = LEGACY_POLICY_MAP[pk] or pk
                        PolicyCircuit[canon_pk] = pv
                        PolicyCircuit[pk] = pv
                    end
                end
                staged_schemas[namespace] = {
                    namespace = namespace,
                    backend = parsed_schema.backend or "ets_doc",
                    schema_version = parsed_schema.schema_version or "1.0.0",
                    key_type = parsed_schema.key_type or "string",
                    value_type = parsed_schema.value_type or "any",
                    rules = parsed_schema.rules or {},
                    contract_tx_id = parsed_schema.contract_tx_id,
                    wasm_tx_id = parsed_schema.wasm_tx_id,
                    wasm_sha256 = parsed_schema.wasm_sha256,
                    policy = parsed_schema.policy
                }
            else
                local schema = staged_schemas[namespace]
                if not schema then
                    ao.send({
                        Target = msg.From,
                        Action = "Audit-Rejected",
                        Status = "FRAUD_DETECTED_AND_CUT",
                        SubStatus = "REJECTED_SCHEMA_VIOLATION",
                        ErrorCode = "ERR_NAMESPACE_UNKNOWN",
                        PolicyAction = safe_policy_action("on_schema_violation", "CUT_BRANCH_AND_REJECT_INVALID_SCHEMA"),
                        Reason = string.format("Unknown namespace %s in event seq %d", namespace, seq)
                    })
                    return
                end

                if ev.schema_version and ev.schema_version ~= schema.schema_version then
                    ao.send({
                        Target = msg.From,
                        Action = "Audit-Rejected",
                        Status = "FRAUD_DETECTED_AND_CUT",
                        SubStatus = "REJECTED_SCHEMA_VIOLATION",
                        ErrorCode = "ERR_SCHEMA_VIOLATION",
                        Reason = string.format("Schema version mismatch for %s at seq %d", namespace, seq)
                    })
                    return
                end

                if action == "put" and schema.rules then
                    local rules = schema.rules
                    local num_val = tonumber(val)
                    if num_val then
                        if rules.min_value ~= nil and num_val < tonumber(rules.min_value) then
                            ao.send({
                                Target = msg.From,
                                Action = "Audit-Rejected",
                                Status = "FRAUD_DETECTED_AND_CUT",
                                SubStatus = "REJECTED_SCHEMA_VIOLATION",
                                ErrorCode = "ERR_SCHEMA_VIOLATION",
                                PolicyAction = safe_policy_action("on_schema_violation", "CUT_BRANCH_AND_REJECT_INVALID_SCHEMA"),
                                Reason = string.format("Schema violation in %s at seq %d: val %d < min_value %s", namespace, seq, num_val, tostring(rules.min_value))
                            })
                            return
                        end
                        if rules.max_value ~= nil and num_val > tonumber(rules.max_value) then
                            ao.send({
                                Target = msg.From,
                                Action = "Audit-Rejected",
                                Status = "FRAUD_DETECTED_AND_CUT",
                                SubStatus = "REJECTED_SCHEMA_VIOLATION",
                                ErrorCode = "ERR_SCHEMA_VIOLATION",
                                PolicyAction = safe_policy_action("on_schema_violation", "CUT_BRANCH_AND_REJECT_INVALID_SCHEMA"),
                                Reason = string.format("Schema violation in %s at seq %d: val %d > max_value %s", namespace, seq, num_val, tostring(rules.max_value))
                            })
                            return
                        end
                    end
                    if type(val) == "string" then
                        if rules.min_length ~= nil and #val < tonumber(rules.min_length) then
                            ao.send({
                                Target = msg.From,
                                Action = "Audit-Rejected",
                                Status = "FRAUD_DETECTED_AND_CUT",
                                SubStatus = "REJECTED_SCHEMA_VIOLATION",
                                ErrorCode = "ERR_SCHEMA_VIOLATION",
                                Reason = string.format("Schema violation in %s at seq %d: string length < min_length", namespace, seq)
                            })
                            return
                        end
                    end
                end
            end

            if seq <= OfficialSeq then
                -- Already committed sequence: Check Deduplication vs Equivocation!
                local existing = EventHistory[seq]
                if existing then
                    if existing.hash ~= given_hash or tostring(existing.key) ~= tostring(key) or existing.action ~= action or existing.namespace ~= namespace then
                        -- EQUIVOCATION PROVEN: Conflicting event for the same sequence!
                        ao.send({
                            Target = msg.From,
                            Action = "Audit-Rejected",
                            Status = "EQUIVOCATION_PROVEN",
                            PolicyAction = safe_policy_action("on_equivocation", "FREEZE_EPOCH_AND_PROMOTE_STANDBY_HOT"),
                            Reason = string.format("Conflicting event reported at seq %d by %s", seq, su_id),
                            ConflictSeq = tostring(seq),
                            OfficialHashAtSeq = existing.hash,
                            ConflictingHashAtSeq = given_hash
                        })
                        return
                    else
                        deduplicated_count = deduplicated_count + 1
                    end
                end
            else
                -- New uncommitted event (seq == staged_seq + 1)
                local action_code = (action == "register_schema") and "24" or ((action == "del") and "22" or "20")
                local payload
                if action_code == "24" then
                    payload = string.format("%s%d24%s:%s", running_hash, seq, namespace, canonical_val)
                elseif namespace == "default" and action_code == "20" and val_type == "int64" then
                    payload = string.format("%s%d20%s%s", running_hash, seq, tostring(key), canonical_val)
                elseif namespace == "default" and action_code == "22" then
                    payload = string.format("%s%d22%s", running_hash, seq, tostring(key))
                elseif action_code == "22" then
                    payload = string.format("%s%d22%s:%s", running_hash, seq, namespace, tostring(key))
                else
                    payload = string.format("%s%d20%s:%s:%s", running_hash, seq, namespace, tostring(key), canonical_val)
                end

                local expected_hash = string.upper(crypto.digest("sha256", payload))
                if expected_hash ~= given_hash then
                    -- Atomic abort: staged state is completely discarded, official state untouched!
                    ao.send({
                        Target = msg.From,
                        Action = "Audit-Rejected",
                        Status = "FRAUD_DETECTED_AND_CUT",
                        SubStatus = "REJECTED_HASH_CHAIN_BROKEN",
                        PolicyAction = safe_policy_action("on_hash_chain_broken", "CUT_BRANCH_AND_FREEZE_EPOCH"),
                        Reason = string.format("Hash chain broken at seq %d! Expected %s, got %s", seq, expected_hash, given_hash),
                        OfficialSeq = tostring(OfficialSeq)
                    })
                    return
                end

                if action ~= "register_schema" then
                    -- Unambiguous Composite Slot Key: namespace .. ":" .. key
                    local slot_id = (namespace == "default") and tostring(key) or (namespace .. ":" .. tostring(key))
                    local schema = staged_schemas[namespace]
                    local backend = (schema and schema.backend) or (val_type == "int64" and "wasm_slot" or "ets_doc")

                    local key_hash_64 = 0
                    local val_hash_64 = 0
                    if backend == "wasm_slot" then
                        key_hash_64 = (namespace == "default") and (tonumber(key) or 0) or ((namespace_id_64(namespace) ~ (tonumber(key) or 0)))
                        val_hash_64 = tonumber(val) or 0
                    else
                        key_hash_64 = bytes_to_u64(namespace .. ":" .. tostring(key))
                        val_hash_64 = bytes_to_u64(canonical_val)
                    end

                    local cur = staged_records[slot_id]

                    if action == "put" then
                        if cur and cur.active then
                            local old_h = record_hash_64(cur.key_hash_64, cur.val_hash_64, cur.ver)
                            local new_ver = cur.ver + 1
                            local new_h = record_hash_64(key_hash_64, val_hash_64, new_ver)
                            staged_root = staged_root ~ old_h ~ new_h
                            staged_records[slot_id] = {
                                namespace = namespace,
                                raw_key = key,
                                val = val,
                                canonical_val = canonical_val,
                                key_hash_64 = key_hash_64,
                                val_hash_64 = val_hash_64,
                                ver = new_ver,
                                active = true
                            }
                        elseif cur and not cur.active then
                            local new_ver = cur.ver + 1
                            local new_h = record_hash_64(key_hash_64, val_hash_64, new_ver)
                            staged_root = staged_root ~ new_h
                            staged_records[slot_id] = {
                                namespace = namespace,
                                raw_key = key,
                                val = val,
                                canonical_val = canonical_val,
                                key_hash_64 = key_hash_64,
                                val_hash_64 = val_hash_64,
                                ver = new_ver,
                                active = true
                            }
                        else
                            local new_h = record_hash_64(key_hash_64, val_hash_64, 1)
                            staged_root = staged_root ~ new_h
                            staged_records[slot_id] = {
                                namespace = namespace,
                                raw_key = key,
                                val = val,
                                canonical_val = canonical_val,
                                key_hash_64 = key_hash_64,
                                val_hash_64 = val_hash_64,
                                ver = 1,
                                active = true
                            }
                        end
                    elseif action == "del" then
                        if cur and cur.active then
                            local old_h = record_hash_64(cur.key_hash_64, cur.val_hash_64, cur.ver)
                            staged_root = staged_root ~ old_h
                            staged_records[slot_id] = {
                                namespace = namespace,
                                raw_key = key,
                                val = cur.val,
                                canonical_val = cur.canonical_val,
                                key_hash_64 = cur.key_hash_64,
                                val_hash_64 = cur.val_hash_64,
                                ver = cur.ver + 1,
                                active = false
                            }
                        end
                    end
                end

                -- Verify per-event state_root if present
                if ev.state_root ~= nil then
                    if tostring(ev.state_root) ~= tostring(staged_root) then
                        ao.send({
                            Target = msg.From,
                            Action = "Audit-Rejected",
                            Status = "FRAUD_DETECTED_AND_CUT",
                            SubStatus = "REJECTED_STATE_ROOT_MISMATCH",
                            PolicyAction = PolicyCircuit.on_state_root_mismatch,
                            Reason = string.format("State root mismatch at seq %d: expected %s, got %s", seq, tostring(staged_root), tostring(ev.state_root)),
                            OfficialSeq = tostring(OfficialSeq)
                        })
                        return
                    end
                end

                running_hash = given_hash
                staged_seq = seq
                table.insert(new_events_to_commit, {
                    epoch = ev_epoch,
                    seq = seq,
                    namespace = namespace,
                    action = action,
                    key = key,
                    val = val,
                    canonical_val = canonical_val,
                    hash = given_hash,
                    state_root = tostring(staged_root),
                    su_id = su_id
                })
            end
        end

        -- If entire batch was already committed (100% duplicate/overlap)
        if #new_events_to_commit == 0 and deduplicated_count > 0 then
            for _, ev in ipairs(events) do
                local s = tonumber(ev.seq)
                if EventHistory[s] then
                    EventHistory[s].attestations[su_id] = true
                end
            end
            ao.send({
                Target = msg.From,
                Action = "Checkpoint-Duplicate-Ack",
                Status = "DUPLICATE_BATCH_IDEMPOTENT",
                PolicyAction = PolicyCircuit.on_duplicate_overlap,
                OfficialSeq = tostring(OfficialSeq),
                OfficialStateRoot = tostring(OfficialStateRoot),
                OfficialCommitment256 = OfficialCommitment256,
                DeduplicatedEvents = tostring(deduplicated_count)
            })
            return
        end

        -- 5. Verify final batch hash tip & proposed state root
        if running_hash ~= proposed_hash then
            ao.send({
                Target = msg.From,
                Action = "Audit-Rejected",
                Status = "FRAUD_DETECTED_AND_CUT",
                SubStatus = "REJECTED_TIP_MISMATCH",
                PolicyAction = PolicyCircuit.on_hash_chain_broken,
                Reason = "Proposed final hash does not match audited hash chain"
            })
            return
        end

        if tostring(staged_root) ~= proposed_root then
            ao.send({
                Target = msg.From,
                Action = "Audit-Rejected",
                Status = "FRAUD_DETECTED_AND_CUT",
                SubStatus = "REJECTED_STATE_ROOT_MISMATCH",
                PolicyAction = PolicyCircuit.on_state_root_mismatch,
                Reason = string.format("Batch state root mismatch: recomputed %s vs proposed %s", tostring(staged_root), proposed_root)
            })
            return
        end

        -- Recompute 256-bit cryptographic state commitment
        local recomputed_commitment_256 = compute_commitment_256(staged_records)
        if proposed_commitment and proposed_commitment ~= recomputed_commitment_256 then
            ao.send({
                Target = msg.From,
                Action = "Audit-Rejected",
                Status = "FRAUD_DETECTED_AND_CUT",
                SubStatus = "REJECTED_STATE_COMMITMENT_MISMATCH",
                ErrorCode = "ERR_STATE_COMMITMENT_MISMATCH",
                Reason = string.format("256-bit State commitment mismatch: recomputed %s vs proposed %s", recomputed_commitment_256, proposed_commitment)
            })
            return
        end

        -- 6. ATOMIC COMMIT: All validations passed! Promote staged state to Official State
        Records = staged_records
        Schemas = staged_schemas
        if batch_epoch > CurrentEpoch then
            CurrentEpoch = batch_epoch
        end
        OfficialSeq = staged_seq
        OfficialHash = running_hash
        OfficialStateRoot = tostring(staged_root)
        OfficialCommitment256 = recomputed_commitment_256

        for _, cev in ipairs(new_events_to_commit) do
            EventHistory[cev.seq] = {
                epoch = cev.epoch,
                seq = cev.seq,
                namespace = cev.namespace,
                action = cev.action,
                key = cev.key,
                val = cev.val,
                canonical_val = cev.canonical_val,
                hash = cev.hash,
                state_root = cev.state_root,
                attestations = { [su_id] = true }
            }
        end

        local checkpoint_entry = {
            epoch = CurrentEpoch,
            from_seq = first_seq,
            to_seq = last_seq,
            new_events_committed = #new_events_to_commit,
            deduplicated_events = deduplicated_count,
            state_root_64 = OfficialStateRoot,
            state_commitment_256 = OfficialCommitment256,
            final_hash = OfficialHash,
            su_id = su_id,
            timestamp = msg.Timestamp,
            tx_id = msg.Id
        }
        table.insert(Checkpoints, checkpoint_entry)

        ao.send({
            Target = msg.From,
            Action = "Checkpoint-Committed",
            Status = "COMMITTED",
            PolicyAction = PolicyCircuit.on_valid_commit,
            Epoch = tostring(CurrentEpoch),
            OfficialSeq = tostring(OfficialSeq),
            OfficialStateRoot = OfficialStateRoot,
            OfficialCommitment256 = OfficialCommitment256,
            OfficialHash = OfficialHash,
            CheckpointsCount = tostring(#Checkpoints)
        })
    end
)

-- Handler: Promote Epoch (Failover Fencing)
Handlers.add(
    "PromoteEpoch",
    Handlers.utils.hasMatchingTag("Action", "PromoteEpoch"),
    function(msg)
        local caller = msg.From
        if not AuthorizedAdmins[caller] and caller ~= Owner then
            ao.send({
                Target = msg.From,
                Action = "Epoch-Promotion-Rejected",
                Status = "REJECTED_UNAUTHORIZED_ADMIN",
                Reason = "Only authorized admin or owner can promote epoch"
            })
            return
        end
        local data = json.decode(msg.Data or "{}")
        local new_epoch = tonumber(data.new_epoch) or (CurrentEpoch + 1)
        if new_epoch <= CurrentEpoch then
            ao.send({
                Target = msg.From,
                Action = "Epoch-Promotion-Rejected",
                Status = "REJECTED_INVALID_EPOCH",
                CurrentEpoch = tostring(CurrentEpoch)
            })
            return
        end
        local old_epoch = CurrentEpoch
        CurrentEpoch = new_epoch
        ao.send({
            Target = msg.From,
            Action = "Epoch-Promoted",
            Status = "EPOCH_PROMOTED",
            PreviousEpoch = tostring(old_epoch),
            CurrentEpoch = tostring(CurrentEpoch),
            OfficialSeq = tostring(OfficialSeq),
            OfficialStateRoot = tostring(OfficialStateRoot),
            OfficialCommitment256 = OfficialCommitment256
        })
    end
)

-- Handler: Query Official State
Handlers.add(
    "GetOfficialState",
    Handlers.utils.hasMatchingTag("Action", "GetOfficialState"),
    function(msg)
        local active_count = count_active_records(Records)
        ao.send({
            Target = msg.From,
            Action = "OfficialStateResponse",
            ProcessId = ProcessId,
            CurrentEpoch = tostring(CurrentEpoch),
            OfficialSeq = tostring(OfficialSeq),
            OfficialStateRoot = tostring(OfficialStateRoot),
            OfficialCommitment256 = OfficialCommitment256,
            OfficialHash = OfficialHash,
            CheckpointsCount = tostring(#Checkpoints),
            Data = json.encode({
                epoch = CurrentEpoch,
                official_seq = OfficialSeq,
                state_root_64 = OfficialStateRoot,
                state_commitment_256 = OfficialCommitment256,
                checkpoints = Checkpoints,
                records_count = active_count
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
        official_seq = OfficialSeq,
        current_epoch = CurrentEpoch,
        official_commitment = OfficialCommitment256,
        official_hash = OfficialHash
    }
end

return {
    handle = handle_message,
    Handlers = Handlers,
    ao = ao,
    compute_commitment_256 = compute_commitment_256,
    record_hash_64 = record_hash_64,
    validate_policy = validate_policy,
    resolve_policy = resolve_policy,
    POLICY_ALLOWLIST = POLICY_ALLOWLIST,
    DEFAULT_POLICY = DEFAULT_POLICY
}
