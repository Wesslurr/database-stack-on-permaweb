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

local json = require("json")
local crypto = require(".crypto")

-- Official Persistent Process State
ProcessId = ProcessId or ao.id
ContractVersion = ContractVersion or "hydb.core@1.0"
CurrentEpoch = CurrentEpoch or 1
OfficialSeq = OfficialSeq or 0
OfficialHash = OfficialHash or "0000000000000000000000000000000000000000000000000000000000000000"
OfficialStateRoot = OfficialStateRoot or "0"
OfficialCommitment256 = OfficialCommitment256 or "0000000000000000000000000000000000000000000000000000000000000000"

Records = Records or {}         -- key_str -> { val = number, ver = number, active = boolean }
Schemas = Schemas or {
    ["default"] = { backend = "wasm_slot", schema_version = "1.0.0", key_type = "int64", value_type = "int64", rules = {} },
    ["finance.balances"] = { backend = "wasm_slot", schema_version = "1.0.0", key_type = "int64", value_type = "int64", rules = { min_value = 0 } },
    ["app.docs"] = { backend = "ets_doc", schema_version = "1.0.0", key_type = "string", value_type = "any", rules = {} }
}
EventHistory = EventHistory or {} -- seq_num -> { epoch, seq, namespace, action, key, val, hash, state_root, attestations = {} }
Checkpoints = Checkpoints or {}
AuthorizedNodes = AuthorizedNodes or {} -- node_address -> true (if empty, initialized on first trusted registration)

-- Pluggable Developer Policy Circuit (Configurable per Application Use-Case)
-- Guardrail C: Policy Circuit can NEVER turn an invalid Arbitrator Verdict into a valid commit.
PolicyCircuit = PolicyCircuit or {
    on_valid_commit = "COMMIT_CHECKPOINT_TO_LEDGER",
    on_duplicate_overlap = "RECORD_SU_ATTESTATION_IDEMPOTENT",
    on_sequence_gap = "REQUEST_CATCHUP_FROM_SU_POOL",
    on_out_of_order = "REJECT_MALFORMED_BATCH",
    on_schema_violation = "CUT_BRANCH_AND_REJECT_INVALID_SCHEMA",
    on_hash_chain_broken = "CUT_BRANCH_AND_FREEZE_EPOCH",
    on_state_root_mismatch = "CUT_BRANCH_AND_SLASH_PROPOSER",
    on_equivocation = "FREEZE_EPOCH_AND_PROMOTE_STANDBY_HOT",
    on_stale_epoch = "FENCE_AND_DROP_ZOMBIE_HOT"
}

local FORBIDDEN_POLICY_OVERRIDES = {
    ["COMMIT_CHECKPOINT_TO_LEDGER"] = true,
    ["FORCE_ACCEPT"] = true,
    ["FORCE_COMMIT"] = true,
    ["IGNORE_AND_COMMIT"] = true,
    ["OVERRIDE_VALID"] = true
}

local function safe_policy_action(policy_key, default_action, event_context)
    local act = PolicyCircuit[policy_key] or default_action
    if type(act) == "function" then
        local ok, res = pcall(act, event_context or {})
        if ok and res then act = res else act = default_action end
    elseif type(act) == "string" and (string.find(act, "function%s*%(") or string.find(act, "^return%s+")) then
        local loader_fn, load_err = load(act)
        if loader_fn then
            local ok_call, handler = pcall(loader_fn)
            if ok_call and type(handler) == "function" then
                local ok_exec, res = pcall(handler, event_context or {})
                if ok_exec and res then act = res else act = default_action end
            end
        end
    end

    if policy_key ~= "on_valid_commit" and policy_key ~= "on_duplicate_overlap" then
        if FORBIDDEN_POLICY_OVERRIDES[string.upper(tostring(act))] then
            return "GUARDRAIL_BLOCKED_OVERRIDE->" .. default_action
        end
    end
    return tostring(act)
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
    local keys = {}
    for k, r in pairs(rec_table) do
        if r.active then
            table.insert(keys, tostring(k))
        end
    end
    table.sort(keys)
    local parts = {}
    for _, k in ipairs(keys) do
        local r = rec_table[k]
        table.insert(parts, string.format("%s:%s:%d", k, tostring(r.val), r.ver))
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
        dst[k] = { val = v.val, ver = v.ver, active = v.active }
    end
    return dst
end

-- Handler: Commit / Verify Checkpoint Batch (Single or Multi-SU)
Handlers.add(
    "CommitCheckpoint",
    Handlers.utils.hasMatchingTag("Action", "CommitCheckpoint"),
    function(msg)
        local batch = json.decode(msg.Data)
        local batch_epoch = tonumber(batch.epoch) or 1
        local su_id = batch.su_id or msg.From or "SU_PRIMARY"
        local proposed_root = tostring(batch.proposed_state_root)
        local proposed_hash = string.upper(tostring(batch.proposed_hash or ""))
        local events = batch.events or {}

        -- 1. Check Epoch Fencing (Reject Zombie / Stale Hot Node)
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

        -- 2. Verify internal batch ordering (strictly monotonic by +1)
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

        -- 3. Check for Sequence Gap before batch starts
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

        -- 4. Stage state for Atomic All-or-Nothing Execution
        local staged_records = clone_records(Records)
        local staged_root = tonumber(OfficialStateRoot) or 0
        local running_hash = OfficialHash
        local staged_seq = OfficialSeq
        local new_events_to_commit = {}
        local deduplicated_count = 0

        for _, ev in ipairs(events) do
            local seq = tonumber(ev.seq)
            local ev_epoch = tonumber(ev.epoch) or batch_epoch
            local namespace = ev.namespace or "default"
            local action = ev.action or ev.op or "put"
            local val_type = ev.val_type or "int64"
            local key = tonumber(ev.key) or tostring(ev.key)
            local val = tonumber(ev.val or 0) or 0
            local canonical_val = tostring(ev.canonical_val or ev.val or 0)
            local given_hash = string.upper(tostring(ev.hash or ""))

            -- Independent Schema Rule Validation in Arbitrator
            local schema = Schemas[namespace]
            if schema and action == "put" and schema.rules and schema.rules.min_value ~= nil then
                if tonumber(ev.val) < schema.rules.min_value then
                    ao.send({
                        Target = msg.From,
                        Action = "Audit-Rejected",
                        Status = "FRAUD_DETECTED_AND_CUT",
                        SubStatus = "REJECTED_SCHEMA_VIOLATION",
                        ErrorCode = "ERR_SCHEMA_VIOLATION",
                        PolicyAction = safe_policy_action("on_schema_violation", "CUT_BRANCH_AND_REJECT_INVALID_SCHEMA"),
                        Reason = string.format("Schema violation in %s at seq %d: val < min_value", namespace, seq)
                    })
                    return
                end
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

            if seq <= OfficialSeq then
                -- Already committed sequence: Check Deduplication vs Equivocation!
                local existing = EventHistory[seq]
                if existing then
                    if existing.hash ~= given_hash or tostring(existing.key) ~= tostring(key) or existing.action ~= action then
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
                        -- Valid idempotent overlap
                        deduplicated_count = deduplicated_count + 1
                    end
                end
            else
                -- New uncommitted event (seq == staged_seq + 1)
                local action_code = (action == "del") and "22" or "20"
                local payload
                if namespace == "default" and action_code == "20" and val_type == "int64" then
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
                    -- Atomic abort: staged_records is discarded, Official state untouched!
                    ao.send({
                        Target = msg.From,
                        Action = "Audit-Rejected",
                        Status = "FRAUD_DETECTED_AND_CUT",
                        SubStatus = "REJECTED_HASH_CHAIN_BROKEN",
                        PolicyAction = safe_policy_action("on_hash_chain_broken", "CUT_BRANCH_AND_FREEZE_EPOCH"),
                        Reason = string.format("Hash chain broken at seq %d", seq),
                        OfficialSeq = tostring(OfficialSeq)
                    })
                    return
                end

                -- Recompute 64-bit Merkle-XOR State Root in Staged State
                local k_str = tostring(key)
                local cur = staged_records[k_str]

                if action == "put" then
                    if cur and cur.active then
                        local old_h = record_hash_64(key, cur.val, cur.ver)
                        local new_ver = cur.ver + 1
                        local new_h = record_hash_64(key, val, new_ver)
                        staged_root = staged_root ~ old_h ~ new_h
                        staged_records[k_str] = { val = val, ver = new_ver, active = true }
                    elseif cur and not cur.active then
                        -- Reactivating tombstone: version continues from tombstone version + 1
                        local new_ver = cur.ver + 1
                        local new_h = record_hash_64(key, val, new_ver)
                        staged_root = staged_root ~ new_h
                        staged_records[k_str] = { val = val, ver = new_ver, active = true }
                    else
                        local new_h = record_hash_64(key, val, 1)
                        staged_root = staged_root ~ new_h
                        staged_records[k_str] = { val = val, ver = 1, active = true }
                    end
                elseif action == "del" then
                    if cur and cur.active then
                        local old_h = record_hash_64(key, cur.val, cur.ver)
                        staged_root = staged_root ~ old_h
                        staged_records[k_str] = { val = cur.val, ver = cur.ver, active = false }
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
                    action = action,
                    key = key,
                    val = val,
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

        -- 6. ATOMIC COMMIT: All validations passed! Promote staged state to Official State
        Records = staged_records
        if batch_epoch > CurrentEpoch then
            CurrentEpoch = batch_epoch
        end
        OfficialSeq = staged_seq
        OfficialHash = running_hash
        OfficialStateRoot = tostring(staged_root)
        OfficialCommitment256 = compute_commitment_256(Records)

        for _, cev in ipairs(new_events_to_commit) do
            EventHistory[cev.seq] = {
                epoch = cev.epoch,
                seq = cev.seq,
                action = cev.action,
                key = cev.key,
                val = cev.val,
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
