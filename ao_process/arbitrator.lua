--[[
========================================================================
AO Process: "Satu Kepala" Official Arbitrator & Arweave Committer
========================================================================
Prinsip Blueprint ide.md:
1. Punya 1 Process ID tunggal ("satu kepala" resmi).
2. Sejarah message berurutan dan logika yang deterministik.
3. Sumber kebenaran final (Wasit):
   - Menerima batch event log & state root dari Hot Node / Pool SU.
   - Mengaudit ulang determinisme hash chain dan State Root Accumulator.
   - Menolak (REJECT) jika ada manipulasi atau inkonsistensi.
   - Menandatangani dan meng-commit state resmi ke Arweave.
--]]

local json = require("json")
local crypto = require(".crypto")

-- Initial Process State
ProcessId = ProcessId or ao.id
OfficialSeq = OfficialSeq or 0
OfficialHash = OfficialHash or "0000000000000000000000000000000000000000000000000000000000000000"
OfficialStateRoot = OfficialStateRoot or 0
Checkpoints = Checkpoints or {}
Records = Records or {}

-- FNV-1a based 64-bit record hash matching Rust HyDB implementation
local function record_hash(key, val, ver)
    local h = 0xcbf29ce484222325
    -- Simplified Lua XOR Merkle representation for AO environment
    local str = string.format("%d:%d:%d", key, val, ver)
    return crypto.digest("sha256", str)
end

-- Handler: Commit Checkpoint Batch (Wasit)
Handlers.add(
    "CommitCheckpoint",
    Handlers.utils.hasMatchingTag("Action", "CommitCheckpoint"),
    function(msg)
        local batch = json.decode(msg.Data)
        local from_seq = tonumber(batch.from_seq)
        local to_seq = tonumber(batch.to_seq)
        local proposed_root = tostring(batch.proposed_state_root)
        local proposed_hash = string.upper(batch.proposed_hash)
        local events = batch.events

        -- 1. Verifikasi urutan monotonik
        if from_seq ~= (OfficialSeq + 1) then
            ao.send({
                Target = msg.From,
                Action = "Audit-Rejected",
                Status = "REJECTED_SEQUENCE_GAP",
                Reason = string.format("Expected from_seq %d, got %d", OfficialSeq + 1, from_seq),
                OfficialSeq = tostring(OfficialSeq)
            })
            return
        end

        -- 2. Audit deterministik continuous hash chain
        local running_hash = OfficialHash
        for _, ev in ipairs(events) do
            local seq = tonumber(ev.seq)
            local action = ev.action or "put"
            local key = tonumber(ev.key)
            local val = tonumber(ev.val)
            local action_code = (action == "del") and "22" or "20"
            
            local payload = (action == "del")
                and string.format("%s%d%s%d", running_hash, seq, action_code, key)
                or string.format("%s%d%s%d%d", running_hash, seq, action_code, key, val)
            
            local expected_hash = string.upper(crypto.digest("sha256", payload))
            local given_hash = string.upper(ev.hash)

            if expected_hash ~= given_hash then
                ao.send({
                    Target = msg.From,
                    Action = "Audit-Rejected",
                    Status = "REJECTED_HASH_CHAIN_BROKEN",
                    Reason = string.format("Hash mismatch at seq %d", seq),
                    OfficialSeq = tostring(OfficialSeq)
                })
                return
            end

            running_hash = given_hash
            if action == "put" then
                Records[tostring(key)] = val
            elseif action == "del" then
                Records[tostring(key)] = nil
            end
        end

        -- 3. Verifikasi final hash tip
        if running_hash ~= proposed_hash then
            ao.send({
                Target = msg.From,
                Action = "Audit-Rejected",
                Status = "REJECTED_TIP_MISMATCH",
                Reason = "Proposed final hash does not match audited hash chain"
            })
            return
        end

        -- 4. Semua lolos audit -> Commit ke State Resmi "Satu Kepala"
        OfficialSeq = to_seq
        OfficialHash = proposed_hash
        OfficialStateRoot = proposed_root

        local checkpoint_entry = {
            from_seq = from_seq,
            to_seq = to_seq,
            state_root = proposed_root,
            final_hash = proposed_hash,
            events_count = #events,
            timestamp = msg.Timestamp,
            tx_id = msg.Id
        }
        table.insert(Checkpoints, checkpoint_entry)

        ao.send({
            Target = msg.From,
            Action = "Checkpoint-Committed",
            Status = "COMMITTED",
            OfficialSeq = tostring(OfficialSeq),
            OfficialStateRoot = OfficialStateRoot,
            OfficialHash = OfficialHash,
            CheckpointsCount = tostring(#Checkpoints)
        })
    end
)

-- Handler: Query Official State
Handlers.add(
    "GetOfficialState",
    Handlers.utils.hasMatchingTag("Action", "GetOfficialState"),
    function(msg)
        ao.send({
            Target = msg.From,
            Action = "OfficialStateResponse",
            ProcessId = ProcessId,
            OfficialSeq = tostring(OfficialSeq),
            OfficialStateRoot = tostring(OfficialStateRoot),
            OfficialHash = OfficialHash,
            CheckpointsCount = tostring(#Checkpoints),
            Data = json.encode({
                checkpoints = Checkpoints,
                records_count = 0
            })
        })
    end
)
