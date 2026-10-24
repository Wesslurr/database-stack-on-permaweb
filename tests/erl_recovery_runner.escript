#!/usr/bin/env escript
%% -*- coding: utf-8 -*-
%% Phase 02: Erlang WAL Recovery & Corruption Handling Test Runner

main(_) ->
    DbDir = case os:getenv("DB_DIR") of
        false -> filename:dirname(filename:dirname(escript:script_name()));
        "" -> filename:dirname(filename:dirname(escript:script_name()));
        D -> D
    end,
    os:putenv("DB_DIR", DbDir),
    WalletPath = filename:join([DbDir, "secrets/wallet.json"]),
    os:putenv("WALLET_PATH", WalletPath),
    OutDir = filename:join([DbDir, "sd/out/ebin"]),
    filelib:ensure_dir(filename:join([OutDir, "dummy"])),
    code:add_path(OutDir),

    %% Compile sd_runtime.erl dynamically
    SdSrc = filename:join([DbDir, "sd/src/sd_runtime.erl"]),
    case compile:file(SdSrc, [{outdir, OutDir}, return_errors]) of
        {ok, sd_runtime} -> code:load_file(sd_runtime);
        Err -> io:format("Compile error sd_runtime: ~p~n", [Err]), halt(1)
    end,

    sd_runtime:ensure_started(),
    TestWal = filename:join([DbDir, "sd/test_recovery_wal.log"]),
    os:putenv("HYDB_WAL_PATH", TestWal),

    %% Helper to generate signed events
    GenesisHash = <<"0000000000000000000000000000000000000000000000000000000000000000">>,

    %% Build 3 valid events
    Ev1Data = #{
        <<"seq">> => 1,
        <<"epoch">> => 1,
        <<"action">> => <<"put">>,
        <<"namespace">> => <<"default">>,
        <<"key">> => 101,
        <<"val">> => 500,
        <<"ver">> => 1,
        <<"prev_hash">> => GenesisHash
    },
    Ev1Hash = compute_hash(Ev1Data, GenesisHash),
    Ev1Root = sd_runtime:canonical_hash_64(101, 500, 1),
    Ev1 = Ev1Data#{ <<"hash">> => Ev1Hash, <<"state_root">> => Ev1Root },

    Ev2Data = #{
        <<"seq">> => 2,
        <<"epoch">> => 1,
        <<"action">> => <<"put">>,
        <<"namespace">> => <<"default">>,
        <<"key">> => 102,
        <<"val">> => 750,
        <<"ver">> => 1,
        <<"prev_hash">> => Ev1Hash
    },
    Ev2Hash = compute_hash(Ev2Data, Ev1Hash),
    H2 = sd_runtime:canonical_hash_64(102, 750, 1),
    Ev2Root = to_signed_64(((Ev1Root bxor 0) bxor H2) band 16#FFFFFFFFFFFFFFFF),
    Ev2 = Ev2Data#{ <<"hash">> => Ev2Hash, <<"state_root">> => Ev2Root },

    Ev3Data = #{
        <<"seq">> => 3,
        <<"epoch">> => 1,
        <<"action">> => <<"put">>,
        <<"namespace">> => <<"default">>,
        <<"key">> => 101,
        <<"val">> => 600,
        <<"ver">> => 2,
        <<"prev_hash">> => Ev2Hash
    },
    Ev3Hash = compute_hash(Ev3Data, Ev2Hash),
    OldH1 = Ev1Root,
    NewH1 = sd_runtime:canonical_hash_64(101, 600, 2),
    Ev3Root = to_signed_64(((Ev2Root bxor OldH1) bxor NewH1) band 16#FFFFFFFFFFFFFFFF),
    Ev3 = Ev3Data#{ <<"hash">> => Ev3Hash, <<"state_root">> => Ev3Root },

    %% ========================================================================
    %% TEST 1: Clean, Valid WAL Recovery
    %% ========================================================================
    ValidContent = [
        json:encode(Ev1), <<"\n">>,
        json:encode(Ev2), <<"\n">>,
        json:encode(Ev3), <<"\n">>
    ],
    ok = file:write_file(TestWal, ValidContent),
    Restart1 = sd_runtime:simulate_restart_from_wal(),
    case Restart1 of
        #{ <<"recovered_entries">> := 3, <<"tick">> := 3, <<"state_root">> := Ev3Root, <<"ready">> := true, <<"health">> := <<"healthy">> } ->
            io:format("PASS: valid WAL recovers exact 3 entries and matching dynamic state_root~n");
        Other1 ->
            io:format("FAIL: unexpected valid recovery: ~p (expected root: ~p)~n", [Other1, Ev3Root]),
            halt(1)
    end,

    %% ========================================================================
    %% TEST 2: Clean EOF Truncation & Physical Repair + Subsequent Append + Restart
    %% ========================================================================
    TruncContent = [
        ValidContent,
        <<"{\"action\":\"put\",\"backend\":\"wasm_slot\",\"canonical_val\":\"999\",\"contract_ver">>
    ],
    ok = file:write_file(TestWal, TruncContent),
    Restart2 = sd_runtime:simulate_restart_from_wal(),
    case Restart2 of
        #{ <<"ok">> := true, <<"recovered_entries">> := 3, <<"tick">> := 3, <<"truncated_detected">> := true, <<"ready">> := true, <<"health">> := <<"healthy">> } ->
            io:format("PASS: EOF truncation detected cleanly, node recovers up to last valid record and stays healthy~n");
        Other2 ->
            io:format("FAIL: unexpected EOF truncation recovery: ~p~n", [Other2]),
            halt(1)
    end,

    %% Now append a 4th event after EOF truncation recovery and restart again:
    %% Because repair_truncated_wal_file physically removed the corrupt trailing bytes,
    %% Ev4 must NOT be glued onto the broken line and must survive the next restart!
    Ev4Data = #{
        <<"seq">> => 4,
        <<"epoch">> => 1,
        <<"action">> => <<"put">>,
        <<"namespace">> => <<"default">>,
        <<"key">> => 103,
        <<"val">> => 900,
        <<"ver">> => 1,
        <<"prev_hash">> => Ev3Hash
    },
    Ev4Hash = compute_hash(Ev4Data, Ev3Hash),
    H4 = sd_runtime:canonical_hash_64(103, 900, 1),
    Ev4Root = to_signed_64(((Ev3Root bxor 0) bxor H4) band 16#FFFFFFFFFFFFFFFF),
    Ev4 = Ev4Data#{ <<"hash">> => Ev4Hash, <<"state_root">> => Ev4Root },
    ok = file:write_file(TestWal, [json:encode(Ev4), <<"\n">>], [append]),
    Restart2B = sd_runtime:simulate_restart_from_wal(),
    case Restart2B of
        #{ <<"ok">> := true, <<"recovered_entries">> := 4, <<"tick">> := 4, <<"state_root">> := Ev4Root, <<"ready">> := true, <<"health">> := <<"healthy">> } ->
            io:format("PASS: physical EOF truncation repair allows subsequent append and restart to recover all 4 records~n");
        Other2B ->
            io:format("FAIL: post-truncation append + restart lost data or corrupted WAL: ~p~n", [Other2B]),
            halt(1)
    end,

    %% ========================================================================
    %% TEST 3: Mid-File Corruption (Unparseable line followed by other records)
    %% ========================================================================
    MidCorruptContent = [
        json:encode(Ev1), <<"\n">>,
        <<"CORRUPT_GARBAGE_PAYLOAD_MID_FILE_CORRUPTION\n">>,
        json:encode(Ev2), <<"\n">>
    ],
    ok = file:write_file(TestWal, MidCorruptContent),
    Restart3 = sd_runtime:simulate_restart_from_wal(),
    case Restart3 of
        #{ <<"ok">> := false, <<"status">> := 503, <<"recovered_entries">> := 1, <<"ready">> := false, <<"health">> := <<"degraded">>, <<"wal_corrupted">> := true } ->
            io:format("PASS: mid-file corruption puts node into degraded status and stops write readiness~n");
        Other3 ->
            io:format("FAIL: mid-file corruption did not degrade node: ~p~n", [Other3]),
            halt(1)
    end,

    %% ========================================================================
    %% TEST 4: Sequence Gap Detection
    %% ========================================================================
    EvGapData = Ev2Data#{ <<"seq">> => 99 },
    EvGapHash = compute_hash(EvGapData, Ev1Hash),
    EvGap = EvGapData#{ <<"hash">> => EvGapHash },
    GapContent = [
        json:encode(Ev1), <<"\n">>,
        json:encode(EvGap), <<"\n">>
    ],
    ok = file:write_file(TestWal, GapContent),
    Restart4 = sd_runtime:simulate_restart_from_wal(),
    case Restart4 of
        #{ <<"recovered_entries">> := 1, <<"ready">> := false, <<"health">> := <<"degraded">> } ->
            io:format("PASS: sequence gap halts recovery and sets health to degraded~n");
        Other4 ->
            io:format("FAIL: sequence gap not handled properly: ~p~n", [Other4]),
            halt(1)
    end,

    %% ========================================================================
    %% TEST 5: Broken Hash Chain Detection
    %% ========================================================================
    EvBrokenChain = Ev2Data#{ <<"prev_hash">> => <<"BAD_HASH_FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF">> },
    BrokenChainHash = compute_hash(EvBrokenChain, <<"BAD_HASH_FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF">>),
    EvBrokenChainSealed = EvBrokenChain#{ <<"hash">> => BrokenChainHash },
    BrokenChainContent = [
        json:encode(Ev1), <<"\n">>,
        json:encode(EvBrokenChainSealed), <<"\n">>
    ],
    ok = file:write_file(TestWal, BrokenChainContent),
    Restart5 = sd_runtime:simulate_restart_from_wal(),
    case Restart5 of
        #{ <<"recovered_entries">> := 1, <<"ready">> := false, <<"health">> := <<"degraded">> } ->
            io:format("PASS: broken hash chain halts recovery and sets health to degraded~n");
        Other5 ->
            io:format("FAIL: broken hash chain not handled properly: ~p~n", [Other5]),
            halt(1)
    end,

    %% ========================================================================
    %% TEST 6: Tampered Record Value (Hash Mismatch)
    %% ========================================================================
    EvTamperedVal = Ev2#{ <<"val">> => 999999 },
    TamperedValContent = [
        json:encode(Ev1), <<"\n">>,
        json:encode(EvTamperedVal), <<"\n">>
    ],
    ok = file:write_file(TestWal, TamperedValContent),
    Restart6 = sd_runtime:simulate_restart_from_wal(),
    case Restart6 of
        #{ <<"recovered_entries">> := 1, <<"ready">> := false, <<"health">> := <<"degraded">> } ->
            io:format("PASS: tampered record value detected via hash mismatch and sets health to degraded~n");
        Other6 ->
            io:format("FAIL: tampered value not detected: ~p~n", [Other6]),
            halt(1)
    end,

    %% ========================================================================
    %% TEST 7: Tampered State Root (State Commitment Mismatch)
    %% ========================================================================
    EvTamperedRoot = Ev2#{ <<"state_root">> => 123456789 },
    TamperedRootContent = [
        json:encode(Ev1), <<"\n">>,
        json:encode(EvTamperedRoot), <<"\n">>
    ],
    ok = file:write_file(TestWal, TamperedRootContent),
    Restart7 = sd_runtime:simulate_restart_from_wal(),
    case Restart7 of
        #{ <<"ok">> := false, <<"recovered_entries">> := 1, <<"ready">> := false, <<"health">> := <<"degraded">> } ->
            io:format("PASS: forged state_root detected by dynamic recomputation and sets health to degraded~n");
        Other7 ->
            io:format("FAIL: forged state_root not caught: ~p~n", [Other7]),
            halt(1)
    end,

    %% ========================================================================
    %% TEST 8: Missing state_root Field on WAL Record Rejected
    %% ========================================================================
    EvMissingRoot = maps:remove(<<"state_root">>, Ev2),
    MissingRootContent = [
        json:encode(Ev1), <<"\n">>,
        json:encode(EvMissingRoot), <<"\n">>
    ],
    ok = file:write_file(TestWal, MissingRootContent),
    Restart8 = sd_runtime:simulate_restart_from_wal(),
    case Restart8 of
        #{ <<"ok">> := false, <<"recovered_entries">> := 1, <<"ready">> := false, <<"health">> := <<"degraded">>, <<"wal_corrupted">> := true } ->
            io:format("PASS: WAL record missing state_root field is rejected and sets health to degraded~n");
        Other8 ->
            io:format("FAIL: missing state_root was not rejected: ~p~n", [Other8]),
            halt(1)
    end,

    %% Cleanup test WAL
    _ = file:delete(TestWal),
    io:format("All Erlang Phase 02 recovery checks passed!~n").

compute_hash(Event, LastHash) ->
    Seq = maps:get(<<"seq">>, Event, 0),
    Key = maps:get(<<"key">>, Event, 1),
    Val = maps:get(<<"val">>, Event, 0),
    Payload = [
        LastHash,
        integer_to_binary(Seq),
        <<"20">>,
        integer_to_binary(Key),
        integer_to_binary(Val)
    ],
    binary:encode_hex(crypto:hash(sha256, Payload)).

to_signed_64(U) when U >= 16#8000000000000000 ->
    U - 16#10000000000000000;
to_signed_64(U) ->
    U.
