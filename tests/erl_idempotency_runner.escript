#!/usr/bin/env escript
%% -*- coding: utf-8 -*-
%% Phase 04 Erlang Verification Runner: Idempotency & Explicit Write Status Semantics

main(_) ->
    DbDir = case os:getenv("DB_DIR") of
        false -> filename:dirname(filename:dirname(escript:script_name()));
        "" -> filename:dirname(filename:dirname(escript:script_name()));
        D -> D
    end,
    os:putenv("DB_DIR", DbDir),
    os:putenv("HYDB_PROFILE", "test"),
    os:putenv("HYDB_CLUSTER_SECRET", "phase04-idempotency-secret-888"),
    os:putenv("HYDB_ADMIN_TOKEN", "phase04-distinct-admin-token-999"),
    os:putenv("HYDB_CLUSTER_SIZE", "1"),
    os:putenv("HYDB_CLUSTER_MEMBERS", "su-node-1"),
    HotWalFile = filename:join([DbDir, "hymessage/test_phase04_hot_wal.log"]),
    HotEpochFile = filename:join([DbDir, "hymessage/test_phase04_hot_epoch.meta"]),
    _ = file:delete(HotWalFile),
    _ = file:delete(HotEpochFile),
    os:putenv("HYDB_HOT_WAL_PATH", HotWalFile),
    os:putenv("HYDB_HOT_EPOCH_PATH", HotEpochFile),

    HotOutDir = filename:join([DbDir, "hymessage/out/ebin"]),
    filelib:ensure_dir(filename:join([HotOutDir, "dummy"])),
    code:add_patha(HotOutDir),

    {ok, hydb_spec} = compile:file(filename:join([DbDir, "hymessage/src/hydb_spec.erl"]), [{outdir, HotOutDir}]),
    {ok, hydb_contract} = compile:file(filename:join([DbDir, "hymessage/src/hydb_contract.erl"]), [{outdir, HotOutDir}]),
    {ok, hymessage_runtime} = compile:file(filename:join([DbDir, "hymessage/src/hymessage_runtime.erl"]), [{outdir, HotOutDir}]),

    code:load_file(hydb_spec),
    code:load_file(hydb_contract),
    code:load_file(hymessage_runtime),

    {ok, _Pid} = hymessage_runtime:ensure_started(),

    %% ========================================================================
    %% 1. Idempotent PUT & Deduplication on Retry
    %% ========================================================================
    PutReq1 = #{
        <<"namespace">> => <<"app.docs">>,
        <<"key">> => <<"doc:order-1">>,
        <<"val">> => <<"initial-order-payload">>,
        <<"request_id">> => <<"req-put-001">>,
        <<"timeout_ms">> => 20
    },
    Res1 = hymessage_runtime:put_from_req(#{}, PutReq1, #{}),
    assert_eq(maps:get(<<"ok">>, Res1), true, "First PUT with request_id succeeds"),
    assert_eq(maps:get(<<"seq">>, Res1), 1, "First PUT allocated seq 1"),
    assert_eq(maps:get(<<"ver">>, Res1), 1, "First PUT record version is 1"),
    assert_eq(maps:get(<<"write_status">>, Res1), <<"accepted">>, "Without SU ACK, write_status is 'accepted'"),
    assert_eq(maps:get(<<"commit_status">>, Res1), <<"provisional">>, "Without SU ACK, commit_status is 'provisional'"),
    assert_eq(maps:get(<<"request_id">>, Res1), <<"req-put-001">>, "Response echoes request_id"),
    RootAfter1 = maps:get(<<"state_root">>, Res1),

    %% Retry the exact same PUT request with the same request_id
    Res1Retry = hymessage_runtime:put_from_req(#{}, PutReq1, #{}),
    assert_eq(maps:get(<<"ok">>, Res1Retry), true, "Retried PUT with same request_id succeeds"),
    assert_eq(maps:get(<<"seq">>, Res1Retry), 1, "Retried PUT returns original seq 1 without incrementing sequence"),
    assert_eq(maps:get(<<"ver">>, Res1Retry), 1, "Retried PUT returns original ver 1 without double mutation"),
    assert_eq(maps:get(<<"state_root">>, Res1Retry), RootAfter1, "Retried PUT preserves exact state_root"),
    assert_eq(maps:get(<<"idempotent_replay">>, Res1Retry), true, "Retried PUT is marked with idempotent_replay=true"),

    %% ========================================================================
    %% 2. Conflict Rejection on Reused request_id with Different Payload
    %% ========================================================================
    PutReqConflict = PutReq1#{ <<"val">> => <<"tampered-different-payload">> },
    ResConflict = hymessage_runtime:put_from_req(#{}, PutReqConflict, #{}),
    assert_eq(maps:get(<<"ok">>, ResConflict), false, "Reusing request_id with different payload is rejected"),
    assert_eq(maps:get(<<"status">>, ResConflict), 409, "Idempotency conflict returns HTTP 409"),
    assert_eq(maps:get(<<"error_code">>, ResConflict), <<"ERR_IDEMPOTENCY_KEY_CONFLICT">>,
        "Idempotency conflict returns ERR_IDEMPOTENCY_KEY_CONFLICT"),

    %% ========================================================================
    %% 3. Client Request Status Lookup & Promotion from 'accepted' -> 'durable'
    %% ========================================================================
    LookupBeforeAck = hymessage_runtime:request_status_from_req(#{}, #{ <<"request_id">> => <<"req-put-001">> }, #{}),
    assert_eq(maps:get(<<"found">>, LookupBeforeAck), true, "Request status lookup finds req-put-001"),
    assert_eq(maps:get(<<"seq">>, LookupBeforeAck), 1, "Request status reports seq 1"),
    assert_eq(maps:get(<<"write_status">>, LookupBeforeAck), <<"accepted">>, "Before SU ACK, lookup reports write_status='accepted'"),

    %% Simulate SU subscriber acknowledging seq 1
    ets:insert(hymessage_subscribers, {su_sock_1, <<"su-node-1">>, 1000, 1}),

    LookupAfterAck = hymessage_runtime:request_status_from_req(#{}, #{ <<"request_id">> => <<"req-put-001">> }, #{}),
    assert_eq(maps:get(<<"write_status">>, LookupAfterAck), <<"durable">>,
        "After SU quorum ACK, request status lookup promotes write_status to 'durable'"),
    assert_eq(maps:get(<<"commit_status">>, LookupAfterAck), <<"durable">>,
        "After SU quorum ACK, request status lookup promotes commit_status to 'durable'"),
    assert_eq(maps:get(<<"replicated">>, LookupAfterAck), true,
        "After SU quorum ACK, request status lookup reports replicated=true"),

    %% Lookup unknown request_id
    LookupUnknown = hymessage_runtime:request_status_from_req(#{}, #{ <<"request_id">> => <<"req-never-sent-999">> }, #{}),
    assert_eq(maps:get(<<"found">>, LookupUnknown), false, "Unknown request_id returns found=false"),
    assert_eq(maps:get(<<"write_status">>, LookupUnknown), <<"unknown">>, "Unknown request_id returns write_status='unknown'"),
    assert_eq(maps:get(<<"commit_status">>, LookupUnknown), <<"unknown">>, "Unknown request_id returns commit_status='unknown'"),

    %% ========================================================================
    %% 4. Idempotent DELETE Retry (Does Not Fail With ERR_NOT_FOUND on Retry)
    %% ========================================================================
    DelReq = #{
        <<"namespace">> => <<"app.docs">>,
        <<"key">> => <<"doc:order-1">>,
        <<"request_id">> => <<"req-del-002">>,
        <<"timeout_ms">> => 20
    },
    DelRes1 = hymessage_runtime:del_from_req(#{}, DelReq, #{}),
    assert_eq(maps:get(<<"ok">>, DelRes1), true, "First DEL with request_id succeeds"),
    assert_eq(maps:get(<<"seq">>, DelRes1), 2, "First DEL allocated seq 2"),
    assert_eq(maps:get(<<"ver">>, DelRes1), 2, "First DEL tombstone version is 2"),

    %% Retry DEL with same request_id (without idempotency this would fail with 404 ERR_NOT_FOUND!)
    DelResRetry = hymessage_runtime:del_from_req(#{}, DelReq, #{}),
    assert_eq(maps:get(<<"ok">>, DelResRetry), true, "Retried DEL with same request_id succeeds instead of 404"),
    assert_eq(maps:get(<<"seq">>, DelResRetry), 2, "Retried DEL returns original seq 2"),
    assert_eq(maps:get(<<"idempotent_replay">>, DelResRetry), true, "Retried DEL is marked idempotent_replay=true"),

    %% ========================================================================
    %% 5. Idempotent Schema Registration Retry
    %% ========================================================================
    SchemaReq = #{
        <<"action">> => <<"register">>,
        <<"namespace">> => <<"custom.inventory">>,
        <<"backend">> => <<"ets_doc">>,
        <<"schema_version">> => <<"1.0.0">>,
        <<"key_type">> => <<"string">>,
        <<"value_type">> => <<"string">>,
        <<"request_id">> => <<"req-schema-003">>
    },
    SchemaRes1 = hymessage_runtime:schema_from_req(#{}, SchemaReq, #{}),
    assert_eq(maps:get(<<"ok">>, SchemaRes1), true, "First schema registration succeeds"),
    assert_eq(maps:get(<<"seq">>, SchemaRes1), 3, "First schema registration allocated seq 3"),

    SchemaResRetry = hymessage_runtime:schema_from_req(#{}, SchemaReq, #{}),
    assert_eq(maps:get(<<"ok">>, SchemaResRetry), true, "Retried schema registration with same request_id succeeds"),
    assert_eq(maps:get(<<"seq">>, SchemaResRetry), 3, "Retried schema registration returns original seq 3"),
    assert_eq(maps:get(<<"idempotent_replay">>, SchemaResRetry), true, "Retried schema registration is marked idempotent_replay=true"),

    %% ========================================================================
    %% 6. Cold Restart Durability: Epoch, State & Idempotency Survive Restart
    %% ========================================================================
    {ok, PromRes} = hymessage_runtime:promote_epoch(3),
    assert_eq(maps:get(<<"epoch">>, PromRes), 3, "Epoch promoted to 3 before restart"),

    PutReqBeforeRestart = #{
        <<"namespace">> => <<"app.docs">>,
        <<"key">> => <<"doc:survive-restart">>,
        <<"val">> => <<"durable-across-hot-restart">>,
        <<"request_id">> => <<"req-restart-004">>,
        <<"epoch">> => 3,
        <<"timeout_ms">> => 20
    },
    ResBeforeRestart = hymessage_runtime:put_from_req(#{}, PutReqBeforeRestart, #{}),
    assert_eq(maps:get(<<"ok">>, ResBeforeRestart), true, "PUT at epoch 3 before restart succeeds"),
    assert_eq(maps:get(<<"seq">>, ResBeforeRestart), 4, "PUT allocated seq 4 before restart"),
    StateBeforeRestart = hymessage_runtime:get_state_from_req(#{}, #{}, #{}),
    RootBeforeRestart = maps:get(<<"db_root_hash">>, StateBeforeRestart),

    %% Simulate full Hot Node process crash/restart (stop gen_server & wipe ETS tables)
    ok = gen_server:stop(hymessage_runtime),
    catch ets:delete(hymessage_store),
    catch ets:delete(hymessage_idempotency_cache),
    catch ets:delete(hymessage_sessions),
    hydb_contract:reset_tables(),

    %% Start Hot Node again — production init([]) reconciles epoch, WAL, and idempotency cache!
    {ok, _NewPid} = hymessage_runtime:ensure_started(),
    StateAfterRestart = hymessage_runtime:get_state_from_req(#{}, #{}, #{}),
    assert_eq(maps:get(<<"epoch">>, StateAfterRestart), 3, "Hot Node init([]) restores promoted epoch 3 from disk"),
    assert_eq(maps:get(<<"seq">>, StateAfterRestart), 4, "Hot Node init([]) restores seq 4 from hot_wal.log"),
    assert_eq(maps:get(<<"db_root_hash">>, StateAfterRestart), RootBeforeRestart, "Hot Node init([]) restores exact state_root from hot_wal.log"),

    %% Verify stale epoch is still fenced after restart
    StaleAfterRestart = hymessage_runtime:put_from_req(#{}, #{
        <<"namespace">> => <<"app.docs">>,
        <<"key">> => <<"doc:stale">>,
        <<"val">> => <<"should-fail">>,
        <<"request_id">> => <<"req-stale-after-restart">>,
        <<"epoch">> => 2
    }, #{}),
    assert_eq(maps:get(<<"error_code">>, StaleAfterRestart), <<"FENCED_STALE_EPOCH">>,
        "Stale epoch write rejected with FENCED_STALE_EPOCH after Hot Node restart"),

    %% Verify idempotency lookup & retry survive Hot Node restart
    LookupAfterRestart = hymessage_runtime:request_status_from_req(#{}, #{ <<"request_id">> => <<"req-restart-004">> }, #{}),
    assert_eq(maps:get(<<"found">>, LookupAfterRestart), true, "request_status lookup finds req-restart-004 after Hot Node restart"),
    assert_eq(maps:get(<<"seq">>, LookupAfterRestart), 4, "request_status preserves seq 4 after Hot Node restart"),

    RetryAfterRestart = hymessage_runtime:put_from_req(#{}, PutReqBeforeRestart, #{}),
    assert_eq(maps:get(<<"ok">>, RetryAfterRestart), true, "Retried PUT after Hot Node restart succeeds"),
    assert_eq(maps:get(<<"seq">>, RetryAfterRestart), 4, "Retried PUT after Hot Node restart returns original seq 4 without re-sequencing"),
    assert_eq(maps:get(<<"idempotent_replay">>, RetryAfterRestart), true, "Retried PUT after Hot Node restart sets idempotent_replay=true"),

    %% ========================================================================
    %% 7. Production Mandatory request_id & Bounded Idempotency Table Eviction
    %% ========================================================================
    os:putenv("HYDB_PROFILE", "production"),
    MissingReqIdRes = hymessage_runtime:put_from_req(#{}, #{
        <<"namespace">> => <<"app.docs">>,
        <<"key">> => <<"doc:no-req-id">>,
        <<"val">> => <<"rejected-in-prod">>,
        <<"epoch">> => 3
    }, #{}),
    assert_eq(maps:get(<<"status">>, MissingReqIdRes), 400, "Missing request_id returns HTTP 400 in production profile"),
    assert_eq(maps:get(<<"error_code">>, MissingReqIdRes), <<"ERR_MISSING_REQUEST_ID">>,
        "Missing request_id rejected with ERR_MISSING_REQUEST_ID in production profile"),
    os:putenv("HYDB_PROFILE", "test"),

    os:putenv("HYDB_IDEMPOTENCY_MAX_ENTRIES", "3"),
    ets:delete_all_objects(hymessage_idempotency_cache),
    lists:foreach(fun(I) ->
        IBin = integer_to_binary(I),
        _ = hymessage_runtime:put_from_req(#{}, #{
            <<"namespace">> => <<"app.docs">>,
            <<"key">> => <<"doc:bounded-", IBin/binary>>,
            <<"val">> => <<"val-", IBin/binary>>,
            <<"request_id">> => <<"req-bounded-", IBin/binary>>,
            <<"epoch">> => 3,
            <<"timeout_ms">> => 10
        }, #{})
    end, [1, 2, 3, 4, 5]),
    CacheSize = ets:info(hymessage_idempotency_cache, size),
    assert_eq(CacheSize =< 3, true, "Idempotency cache size is bounded by HYDB_IDEMPOTENCY_MAX_ENTRIES"),

    _ = file:delete(HotWalFile),
    _ = file:delete(HotEpochFile),
    io:format("ALL_PHASE_04_ERL_TESTS_PASSED~n").

assert_eq(Actual, Expected, Label) ->
    case Actual =:= Expected of
        true ->
            io:format("PASS: ~s~n", [Label]);
        false ->
            io:format("FAIL: ~s (expected ~p, got ~p)~n", [Label, Expected, Actual]),
            halt(1)
    end.
