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
    os:putenv("HYDB_CLUSTER_SECRET", "phase04-idempotency-secret-888"),
    os:putenv("HYDB_CLUSTER_SIZE", "1"),

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

    io:format("ALL_PHASE_04_ERL_TESTS_PASSED~n").

assert_eq(Actual, Expected, Label) ->
    case Actual =:= Expected of
        true ->
            io:format("PASS: ~s~n", [Label]);
        false ->
            io:format("FAIL: ~s (expected ~p, got ~p)~n", [Label, Expected, Actual]),
            halt(1)
    end.
