#!/usr/bin/env escript
%% -*- coding: utf-8 -*-
%% Phase 03 Erlang Verification Runner: Canonical Event Hashing & Fixed Membership Quorum

main(_) ->
    DbDir = case os:getenv("DB_DIR") of
        false -> filename:dirname(filename:dirname(escript:script_name()));
        "" -> filename:dirname(filename:dirname(escript:script_name()));
        D -> D
    end,
    os:putenv("DB_DIR", DbDir),
    os:putenv("HYDB_CLUSTER_SECRET", "phase03-quorum-test-secret-999"),
    os:putenv("HYDB_CLUSTER_SIZE", "3"),
    os:putenv("HYDB_CLUSTER_MEMBERS", "su-node-1,su-node-2,su-node-3"),

    HotOutDir = filename:join([DbDir, "hymessage/out/ebin"]),
    SdOutDir = filename:join([DbDir, "sd/out/ebin"]),
    filelib:ensure_dir(filename:join([HotOutDir, "dummy"])),
    filelib:ensure_dir(filename:join([SdOutDir, "dummy"])),
    code:add_patha(HotOutDir),
    code:add_patha(SdOutDir),

    {ok, hydb_spec} = compile:file(filename:join([DbDir, "hymessage/src/hydb_spec.erl"]), [{outdir, HotOutDir}]),
    {ok, hydb_contract} = compile:file(filename:join([DbDir, "hymessage/src/hydb_contract.erl"]), [{outdir, HotOutDir}]),
    {ok, hymessage_runtime} = compile:file(filename:join([DbDir, "hymessage/src/hymessage_runtime.erl"]), [{outdir, HotOutDir}]),
    {ok, sd_runtime} = compile:file(filename:join([DbDir, "sd/src/sd_runtime.erl"]), [{outdir, SdOutDir}]),

    code:load_file(hydb_spec),
    code:load_file(hydb_contract),
    code:load_file(hymessage_runtime),
    code:load_file(sd_runtime),

    %% ========================================================================
    %% PART A: Production Canonical Event Hashing vs Golden Vectors
    %% ========================================================================
    Genesis = <<"0000000000000000000000000000000000000000000000000000000000000000">>,
    ValidSchema = #{
        namespace => <<"finance.balances">>,
        schema_version => <<"1.0.0">>,
        backend => <<"wasm_slot">>,
        key_type => <<"int64">>,
        value_type => <<"int64">>,
        require_auth => true,
        requires => [<<"cas">>],
        rules => #{ <<"min_value">> => 0, <<"max_records">> => 1000 }
    },

    Ev1 = #{
        contract_version => <<"hydb.core@1.0">>,
        prev_hash => Genesis,
        epoch => 1,
        seq => 1,
        op => <<"register">>,
        namespace => <<"finance.balances">>,
        schema_version => <<"1.0.0">>,
        key => absent,
        value_type => <<"schema">>,
        value => ValidSchema,
        ver => 1
    },
    ExpectedEv1 = <<"E561253C5DFCAD802E3BF463634820625D89599C7AAABD00A52AC2503991F091">>,
    HotEv1 = hymessage_runtime:compute_canonical_event_hash(Ev1),
    SdEv1 = sd_runtime:compute_canonical_event_hash(Ev1),
    assert_eq(HotEv1, ExpectedEv1, "Hot Node production canonical hash Ev1 matches golden vector"),
    assert_eq(SdEv1, ExpectedEv1, "SD Worker production canonical hash Ev1 matches golden vector"),

    Ev2 = #{
        <<"contract_version">> => <<"hydb.core@1.0">>,
        <<"prev_hash">> => ExpectedEv1,
        <<"epoch">> => 1,
        <<"seq">> => 2,
        <<"action">> => <<"put">>,
        <<"namespace">> => <<"finance.balances">>,
        <<"schema_version">> => <<"1.0.0">>,
        <<"key">> => 1001,
        <<"val_type">> => <<"int64">>,
        <<"val">> => 50000,
        <<"ver">> => 1
    },
    ExpectedEv2 = <<"39D70AAD7E1BAD407BFCAA7C9CB9D4FAE0C5E375421F971DE50B40751699AF74">>,
    HotEv2 = hymessage_runtime:compute_canonical_event_hash(Ev2),
    SdEv2 = sd_runtime:compute_canonical_event_hash(Ev2),
    assert_eq(HotEv2, ExpectedEv2, "Hot Node production canonical hash Ev2 (binary map keys) matches golden vector"),
    assert_eq(SdEv2, ExpectedEv2, "SD Worker production canonical hash Ev2 (binary map keys) matches golden vector"),

    Ev3 = #{
        <<"contract_version">> => <<"hydb.core@1.0">>,
        <<"prev_hash">> => ExpectedEv2,
        <<"epoch">> => 1,
        <<"seq">> => 3,
        <<"action">> => <<"del">>,
        <<"namespace">> => <<"finance.balances">>,
        <<"schema_version">> => <<"1.0.0">>,
        <<"key">> => 1001,
        <<"val_type">> => <<"int64">>,
        <<"ver">> => 2
    },
    ExpectedEv3 = <<"7FDF85C153633DC8FC34593EE8BDF295DB4B8768583FE925828DDA3C19299F54">>,
    HotEv3 = hymessage_runtime:compute_canonical_event_hash(Ev3),
    SdEv3 = sd_runtime:compute_canonical_event_hash(Ev3),
    assert_eq(HotEv3, ExpectedEv3, "Hot Node production canonical hash Ev3 (delete) matches golden vector"),
    assert_eq(SdEv3, ExpectedEv3, "SD Worker production canonical hash Ev3 (delete) matches golden vector"),

    Ev4 = #{
        <<"contract_version">> => <<"hydb.core@1.0">>,
        <<"prev_hash">> => ExpectedEv3,
        <<"epoch">> => 1,
        <<"seq">> => 4,
        <<"action">> => <<"put">>,
        <<"namespace">> => <<"finance.balances">>,
        <<"schema_version">> => <<"1.0.0">>,
        <<"key">> => 1001,
        <<"val_type">> => <<"int64">>,
        <<"val">> => 75000,
        <<"ver">> => 3
    },
    ExpectedEv4 = <<"045A706E4E1237BB1075722FFFBD9241E2F524322DD4D41BACBA1C4F8D03CF95">>,
    HotEv4 = hymessage_runtime:compute_canonical_event_hash(Ev4),
    SdEv4 = sd_runtime:compute_canonical_event_hash(Ev4),
    assert_eq(HotEv4, ExpectedEv4, "Hot Node production canonical hash Ev4 (re-put) matches golden vector"),
    assert_eq(SdEv4, ExpectedEv4, "SD Worker production canonical hash Ev4 (re-put) matches golden vector"),

    %% Verify SD Worker replay validator accepts canonical event hash and rejects tampered hash
    Ev2WithValidHash = Ev2#{ <<"hash">> => ExpectedEv2 },
    assert_eq(sd_runtime:compute_event_hash(Ev2WithValidHash, ExpectedEv1), ExpectedEv2,
        "SD Worker compute_event_hash accepts valid canonical hash"),

    Ev2WithTamperedHash = Ev2#{ <<"hash">> => <<"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF">> },
    ComputedForTampered = sd_runtime:compute_event_hash(Ev2WithTamperedHash, ExpectedEv1),
    assert_true(ComputedForTampered =/= <<"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF">>,
        "SD Worker compute_event_hash detects tampered hash"),

    %% ========================================================================
    %% PART B: Fixed Cluster Membership & Quorum Invariance on Node Disconnect
    %% ========================================================================
    ets:new(hymessage_store, [public, named_table, set]),
    ets:new(hymessage_subscribers, [public, named_table, set]),
    ets:insert(hymessage_store, {cluster_size, 3}),
    ets:insert(hymessage_store, {cluster_members, [<<"su-node-1">>, <<"su-node-2">>, <<"su-node-3">>]}),

    assert_eq(hymessage_runtime:get_configured_cluster_size(), 3, "Configured cluster size is 3"),

    %% 1. All 3 SU subscribers connected and ACK seq 1
    ets:insert(hymessage_subscribers, {sock1, <<"su-node-1">>, 1000, 1}),
    ets:insert(hymessage_subscribers, {sock2, <<"su-node-2">>, 1000, 1}),
    ets:insert(hymessage_subscribers, {sock3, <<"su-node-3">>, 1000, 1}),

    assert_true(hymessage_runtime:is_quorum_satisfied(1, <<"majority">>),
        "Majority quorum satisfied when 3 of 3 configured nodes ACK seq 1"),
    assert_true(hymessage_runtime:is_quorum_satisfied(1, <<"all">>),
        "All quorum satisfied when 3 of 3 configured nodes ACK seq 1"),

    %% 2. 1 node lags (only 2 of 3 ACK seq 2) -> majority (2/3) still satisfied, all (3/3) NOT satisfied
    ets:insert(hymessage_subscribers, {sock1, <<"su-node-1">>, 1000, 2}),
    ets:insert(hymessage_subscribers, {sock2, <<"su-node-2">>, 1000, 2}),
    assert_true(hymessage_runtime:is_quorum_satisfied(2, <<"majority">>),
        "Majority quorum satisfied when 2 of 3 configured nodes ACK seq 2"),
    assert_true(not hymessage_runtime:is_quorum_satisfied(2, <<"all">>),
        "All quorum NOT satisfied when only 2 of 3 configured nodes ACK seq 2"),

    %% 3. CRITICAL TEST: 2 of 3 nodes DISCONNECT (sockets removed from hymessage_subscribers)
    ets:delete(hymessage_subscribers, sock2),
    ets:delete(hymessage_subscribers, sock3),
    %% Remaining 1 node ACKs seq 3
    ets:insert(hymessage_subscribers, {sock1, <<"su-node-1">>, 1000, 3}),
    RemainingConnected = length(ets:tab2list(hymessage_subscribers)),
    assert_eq(RemainingConnected, 1, "Active connected subscribers dropped to 1"),
    assert_eq(hymessage_runtime:get_configured_cluster_size(), 3,
        "Configured cluster size remains 3 despite socket disconnections"),

    %% Even if caller passes TotalSubs=1 to legacy 3-arity function, majority quorum MUST BE FALSE!
    assert_true(not hymessage_runtime:is_quorum_satisfied(3, <<"majority">>),
        "Majority quorum (2/3) FAILS when only 1 node is connected and ACKs (quorum does not shrink)"),
    assert_true(not hymessage_runtime:is_quorum_satisfied(3, <<"majority">>, 1),
        "3-arity is_quorum_satisfied also FAILS with TotalSubs=1 when configured cluster size is 3"),
    assert_true(hymessage_runtime:is_quorum_satisfied(3, <<"single">>),
        "Single quorum (1 ACK) still succeeds with 1 connected node"),

    %% 4. CRITICAL TEST: Single node opening multiple sockets MUST be deduplicated by NodeId (counts as 1 ACK, not 3)
    ets:insert(hymessage_subscribers, {sock1, <<"su-node-1">>, 1000, 10}),
    ets:insert(hymessage_subscribers, {sock1_dup2, <<"su-node-1">>, 1001, 10}),
    ets:insert(hymessage_subscribers, {sock1_dup3, <<"su-node-1">>, 1002, 10}),
    ets:insert(hymessage_subscribers, {sock_rogue, <<"unconfigured-rogue-node">>, 1003, 10}),
    assert_eq(hymessage_runtime:count_acked_subscribers(10), 1,
        "Duplicate sockets for same NodeId and unconfigured NodeIds are deduplicated/filtered to 1 ACK"),
    assert_true(not hymessage_runtime:is_quorum_satisfied(10, <<"majority">>),
        "Single NodeId with 3 duplicate sockets CANNOT spoof 2/3 majority quorum"),

    %% 5. CRITICAL TEST: Handshake rejects valid-HMAC node if NodeId is not in configured cluster_members
    Nonce = <<"ABCDEF0123456789">>,
    SecretKey = crypto:hash(sha256, <<"phase03-quorum-test-secret-999">>),
    ValidNodeMsg = <<Nonce/binary, ":su-node-2">>,
    ValidNodeSig = binary:encode_hex(crypto:mac(hmac, sha256, SecretKey, ValidNodeMsg)),
    ValidHandshakePayload = iolist_to_binary(json:encode(#{
        <<"type">> => <<"AUTH_RESPONSE">>,
        <<"node_id">> => <<"su-node-2">>,
        <<"sig">> => ValidNodeSig,
        <<"last_tick">> => 5
    })),
    assert_eq(hymessage_runtime:verify_su_handshake(ValidHandshakePayload, Nonce),
        {ok, <<"su-node-2">>, 5},
        "verify_su_handshake accepts valid HMAC for configured cluster member su-node-2"),

    RogueNodeMsg = <<Nonce/binary, ":rogue-node-99">>,
    RogueNodeSig = binary:encode_hex(crypto:mac(hmac, sha256, SecretKey, RogueNodeMsg)),
    RogueHandshakePayload = iolist_to_binary(json:encode(#{
        <<"type">> => <<"AUTH_RESPONSE">>,
        <<"node_id">> => <<"rogue-node-99">>,
        <<"sig">> => RogueNodeSig,
        <<"last_tick">> => 5
    })),
    assert_eq(hymessage_runtime:verify_su_handshake(RogueHandshakePayload, Nonce),
        {error, not_in_cluster_members},
        "verify_su_handshake rejects valid HMAC when NodeId is not in configured cluster_members"),

    %% 6. Explicit membership change requires admin authorization & enforces cluster_size == length(cluster_members)
    os:putenv("HYDB_ADMIN_TOKEN", "phase03-distinct-admin-token-777"),
    MembershipFile = filename:join([DbDir, "hymessage/test_phase03_membership.json"]),
    _ = file:delete(MembershipFile),
    os:putenv("HYDB_MEMBERSHIP_PATH", MembershipFile),
    catch ets:delete(hymessage_store),
    catch ets:delete(hymessage_subscribers),
    {ok, _RuntimePid} = hymessage_runtime:ensure_started(),
    ets:insert(hymessage_subscribers, {sock1, <<"su-node-1">>, 1000, 3}),

    %% 6A. Mismatched cluster_size (1) vs cluster_members (3 nodes) MUST be rejected with 400 ERR_CLUSTER_SIZE_MISMATCH
    MismatchRes = hymessage_runtime:cluster_membership_from_req(#{}, #{
        <<"admin_token">> => <<"phase03-distinct-admin-token-777">>,
        <<"cluster_size">> => 1,
        <<"cluster_members">> => <<"su-node-1,su-node-2,su-node-3">>
    }, #{}),
    assert_eq(maps:get(<<"ok">>, MismatchRes), false,
        "Mismatched cluster_size=1 with 3 cluster_members is rejected"),
    assert_eq(maps:get(<<"status">>, MismatchRes), 400,
        "Mismatched cluster_size vs cluster_members returns HTTP 400"),
    assert_eq(maps:get(<<"error_code">>, MismatchRes), <<"ERR_CLUSTER_SIZE_MISMATCH">>,
        "Mismatched cluster_size vs cluster_members returns ERR_CLUSTER_SIZE_MISMATCH"),

    %% 6B. Even if ETS has cluster_size=1 while cluster_members has 3 nodes, get_configured_cluster_size returns 3
    ets:insert(hymessage_store, {cluster_size, 1}),
    ets:insert(hymessage_store, {cluster_members, [<<"su-node-1">>, <<"su-node-2">>, <<"su-node-3">>]}),
    assert_eq(hymessage_runtime:get_configured_cluster_size(), 3,
        "get_configured_cluster_size enforces max(cluster_size, length(cluster_members)) = 3"),
    assert_true(not hymessage_runtime:is_quorum_satisfied(3, <<"majority">>),
        "Majority quorum with 1 ACK still FAILS when cluster_members has 3 nodes despite cluster_size=1"),

    %% 6C. Membership persistence failure MUST fail closed without mutating ETS
    UnwritableDir = filename:join([DbDir, "hymessage/test_phase03_unwritable_dir"]),
    filelib:ensure_dir(filename:join([UnwritableDir, "dummy"])),
    os:putenv("HYDB_MEMBERSHIP_PATH", UnwritableDir),
    PersistFailRes = hymessage_runtime:cluster_membership_from_req(#{}, #{
        <<"admin_token">> => <<"phase03-distinct-admin-token-777">>,
        <<"cluster_size">> => 1,
        <<"cluster_members">> => <<"su-node-1">>
    }, #{}),
    assert_eq(maps:get(<<"ok">>, PersistFailRes), false,
        "Membership change fails when disk persistence fails"),
    assert_eq(maps:get(<<"status">>, PersistFailRes), 503,
        "Membership disk persistence failure returns HTTP 503"),
    assert_eq(maps:get(<<"error_code">>, PersistFailRes), <<"ERR_MEMBERSHIP_PERSIST_FAILED">>,
        "Membership disk persistence failure returns ERR_MEMBERSHIP_PERSIST_FAILED"),
    assert_eq(hymessage_runtime:get_configured_cluster_size(), 3,
        "ETS cluster membership remains unchanged (3) when disk persistence fails"),
    _ = file:delete(UnwritableDir ++ ".tmp"),
    _ = file:del_dir(UnwritableDir),

    %% 6D. Valid consistent membership change persists to disk and updates ETS
    os:putenv("HYDB_MEMBERSHIP_PATH", MembershipFile),
    ValidMemRes = hymessage_runtime:cluster_membership_from_req(#{}, #{
        <<"admin_token">> => <<"phase03-distinct-admin-token-777">>,
        <<"cluster_size">> => 1,
        <<"cluster_members">> => <<"su-node-1">>
    }, #{}),
    assert_eq(maps:get(<<"ok">>, ValidMemRes), true,
        "Consistent admin membership change (size=1, members=[su-node-1]) succeeds"),
    assert_true(hymessage_runtime:is_quorum_satisfied(3, <<"majority">>),
        "After explicit admin membership change to 1, majority quorum succeeds with 1 node"),
    _ = file:delete(MembershipFile),

    io:format("ALL_PHASE_03_ERL_TESTS_PASSED~n").

assert_eq(Actual, Expected, Label) ->
    case Actual =:= Expected of
        true ->
            io:format("PASS: ~s~n", [Label]);
        false ->
            io:format("FAIL: ~s (expected ~p, got ~p)~n", [Label, Expected, Actual]),
            halt(1)
    end.

assert_true( true, Label) ->
    io:format("PASS: ~s~n", [Label]);
assert_true(false, Label) ->
    io:format("FAIL: ~s~n", [Label]),
    halt(1).
