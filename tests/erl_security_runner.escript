#!/usr/bin/env escript
%% -*- coding: utf-8 -*-
%% Phase 01: Erlang Control Endpoint & Cluster Secret Security Verification

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
    HotOutDir = filename:join([DbDir, "hymessage/out/ebin"]),
    filelib:ensure_dir(filename:join([OutDir, "dummy"])),
    filelib:ensure_dir(filename:join([HotOutDir, "dummy"])),
    code:add_patha(OutDir),
    code:add_patha(HotOutDir),

    %% Compile modules dynamically
    {ok, hydb_spec} = compile:file(filename:join([DbDir, "hymessage/src/hydb_spec.erl"]), [{outdir, HotOutDir}, return_errors]),
    {ok, hydb_contract} = compile:file(filename:join([DbDir, "hymessage/src/hydb_contract.erl"]), [{outdir, HotOutDir}, return_errors]),
    {ok, hymessage_runtime} = compile:file(filename:join([DbDir, "hymessage/src/hymessage_runtime.erl"]), [{outdir, HotOutDir}, return_errors]),
    {ok, sd_runtime} = compile:file(filename:join([DbDir, "sd/src/sd_runtime.erl"]), [{outdir, OutDir}, return_errors]),
    code:load_file(hydb_spec),
    code:load_file(hydb_contract),
    code:load_file(hymessage_runtime),
    code:load_file(sd_runtime),

    %% Test 0A: Default profile when env vars are unset MUST be fail-closed (is_test_profile() == false)
    os:unsetenv("HYDB_PROFILE"),
    os:unsetenv("HYDB_ENV"),
    os:unsetenv("NODE_ENV"),
    case {sd_runtime:is_test_profile(), hymessage_runtime:is_test_profile()} of
        {false, false} ->
            io:format("PASS: default profile is fail-closed (production) when profile env vars are unset~n");
        OtherProfile ->
            io:format("FAIL: expected {false, false} for default is_test_profile, got: ~p~n", [OtherProfile]),
            halt(1)
    end,

    %% Test 0B: Production startup security validation rejects missing/short secrets and identical admin/cluster secrets
    os:unsetenv("HYDB_CLUSTER_SECRET"),
    os:unsetenv("HYDB_ADMIN_TOKEN"),
    case catch sd_runtime:validate_startup_security() of
        {error, missing_cluster_auth_secret} ->
            io:format("PASS: production startup rejects missing HYDB_CLUSTER_SECRET without falling back to wallet~n");
        OtherMissingSec ->
            io:format("FAIL: expected missing_cluster_auth_secret in prod, got: ~p~n", [OtherMissingSec]),
            halt(1)
    end,

    os:putenv("HYDB_CLUSTER_SECRET", "short_secret"),
    case catch sd_runtime:validate_startup_security() of
        {error, weak_cluster_auth_secret} ->
            io:format("PASS: startup rejects HYDB_CLUSTER_SECRET shorter than 16 bytes~n");
        {error, cluster_secret_too_short} ->
            io:format("PASS: startup rejects HYDB_CLUSTER_SECRET shorter than 16 bytes~n");
        OtherShortSec ->
            io:format("FAIL: expected weak_cluster_auth_secret, got: ~p~n", [OtherShortSec]),
            halt(1)
    end,

    os:putenv("HYDB_CLUSTER_SECRET", "prod-cluster-secret-minimum-16-bytes"),
    os:putenv("HYDB_ADMIN_TOKEN", "prod-cluster-secret-minimum-16-bytes"),
    case catch sd_runtime:validate_startup_security() of
        {error, admin_token_must_differ_from_cluster_secret} ->
            io:format("PASS: production startup rejects HYDB_ADMIN_TOKEN identical to HYDB_CLUSTER_SECRET~n");
        OtherSameSec ->
            io:format("FAIL: expected admin_token_must_differ_from_cluster_secret, got: ~p~n", [OtherSameSec]),
            halt(1)
    end,

    %% Configure valid distinct production secrets and test profile for runtime start
    os:putenv("HYDB_PROFILE", "test"),
    os:putenv("HYDB_CLUSTER_SECRET", "valid-cluster-secret-for-phase01-tests"),
    os:putenv("HYDB_ADMIN_TOKEN", "valid-distinct-admin-token-phase01"),
    os:putenv("HYDB_EPOCH_PATH", filename:join([DbDir, "sd/test_sec_epoch.meta"])),
    _ = file:delete(filename:join([DbDir, "sd/test_sec_epoch.meta"])),

    {ok, _} = sd_runtime:ensure_started(),

    ValidAdminSecret = crypto:hash(sha256, <<"valid-distinct-admin-token-phase01">>),
    ValidTokenHex = string:uppercase(binary:encode_hex(ValidAdminSecret)),
    ClusterSecretHash = crypto:hash(sha256, <<"valid-cluster-secret-for-phase01-tests">>),
    ClusterTokenHex = string:uppercase(binary:encode_hex(ClusterSecretHash)),

    %% Test 1: Unauthenticated verify_admin_req must return 401
    Res1 = sd_runtime:verify_admin_req(#{}),
    case Res1 of
        {error, #{ <<"status">> := 401, <<"error_code">> := <<"ERR_ADMIN_UNAUTHORIZED">> }} ->
            io:format("PASS: unauthenticated verify_admin_req returns 401~n");
        Other1 ->
            io:format("FAIL: expected 401 on unauthenticated verify_admin_req, got: ~p~n", [Other1]),
            halt(1)
    end,

    %% Test 2: Forged/invalid admin_token or cluster secret used as admin token must return 403
    Res2 = sd_runtime:verify_admin_req(#{ <<"admin_token">> => <<"DEADBEEFCAFE">> }),
    Res2Cluster = sd_runtime:verify_admin_req(#{ <<"x-admin-token">> => ClusterTokenHex }),
    case {Res2, Res2Cluster} of
        {{error, #{ <<"status">> := 403, <<"error_code">> := <<"ERR_ADMIN_UNAUTHORIZED">> }},
         {error, #{ <<"status">> := 403, <<"error_code">> := <<"ERR_ADMIN_UNAUTHORIZED">> }}} ->
            io:format("PASS: forged admin_token and cluster secret rejected with 403~n");
        Other2 ->
            io:format("FAIL: expected 403 on forged/cluster token, got: ~p~n", [Other2]),
            halt(1)
    end,

    %% Test 3: Valid admin_token & x-admin-token header must succeed via constant-time comparison
    case {sd_runtime:verify_admin_req(#{ <<"admin_token">> => ValidTokenHex }),
          sd_runtime:verify_admin_req(#{ <<"x-admin-token">> => ValidTokenHex }),
          sd_runtime:verify_admin_req(#{ <<"headers">> => #{ <<"x-admin-token">> => ValidTokenHex } })} of
        {ok, ok, ok} -> io:format("PASS: valid admin_token and x-admin-token headers return ok~n");
        Other3 -> io:format("FAIL: expected ok on valid admin_token / x-admin-token, got: ~p~n", [Other3]), halt(1)
    end,

    %% Test 4: Valid bearer token must succeed
    BearerHdr = <<"Bearer ", ValidTokenHex/binary>>,
    case sd_runtime:verify_admin_req(#{ <<"authorization">> => BearerHdr }) of
        ok -> io:format("PASS: valid Authorization: Bearer token returns ok~n");
        Other4 -> io:format("FAIL: expected ok on bearer token, got: ~p~n", [Other4]), halt(1)
    end,

    %% Test 5: Unauthenticated fence_epoch rejected and epoch NOT changed
    InitEpoch = sd_runtime:get_epoch(),
    FenceRes1 = sd_runtime:fence_from_req(nil, #{ <<"epoch">> => 999 }, nil),
    case FenceRes1 of
        #{ <<"status">> := 401, <<"error_code">> := <<"ERR_ADMIN_UNAUTHORIZED">> } ->
            EpochAfterFail = sd_runtime:get_epoch(),
            case EpochAfterFail =:= InitEpoch of
                true -> io:format("PASS: unauthenticated fence_epoch rejected and epoch unchanged~n");
                false -> io:format("FAIL: epoch changed after rejected fence_epoch~n"), halt(1)
            end;
        Other5 ->
            io:format("FAIL: unexpected unauthenticated fence_epoch result: ~p~n", [Other5]),
            halt(1)
    end,

    %% Test 6: Authenticated fence_epoch succeeds and updates epoch
    NewEpoch = InitEpoch + 1,
    FenceRes2 = sd_runtime:fence_from_req(nil, #{ <<"epoch">> => NewEpoch, <<"x-admin-token">> => ValidTokenHex }, nil),
    case sd_runtime:get_epoch() =:= NewEpoch of
        true -> io:format("PASS: authenticated fence_epoch updated epoch to ~p~n", [NewEpoch]);
        false -> io:format("FAIL: authenticated fence_epoch failed: ~p~n", [FenceRes2]), halt(1)
    end,

    %% Test 7: Both /reset AND /simulate_restart strictly blocked in production profile
    os:putenv("HYDB_PROFILE", "production"),
    ResetProdRes = sd_runtime:reset_from_req(nil, #{ <<"x-admin-token">> => ValidTokenHex }, nil),
    RestartProdRes = sd_runtime:restart_from_req(nil, #{ <<"x-admin-token">> => ValidTokenHex }, nil),
    case {ResetProdRes, RestartProdRes} of
        {#{ <<"status">> := 403, <<"error_code">> := <<"ERR_RESET_DISABLED">> },
         #{ <<"status">> := 403, <<"error_code">> := <<"ERR_TEST_ENDPOINT_DISABLED">> }} ->
            io:format("PASS: reset and simulate_restart rejected in production profile~n");
        Other7 ->
            io:format("FAIL: expected 403 for reset and simulate_restart in prod, got: ~p~n", [Other7]),
            halt(1)
    end,

    %% Test 8: Reset succeeds in test profile with valid admin token
    os:putenv("HYDB_PROFILE", "test"),
    ResetTestRes = sd_runtime:reset_from_req(nil, #{ <<"x-admin-token">> => ValidTokenHex }, nil),
    case ResetTestRes of
        #{ <<"ok">> := true, <<"reset">> := true } ->
            io:format("PASS: reset succeeds in test profile with valid admin token~n");
        Other8 ->
            io:format("FAIL: expected ok reset in test profile, got: ~p~n", [Other8]),
            halt(1)
    end,

    _ = file:delete(filename:join([DbDir, "sd/test_sec_epoch.meta"])),
    io:format("All Erlang Phase 01 security checks passed!~n").
