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
    filelib:ensure_dir(filename:join([OutDir, "dummy"])),
    code:add_path(OutDir),

    %% Compile sd_runtime.erl dynamically
    SdSrc = filename:join([DbDir, "sd/src/sd_runtime.erl"]),
    case compile:file(SdSrc, [{outdir, OutDir}, return_errors]) of
        {ok, sd_runtime} -> code:load_file(sd_runtime);
        Err -> io:format("Compile error sd_runtime: ~p~n", [Err]), halt(1)
    end,

    %% Ensure node started
    sd_runtime:ensure_started(),

    %% Derive valid admin token from secret
    WalletPath = filename:join([DbDir, "secrets/wallet.json"]),
    {ok, WalletBin} = file:read_file(WalletPath),
    ValidSecret = crypto:hash(sha256, WalletBin),
    ValidTokenHex = string:uppercase(binary:encode_hex(ValidSecret)),

    %% Test 1: Unauthenticated verify_admin_req must return 401
    Res1 = sd_runtime:verify_admin_req(#{}),
    case Res1 of
        {error, #{ <<"status">> := 401, <<"error_code">> := <<"ERR_ADMIN_UNAUTHORIZED">> }} ->
            io:format("PASS: unauthenticated verify_admin_req returns 401~n");
        Other1 ->
            io:format("FAIL: expected 401 on unauthenticated verify_admin_req, got: ~p~n", [Other1]),
            halt(1)
    end,

    %% Test 2: Forged/invalid admin_token must return 403
    Res2 = sd_runtime:verify_admin_req(#{ <<"admin_token">> => <<"DEADBEEFCAFE">> }),
    case Res2 of
        {error, #{ <<"status">> := 403, <<"error_code">> := <<"ERR_ADMIN_UNAUTHORIZED">> }} ->
            io:format("PASS: forged admin_token returns 403~n");
        Other2 ->
            io:format("FAIL: expected 403 on forged admin_token, got: ~p~n", [Other2]),
            halt(1)
    end,

    %% Test 3: Valid admin_token must succeed
    case sd_runtime:verify_admin_req(#{ <<"admin_token">> => ValidTokenHex }) of
        ok -> io:format("PASS: valid admin_token returns ok~n");
        Other3 -> io:format("FAIL: expected ok on valid admin_token, got: ~p~n", [Other3]), halt(1)
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
    FenceRes2 = sd_runtime:fence_from_req(nil, #{ <<"epoch">> => NewEpoch, <<"admin_token">> => ValidTokenHex }, nil),
    case sd_runtime:get_epoch() =:= NewEpoch of
        true -> io:format("PASS: authenticated fence_epoch updated epoch to ~p~n", [NewEpoch]);
        false -> io:format("FAIL: authenticated fence_epoch failed: ~p~n", [FenceRes2]), halt(1)
    end,

    %% Test 7: Reset strictly blocked in production profile
    os:putenv("HYDB_PROFILE", "production"),
    ResetProdRes = sd_runtime:reset_from_req(nil, #{ <<"admin_token">> => ValidTokenHex }, nil),
    case ResetProdRes of
        #{ <<"status">> := 403, <<"error_code">> := <<"ERR_RESET_DISABLED">> } ->
            io:format("PASS: reset rejected in production profile with ERR_RESET_DISABLED~n");
        Other7 ->
            io:format("FAIL: expected ERR_RESET_DISABLED in prod, got: ~p~n", [Other7]),
            halt(1)
    end,

    %% Test 8: Reset succeeds in test profile with valid admin token
    os:putenv("HYDB_PROFILE", "test"),
    ResetTestRes = sd_runtime:reset_from_req(nil, #{ <<"admin_token">> => ValidTokenHex }, nil),
    case ResetTestRes of
        #{ <<"ok">> := true, <<"reset">> := true } ->
            io:format("PASS: reset succeeds in test profile with valid admin token~n");
        Other8 ->
            io:format("FAIL: expected ok reset in test profile, got: ~p~n", [Other8]),
            halt(1)
    end,

    io:format("All Erlang Phase 01 security checks passed!~n").
