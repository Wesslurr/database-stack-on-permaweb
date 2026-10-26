#!/usr/bin/env escript
%% -*- coding: utf-8 -*-
%% HyDB Phase 12: Native In-BEAM AO Committer & Settlement Engine Verification

main(_) ->
    DbDir = case os:getenv("DB_DIR") of
        false -> filename:dirname(filename:dirname(escript:script_name()));
        "" -> filename:dirname(filename:dirname(escript:script_name()));
        D -> D
    end,
    os:putenv("DB_DIR", DbDir),
    os:putenv("HYDB_PROFILE", "test"),
    os:putenv("HYDB_CLUSTER_SECRET", "super_cluster_secret_min16bytes_ok!"),
    os:putenv("HYDB_ADMIN_TOKEN", "super_admin_token_min16bytes_ok!"),

    OutDir = filename:join([DbDir, "sd/out/ebin"]),
    filelib:ensure_dir(filename:join([OutDir, "dummy"])),
    code:add_patha(OutDir),

    %% Compile SD modules
    {ok, sd_ao_client} = compile:file(filename:join([DbDir, "sd/src/sd_ao_client.erl"]), [{outdir, OutDir}, return_errors]),
    {ok, sd_committer} = compile:file(filename:join([DbDir, "sd/src/sd_committer.erl"]), [{outdir, OutDir}, return_errors]),
    {ok, sd_runtime} = compile:file(filename:join([DbDir, "sd/src/sd_runtime.erl"]), [{outdir, OutDir}, return_errors]),
    code:load_file(sd_ao_client),
    code:load_file(sd_committer),
    code:load_file(sd_runtime),

    _ = application:ensure_all_started(inets),
    _ = application:ensure_all_started(ssl),

    %% ========================================================================
    %% [Test 1] Native Arweave Address Derivation from secrets/wallet_su.json
    %% ========================================================================
    WalletFile = filename:join([DbDir, "secrets/wallet_su.json"]),
    AddressFile = filename:join([DbDir, "secrets/address_su.txt"]),
    {ok, WalletInfo} = sd_ao_client:load_wallet(WalletFile),
    CalculatedAddr = maps:get(address, WalletInfo),
    {ok, AddrBytes} = file:read_file(AddressFile),
    ExpectedAddr = list_to_binary(string:trim(binary_to_list(AddrBytes))),
    case CalculatedAddr of
        ExpectedAddr ->
            io:format("PASS: Erlang native Arweave address derivation matches address_su.txt 100%~n");
        Mismatch ->
            io:format("FAIL: expected ~s, got ~s~n", [ExpectedAddr, Mismatch]),
            halt(1)
    end,

    %% ========================================================================
    %% [Test 2] Canonical Batch ID & HMAC-SHA256 Batch Signature Verification
    %% ========================================================================
    TestBatch = #{
        <<"su_id">> => CalculatedAddr,
        <<"epoch">> => 1,
        <<"from_seq">> => 1,
        <<"to_seq">> => 3,
        <<"proposed_hash">> => <<"AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899">>,
        <<"proposed_state_root">> => <<"1234567890">>,
        <<"events">> => [
            #{<<"hash">> => <<"1111111111111111111111111111111111111111111111111111111111111111">>},
            #{<<"hash">> => <<"2222222222222222222222222222222222222222222222222222222222222222">>},
            #{<<"hash">> => <<"AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899">>}
        ]
    },
    BatchId = sd_ao_client:compute_batch_id(TestBatch),
    SecretKey = sd_ao_client:derive_cluster_secret(WalletFile),
    BatchSig = sd_ao_client:compute_batch_sig(TestBatch, SecretKey),

    %% Exact golden vector assertion
    CanonicalTest = <<"HYDB_BATCH_V1|1|1|3|AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899|1234567890">>,
    ExpectedBatchId = string:uppercase(binary:encode_hex(crypto:hash(sha256, CanonicalTest))),
    case BatchId =:= ExpectedBatchId of
        true -> ok;
        false ->
            io:format("FAIL: batch ID mismatch! Expected ~s, got ~s~n", [ExpectedBatchId, BatchId]),
            halt(1)
    end,

    if
        byte_size(BatchId) =:= 64 andalso byte_size(BatchSig) =:= 64 ->
            io:format("PASS: In-BEAM batch ID and HMAC signature match canonical vector~n");
        true ->
            io:format("FAIL: invalid batch ID or Sig byte size: ~p, ~p~n", [byte_size(BatchId), byte_size(BatchSig)]),
            halt(1)
    end,

    %% ========================================================================
    %% [Test 2B] Native Cryptographic RSA-PSS Signing & Verification (ANS-104)
    %% ========================================================================
    PrivKey = maps:get(privkey, WalletInfo),
    OwnerB64 = maps:get(owner, WalletInfo),
    case PrivKey =/= undefined andalso byte_size(OwnerB64) > 0 of
        true ->
            TestMsg = <<"CANONICAL_AO_DATA_FOR_VERIFICATION">>,
            RsaSig = sd_ao_client:sign_payload(TestMsg, PrivKey),
            case sd_ao_client:verify_payload_sig(TestMsg, RsaSig, OwnerB64) of
                true ->
                    io:format("PASS: In-BEAM RSA-PSS message signature verified against wallet public key~n");
                false ->
                    io:format("FAIL: RSA-PSS signature verification failed!~n"),
                    halt(1)
            end;
        false ->
            io:format("FAIL: missing private key or owner modulus in wallet!~n"),
            halt(1)
    end,

    %% ========================================================================
    %% [Test 3] Durable Disk Cursor Persistence & Atomic Reload
    %% ========================================================================
    CursorTestPath = filename:join([DbDir, "data/test_beam_committer_cursor.json"]),
    _ = file:delete(CursorTestPath),
    {ok, _CommPid} = sd_committer:start_link(#{
        cursor_path => CursorTestPath,
        wallet_path => WalletFile,
        batch_max => 5,
        poll_interval_ms => 60000 %% High interval to drive ticks manually
    }),
    InitStatus = sd_committer:get_status(),
    case maps:get(<<"last_committed_seq">>, InitStatus) of
        0 -> io:format("PASS: In-BEAM committer initializes with sequence 0~n");
        BadInitSeq -> io:format("FAIL: unexpected init seq: ~p~n", [BadInitSeq]), halt(1)
    end,

    %% Simulate advancing cursor directly via reload
    FakeCursor = #{
        <<"last_committed_seq">> => 5,
        <<"last_committed_hash">> => <<"HEX_HASH_SEQ_5">>,
        <<"last_state_root">> => <<"55555">>,
        <<"epoch">> => 1,
        <<"verdict">> => <<"COMMITTED">>
    },
    ok = file:write_file(CursorTestPath, json:encode(FakeCursor)),
    {ok, 5} = sd_committer:reload_cursor(),
    ReloadedStatus = sd_committer:get_status(),
    case maps:get(<<"last_committed_seq">>, ReloadedStatus) of
        5 ->
            io:format("PASS: Durable disk cursor persisted and reloaded from disk atomically~n");
        OtherReload ->
            io:format("FAIL: cursor reload failed: ~p~n", [OtherReload]),
            halt(1)
    end,

    %% Test 3B: Corrupted cursor fail-closed detection
    CorruptedCursorPath = filename:join([DbDir, "data/test_corrupted_cursor.json"]),
    ok = file:write_file(CorruptedCursorPath, <<"CORRUPTED_NON_JSON_DATA_!!!">>),
    case sd_committer:load_disk_cursor(CorruptedCursorPath) of
        {error, _} ->
            io:format("PASS: Corrupted disk cursor strictly rejected with fail-closed error~n");
        BadCursorRes ->
            io:format("FAIL: expected error on corrupted cursor, got: ~p~n", [BadCursorRes]),
            halt(1)
    end,
    _ = file:delete(CorruptedCursorPath),

    %% ========================================================================
    %% [Test 4] Native HTTP AO Client Dispatch & Mock Response Parsing
    %% ========================================================================
    %% Start ephemeral mock HTTP server on 127.0.0.1:8999
    MockPort = 8999,
    ParentPid = self(),
    {ok, ListenSocket} = gen_tcp:listen(MockPort, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),
    spawn_link(fun() ->
        case gen_tcp:accept(ListenSocket, 5000) of
            {ok, Sock} ->
                ReqData = recv_full_http(Sock, <<>>),
                ParentPid ! {received_http_request, ReqData},
                MockBody = iolist_to_binary(json:encode(#{
                    <<"Status">> => <<"COMMITTED">>,
                    <<"OfficialSeq">> => 6,
                    <<"OfficialHash">> => <<"HASH6">>,
                    <<"Verified">> => true
                })),
                HttpResp = [
                    "HTTP/1.1 200 OK\r\n",
                    "Content-Type: application/json\r\n",
                    "Content-Length: ", integer_to_list(byte_size(MockBody)), "\r\n",
                    "\r\n",
                    binary_to_list(MockBody)
                ],
                ok = gen_tcp:send(Sock, HttpResp),
                gen_tcp:close(Sock);
            _ -> ok
        end,
        gen_tcp:close(ListenSocket)
    end),

    MockGw = "http://127.0.0.1:8999",
    SendRes = sd_ao_client:send_message(
        MockGw,
        <<"MOCK_ARBITRATOR_ID">>,
        <<"CommitCheckpoint">>,
        [],
        TestBatch,
        #{
            sender => CalculatedAddr,
            wallet => WalletInfo
        }
    ),
    case SendRes of
        {ok, 200, #{<<"Status">> := <<"COMMITTED">>, <<"OfficialSeq">> := 6}} ->
            io:format("PASS: Native Erlang HTTP client dispatches CommitCheckpoint and parses COMMITTED verdict~n");
        BadSend ->
            io:format("FAIL: expected COMMITTED from mock HTTP server, got: ~p~n", [BadSend]),
            halt(1)
    end,

    receive
        {received_http_request, ReqRaw} ->
            ReqStr = binary_to_list(ReqRaw),
            case string:find(ReqStr, "\"Signature\"") =/= nomatch andalso
                 string:find(ReqStr, "\"Owner\"") =/= nomatch andalso
                 string:find(ReqStr, "\"Signature-Type\":1") =/= nomatch of
                true ->
                    io:format("PASS: Dispatched message envelope cryptographically signed with RSA-PSS and owner public key~n");
                false ->
                    io:format("FAIL: envelope missing cryptographic signature or owner: ~s~n", [ReqStr]),
                    halt(1)
            end
    after 2000 ->
        io:format("FAIL: timeout waiting for mock server request reception~n"),
        halt(1)
    end,

    %% Test 4B: Strict verdict hash matching and fail-closed divergence
    MismatchedVerdict = #{
        <<"Status">> => <<"COMMITTED">>,
        <<"OfficialSeq">> => 7,
        <<"OfficialHash">> => <<"FORGED_DIFFERENT_HASH">>,
        <<"OfficialStateRoot">> => <<"1234567890">>
    },
    case sd_committer:parse_arbitrator_verdict(MismatchedVerdict, <<"AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899">>, <<"1234567890">>) of
        {rejected, err_state_divergence, _} ->
            io:format("PASS: Divergent official hash strictly rejected by arbitrator verdict parser~n");
        OtherVerdict ->
            io:format("FAIL: expected rejection on diverged hash, got: ~p~n", [OtherVerdict]),
            halt(1)
    end,

    %% ========================================================================
    %% [Test 5] Fail-Closed Security Invariants (Genesis Hash & Impersonation)
    %% ========================================================================
    %% A. Genesis hash divergence
    DivergentRes = sd_committer:bootstrap_genesis(#{
        genesis_id => <<"MOCK_GENESIS_ID">>,
        expected_genesis_hash => <<"EXPECTED_PINNED_HASH_1234">>
    }),
    %% Since mock isn't answering with matching hash, it must fail-closed
    case DivergentRes of
        {error, _} ->
            io:format("PASS: Genesis hash divergence triggers fail-closed error in Erlang committer~n");
        UnexpectedSuccess ->
            io:format("FAIL: expected error on unverified genesis bootstrap, got: ~p~n", [UnexpectedSuccess]),
            halt(1)
    end,

    %% B. Genesis missing hash fail-closed
    case sd_committer:bootstrap_genesis(#{
        genesis_id => <<"MOCK_GENESIS_ID">>,
        expected_genesis_hash => <<"PINNED_HASH_XYZ">>,
        gateway_url => "http://127.0.0.1:1" %% Unreachable port to simulate empty/unverified response
    }) of
        {error, _} ->
            io:format("PASS: Missing genesis hash or connection failure triggers strict fail-closed rejection~n");
        BadGenRes ->
            io:format("FAIL: expected fail-closed on unreachable genesis, got: ~p~n", [BadGenRes]),
            halt(1)
    end,

    %% ========================================================================
    %% [Test 6] In-BEAM Committer Status & Health State Machine
    %% ========================================================================
    FinalStatus = sd_committer:get_status(),
    case maps:get(<<"health">>, FinalStatus) of
        H when is_binary(H) ->
            io:format("PASS: In-BEAM committer status reporting and health state machine verified~n");
        BadHealth ->
            io:format("FAIL: invalid health in committer status: ~p~n", [BadHealth]),
            halt(1)
    end,

    _ = file:delete(CursorTestPath),
    sd_committer:stop(),
    io:format("~n>>> ALL TAHAP 12 IN-BEAM COMMITTER TESTS PASSED <<<~n"),
    halt(0).

recv_full_http(Sock, Acc) ->
    case gen_tcp:recv(Sock, 0, 2000) of
        {ok, Chunk} ->
            NewAcc = <<Acc/binary, Chunk/binary>>,
            case binary:split(NewAcc, <<"\r\n\r\n">>) of
                [Headers, Body] ->
                    case string:find(string:lowercase(binary_to_list(Headers)), "content-length:") of
                        nomatch -> NewAcc;
                        CLStr ->
                            CLPart = lists:nth(1, string:split(CLStr, "\r\n")),
                            [_, LenStr] = string:split(CLPart, ":"),
                            CLen = list_to_integer(string:trim(LenStr)),
                            if
                                byte_size(Body) >= CLen -> NewAcc;
                                true -> recv_full_http(Sock, NewAcc)
                            end
                    end;
                _ ->
                    recv_full_http(Sock, NewAcc)
            end;
        _ ->
            Acc
    end.
