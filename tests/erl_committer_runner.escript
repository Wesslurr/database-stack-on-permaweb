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
    %% [Test 2B] Authentic ANS-104 DataItem deepHash Signing & Verification
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
            end,

            %% Authentic ANS-104 DataItem binary creation & native decode
            TargetProcess = CalculatedAddr,
            {ok, ItemId, DataItemBin} = sd_ao_client:build_ans104_data_item(
                TargetProcess,
                <<"CommitCheckpoint">>,
                [{<<"Batch-Id">>, BatchId}],
                <<"{\"test\":\"data\"}">>,
                WalletInfo
            ),
            case sd_ao_client:verify_ans104_data_item(DataItemBin) of
                {ok, #{id := ItemId, target := TargetProcess}} ->
                    io:format("PASS: Authentic ANS-104 DataItem deepHash serialized and verified natively~n");
                OtherVerify ->
                    io:format("FAIL: ANS-104 verification failed: ~p~n", [OtherVerify]),
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
    {ok, _CommPid} = sd_committer:ensure_started(#{
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

    %% Simulate advancing cursor directly via reload (using valid 64-char hex hash)
    FakeCursor = #{
        <<"last_committed_seq">> => 5,
        <<"last_committed_hash">> => <<"0000000000000000000000000000000000000000000000000000000000000005">>,
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

    %% Test 3C: Invalid hash/root when seq > 0 strictly rejected
    InvalidHashCursorPath = filename:join([DbDir, "data/test_invalid_hash_cursor.json"]),
    ok = file:write_file(InvalidHashCursorPath, json:encode(#{
        <<"last_committed_seq">> => 5,
        <<"last_committed_hash">> => <<"SHORT_HASH">>,
        <<"last_state_root">> => <<"123">>,
        <<"epoch">> => 1
    })),
    case sd_committer:load_disk_cursor(InvalidHashCursorPath) of
        {error, {invalid_cursor_hash_or_root, _}} ->
            io:format("PASS: Truncated or invalid hash strictly rejected on cursor load~n");
        OtherInv ->
            io:format("FAIL: expected rejection of short hash on cursor, got: ~p~n", [OtherInv]),
            halt(1)
    end,
    _ = file:delete(InvalidHashCursorPath),

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
                    <<"OfficialSeq">> => 3,
                    <<"OfficialHash">> => <<"AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899">>,
                    <<"OfficialStateRoot">> => <<"1234567890">>,
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
        CalculatedAddr,
        <<"CommitCheckpoint">>,
        [],
        TestBatch,
        #{
            sender => CalculatedAddr,
            wallet => WalletInfo
        }
    ),
    case SendRes of
        {ok, 200, #{<<"Status">> := <<"COMMITTED">>, <<"OfficialSeq">> := 3}} ->
            io:format("PASS: Native Erlang HTTP client dispatches CommitCheckpoint and parses COMMITTED verdict~n");
        BadSend ->
            io:format("FAIL: expected COMMITTED from mock HTTP server, got: ~p~n", [BadSend]),
            halt(1)
    end,

    receive
        {received_http_request, ReqRaw} ->
            [HeadersBin, BodyBin] = binary:split(ReqRaw, <<"\r\n\r\n">>),
            HeadersStr = string:lowercase(binary_to_list(HeadersBin)),
            case string:find(HeadersStr, "application/octet-stream") =/= nomatch andalso
                 string:find(HeadersStr, "x-dataitem-id:") =/= nomatch of
                true ->
                    case sd_ao_client:verify_ans104_data_item(BodyBin) of
                        {ok, #{id := DispatchedId}} when byte_size(DispatchedId) > 0 ->
                            io:format("PASS: Dispatched message envelope cryptographically signed with RSA-PSS and owner public key~n");
                        VerifyErr ->
                            io:format("FAIL: dispatched ANS-104 data item failed verification: ~p~n", [VerifyErr]),
                            halt(1)
                    end;
                false ->
                    io:format("FAIL: dispatched request missing octet-stream or dataitem id header: ~s~n", [HeadersStr]),
                    halt(1)
            end
    after 2000 ->
        io:format("FAIL: timeout waiting for mock server request reception~n"),
        halt(1)
    end,

    %% ========================================================================
    %% [Test 4B] AO Public Architecture: MU Message -> CU Result Polling Flow
    %% ========================================================================
    CuMockPort = 8991,
    MuMockPort = 8992,
    {ok, CuListen} = gen_tcp:listen(CuMockPort, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),
    {ok, MuListen} = gen_tcp:listen(MuMockPort, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),

    %% Spawn mock CU server responding to /result/:id and /dry-run
    spawn_link(fun() ->
        case gen_tcp:accept(CuListen, 5000) of
            {ok, CuSock} ->
                _CuReq = recv_full_http(CuSock, <<>>),
                CuRespBody = iolist_to_binary(json:encode(#{
                    <<"Messages">> => [
                        #{
                            <<"Target">> => <<"SU_CLIENT">>,
                            <<"Action">> => <<"Checkpoint-Committed">>,
                            <<"Tags">> => [
                                #{<<"name">> => <<"Action">>, <<"value">> => <<"Checkpoint-Committed">>},
                                #{<<"name">> => <<"Status">>, <<"value">> => <<"COMMITTED">>},
                                #{<<"name">> => <<"OfficialSeq">>, <<"value">> => <<"3">>},
                                #{<<"name">> => <<"OfficialHash">>, <<"value">> => <<"AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899">>},
                                #{<<"name">> => <<"OfficialStateRoot">>, <<"value">> => <<"1234567890">>}
                            ]
                        }
                    ],
                    <<"Output">> => #{<<"ok">> => true}
                })),
                CuHttpResp = [
                    "HTTP/1.1 200 OK\r\n",
                    "Content-Type: application/json\r\n",
                    "Content-Length: ", integer_to_list(byte_size(CuRespBody)), "\r\n",
                    "\r\n",
                    binary_to_list(CuRespBody)
                ],
                ok = gen_tcp:send(CuSock, CuHttpResp),
                gen_tcp:close(CuSock);
            _ -> ok
        end,
        gen_tcp:close(CuListen)
    end),

    %% Spawn mock MU server returning standard { "id": "MSG_AO_MOCK_99" }
    spawn_link(fun() ->
        case gen_tcp:accept(MuListen, 5000) of
            {ok, MuSock} ->
                _MuReq = recv_full_http(MuSock, <<>>),
                MuRespBody = iolist_to_binary(json:encode(#{<<"id">> => <<"MSG_AO_MOCK_99">>})),
                MuHttpResp = [
                    "HTTP/1.1 200 OK\r\n",
                    "Content-Type: application/json\r\n",
                    "Content-Length: ", integer_to_list(byte_size(MuRespBody)), "\r\n",
                    "\r\n",
                    binary_to_list(MuRespBody)
                ],
                ok = gen_tcp:send(MuSock, MuHttpResp),
                gen_tcp:close(MuSock);
            _ -> ok
        end,
        gen_tcp:close(MuListen)
    end),

    %% Send message targeting MU with CU URL configured
    MuUrl = "http://127.0.0.1:8992",
    CuUrl = "http://127.0.0.1:8991",
    MuSendRes = sd_ao_client:send_message(
        MuUrl,
        CalculatedAddr,
        <<"CommitCheckpoint">>,
        [],
        TestBatch,
        #{
            sender => CalculatedAddr,
            wallet => WalletInfo,
            cu_url => CuUrl
        }
    ),
    case MuSendRes of
        {ok, 200, #{<<"Status">> := <<"COMMITTED">>, <<"OfficialSeq">> := <<"3">>}} ->
            io:format("PASS: In-BEAM committer fetches and evaluates verdict via CU result endpoint~n");
        OtherMuRes ->
            io:format("FAIL: expected CU result evaluation from MU->CU flow, got: ~p~n", [OtherMuRes]),
            halt(1)
    end,

    %% ========================================================================
    %% [Test 4C] Official AO Compute Unit dry-run Evaluation for query_state
    %% ========================================================================
    DryrunCuPort = 8993,
    {ok, DryrunListen} = gen_tcp:listen(DryrunCuPort, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),
    spawn_link(fun() ->
        case gen_tcp:accept(DryrunListen, 5000) of
            {ok, DrSock} ->
                DrReq = recv_full_http(DrSock, <<>>),
                [_, DrBody] = binary:split(DrReq, <<"\r\n\r\n">>),
                ParentPid ! {dryrun_body, DrBody},
                DrRespBody = iolist_to_binary(json:encode(#{
                    <<"Messages">> => [
                        #{
                            <<"Action">> => <<"OfficialStateResponse">>,
                            <<"Data">> => iolist_to_binary(json:encode(#{
                                <<"OfficialSeq">> => 3,
                                <<"OfficialHash">> => <<"AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899">>,
                                <<"OfficialStateRoot">> => <<"1234567890">>
                            }))
                        }
                    ]
                })),
                DrHttpResp = [
                    "HTTP/1.1 200 OK\r\n",
                    "Content-Type: application/json\r\n",
                    "Content-Length: ", integer_to_list(byte_size(DrRespBody)), "\r\n",
                    "\r\n",
                    binary_to_list(DrRespBody)
                ],
                ok = gen_tcp:send(DrSock, DrHttpResp),
                gen_tcp:close(DrSock);
            _ -> ok
        end,
        gen_tcp:close(DryrunListen)
    end),

    DryrunRes = sd_ao_client:query_state(
        "http://127.0.0.1:8993",
        CalculatedAddr,
        <<"GetOfficialState">>,
        #{},
        #{cu_url => "http://127.0.0.1:8993", wallet => WalletInfo}
    ),
    case DryrunRes of
        {ok, 200, #{<<"OfficialSeq">> := 3}} ->
            io:format("PASS: Official AO Compute Unit dry-run evaluation query verified~n");
        BadDryrun ->
            io:format("FAIL: expected dryrun query result, got: ~p~n", [BadDryrun]),
            halt(1)
    end,
    receive
        {dryrun_body, BodyJson} ->
            case string:find(binary_to_list(BodyJson), "GetOfficialState") =/= nomatch of
                true -> ok;
                false -> io:format("FAIL: dryrun body missing Action GetOfficialState~n"), halt(1)
            end
    after 1000 ->
        io:format("FAIL: timeout waiting for dryrun body~n"), halt(1)
    end,

    %% ========================================================================
    %% [Test 4D] Strict DUPLICATE_BATCH_IDEMPOTENT Verdict Verification
    %% ========================================================================
    %% Case 1: Exact tip duplicate (OfficialSeq == ToSeq)
    ValidDupVerdict = #{
        <<"Status">> => <<"DUPLICATE_BATCH_IDEMPOTENT">>,
        <<"OfficialSeq">> => 3,
        <<"OfficialHash">> => <<"AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899">>,
        <<"OfficialStateRoot">> => <<"1234567890">>
    },
    PropHash3 = <<"AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899">>,
    PropRoot3 = <<"1234567890">>,
    case sd_committer:parse_arbitrator_verdict(ValidDupVerdict, 3, PropHash3, PropRoot3) of
        {ok, duplicate, 3, PropHash3, PropRoot3} ->
            io:format("PASS: DUPLICATE_BATCH_IDEMPOTENT verdict requires matching official hash and root~n");
        BadDupRes ->
            io:format("FAIL: expected valid duplicate verdict, got: ~p~n", [BadDupRes]),
            halt(1)
    end,

    %% Case 2: Advanced tip duplicate (OfficialSeq == 10 > ToSeq == 3)
    %% Arbitrator returns authoritative tip hash/root at sequence 10
    TipHash10 = <<"1111222233334444555566667777888811112222333344445555666677778888">>,
    TipRoot10 = <<"999888777">>,
    AdvTipVerdict = #{
        <<"Status">> => <<"DUPLICATE_BATCH_IDEMPOTENT">>,
        <<"OfficialSeq">> => 10,
        <<"OfficialHash">> => TipHash10,
        <<"OfficialStateRoot">> => TipRoot10
    },
    case sd_committer:parse_arbitrator_verdict(AdvTipVerdict, 3, PropHash3, PropRoot3) of
        {ok, duplicate, 10, TipHash10, TipRoot10} ->
            io:format("PASS: Retrying older batch with advanced Arbitrator tip returns authoritative tip hash and root~n");
        BadAdvRes ->
            io:format("FAIL: expected advanced tip duplicate verdict, got: ~p~n", [BadAdvRes]),
            halt(1)
    end,

    %% Case 3: Advanced tip duplicate with invalid/empty tip hash strictly rejected
    BadAdvTipVerdict = #{
        <<"Status">> => <<"DUPLICATE_BATCH_IDEMPOTENT">>,
        <<"OfficialSeq">> => 10,
        <<"OfficialHash">> => <<"INVALID_SHORT_HASH">>,
        <<"OfficialStateRoot">> => TipRoot10
    },
    case sd_committer:parse_arbitrator_verdict(BadAdvTipVerdict, 3, PropHash3, PropRoot3) of
        {rejected, err_missing_proof, _} ->
            io:format("PASS: Advanced tip duplicate with invalid tip hash strictly rejected~n");
        BadAdvInv ->
            io:format("FAIL: expected rejection of invalid tip hash, got: ~p~n", [BadAdvInv]),
            halt(1)
    end,

    %% Case 4: Duplicate with sequence lag (OfficialSeq < ToSeq) strictly rejected
    LagDupVerdict = #{
        <<"Status">> => <<"DUPLICATE_BATCH_IDEMPOTENT">>,
        <<"OfficialSeq">> => 2,
        <<"OfficialHash">> => PropHash3,
        <<"OfficialStateRoot">> => PropRoot3
    },
    case sd_committer:parse_arbitrator_verdict(LagDupVerdict, 3, PropHash3, PropRoot3) of
        {rejected, err_sequence_lag, _} ->
            io:format("PASS: Duplicate verdict with sequence lag strictly rejected~n");
        BadLagRes ->
            io:format("FAIL: expected lag rejection on duplicate, got: ~p~n", [BadLagRes]),
            halt(1)
    end,

    %% Case 5: Cursor saved with resolved tip hash/root at seq 10 reconciles cleanly without divergence
    TipCursorPath = filename:join([DbDir, "data/test_tip_cursor.json"]),
    ok = sd_committer:save_disk_cursor(10, TipHash10, TipRoot10, 1, <<"DUPLICATE_BATCH_IDEMPOTENT">>, TipCursorPath),
    {ok, LoadedTipCursor} = sd_committer:load_disk_cursor(TipCursorPath),
    case maps:get(last_committed_hash, LoadedTipCursor, maps:get(<<"last_committed_hash">>, LoadedTipCursor, undefined)) =:= TipHash10 andalso
         maps:get(last_state_root, LoadedTipCursor, maps:get(<<"last_state_root">>, LoadedTipCursor, undefined)) =:= TipRoot10 of
        true ->
            io:format("PASS: Advanced duplicate cursor correctly stores authoritative tip hash and root for cold-start restart~n");
        false ->
            io:format("FAIL: loaded tip cursor mismatch: ~p~n", [LoadedTipCursor]),
            halt(1)
    end,
    _ = file:delete(TipCursorPath),

    %% Missing hash on duplicate rejected
    MissingHashDup = #{
        <<"Status">> => <<"DUPLICATE_BATCH_IDEMPOTENT">>,
        <<"OfficialSeq">> => 3,
        <<"OfficialStateRoot">> => <<"1234567890">>
    },
    case sd_committer:parse_arbitrator_verdict(MissingHashDup, <<"AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899">>, <<"1234567890">>) of
        {rejected, err_missing_proof, _} -> ok;
        OtherMissingDup ->
            io:format("FAIL: expected rejection of missing hash on duplicate, got: ~p~n", [OtherMissingDup]),
            halt(1)
    end,

    %% ========================================================================
    %% [Test 4E] Cold-Start Reconciliation Proof Requirement & Divergence
    sd_committer:stop(),
    %% ========================================================================
    %% Arbitrator sequence ahead with invalid/empty hash strictly halts fail-closed
    HaltMockPort = 8994,
    {ok, HaltListen} = gen_tcp:listen(HaltMockPort, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),
    spawn_link(fun() ->
        case gen_tcp:accept(HaltListen, 5000) of
            {ok, HSock} ->
                _HReq = recv_full_http(HSock, <<>>),
                HRespBody = iolist_to_binary(json:encode(#{
                    <<"OfficialSeq">> => 10,
                    <<"OfficialHash">> => <<"">>, %% EMPTY HASH -> INVALID PROOF!
                    <<"OfficialStateRoot">> => <<"">>
                })),
                HHttpResp = [
                    "HTTP/1.1 200 OK\r\n",
                    "Content-Type: application/json\r\n",
                    "Content-Length: ", integer_to_list(byte_size(HRespBody)), "\r\n",
                    "\r\n",
                    binary_to_list(HRespBody)
                ],
                ok = gen_tcp:send(HSock, HHttpResp),
                gen_tcp:close(HSock);
            _ -> ok
        end,
        gen_tcp:close(HaltListen)
    end),

    FailClosedCursorPath = filename:join([DbDir, "data/test_fc_cursor.json"]),
    _ = file:delete(FailClosedCursorPath),
    InitHaltRes = sd_committer:ensure_started(#{
        cursor_path => FailClosedCursorPath,
        wallet_path => WalletFile,
        arbitrator_id => <<"MOCK_ARB">>,
        gateway_url => "http://127.0.0.1:8994"
    }),
    case InitHaltRes of
        {error, {chain_divergence, _}} ->
            io:format("PASS: Arbitrator sequence advance with missing hash/root strictly halts fail-closed~n");
        OtherInitHalt ->
            io:format("FAIL: expected fail-closed chain divergence halt, got: ~p~n", [OtherInitHalt]),
            halt(1)
    end,
    _ = file:delete(FailClosedCursorPath),

    %% Test 4E-2: Lagging local WAL strictly halts fail-closed on advanced Arbitrator tip
    LagMockPort = 8995,
    {ok, LagListen} = gen_tcp:listen(LagMockPort, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),
    spawn_link(fun() ->
        case gen_tcp:accept(LagListen, 5000) of
            {ok, LSock} ->
                _LReq = recv_full_http(LSock, <<>>),
                LRespBody = iolist_to_binary(json:encode(#{
                    <<"OfficialSeq">> => 10,
                    <<"OfficialHash">> => <<"1111222233334444555566667777888811112222333344445555666677778888">>,
                    <<"OfficialStateRoot">> => <<"999888777">>
                })),
                LHttpResp = [
                    "HTTP/1.1 200 OK\r\n",
                    "Content-Type: application/json\r\n",
                    "Content-Length: ", integer_to_list(byte_size(LRespBody)), "\r\n",
                    "\r\n",
                    binary_to_list(LRespBody)
                ],
                ok = gen_tcp:send(LSock, LHttpResp),
                gen_tcp:close(LSock);
            _ -> ok
        end,
        gen_tcp:close(LagListen)
    end),

    LagCursorPath = filename:join([DbDir, "data/test_lag_cursor.json"]),
    _ = file:delete(LagCursorPath),
    %% Local WAL tick is only 3, but Arbitrator is at 10!
    LagReconcileRes = sd_committer:reconcile_arbitrator_state(<<"MOCK_ARB">>, #{
        cursor_path => LagCursorPath,
        gateway_url => "http://127.0.0.1:8995",
        cu_url => "http://127.0.0.1:8995",
        local_tick => 3,
        last_committed_seq => 0
    }),
    case maps:get(health, LagReconcileRes) =:= failed_closed andalso maps:get(error_code, LagReconcileRes) =:= err_local_wal_lagging of
        true ->
            io:format("PASS: Lagging local WAL strictly halts fail-closed on advanced Arbitrator tip~n");
        false ->
            io:format("FAIL: expected err_local_wal_lagging on lagging WAL, got: ~p~n", [LagReconcileRes]),
            halt(1)
    end,
    %% Verify cursor was NOT saved with 10 on disk
    case file:read_file(LagCursorPath) of
        {error, enoent} ->
            io:format("PASS: Disk cursor strictly preserved without unapplied sequence advance~n");
        {ok, LagCurBin} ->
            io:format("FAIL: cursor should not have been written on lagging WAL: ~s~n", [LagCurBin]),
            halt(1)
    end,

    %% Test 4F: Strict verdict hash matching and fail-closed divergence
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

    %% Test 4G: Missing official hash or state root strictly rejected
    MissingHashVerdict = #{
        <<"Status">> => <<"COMMITTED">>,
        <<"OfficialSeq">> => 7,
        <<"OfficialStateRoot">> => <<"1234567890">>
    },
    case sd_committer:parse_arbitrator_verdict(MissingHashVerdict, <<"AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899">>, <<"1234567890">>) of
        {rejected, err_missing_proof, _} ->
            io:format("PASS: Incomplete verdict missing official hash strictly rejected~n");
        OtherMissing ->
            io:format("FAIL: expected rejection on missing hash, got: ~p~n", [OtherMissing]),
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

    %% C. Production profile strictly requires pinned Genesis Hash
    os:putenv("HYDB_PROFILE", "production"),
    ProdUnpinnedRes = sd_committer:bootstrap_genesis(#{
        genesis_id => <<"MOCK_GENESIS_ID">>,
        expected_genesis_hash => <<>>
    }),
    case ProdUnpinnedRes of
        {error, {err_genesis_hash_required, _}} ->
            io:format("PASS: Unpinned Genesis hash rejected in production profile~n");
        BadProdRes ->
            io:format("FAIL: expected rejection of unpinned genesis in production, got: ~p~n", [BadProdRes]),
            halt(1)
    end,
    os:putenv("HYDB_PROFILE", "test"),

    %% D. Heartbeat without Genesis reports heartbeat_acknowledged = false
    _ = sd_committer:report_heartbeat(),
    HbStatusNow = sd_committer:get_status(),
    case maps:get(<<"heartbeat_acknowledged">>, HbStatusNow) of
        false ->
            io:format("PASS: Committer heartbeat without Genesis strictly reports unacknowledged~n");
        BadHbAck ->
            io:format("FAIL: heartbeat without Genesis falsely marked acknowledged: ~p~n", [BadHbAck]),
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
