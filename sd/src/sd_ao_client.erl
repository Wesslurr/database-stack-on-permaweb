-module(sd_ao_client).

%% ============================================================================
%% Native In-BEAM AO Computer & Arweave Gateway Client
%% ============================================================================
%% Responsibilities:
%% 1. Pure Erlang HTTP communication with AO Message Unit (MU) & Compute Unit (CU).
%% 2. Native Arweave JWK parsing & cryptographic address calculation.
%% 3. Authentic ANS-104 DataItem deepHash signing & verification (RSA-PSS SHA-256).
%% 4. Avro binary tag encoding & decoding conforming to Arweave standard.
%% 5. Complete AO Message-to-Result lifecycle: MU message dispatch -> CU evaluation polling.
%% 6. Official AO Compute Unit dry-run evaluation for state queries (Genesis & Arbitrator).
%% 7. Formatting of AO URLs and process routes.
%% ============================================================================

-export([load_wallet/1, b64url_encode/1, b64url_decode/1, get_arweave_address/1]).
-export([sign_payload/2, verify_payload_sig/3]).
-export([deep_hash/1, encode_avro_tags/1, decode_avro_tags/1]).
-export([build_ans104_data_item/5, verify_ans104_data_item/1]).
-export([send_message/5, send_message/6, query_state/4, query_state/5]).
-export([fetch_cu_result/3, fetch_cu_result/4, dryrun_cu_query/4, dryrun_cu_query/5]).
-export([fetch_hyperbeam_process_result/2, fetch_hyperbeam_process_result/3]).
-export([compute_batch_id/1, compute_batch_sig/2, derive_cluster_secret/1]).
-export([format_ao_url/3, format_ao_url/4, is_hyperbeam_gateway/2, resolve_cu_url/1, resolve_mu_url/1]).

%% ----------------------------------------------------------------------------
%% Wallet & Cryptographic Utilities
%% ----------------------------------------------------------------------------

load_wallet(WalletPath) ->
    case file:read_file(WalletPath) of
        {ok, Bin} ->
            try json:decode(Bin) of
                Map when is_map(Map) ->
                    Addr = get_arweave_address(Map),
                    OwnerB64 = maps:get(<<"n">>, Map, maps:get(n, Map, <<>>)),
                    PrivKey = extract_rsa_privkey(Map),
                    {ok, #{
                        address => Addr,
                        owner => to_bin(OwnerB64),
                        privkey => PrivKey,
                        jwk => Map,
                        raw => Bin
                    }};
                _ ->
                    {error, invalid_jwk_json}
            catch
                _:Err -> {error, {json_decode_failed, Err}}
            end;
        Err -> Err
    end.

extract_rsa_privkey(Map) ->
    try
        Keys = [<<"e">>, <<"n">>, <<"d">>, <<"p">>, <<"q">>, <<"dp">>, <<"dq">>, <<"qi">>],
        AltKeys = [e, n, d, p, q, dp, dq, qi],
        Vals = lists:map(fun({K1, K2}) ->
            b64url_decode(maps:get(K1, Map, maps:get(K2, Map, <<>>)))
        end, lists:zip(Keys, AltKeys)),
        case lists:all(fun(B) -> is_binary(B) andalso byte_size(B) > 0 end, Vals) of
            true -> Vals;
            false -> undefined
        end
    catch
        _:_ -> undefined
    end.

b64url_decode(Bin) when is_binary(Bin) ->
    S1 = binary:replace(binary:replace(Bin, <<"-">>, <<"+">>, [global]), <<"_">>, <<"/">>, [global]),
    PadLen = (4 - (byte_size(S1) rem 4)) rem 4,
    Padded = <<S1/binary, (binary:copy(<<"=">>, PadLen))/binary>>,
    base64:decode(Padded);
b64url_decode(List) when is_list(List) ->
    b64url_decode(list_to_binary(List)).

b64url_encode(Bin) when is_binary(Bin) ->
    B64 = base64:encode(Bin),
    S1 = binary:replace(binary:replace(B64, <<"+">>, <<"-">>, [global]), <<"/">>, <<"_">>, [global]),
    binary:replace(S1, <<"=">>, <<>>, [global]).

get_arweave_address(#{<<"n">> := N_b64}) ->
    ModulusBytes = b64url_decode(N_b64),
    Hash = crypto:hash(sha256, ModulusBytes),
    b64url_encode(Hash);
get_arweave_address(#{n := N_b64}) ->
    ModulusBytes = b64url_decode(N_b64),
    Hash = crypto:hash(sha256, ModulusBytes),
    b64url_encode(Hash);
get_arweave_address(_) ->
    <<"unknown_address">>.

sign_payload(PayloadBin, PrivKey) when is_binary(PayloadBin), is_list(PrivKey) ->
    Digest = crypto:hash(sha256, PayloadBin),
    Opts = [{rsa_padding, rsa_pkcs1_pss_padding}, {rsa_pss_saltlen, 32}],
    Sig = crypto:sign(rsa, sha256, Digest, PrivKey, Opts),
    b64url_encode(Sig).

verify_payload_sig(PayloadBin, SigB64, OwnerB64) ->
    try
        Digest = crypto:hash(sha256, PayloadBin),
        Sig = b64url_decode(SigB64),
        Modulus = b64url_decode(OwnerB64),
        E = <<1, 0, 1>>, %% standard 65537 public exponent
        Opts = [{rsa_padding, rsa_pkcs1_pss_padding}, {rsa_pss_saltlen, 32}],
        crypto:verify(rsa, sha256, Digest, Sig, [E, Modulus], Opts)
    catch
        _:_ -> false
    end.

derive_cluster_secret(WalletPath) ->
    case file:read_file(WalletPath) of
        {ok, Bytes} -> crypto:hash(sha256, Bytes);
        _ ->
            case os:getenv("HYDB_CLUSTER_SECRET") of
                false ->
                    error({missing_cluster_secret, "HYDB_CLUSTER_SECRET must be configured in environment or wallet provided"});
                "" ->
                    error({missing_cluster_secret, "HYDB_CLUSTER_SECRET must not be empty"});
                Sec when length(Sec) >= 16 ->
                    crypto:hash(sha256, list_to_binary(Sec));
                _ ->
                    error({invalid_cluster_secret, "HYDB_CLUSTER_SECRET must be at least 16 bytes"})
            end
    end.

compute_batch_id(BatchData) ->
    Epoch = to_int(maps:get(<<"epoch">>, BatchData, maps:get(epoch, BatchData, 1)), 1),
    FromSeq = to_int(maps:get(<<"from_seq">>, BatchData, maps:get(from_seq, BatchData, 0)), 0),
    ToSeq = to_int(maps:get(<<"to_seq">>, BatchData, maps:get(to_seq, BatchData, 0)), 0),
    PropHash = to_bin(maps:get(<<"proposed_hash">>, BatchData, maps:get(proposed_hash, BatchData, <<>>))),
    PropRoot = to_bin(maps:get(<<"proposed_state_root">>, BatchData, maps:get(proposed_state_root, BatchData, <<"0">>))),
    Canonical = list_to_binary([
        <<"HYDB_BATCH_V1|">>,
        integer_to_binary(Epoch), <<"|">>,
        integer_to_binary(FromSeq), <<"|">>,
        integer_to_binary(ToSeq), <<"|">>,
        string:uppercase(PropHash), <<"|">>,
        PropRoot
    ]),
    string:uppercase(binary:encode_hex(crypto:hash(sha256, Canonical))).

compute_batch_sig(BatchData, SecretKey) ->
    Events = maps:get(<<"events">>, BatchData, maps:get(events, BatchData, [])),
    EventHashesList = [to_bin(maps:get(<<"hash">>, Ev, maps:get(hash, Ev, <<>>))) || Ev <- Events],
    EventHashes = join_binaries(EventHashesList, <<":">>),
    SuId = to_bin(maps:get(<<"su_id">>, BatchData, maps:get(su_id, BatchData, <<"SU_NODE_1">>))),
    Epoch = integer_to_binary(to_int(maps:get(<<"epoch">>, BatchData, maps:get(epoch, BatchData, 1)), 1)),
    FromSeq = integer_to_binary(to_int(maps:get(<<"from_seq">>, BatchData, maps:get(from_seq, BatchData, 0)), 0)),
    ToSeq = integer_to_binary(to_int(maps:get(<<"to_seq">>, BatchData, maps:get(to_seq, BatchData, 0)), 0)),
    PropRoot = to_bin(maps:get(<<"proposed_state_root">>, BatchData, maps:get(proposed_state_root, BatchData, <<"0">>))),
    PropHash = to_bin(maps:get(<<"proposed_hash">>, BatchData, maps:get(proposed_hash, BatchData, <<>>))),
    Payload = list_to_binary([
        SuId, <<"|">>,
        Epoch, <<"|">>,
        FromSeq, <<"|">>,
        ToSeq, <<"|">>,
        PropRoot, <<"|">>,
        PropHash, <<"|">>,
        EventHashes
    ]),
    Mac = crypto:mac(hmac, sha256, SecretKey, Payload),
    string:uppercase(binary:encode_hex(Mac)).

%% ----------------------------------------------------------------------------
%% Arweave deepHash & ANS-104 DataItem Protocol
%% ----------------------------------------------------------------------------

deep_hash(Bin) when is_binary(Bin) ->
    LenBin = integer_to_binary(byte_size(Bin)),
    Tag = <<"blob", LenBin/binary>>,
    TagHash = crypto:hash(sha384, Tag),
    BlobHash = crypto:hash(sha384, Bin),
    crypto:hash(sha384, <<TagHash/binary, BlobHash/binary>>);
deep_hash(List) when is_list(List) ->
    LenBin = integer_to_binary(length(List)),
    Tag = <<"list", LenBin/binary>>,
    TagHash = crypto:hash(sha384, Tag),
    deep_hash_chunks(List, TagHash).

deep_hash_chunks([], AccHash) -> AccHash;
deep_hash_chunks([H | T], AccHash) ->
    ChunkHash = deep_hash(H),
    NextAcc = crypto:hash(sha384, <<AccHash/binary, ChunkHash/binary>>),
    deep_hash_chunks(T, NextAcc).

encode_avro_tags([]) -> <<>>;
encode_avro_tags(Tags) when is_list(Tags) ->
    Count = length(Tags),
    BlockHeader = encode_varint(Count bsl 1),
    Items = [encode_tag(T) || T <- Tags],
    list_to_binary([BlockHeader, Items, <<0>>]).

encode_tag({Name, Value}) ->
    NameBin = to_bin(Name),
    ValBin = to_bin(Value),
    [
        encode_varint(byte_size(NameBin) bsl 1), NameBin,
        encode_varint(byte_size(ValBin) bsl 1), ValBin
    ];
encode_tag(#{<<"name">> := Name, <<"value">> := Value}) ->
    encode_tag({Name, Value});
encode_tag(#{name := Name, value := Value}) ->
    encode_tag({Name, Value}).

encode_varint(N) when N < 128 -> <<N>>;
encode_varint(N) ->
    Byte = (N band 127) bor 128,
    Rest = encode_varint(N bsr 7),
    <<Byte, Rest/binary>>.

decode_avro_tags(<<>>) -> {ok, [], <<>>};
decode_avro_tags(Bin) when is_binary(Bin) ->
    try
        {Count, Rest} = decode_zigzag_varint(Bin),
        decode_tags_items(Count, Rest, [])
    catch
        _:Err -> {error, {invalid_avro_tags, Err}}
    end.

decode_tags_items(0, Rest, Acc) ->
    {ok, lists:reverse(Acc), Rest};
decode_tags_items(Remaining, Bin, Acc) ->
    {NameLen, R1} = decode_zigzag_varint(Bin),
    <<Name:NameLen/binary, R2/binary>> = R1,
    {ValLen, R3} = decode_zigzag_varint(R2),
    <<Val:ValLen/binary, R4/binary>> = R3,
    NewAcc = [#{<<"name">> => Name, <<"value">> => Val} | Acc],
    if
        Remaining =:= 1 ->
            case R4 of
                <<0, RestFinal/binary>> -> {ok, lists:reverse(NewAcc), RestFinal};
                _ -> {ok, lists:reverse(NewAcc), R4}
            end;
        true ->
            decode_tags_items(Remaining - 1, R4, NewAcc)
    end.

decode_zigzag_varint(Bin) ->
    {N, Rest} = decode_varint(Bin, 0, 0),
    Decoded = (N bsr 1) bxor (-(N band 1)),
    {Decoded, Rest}.

decode_varint(<<1:1, Val:7, Rest/binary>>, Shift, Acc) ->
    decode_varint(Rest, Shift + 7, Acc bor (Val bsl Shift));
decode_varint(<<0:1, Val:7, Rest/binary>>, Shift, Acc) ->
    {Acc bor (Val bsl Shift), Rest}.

%% Build authentic ANS-104 DataItem binary and compute deterministic ID
build_ans104_data_item(ProcessId, Action, ExtraTags, DataBin, WalletInfo) ->
    PrivKey = maps:get(privkey, WalletInfo, undefined),
    OwnerB64 = maps:get(owner, WalletInfo, <<>>),
    if
        PrivKey =:= undefined orelse byte_size(OwnerB64) =:= 0 ->
            {error, missing_private_key_or_owner};
        true ->
            Owner = b64url_decode(OwnerB64),
            TargetStr = to_bin(ProcessId),
            {TargetBytes, TargetChunk} = case TargetStr of
                <<>> ->
                    {<<>>, <<0:8>>};
                _ ->
                    TB = if
                        byte_size(TargetStr) =:= 32 -> TargetStr;
                        byte_size(TargetStr) =:= 43 -> b64url_decode(TargetStr);
                        true ->
                            crypto:hash(sha256, TargetStr)
                    end,
                    {TB, <<1:8, TB:32/binary>>}
            end,
            AnchorBytes = <<>>,
            AnchorChunk = <<0:8>>,

            BaseTags = [
                {<<"Data-Protocol">>, <<"ao">>},
                {<<"Variant">>, <<"ao.TN.1">>},
                {<<"Type">>, <<"Message">>},
                {<<"Action">>, to_bin(Action)}
            ],
            NormalizedExtra = lists:map(fun
                ({K, V}) -> {to_bin(K), to_bin(V)};
                (#{<<"name">> := K, <<"value">> := V}) -> {to_bin(K), to_bin(V)};
                (#{name := K, value := V}) -> {to_bin(K), to_bin(V)}
            end, ExtraTags),
            AllTags = BaseTags ++ NormalizedExtra,
            RawTags = encode_avro_tags(AllTags),
            TagsCount = length(AllTags),
            TagsByteLen = byte_size(RawTags),
            TagsChunk = <<TagsCount:64/little, TagsByteLen:64/little, RawTags/binary>>,

            SigData = deep_hash([
                <<"dataitem">>,
                <<"1">>,
                <<"1">>,
                Owner,
                TargetBytes,
                AnchorBytes,
                RawTags,
                DataBin
            ]),
            Opts = [{rsa_padding, rsa_pkcs1_pss_padding}, {rsa_pss_saltlen, 32}],
            Signature = crypto:sign(rsa, sha256, SigData, PrivKey, Opts),
            ItemId = b64url_encode(crypto:hash(sha256, Signature)),

            DataItemBin = <<
                1:16/little,
                Signature:512/binary,
                Owner:512/binary,
                TargetChunk/binary,
                AnchorChunk/binary,
                TagsChunk/binary,
                DataBin/binary
            >>,
            {ok, ItemId, DataItemBin}
    end.

%% Cryptographic verification of ANS-104 DataItem binary
verify_ans104_data_item(DataItemBin) when is_binary(DataItemBin) ->
    case DataItemBin of
        <<1:16/little, Signature:512/binary, Owner:512/binary, Rest/binary>> ->
            case Rest of
                <<0:8, Rest1/binary>> ->
                    parse_ans104_anchor(Signature, Owner, <<>>, Rest1);
                <<1:8, TargetBytes:32/binary, Rest1/binary>> ->
                    parse_ans104_anchor(Signature, Owner, TargetBytes, Rest1);
                _ ->
                    {error, invalid_target_chunk}
            end;
        _ ->
            {error, invalid_ans104_header}
    end.

parse_ans104_anchor(Sig, Owner, TargetBytes, <<0:8, Rest2/binary>>) ->
    parse_ans104_tags(Sig, Owner, TargetBytes, <<>>, Rest2);
parse_ans104_anchor(Sig, Owner, TargetBytes, <<1:8, AnchorBytes:32/binary, Rest2/binary>>) ->
    parse_ans104_tags(Sig, Owner, TargetBytes, AnchorBytes, Rest2);
parse_ans104_anchor(_Sig, _Owner, _Target, _) ->
    {error, invalid_anchor_chunk}.

parse_ans104_tags(Sig, Owner, TargetBytes, AnchorBytes, <<TagsCount:64/little, TagsByteLen:64/little, Rest3/binary>>) ->
    case Rest3 of
        <<RawTags:TagsByteLen/binary, Data/binary>> ->
            SigData = deep_hash([
                <<"dataitem">>,
                <<"1">>,
                <<"1">>,
                Owner,
                TargetBytes,
                AnchorBytes,
                RawTags,
                Data
            ]),
            E = <<1, 0, 1>>,
            Opts = [{rsa_padding, rsa_pkcs1_pss_padding}, {rsa_pss_saltlen, 32}],
            case crypto:verify(rsa, sha256, SigData, Sig, [E, Owner], Opts) of
                true ->
                    TagsList = case TagsCount of
                        0 -> [];
                        _ ->
                            case decode_avro_tags(RawTags) of
                                {ok, DecTags, _} -> DecTags;
                                _ -> []
                            end
                    end,
                    ItemId = b64url_encode(crypto:hash(sha256, Sig)),
                    TargetFormatted = if
                        byte_size(TargetBytes) =:= 32 -> b64url_encode(TargetBytes);
                        true -> <<>>
                    end,
                    {ok, #{
                        id => ItemId,
                        owner => b64url_encode(Owner),
                        target => TargetFormatted,
                        tags => TagsList,
                        data => Data,
                        raw => Sig
                    }};
                false ->
                    {error, invalid_data_item_signature}
            end;
        _ ->
            {error, invalid_tags_length}
    end;
parse_ans104_tags(_Sig, _Owner, _Target, _Anchor, _) ->
    {error, invalid_tags_header}.

%% ----------------------------------------------------------------------------
%% HTTP Dispatch to AO Message Units & Gateways
%% ----------------------------------------------------------------------------

send_message(GatewayUrl, ProcessId, Action, Tags, PayloadMap) ->
    send_message(GatewayUrl, ProcessId, Action, Tags, PayloadMap, #{}).

send_message(GatewayUrl, ProcessId, Action, Tags, PayloadMap, Opts) ->
    ensure_http_client(),
    DataBin = if
        is_binary(PayloadMap) -> PayloadMap;
        is_map(PayloadMap) -> iolist_to_binary(json:encode(PayloadMap));
        true -> <<>>
    end,
    Sender = maps:get(sender, Opts, maps:get(<<"From">>, Opts, <<"SU_NODE_1">>)),
    WalletInfo = maps:get(wallet, Opts, #{}),
    Url = format_ao_url(GatewayUrl, ProcessId, Action, Opts),
    HttpTimeout = maps:get(timeout, Opts, 8000),
    IsHb = is_hyperbeam_gateway(GatewayUrl, Opts),

    case build_ans104_data_item(ProcessId, Action, Tags, DataBin, WalletInfo) of
        {ok, ItemId, DataItemBin} ->
            ReqFormat = maps:get(format, Opts, ans104_binary),
            {ContentType, Body} = case ReqFormat of
                json ->
                    BaseTags = [
                        #{<<"name">> => <<"Data-Protocol">>, <<"value">> => <<"ao">>},
                        #{<<"name">> => <<"Variant">>, <<"value">> => <<"ao.TN.1">>},
                        #{<<"name">> => <<"Type">>, <<"value">> => <<"Message">>},
                        #{<<"name">> => <<"Action">>, <<"value">> => to_bin(Action)}
                    ],
                    AllTags = BaseTags ++ Tags,
                    Owner = maps:get(owner, WalletInfo, <<>>),
                    Envelope = #{
                        <<"Id">> => ItemId,
                        <<"Target">> => to_bin(ProcessId),
                        <<"Action">> => to_bin(Action),
                        <<"Tags">> => AllTags,
                        <<"Data">> => DataBin,
                        <<"From">> => to_bin(Sender),
                        <<"Owner">> => to_bin(Owner),
                        <<"Signature-Type">> => 1,
                        <<"Timestamp">> => erlang:system_time(millisecond)
                    },
                    {"application/json", iolist_to_binary(json:encode(Envelope))};
                _ ->
                    case IsHb of
                        true -> {"application/ans104", DataItemBin};
                        false -> {"application/octet-stream", DataItemBin}
                    end
            end,

            Headers = [
                {"content-type", ContentType},
                {"accept", "application/json"},
                {"connection", "close"},
                {"x-dataitem-id", binary_to_list(ItemId)}
            ],
            case execute_http_post(Url, Headers, ContentType, Body, HttpTimeout) of
                {ok, HttpStatus, RespMap} when is_map(RespMap) ->
                    %% Check if response contains immediate verdict (HyperBEAM route or mock)
                    case has_verdict(RespMap) of
                        true ->
                            {ok, HttpStatus, RespMap};
                        false ->
                            case IsHb of
                                true ->
                                    fetch_hyperbeam_process_result(GatewayUrl, ProcessId, Opts);
                                false ->
                                    %% AO Message Unit asynchronous response: fetch evaluation from Compute Unit (CU)
                                    MsgId = maps:get(<<"messageId">>, RespMap, maps:get(<<"id">>, RespMap, ItemId)),
                                    CuUrl = resolve_cu_url(Opts),
                                    if
                                        CuUrl =/= "" ->
                                            fetch_cu_result(CuUrl, ProcessId, MsgId, Opts);
                                        true ->
                                            {ok, HttpStatus, RespMap}
                                    end
                            end
                    end;
                OtherResp ->
                    OtherResp
            end;

        {error, _BuildErr} ->
            %% Unsigned fallback when wallet is missing (dryrun/query fallback)
            BaseTags = [
                #{<<"name">> => <<"Data-Protocol">>, <<"value">> => <<"ao">>},
                #{<<"name">> => <<"Variant">>, <<"value">> => <<"ao.TN.1">>},
                #{<<"name">> => <<"Type">>, <<"value">> => <<"Message">>},
                #{<<"name">> => <<"Action">>, <<"value">> => to_bin(Action)}
            ],
            AllTags = BaseTags ++ Tags,
            Envelope = #{
                <<"Target">> => to_bin(ProcessId),
                <<"Action">> => to_bin(Action),
                <<"Tags">> => AllTags,
                <<"Data">> => DataBin,
                <<"From">> => to_bin(Sender),
                <<"Timestamp">> => erlang:system_time(millisecond)
            },
            EnvelopeJson = iolist_to_binary(json:encode(Envelope)),
            Headers = [
                {"content-type", "application/json"},
                {"accept", "application/json"}
            ],
            execute_http_post(Url, Headers, "application/json", EnvelopeJson, HttpTimeout)
    end.

execute_http_post(Url, Headers, ContentType, Body, HttpTimeout) ->
    case httpc:request(post, {Url, Headers, ContentType, Body}, [{timeout, HttpTimeout}], [{body_format, binary}]) of
        {ok, {{_Ver, Status, _Phrase}, _RespHeaders, RespBody}} when Status >= 200, Status < 300 ->
            case catch json:decode(RespBody) of
                Parsed when is_map(Parsed) -> {ok, Status, Parsed};
                _ -> {ok, Status, RespBody}
            end;
        {ok, {{_Ver, Status, _Phrase}, _RespHeaders, RespBody}} ->
            {error, {http_error, Status, RespBody}};
        {error, Reason} ->
            {error, {network_failed, Reason}}
    end.

has_verdict(Map) when is_map(Map) ->
    case maps:get(<<"Status">>, Map, undefined) of
        S when is_binary(S), S =/= <<>> -> true;
        _ ->
            case maps:get(<<"status">>, Map, undefined) of
                S when is_binary(S), S =/= <<>> -> true;
                _ -> false
            end
    end.

%% ----------------------------------------------------------------------------
%% Compute Unit (CU) Evaluation Result Polling & Extraction
%% ----------------------------------------------------------------------------

fetch_cu_result(CuUrl, ProcessId, MessageId) ->
    fetch_cu_result(CuUrl, ProcessId, MessageId, #{}).

fetch_cu_result(CuUrl, ProcessId, MessageId, Opts) ->
    ensure_http_client(),
    CleanCu = string:trim(to_list(CuUrl), trailing, "/"),
    CleanProc = to_list(ProcessId),
    CleanMsgId = to_list(MessageId),
    Url = CleanCu ++ "/result/" ++ CleanMsgId ++ "?process-id=" ++ CleanProc,
    Headers = [{"accept", "application/json"}],
    HttpTimeout = maps:get(timeout, Opts, 5000),
    MaxRetries = maps:get(cu_retries, Opts, 6),
    RetryDelayMs = maps:get(cu_retry_delay_ms, Opts, 400),
    poll_cu_result(Url, Headers, HttpTimeout, MaxRetries, RetryDelayMs).

poll_cu_result(Url, Headers, HttpTimeout, RetriesLeft, DelayMs) ->
    case httpc:request(get, {Url, Headers}, [{timeout, HttpTimeout}], [{body_format, binary}]) of
        {ok, {{_Ver, 200, _Phrase}, _RespHeaders, RespBody}} ->
            case catch json:decode(RespBody) of
                Parsed when is_map(Parsed) ->
                    case extract_cu_verdict(Parsed) of
                        {ok, Verdict} ->
                            {ok, 200, Verdict};
                        {pending, _} when RetriesLeft > 0 ->
                            timer:sleep(DelayMs),
                            poll_cu_result(Url, Headers, HttpTimeout, RetriesLeft - 1, DelayMs);
                        {pending, Reason} ->
                            {error, {cu_evaluation_pending, Reason}};
                        {error, Err} ->
                            {error, {cu_execution_error, Err}}
                    end;
                _ ->
                    if
                        RetriesLeft > 0 ->
                            timer:sleep(DelayMs),
                            poll_cu_result(Url, Headers, HttpTimeout, RetriesLeft - 1, DelayMs);
                        true ->
                            {error, invalid_cu_json_response}
                    end
            end;
        {ok, {{_Ver, Status, _Phrase}, _RespHeaders, _RespBody}} when Status =:= 404 orelse Status =:= 202 ->
            if
                RetriesLeft > 0 ->
                    timer:sleep(DelayMs),
                    poll_cu_result(Url, Headers, HttpTimeout, RetriesLeft - 1, DelayMs);
                true ->
                    {error, {cu_not_ready, Status}}
            end;
        {ok, {{_Ver, Status, _Phrase}, _RespHeaders, RespBody}} ->
            {error, {cu_http_error, Status, RespBody}};
        {error, _NetErr} when RetriesLeft > 0 ->
            timer:sleep(DelayMs),
            poll_cu_result(Url, Headers, HttpTimeout, RetriesLeft - 1, DelayMs);
        {error, Reason} ->
            {error, {cu_network_failed, Reason}}
    end.

extract_cu_verdict(Parsed) ->
    case maps:get(<<"Error">>, Parsed, undefined) of
        Err when Err =/= undefined andalso Err =/= null andalso Err =/= <<>> ->
            {error, Err};
        _ ->
            case maps:get(<<"Messages">>, Parsed, undefined) of
                Messages when is_list(Messages), length(Messages) > 0 ->
                    extract_message_verdict(Messages);
                _ ->
                    case maps:is_key(<<"Status">>, Parsed) of
                        true -> {ok, Parsed};
                        false -> {pending, empty_messages}
                    end
            end
    end.

extract_message_verdict([]) ->
    {pending, no_matching_verdict_message};
extract_message_verdict([Msg | Rest]) when is_map(Msg) ->
    Tags = maps:get(<<"Tags">>, Msg, maps:get(tags, Msg, [])),
    TagsMap = tags_to_map(Tags),
    DataMap = case maps:get(<<"Data">>, Msg, maps:get(data, Msg, undefined)) of
        DataBin when is_binary(DataBin) ->
            case catch json:decode(DataBin) of
                ParsedData when is_map(ParsedData) -> ParsedData;
                _ -> #{}
            end;
        DataMapRaw when is_map(DataMapRaw) -> DataMapRaw;
        _ -> #{}
    end,
    Merged = maps:merge(maps:merge(TagsMap, DataMap), Msg),
    case maps:is_key(<<"Status">>, Merged) orelse maps:is_key(<<"status">>, Merged) orelse maps:is_key(<<"ok">>, Merged) of
        true ->
            {ok, Merged};
        false ->
            extract_message_verdict(Rest)
    end;
extract_message_verdict([_ | Rest]) ->
    extract_message_verdict(Rest).

tags_to_map(Tags) when is_list(Tags) ->
    lists:foldl(fun
        (#{<<"name">> := K, <<"value">> := V}, Acc) -> Acc#{K => V};
        (#{name := K, value := V}, Acc) -> Acc#{to_bin(K) => to_bin(V)};
        ({K, V}, Acc) -> Acc#{to_bin(K) => to_bin(V)};
        (_, Acc) -> Acc
    end, #{}, Tags);
tags_to_map(_) -> #{}.

%% ----------------------------------------------------------------------------
%% Query State via Official AO Compute Unit (CU) dry-run Endpoint
%% ----------------------------------------------------------------------------

query_state(GatewayUrl, ProcessId, Action, PayloadMap) ->
    query_state(GatewayUrl, ProcessId, Action, PayloadMap, #{}).

query_state(GatewayUrl, ProcessId, Action, PayloadMap, Opts) ->
    CleanGw = to_list(GatewayUrl),
    IsHb = is_hyperbeam_gateway(CleanGw, Opts),
    if
        IsHb ->
            send_message(GatewayUrl, ProcessId, Action, [], PayloadMap, Opts);
        true ->
            CuUrl = resolve_cu_url(Opts),
            IsHyperbeamDirect = string:find(CleanGw, "/ao") =/= nomatch,
            if
                CuUrl =/= "" andalso not IsHyperbeamDirect ->
                    dryrun_cu_query(CuUrl, ProcessId, Action, PayloadMap, Opts);
                true ->
                    send_message(GatewayUrl, ProcessId, Action, [], PayloadMap, Opts#{format => json})
            end
    end.

dryrun_cu_query(CuUrl, ProcessId, Action, PayloadMap) ->
    dryrun_cu_query(CuUrl, ProcessId, Action, PayloadMap, #{}).

dryrun_cu_query(CuUrl, ProcessId, Action, PayloadMap, Opts) ->
    ensure_http_client(),
    CleanCu = string:trim(to_list(CuUrl), trailing, "/"),
    CleanProc = to_list(ProcessId),
    Url = CleanCu ++ "/dry-run?process-id=" ++ CleanProc,

    DataBin = if
        is_binary(PayloadMap) -> PayloadMap;
        is_map(PayloadMap) -> iolist_to_binary(json:encode(PayloadMap));
        true -> <<>>
    end,
    WalletInfo = maps:get(wallet, Opts, #{}),
    Owner = maps:get(owner, WalletInfo, <<>>),
    RandomId = b64url_encode(crypto:strong_rand_bytes(16)),

    BaseTags = [
        #{<<"name">> => <<"Data-Protocol">>, <<"value">> => <<"ao">>},
        #{<<"name">> => <<"Variant">>, <<"value">> => <<"ao.TN.1">>},
        #{<<"name">> => <<"Type">>, <<"value">> => <<"Message">>},
        #{<<"name">> => <<"Action">>, <<"value">> => to_bin(Action)}
    ],
    ExtraTags = maps:get(tags, Opts, []),
    AllTags = BaseTags ++ ExtraTags,

    DryrunPayload = #{
        <<"Id">> => <<"DRYRUN_", RandomId/binary>>,
        <<"Target">> => to_bin(ProcessId),
        <<"Owner">> => to_bin(Owner),
        <<"Tags">> => AllTags,
        <<"Data">> => DataBin
    },
    DryrunJson = iolist_to_binary(json:encode(DryrunPayload)),
    Headers = [
        {"content-type", "application/json"},
        {"accept", "application/json"}
    ],
    HttpTimeout = maps:get(timeout, Opts, 8000),

    case httpc:request(post, {Url, Headers, "application/json", DryrunJson}, [{timeout, HttpTimeout}], [{body_format, binary}]) of
        {ok, {{_Ver, Status, _Phrase}, _RespHeaders, RespBody}} when Status >= 200, Status < 300 ->
            case catch json:decode(RespBody) of
                Parsed when is_map(Parsed) ->
                    extract_dryrun_result(Parsed);
                _ ->
                    {ok, Status, RespBody}
            end;
        {ok, {{_Ver, Status, _Phrase}, _RespHeaders, RespBody}} ->
            {error, {cu_dryrun_error, Status, RespBody}};
        {error, Reason} ->
            {error, {cu_network_failed, Reason}}
    end.

extract_dryrun_result(Parsed) ->
    case maps:get(<<"Error">>, Parsed, undefined) of
        Err when Err =/= undefined andalso Err =/= null andalso Err =/= <<>> ->
            {error, {dryrun_error, Err}};
        _ ->
            case maps:get(<<"Messages">>, Parsed, undefined) of
                [FirstMsg | _] when is_map(FirstMsg) ->
                    DataDecoded = case maps:get(<<"Data">>, FirstMsg, undefined) of
                        DataStr when is_binary(DataStr) ->
                            case catch json:decode(DataStr) of
                                DMap when is_map(DMap) -> DMap;
                                _ -> #{<<"data">> => DataStr}
                            end;
                        _ -> #{}
                    end,
                    Tags = maps:get(<<"Tags">>, FirstMsg, []),
                    TagsMap = tags_to_map(Tags),
                    Merged = maps:merge(maps:merge(FirstMsg, TagsMap), DataDecoded),
                    {ok, 200, Merged};
                _ ->
                    case maps:get(<<"Output">>, Parsed, undefined) of
                        OutMap when is_map(OutMap) -> {ok, 200, OutMap};
                        _ -> {ok, 200, Parsed}
                    end
            end
    end.

%% ----------------------------------------------------------------------------
%% Internal Resolution & Environment Helpers
%% ----------------------------------------------------------------------------

ensure_http_client() ->
    _ = application:ensure_all_started(inets),
    _ = application:ensure_all_started(ssl),
    ok.

format_ao_url(GatewayUrl, ProcessId, Action) ->
    format_ao_url(GatewayUrl, ProcessId, Action, #{}).

format_ao_url(GatewayUrl, ProcessId, Action, Opts) ->
    CleanGw = string:trim(to_list(GatewayUrl), trailing, "/"),
    CleanProc = to_list(ProcessId),
    CleanAction = to_list(Action),
    case is_hyperbeam_gateway(CleanGw, Opts) of
        true ->
            case string:find(CleanGw, "~process@1.0") of
                nomatch ->
                    CleanGw ++ "/" ++ CleanProc ++ "~process@1.0/schedule";
                _ ->
                    case string:find(CleanGw, "/schedule") of
                        nomatch -> CleanGw ++ "/schedule";
                        _ -> CleanGw
                    end
            end;
        false ->
            case CleanGw of
                "" ->
                    "http://127.0.0.1:8735/ao/" ++ CleanProc ++ "/" ++ CleanAction;
                Gw ->
                    case string:find(Gw, "/ao") of
                        nomatch ->
                            case string:find(Gw, "/message") of
                                nomatch -> Gw ++ "/message";
                                _ -> Gw
                            end;
                        _ ->
                            case string:find(Gw, CleanProc) of
                                nomatch -> Gw ++ "/" ++ CleanProc ++ "/" ++ CleanAction;
                                _ ->
                                    case string:find(Gw, CleanAction) of
                                        nomatch -> Gw ++ "/" ++ CleanAction;
                                        _ -> Gw
                                    end
                            end
                    end
            end
    end.

is_hyperbeam_gateway(GwUrl, Opts) ->
    Backend = case maps:get(backend, Opts, maps:get(<<"backend">>, Opts, auto)) of
        <<"hyperbeam">> -> hyperbeam;
        <<"ao_mu">> -> ao_mu;
        Other -> Other
    end,
    case Backend of
        hyperbeam -> true;
        ao_mu -> false;
        auto ->
            HasCuUrl = maps:is_key(cu_url, Opts) orelse maps:is_key(<<"cu_url">>, Opts),
            case HasCuUrl of
                true -> false;
                false ->
                    Clean = to_list(GwUrl),
                    case string:find(Clean, "~process@1.0") of
                        nomatch ->
                            case string:find(Clean, "/message") of
                                nomatch ->
                                    case string:find(Clean, "mu.ao") of
                                        nomatch ->
                                            case string:find(Clean, "ao-testnet") of
                                                nomatch ->
                                                    case string:find(Clean, "/ao/") of
                                                        nomatch -> true;
                                                        _ -> false
                                                    end;
                                                _ -> false
                                            end;
                                        _ -> false
                                    end;
                                _ -> false
                            end;
                        _ -> true
                    end
            end
    end.

fetch_hyperbeam_process_result(GatewayUrl, ProcessId) ->
    fetch_hyperbeam_process_result(GatewayUrl, ProcessId, #{}).

fetch_hyperbeam_process_result(GatewayUrl, ProcessId, Opts) ->
    ensure_http_client(),
    CleanGw = string:trim(to_list(GatewayUrl), trailing, "/"),
    CleanProc = to_list(ProcessId),
    HttpTimeout = maps:get(timeout, Opts, 90000),
    MaxRetries = maps:get(retries, Opts, 90),
    RetryDelayMs = maps:get(retry_delay_ms, Opts, 1000),
    poll_hyperbeam_result(CleanGw, CleanProc, HttpTimeout, MaxRetries, RetryDelayMs).

poll_hyperbeam_result(BaseUrl, ProcessId, Timeout, RetriesLeft, DelayMs) ->
    NowUrl = BaseUrl ++ "/" ++ ProcessId ++ "~process@1.0/now",
    Headers = [{"accept", "application/json"}, {"connection", "close"}],
    case httpc:request(get, {NowUrl, Headers}, [{timeout, Timeout}], [{body_format, binary}]) of
        {ok, {{_Ver, 200, _Phrase}, _RespHeaders, RespBody}} ->
            case catch json:decode(RespBody) of
                NowMap when is_map(NowMap) ->
                    case maps:get(<<"results+link">>, NowMap, undefined) of
                        ResultsLink when is_binary(ResultsLink), byte_size(ResultsLink) > 0 ->
                            resolve_hyperbeam_results(BaseUrl, binary_to_list(ResultsLink), Timeout);
                        _ ->
                            if
                                RetriesLeft > 0 ->
                                    timer:sleep(DelayMs),
                                    poll_hyperbeam_result(BaseUrl, ProcessId, Timeout, RetriesLeft - 1, DelayMs);
                                true ->
                                    {error, {hyperbeam_error, missing_results_link, NowMap}}
                            end
                    end;
                _ ->
                    {error, {hyperbeam_error, invalid_now_json, RespBody}}
            end;
        {ok, {{_Ver, Status, _Phrase}, _RespHeaders, RespBody}} ->
            if
                RetriesLeft > 0 ->
                    timer:sleep(DelayMs),
                    poll_hyperbeam_result(BaseUrl, ProcessId, Timeout, RetriesLeft - 1, DelayMs);
                true ->
                    {error, {hyperbeam_http_error, Status, RespBody}}
            end;
        {error, _Reason} when RetriesLeft > 0 ->
            timer:sleep(DelayMs),
            poll_hyperbeam_result(BaseUrl, ProcessId, Timeout, RetriesLeft - 1, DelayMs);
        {error, Reason} ->
            {error, {hyperbeam_network_failed, Reason}}
    end.

resolve_hyperbeam_results(BaseUrl, ResultsLink, Timeout) ->
    Url = BaseUrl ++ "/" ++ ResultsLink,
    Headers = [{"accept", "application/json"}, {"connection", "close"}],
    case httpc:request(get, {Url, Headers}, [{timeout, Timeout}], [{body_format, binary}]) of
        {ok, {{_Ver, 200, _Phrase}, _RespHeaders, RespBody}} ->
            case catch json:decode(RespBody) of
                ResultsMap when is_map(ResultsMap) ->
                    case maps:get(<<"outbox+link">>, ResultsMap, undefined) of
                        OutboxLink when is_binary(OutboxLink), byte_size(OutboxLink) > 0 ->
                            resolve_hyperbeam_outbox(BaseUrl, binary_to_list(OutboxLink), Timeout);
                        _ ->
                            case maps:get(<<"Error">>, ResultsMap, undefined) of
                                Err when Err =/= undefined andalso Err =/= null andalso Err =/= <<>> ->
                                    {error, {hyperbeam_compute_error, Err}};
                                _ ->
                                    {ok, 200, ResultsMap}
                            end
                    end;
                _ ->
                    {error, {hyperbeam_error, invalid_results_json, RespBody}}
            end;
        {ok, {{_Ver, Status, _Phrase}, _RespHeaders, RespBody}} ->
            {error, {hyperbeam_http_error, Status, RespBody}};
        {error, Reason} ->
            {error, {hyperbeam_network_failed, Reason}}
    end.

resolve_hyperbeam_outbox(BaseUrl, OutboxLink, Timeout) ->
    Url = BaseUrl ++ "/" ++ OutboxLink,
    Headers = [{"accept", "application/json"}, {"connection", "close"}],
    case httpc:request(get, {Url, Headers}, [{timeout, Timeout}], [{body_format, binary}]) of
        {ok, {{_Ver, 200, _Phrase}, _RespHeaders, RespBody}} ->
            case catch json:decode(RespBody) of
                OutboxMap when is_map(OutboxMap) ->
                    extract_latest_outbox_verdict(BaseUrl, OutboxMap, Timeout);
                _ ->
                    {error, {hyperbeam_error, invalid_outbox_json, RespBody}}
            end;
        {ok, {{_Ver, Status, _Phrase}, _RespHeaders, RespBody}} ->
            {error, {hyperbeam_http_error, Status, RespBody}};
        {error, Reason} ->
            {error, {hyperbeam_network_failed, Reason}}
    end.

extract_latest_outbox_verdict(BaseUrl, OutboxMap, Timeout) ->
    ParsedEntries = lists:filtermap(fun({K, V}) ->
        case is_binary(V) andalso (K =/= <<"status">>) of
            true ->
                KeyStr = binary_to_list(K),
                SlotNum = case string:split(KeyStr, "+") of
                    [NumStr | _] ->
                        try list_to_integer(NumStr)
                        catch _:_ -> 0 end;
                    _ -> 0
                end,
                {true, {SlotNum, binary_to_list(V)}};
            false -> false
        end
    end, maps:to_list(OutboxMap)),

    case ParsedEntries of
        [] ->
            {ok, 200, OutboxMap};
        _ ->
            SortedDesc = lists:reverse(lists:keysort(1, ParsedEntries)),
            LinksDesc = [Link || {_, Link} <- SortedDesc],
            find_first_verdict_http(BaseUrl, LinksDesc, Timeout, OutboxMap)
    end.

find_first_verdict_http(_BaseUrl, [], _Timeout, DefaultMap) ->
    {ok, 200, DefaultMap};
find_first_verdict_http(BaseUrl, [Link | Rest], Timeout, DefaultMap) ->
    MsgUrl = BaseUrl ++ "/" ++ Link,
    Headers = [{"accept", "application/json"}, {"connection", "close"}],
    case httpc:request(get, {MsgUrl, Headers}, [{timeout, Timeout}], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            case catch json:decode(Body) of
                M when is_map(M) ->
                    Merged = case maps:get(<<"Data">>, M, undefined) of
                        DataBin when is_binary(DataBin) ->
                            case catch json:decode(DataBin) of
                                DataMap when is_map(DataMap) ->
                                    maps:merge(M, DataMap);
                                _ -> M
                            end;
                        _ -> M
                    end,
                    case has_verdict(Merged) orelse maps:is_key(<<"Action">>, Merged) of
                        true -> {ok, 200, Merged};
                        false ->
                            case find_first_verdict_http(BaseUrl, Rest, Timeout, Merged) of
                                {ok, 200, DefaultMap} -> {ok, 200, Merged};
                                Other -> Other
                            end
                    end;
                _ ->
                    find_first_verdict_http(BaseUrl, Rest, Timeout, DefaultMap)
            end;
        _ ->
            find_first_verdict_http(BaseUrl, Rest, Timeout, DefaultMap)
    end.

resolve_cu_url(Opts) ->
    case maps:get(cu_url, Opts, undefined) of
        undefined ->
            case os:getenv("AO_CU_URL") of
                false -> "";
                "" -> "";
                Url -> Url
            end;
        Explicit -> to_list(Explicit)
    end.

resolve_mu_url(Opts) ->
    case maps:get(mu_url, Opts, undefined) of
        undefined ->
            case os:getenv("AO_MU_URL") of
                false -> "";
                "" -> "";
                Url -> Url
            end;
        Explicit -> to_list(Explicit)
    end.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_list(V) -> list_to_binary(V);
to_bin(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_bin(V) when is_integer(V) -> integer_to_binary(V);
to_bin(_) -> <<>>.

to_list(V) when is_list(V) -> V;
to_list(V) when is_binary(V) -> binary_to_list(V);
to_list(V) when is_atom(V) -> atom_to_list(V);
to_list(V) when is_integer(V) -> integer_to_list(V);
to_list(_) -> "".

to_int(V, _Def) when is_integer(V) -> V;
to_int(V, Def) when is_binary(V) ->
    case catch binary_to_integer(string:trim(V)) of
        I when is_integer(I) -> I;
        _ -> Def
    end;
to_int(V, Def) when is_list(V) ->
    case catch list_to_integer(string:trim(V)) of
        I when is_integer(I) -> I;
        _ -> Def
    end;
to_int(_, Def) -> Def.

join_binaries([], _Sep) -> <<>>;
join_binaries([H], _Sep) -> to_bin(H);
join_binaries([H | T], Sep) ->
    lists:foldl(fun(Elem, Acc) -> <<Acc/binary, Sep/binary, (to_bin(Elem))/binary>> end, to_bin(H), T).
