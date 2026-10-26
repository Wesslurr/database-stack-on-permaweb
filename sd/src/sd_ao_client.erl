-module(sd_ao_client).

%% ============================================================================
%% Native In-BEAM AO Computer & Arweave Gateway Client
%% ============================================================================
%% Responsibilities:
%% 1. Pure Erlang HTTP communication with AO Scheduler Unit (SU) & Gateways.
%% 2. Native Arweave JWK parsing & cryptographic address calculation.
%% 3. Authentic ANS-104 DataItem deepHash signing & verification (RSA-PSS SHA-256).
%% 4. Avro binary tag encoding & decoding conforming to Arweave standard.
%% 5. Formatting of AO URLs and process routes.
%% 6. Dryrun / query evaluation against AO Genesis & Arbitrator processes.
%% ============================================================================

-export([load_wallet/1, b64url_encode/1, b64url_decode/1, get_arweave_address/1]).
-export([sign_payload/2, verify_payload_sig/3]).
-export([deep_hash/1, encode_avro_tags/1, decode_avro_tags/1]).
-export([build_ans104_data_item/5, verify_ans104_data_item/1]).
-export([send_message/5, send_message/6, query_state/4, query_state/5]).
-export([compute_batch_id/1, compute_batch_sig/2, derive_cluster_secret/1]).
-export([format_ao_url/3]).

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
                            %% Fallback pad or hash to 32 bytes
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
%% HTTP Dispatch to AO Gateways & Processes
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
    Url = format_ao_url(GatewayUrl, ProcessId, Action),
    HttpTimeout = maps:get(timeout, Opts, 8000),

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
                    {"application/octet-stream", DataItemBin}
            end,

            Headers = [
                {"content-type", ContentType},
                {"accept", "application/json, application/octet-stream"},
                {"x-dataitem-id", binary_to_list(ItemId)}
            ],
            execute_http_post(Url, Headers, ContentType, Body, HttpTimeout);

        {error, _BuildErr} ->
            %% Unsigned fallback when wallet is missing (dryrun/query)
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

query_state(GatewayUrl, ProcessId, Action, PayloadMap) ->
    query_state(GatewayUrl, ProcessId, Action, PayloadMap, #{}).

query_state(GatewayUrl, ProcessId, Action, PayloadMap, Opts) ->
    send_message(GatewayUrl, ProcessId, Action, [], PayloadMap, Opts#{format => json}).

%% ----------------------------------------------------------------------------
%% Internal Helpers
%% ----------------------------------------------------------------------------

ensure_http_client() ->
    _ = application:ensure_all_started(inets),
    _ = application:ensure_all_started(ssl),
    ok.

format_ao_url(GatewayUrl, ProcessId, Action) ->
    CleanGw = string:trim(to_list(GatewayUrl), trailing, "/"),
    CleanProc = to_list(ProcessId),
    CleanAction = to_list(Action),
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
