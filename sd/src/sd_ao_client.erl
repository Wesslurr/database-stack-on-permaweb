-module(sd_ao_client).

%% ============================================================================
%% Native In-BEAM AO Computer & Arweave Gateway Client
%% ============================================================================
%% Responsibilities:
%% 1. Pure Erlang HTTP communication with AO Scheduler Unit (SU) & Gateways.
%% 2. Native Arweave JWK parsing & cryptographic address calculation.
%% 3. Formatting of AO Messages (Tags, Action, Variant, Data).
%% 4. Dryrun / query evaluation against AO Genesis & Arbitrator processes.
%% ============================================================================

-export([load_wallet/1, b64url_encode/1, b64url_decode/1, get_arweave_address/1]).
-export([send_message/5, send_message/6, query_state/4, query_state/5]).
-export([compute_batch_id/1, compute_batch_sig/2, derive_cluster_secret/1]).

%% ----------------------------------------------------------------------------
%% Wallet & Cryptographic Utilities
%% ----------------------------------------------------------------------------

load_wallet(WalletPath) ->
    case file:read_file(WalletPath) of
        {ok, Bin} ->
            try json:decode(Bin) of
                Map when is_map(Map) ->
                    Addr = get_arweave_address(Map),
                    {ok, #{
                        address => Addr,
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

derive_cluster_secret(WalletPath) ->
    case file:read_file(WalletPath) of
        {ok, Bytes} -> crypto:hash(sha256, Bytes);
        _ ->
            case os:getenv("HYDB_CLUSTER_SECRET") of
                false -> crypto:hash(sha256, <<"hydb_default_cluster_secret">>);
                Sec -> crypto:hash(sha256, list_to_binary(Sec))
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
    Url = format_ao_url(GatewayUrl, ProcessId, Action),
    Headers = [
        {"content-type", "application/json"},
        {"accept", "application/json"}
    ],
    HttpTimeout = maps:get(timeout, Opts, 8000),
    case httpc:request(post, {Url, Headers, "application/json", EnvelopeJson}, [{timeout, HttpTimeout}], [{body_format, binary}]) of
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
    send_message(GatewayUrl, ProcessId, Action, [], PayloadMap, Opts).

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
        "" -> "http://127.0.0.1:8735/ao/" ++ CleanProc ++ "/" ++ CleanAction;
        Gw -> Gw ++ "/message"
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
