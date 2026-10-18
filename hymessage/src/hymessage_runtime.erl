-module(hymessage_runtime).
-behaviour(gen_server).

-export([start_link/0, ensure_started/0, submit/2, submit/3, get_state/0, reset/0]).
-export([join/1, sync_events/1, replay/1, promote_epoch/1]).
-export([db_put/3, db_put/4, db_get/1, db_del/2, db_del/3, db_snapshot/0, db_restore/0]).
-export([submit_from_req/3, get_state_from_req/3, reset_from_req/3, join_from_req/3, sync_from_req/3, replay_from_req/3, promote_from_req/3]).
-export([put_from_req/3, get_from_req/3, del_from_req/3, exists_from_req/3, schema_from_req/3, scan_from_req/3, snapshot_from_req/3, restore_from_req/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    wasm_base,
    node_msg
}).

-define(TABLE, hymessage_store).
-define(SESSIONS_TABLE, hymessage_sessions).
-define(SERVER, ?MODULE).
-define(GENESIS_HASH, <<"0000000000000000000000000000000000000000000000000000000000000000">>).

ensure_started() ->
    case whereis(?SERVER) of
        undefined ->
            case start_link() of
                {ok, Pid} -> {ok, Pid};
                {error, {already_started, Pid}} -> {ok, Pid};
                Error -> Error
            end;
        Pid -> {ok, Pid}
    end.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

submit(Action, Value) when is_integer(Action), is_integer(Value) ->
    submit(Action, Value, undefined).

submit(Action, Value, Token) when is_integer(Action), is_integer(Value) ->
    ensure_started(),
    gen_server:call(?SERVER, {submit, Action, Value, Token}, 5000).

join(PlayerId) when is_integer(PlayerId) ->
    ensure_started(),
    gen_server:call(?SERVER, {join, PlayerId}, 5000).

get_state() ->
    ensure_started(),
    gen_server:call(?SERVER, get_state, 5000).

sync_events(SinceSeq) when is_integer(SinceSeq) ->
    ensure_started(),
    gen_server:call(?SERVER, {sync_events, SinceSeq}, 5000).

replay(Events) when is_list(Events) ->
    ensure_started(),
    gen_server:call(?SERVER, {replay, Events}, 15000).

promote_epoch(NewEpoch) when is_integer(NewEpoch) ->
    ensure_started(),
    gen_server:call(?SERVER, {promote_epoch, NewEpoch}, 5000).

db_put(Key, Val, Token) ->
    db_put(Key, Val, Token, undefined).

db_put(Key, Val, Token, ReqEpoch) ->
    ensure_started(),
    Opts = #{ <<"token">> => Token, <<"epoch">> => ReqEpoch },
    gen_server:call(?SERVER, {contract_put, <<"default">>, Key, Val, Opts}, 5000).

db_get(Key) ->
    ensure_started(),
    gen_server:call(?SERVER, {contract_get, <<"default">>, Key}, 5000).

db_del(Key, Token) ->
    db_del(Key, Token, undefined).

db_del(Key, Token, ReqEpoch) ->
    ensure_started(),
    Opts = #{ <<"token">> => Token, <<"epoch">> => ReqEpoch },
    gen_server:call(?SERVER, {contract_del, <<"default">>, Key, Opts}, 5000).

db_snapshot() ->
    ensure_started(),
    gen_server:call(?SERVER, db_snapshot, 5000).

db_restore() ->
    ensure_started(),
    gen_server:call(?SERVER, db_restore, 5000).

reset() ->
    ensure_started(),
    gen_server:call(?SERVER, reset, 5000).

%% ============================================================================
%% HTTP / HyperBEAM Device Request Handlers
%% ============================================================================

submit_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Action = parse_int_field(<<"action">>, Req, 1),
    Value = parse_int_field(<<"value">>, Req, 1),
    Token = parse_bin_field(<<"token">>, Req, undefined),
    case submit(Action, Value, Token) of
        {ok, Res} -> Res;
        {error, Reason} ->
            #{ <<"ok">> => false, <<"status">> => 403, <<"error">> => hb_util:bin(Reason) }
    end.

put_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Namespace = parse_bin_field(<<"namespace">>, Req, <<"default">>),
    RawKey = parse_raw_field(<<"key">>, Req, 1),
    RawVal0 = parse_raw_field(<<"val">>, Req, undefined),
    RawVal = case RawVal0 of
        undefined -> parse_raw_field(<<"value">>, Req, 0);
        _ -> RawVal0
    end,
    Token = parse_bin_field(<<"token">>, Req, undefined),
    ReqEpoch = parse_opt_int_field(<<"epoch">>, Req),
    ExpectedVer = parse_opt_int_field(<<"expected_ver">>, Req),
    ValType = parse_bin_field(<<"type">>, Req, undefined),
    BaseOpts = #{
        <<"token">> => Token,
        <<"epoch">> => ReqEpoch,
        <<"expected_ver">> => ExpectedVer
    },
    Opts = case ValType of
        undefined -> BaseOpts;
        _ -> BaseOpts#{ <<"type">> => ValType }
    end,
    case gen_server:call(?SERVER, {contract_put, Namespace, RawKey, RawVal, Opts}, 5000) of
        {ok, Res} -> Res;
        {error, ErrMap} when is_map(ErrMap) -> ErrMap;
        {error, Reason} ->
            #{ <<"ok">> => false, <<"status">> => 403, <<"error">> => hb_util:bin(Reason) }
    end.

get_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Namespace = parse_bin_field(<<"namespace">>, Req, <<"default">>),
    RawKey = parse_raw_field(<<"key">>, Req, 1),
    case gen_server:call(?SERVER, {contract_get, Namespace, RawKey}, 5000) of
        {ok, Res} -> Res;
        {error, ErrMap} when is_map(ErrMap) -> ErrMap;
        {error, Reason} ->
            #{ <<"ok">> => false, <<"status">> => 404, <<"error">> => hb_util:bin(Reason) }
    end.

exists_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Namespace = parse_bin_field(<<"namespace">>, Req, <<"default">>),
    RawKey = parse_raw_field(<<"key">>, Req, 1),
    case gen_server:call(?SERVER, {contract_exists, Namespace, RawKey}, 5000) of
        {ok, Res} -> Res;
        {error, ErrMap} when is_map(ErrMap) -> ErrMap
    end.

del_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Namespace = parse_bin_field(<<"namespace">>, Req, <<"default">>),
    RawKey = parse_raw_field(<<"key">>, Req, 1),
    Token = parse_bin_field(<<"token">>, Req, undefined),
    ReqEpoch = parse_opt_int_field(<<"epoch">>, Req),
    ExpectedVer = parse_opt_int_field(<<"expected_ver">>, Req),
    Opts = #{
        <<"token">> => Token,
        <<"epoch">> => ReqEpoch,
        <<"expected_ver">> => ExpectedVer
    },
    case gen_server:call(?SERVER, {contract_del, Namespace, RawKey, Opts}, 5000) of
        {ok, Res} -> Res;
        {error, ErrMap} when is_map(ErrMap) -> ErrMap;
        {error, Reason} ->
            #{ <<"ok">> => false, <<"status">> => 403, <<"error">> => hb_util:bin(Reason) }
    end.

schema_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Action = parse_bin_field(<<"action">>, Req, <<"list">>),
    case Action of
        <<"lock_admin">> ->
            case verify_admin_req(Req, true) of
                ok ->
                    ets:insert(?TABLE, {admin_locked, true}),
                    #{
                        <<"ok">> => true,
                        <<"error_code">> => <<"OK">>,
                        <<"admin_locked">> => true
                    };
                {error, ErrMap} ->
                    ErrMap
            end;
        <<"register">> ->
            case verify_admin_req(Req, false) of
                {error, ErrMap} ->
                    ErrMap;
                ok ->
                    Namespace = parse_bin_field(<<"namespace">>, Req, <<"custom">>),
                    Backend = parse_bin_field(<<"backend">>, Req, <<"ets_doc">>),
                    SchemaVsn = parse_bin_field(<<"schema_version">>, Req, <<"1.0.0">>),
                    KeyType = parse_bin_field(<<"key_type">>, Req, <<"string">>),
                    ValType = parse_bin_field(<<"value_type">>, Req, <<"any">>),
                    ReqEpoch = parse_opt_int_field(<<"epoch">>, Req),
                    BaseRules = parse_map_field(<<"rules">>, Req, #{}),
                    Rules1 = case parse_opt_int_field(<<"min_value">>, Req) of
                        undefined -> BaseRules;
                        MinV -> BaseRules#{ <<"min_value">> => MinV }
                    end,
                    Rules2 = case parse_opt_int_field(<<"max_value">>, Req) of
                        undefined -> Rules1;
                        MaxV -> Rules1#{ <<"max_value">> => MaxV }
                    end,
                    Rules3 = case parse_opt_int_field(<<"max_records">>, Req) of
                        undefined -> Rules2;
                        MaxRec -> Rules2#{ <<"max_records">> => MaxRec }
                    end,
                    Rules4 = case parse_bin_field(<<"require_auth">>, Req, undefined) of
                        <<"true">> -> Rules3#{ <<"require_auth">> => true };
                        <<"false">> -> Rules3#{ <<"require_auth">> => false };
                        _ -> Rules3
                    end,
                    Cfg0 = #{
                        <<"backend">> => Backend,
                        <<"schema_version">> => SchemaVsn,
                        <<"key_type">> => KeyType,
                        <<"value_type">> => ValType,
                        <<"rules">> => Rules4
                    },
                    UnsupportedCheckKeys = [<<"indexes">>, <<"secondary_indexes">>, <<"query_language">>, <<"migrations">>],
                    Cfg = lists:foldl(fun(K, Acc) ->
                        case maps:find(K, Req) of
                            {ok, V} -> Acc#{ K => V };
                            error -> Acc
                        end
                    end, Cfg0, UnsupportedCheckKeys),
                    case gen_server:call(?SERVER, {contract_register_schema, Namespace, Cfg, ReqEpoch}, 5000) of
                        {ok, Res} -> Res;
                        {error, ErrMap} when is_map(ErrMap) -> ErrMap
                    end
            end;
        <<"load_tx">> ->
            case verify_admin_req(Req, false) of
                {error, ErrMap} ->
                    ErrMap;
                ok ->
                    TxId = parse_bin_field(<<"tx_id">>, Req, <<>>),
                    Gateway = parse_bin_field(<<"gateway">>, Req, <<"http://127.0.0.1:1984">>),
                    ReqEpoch = parse_opt_int_field(<<"epoch">>, Req),
                    case fetch_and_parse_arweave_contract(Gateway, TxId) of
                        {ok, Namespace, Cfg} ->
                            case gen_server:call(?SERVER, {contract_register_schema, Namespace, Cfg, ReqEpoch}, 5000) of
                                {ok, Res} ->
                                    Res#{
                                        <<"loaded_from_arweave">> => true,
                                        <<"contract_tx_id">> => TxId,
                                        <<"wasm_tx_id">> => maps:get(<<"wasm_tx_id">>, Cfg, null)
                                    };
                                {error, ErrMap} when is_map(ErrMap) ->
                                    ErrMap
                            end;
                        {error, ErrMap} when is_map(ErrMap) ->
                            ErrMap
                    end
            end;
        _ ->
            Namespaces = hydb_contract:list_namespaces(),
            #{
                <<"ok">> => true,
                <<"error_code">> => <<"OK">>,
                <<"device">> => <<"hymessage@1.0">>,
                <<"contract_version">> => hydb_contract:contract_version(),
                <<"namespaces">> => Namespaces,
                <<"backends">> => #{
                    <<"wasm_slot">> => hydb_contract:backend_capabilities(<<"wasm_slot">>),
                    <<"ets_doc">> => hydb_contract:backend_capabilities(<<"ets_doc">>)
                }
            }
    end.

is_trusted_arweave_gateway(<<"http://127.0.0.1:1984">>) -> true;
is_trusted_arweave_gateway(<<"http://localhost:1984">>) -> true;
is_trusted_arweave_gateway(<<"https://arweave.net">>) -> true;
is_trusted_arweave_gateway(<<"https://ar-io.dev">>) -> true;
is_trusted_arweave_gateway(_) -> false.

verify_admin_req(Req, ForceRequire) ->
    GivenToken = parse_bin_field(<<"admin_token">>, Req, undefined),
    IsLocked = case ets:lookup(?TABLE, admin_locked) of
        [{admin_locked, true}] -> true;
        _ -> false
    end,
    ExpectedHex = string:uppercase(binary:encode_hex(node_auth_secret())),
    case {GivenToken, ForceRequire orelse IsLocked} of
        {undefined, false} ->
            ok;
        {undefined, true} ->
            {error, #{
                <<"ok">> => false,
                <<"status">> => 403,
                <<"error_code">> => <<"ERR_ADMIN_UNAUTHORIZED">>,
                <<"error">> => <<"Missing operator admin_token for protected administrative action">>
            }};
        {TokenBin, _} when is_binary(TokenBin) ->
            case string:uppercase(TokenBin) =:= ExpectedHex of
                true -> ok;
                false ->
                    {error, #{
                        <<"ok">> => false,
                        <<"status">> => 403,
                        <<"error_code">> => <<"ERR_ADMIN_UNAUTHORIZED">>,
                        <<"error">> => <<"Invalid operator admin_token">>
                    }}
            end
    end.

fetch_and_parse_arweave_contract(_Gateway, <<>>) ->
    {error, #{
        <<"ok">> => false,
        <<"status">> => 400,
        <<"error_code">> => <<"ERR_INVALID_INPUT">>,
        <<"error">> => <<"Missing tx_id for Arweave contract load">>
    }};
fetch_and_parse_arweave_contract(Gateway, TxId) ->
    case is_trusted_arweave_gateway(Gateway) of
        false ->
            {error, #{
                <<"ok">> => false,
                <<"status">> => 403,
                <<"error_code">> => <<"ERR_UNTRUSTED_GATEWAY">>,
                <<"error">> => iolist_to_binary([<<"Untrusted Arweave gateway blocked (SSRF protection): ">>, Gateway])
            }};
        true ->
            _ = inets:start(),
            Url = binary_to_list(<<Gateway/binary, "/", TxId/binary>>),
            case httpc:request(get, {Url, []}, [{timeout, 5000}], [{body_format, binary}]) of
                {ok, {{_, 200, _}, _Headers, BodyBin}} ->
                    try json:decode(BodyBin) of
                        Manifest when is_map(Manifest) ->
                            Namespace = maps:get(<<"namespace">>, Manifest, TxId),
                            SchemaMap = maps:get(<<"schema">>, Manifest, Manifest),
                            Backend = maps:get(<<"backend">>, Manifest, maps:get(<<"backend">>, SchemaMap, <<"wasm_slot">>)),
                            SchemaVsn = maps:get(<<"schema_version">>, Manifest, maps:get(<<"schema_version">>, SchemaMap, <<"1.0.0">>)),
                            KeyType = maps:get(<<"key_type">>, SchemaMap, <<"int64">>),
                            ValType = maps:get(<<"value_type">>, SchemaMap, <<"int64">>),
                            Rules = maps:get(<<"rules">>, SchemaMap, #{}),
                            Policy = maps:get(<<"policy">>, Manifest, #{}),
                            Connector = maps:get(<<"connector">>, Manifest, #{}),
                            WasmTxId = maps:get(<<"wasm_tx_id">>, Manifest, maps:get(<<"wasm_tx_id">>, Connector, undefined)),
                            WasmSha256 = maps:get(<<"wasm_sha256">>, Manifest, maps:get(<<"wasm_sha256">>, Connector, undefined)),
                            case verify_arweave_wasm_connector(Gateway, WasmTxId, WasmSha256) of
                                {ok, MaybeWasmBin} ->
                                    Cfg0 = #{
                                        <<"contract_tx_id">> => TxId,
                                        <<"arweave_gateway">> => Gateway,
                                        <<"backend">> => Backend,
                                        <<"schema_version">> => SchemaVsn,
                                        <<"key_type">> => KeyType,
                                        <<"value_type">> => ValType,
                                        <<"rules">> => Rules,
                                        <<"policy">> => Policy,
                                        <<"connector">> => Connector
                                    },
                                    Cfg1 = case WasmTxId of
                                        undefined -> Cfg0;
                                        _ -> Cfg0#{ <<"wasm_tx_id">> => WasmTxId }
                                    end,
                                    Cfg2 = case WasmSha256 of
                                        undefined -> Cfg1;
                                        _ when is_binary(WasmSha256) -> Cfg1#{ <<"wasm_sha256">> => WasmSha256 };
                                        _ -> Cfg1
                                    end,
                                    Cfg = case MaybeWasmBin of
                                        undefined -> Cfg2;
                                        Bin when is_binary(Bin) -> Cfg2#{ <<"wasm_bin">> => Bin }
                                    end,
                                    {ok, Namespace, Cfg};
                                {error, WasmErr} ->
                                    {error, WasmErr}
                            end;
                        _ ->
                            {error, #{
                                <<"ok">> => false,
                                <<"status">> => 422,
                                <<"error_code">> => <<"ERR_INVALID_CONTRACT_TX">>,
                                <<"error">> => <<"Contract transaction payload is not a JSON object">>
                            }}
                    catch _:_ ->
                        {error, #{
                            <<"ok">> => false,
                            <<"status">> => 422,
                            <<"error_code">> => <<"ERR_INVALID_CONTRACT_TX">>,
                            <<"error">> => <<"Failed to decode Contract JSON from Arweave">>
                        }}
                    end;
                _ ->
                    {error, #{
                        <<"ok">> => false,
                        <<"status">> => 404,
                        <<"error_code">> => <<"ERR_CONTRACT_TX_NOT_FOUND">>,
                        <<"error">> => iolist_to_binary([<<"Contract TX not found on Arweave: ">>, TxId])
                    }}
            end
    end.

verify_arweave_wasm_connector(_Gateway, undefined, _WasmSha256) ->
    {ok, undefined};
verify_arweave_wasm_connector(_Gateway, <<>>, _WasmSha256) ->
    {ok, undefined};
verify_arweave_wasm_connector(Gateway, WasmTxId, ExpectedSha256) when is_binary(WasmTxId) ->
    WasmUrl = binary_to_list(<<Gateway/binary, "/", WasmTxId/binary>>),
    case httpc:request(get, {WasmUrl, []}, [{timeout, 5000}], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _Headers, WasmBin}} when byte_size(WasmBin) >= 4 ->
            case ExpectedSha256 of
                undefined -> {ok, WasmBin};
                <<>> -> {ok, WasmBin};
                ExpHex when is_binary(ExpHex) ->
                    ActualHex = string:uppercase(binary:encode_hex(crypto:hash(sha256, WasmBin))),
                    case ActualHex =:= string:uppercase(ExpHex) of
                        true -> {ok, WasmBin};
                        false ->
                            {error, #{
                                <<"ok">> => false,
                                <<"status">> => 422,
                                <<"error_code">> => <<"ERR_WASM_HASH_MISMATCH">>,
                                <<"error">> => <<"Fetched WASM binary hash does not match contract manifest">>
                            }}
                    end
            end;
        _ ->
            {error, #{
                <<"ok">> => false,
                <<"status">> => 404,
                <<"error_code">> => <<"ERR_WASM_TX_NOT_FOUND">>,
                <<"error">> => iolist_to_binary([<<"Linked WASM TX not found on Arweave: ">>, WasmTxId])
            }}
    end.

scan_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Namespace = parse_bin_field(<<"namespace">>, Req, <<"default">>),
    Prefix = parse_bin_field(<<"prefix">>, Req, <<>>),
    case gen_server:call(?SERVER, {contract_scan, Namespace, Prefix}, 5000) of
        {ok, Res} -> Res;
        {error, ErrMap} when is_map(ErrMap) -> ErrMap
    end.

promote_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    case verify_admin_req(Req, false) of
        {error, ErrMap} ->
            ErrMap;
        ok ->
            NewEpoch = parse_int_field(<<"epoch">>, Req, 1),
            case promote_epoch(NewEpoch) of
                {ok, Res} -> Res;
                {error, Reason} ->
                    #{ <<"ok">> => false, <<"status">> => 409, <<"error">> => hb_util:bin(Reason) }
            end
    end.

snapshot_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    case db_snapshot() of
        {ok, Res} -> Res;
        {error, Reason} ->
            #{ <<"ok">> => false, <<"status">> => 500, <<"error">> => hb_util:bin(Reason) }
    end.

restore_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    case db_restore() of
        {ok, Res} -> Res;
        {error, Reason} ->
            #{ <<"ok">> => false, <<"status">> => 500, <<"error">> => hb_util:bin(Reason) }
    end.

join_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    PlayerId = parse_int_field(<<"player_id">>, Req, 1),
    case join(PlayerId) of
        {ok, Res} -> Res;
        {error, Reason} ->
            #{ <<"ok">> => false, <<"status">> => 400, <<"error">> => hb_util:bin(Reason) }
    end.

get_state_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    {ok, Res} = get_state(),
    Res.

sync_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Since = parse_int_field(<<"since">>, Req, 0),
    {ok, Res} = sync_events(Since),
    Res.

replay_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    case verify_admin_req(Req, false) of
        {error, ErrMap} ->
            ErrMap;
        ok ->
            Events = parse_events_field(Req),
            case replay(Events) of
                {ok, Res} -> Res;
                {error, Reason} ->
                    #{ <<"ok">> => false, <<"status">> => 400, <<"error">> => hb_util:bin(Reason) }
            end
    end.

reset_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    case verify_admin_req(Req, false) of
        {error, ErrMap} ->
            ErrMap;
        ok ->
            ok = reset(),
            #{
                <<"ok">> => true,
                <<"device">> => <<"hymessage@1.0">>,
                <<"contract_version">> => hydb_contract:contract_version(),
                <<"message">> => <<"state reset successfully">>
            }
    end.

%% ============================================================================
%% Field Parsing Utilities
%% ============================================================================

parse_raw_field(Key, Req, Default) when is_map(Req) ->
    case maps:get(Key, Req, not_found) of
        not_found ->
            QueryKey = << "query/", Key/binary >>,
            case maps:get(QueryKey, Req, not_found) of
                not_found ->
                    BodyKey = << "body/", Key/binary >>,
                    case maps:get(BodyKey, Req, not_found) of
                        not_found ->
                            case maps:get(<<"body">>, Req, not_found) of
                                Bin when is_binary(Bin), byte_size(Bin) > 0 ->
                                    try json:decode(Bin) of
                                        Decoded when is_map(Decoded) ->
                                            maps:get(Key, Decoded, Default);
                                        _ -> Default
                                    catch _:_ -> Default
                                    end;
                                Map when is_map(Map) ->
                                    maps:get(Key, Map, Default);
                                _ ->
                                    case hb_ao:get(Key, Req, #{}) of
                                        not_found ->
                                            case hb_ao:get(QueryKey, Req, #{}) of
                                                not_found ->
                                                    case hb_ao:get(BodyKey, Req, #{}) of
                                                        not_found -> Default;
                                                        V3 -> V3
                                                    end;
                                                V2 -> V2
                                            end;
                                        V1 -> V1
                                    end
                            end;
                        VB -> VB
                    end;
                VQ -> VQ
            end;
        VK -> VK
    end;
parse_raw_field(_Key, _Req, Default) ->
    Default.

parse_map_field(Key, Req, Default) ->
    Raw = parse_raw_field(Key, Req, Default),
    if is_map(Raw) -> Raw;
       is_binary(Raw) ->
           try json:decode(Raw) of
               M when is_map(M) -> M;
               _ -> Default
           catch _:_ -> Default
           end;
       true -> Default
    end.

parse_opt_int_field(Key, Req) ->
    case parse_int_field(Key, Req, undefined) of
        undefined -> undefined;
        V when is_integer(V) -> V
    end.

parse_int_field(Key, Req, Default) when is_map(Req) ->
    Val = parse_raw_field(Key, Req, Default),
    to_integer(Val, Default);
parse_int_field(_Key, _Req, Default) ->
    Default.

parse_bin_field(Key, Req, Default) when is_map(Req) ->
    Val = parse_raw_field(Key, Req, Default),
    if is_binary(Val) -> Val;
       is_atom(Val) andalso Val =/= undefined -> atom_to_binary(Val, utf8);
       is_integer(Val) -> integer_to_binary(Val);
       true -> Default
    end;
parse_bin_field(_Key, _Req, Default) ->
    Default.

to_integer(undefined, Default) -> Default;
to_integer(V, _Default) when is_integer(V) -> V;
to_integer(V, Default) when is_binary(V) ->
    try binary_to_integer(V)
    catch _:_ -> Default
    end;
to_integer(V, Default) when is_list(V) ->
    try list_to_integer(V)
    catch _:_ -> Default
    end;
to_integer(_, Default) -> Default.

parse_events_field(Req) when is_map(Req) ->
    case hb_ao:get(<<"events">>, Req, #{}) of
        not_found ->
            case hb_ao:get(<<"body">>, Req, #{}) of
                Bin when is_binary(Bin), byte_size(Bin) > 0 ->
                    try json:decode(Bin) of
                        Decoded when is_map(Decoded) ->
                            maps:get(<<"events">>, Decoded, []);
                        _ -> []
                    catch _:_ -> []
                    end;
                Map when is_map(Map) ->
                    maps:get(<<"events">>, Map, []);
                _ -> []
            end;
        EvList when is_list(EvList) -> EvList;
        _ -> []
    end;
parse_events_field(_) -> [].

%% ============================================================================
%% GenServer Lifecycle & ETS Setup
%% ============================================================================

init([]) ->
    init_ets(),
    hydb_contract:init_tables(),
    start_stream_hub(),
    NodeMsg = get_node_msg(),
    WasmBase = init_wasm(NodeMsg),
    {ok, #state{wasm_base = WasmBase, node_msg = NodeMsg}}.

init_ets() ->
    case ets:info(?TABLE) of
        undefined ->
            ets:new(?TABLE, [public, named_table, set]),
            ets:insert(?TABLE, {epoch, 1}),
            ets:insert(?TABLE, {seq, 0}),
            ets:insert(?TABLE, {latest_hash, ?GENESIS_HASH}),
            ets:insert(?TABLE, {state_root, 0}),
            ets:insert(?TABLE, {wasm_root, 0}),
            ets:insert(?TABLE, {doc_root, 0}),
            ets:insert(?TABLE, {history, []}),
            ets:insert(?TABLE, {last_su_ack, 0}),
            ets:insert(?TABLE, {unauthorized_tcp_rejections, 0}),
            ets:insert(?TABLE, {last_snapshot_bytes, 0});
        _ -> ok
    end,
    case ets:info(?SESSIONS_TABLE) of
        undefined ->
            ets:new(?SESSIONS_TABLE, [public, named_table, set]);
        _ -> ok
    end,
    case ets:info(hymessage_subscribers) of
        undefined ->
            ets:new(hymessage_subscribers, [public, named_table, set]);
        _ -> ok
    end.

get_node_msg() ->
    try
        Wallet = hb:wallet(),
        ServerID = hb_util:human_id(ar_wallet:to_address(Wallet)),
        cowboy:get_env(ServerID, node_msg, #{})
    catch
        _:_ -> #{}
    end.

wrap_fast_wasm({fast_wasm, _Pid, _Base} = Fast, _NodeMsg) ->
    Fast;
wrap_fast_wasm(Base, NodeMsg) when is_map(Base) ->
    case catch dev_wasm:instance(Base, #{}, NodeMsg) of
        Pid when is_pid(Pid) -> {fast_wasm, Pid, Base};
        _ -> Base
    end;
wrap_fast_wasm(Other, _NodeMsg) ->
    Other.

init_wasm(NodeMsg) ->
    WasmBinFile = "/home/weslur/database/hydb/out/hydb.wasm",
    case file:read_file(WasmBinFile) of
        {ok, WasmBin} when byte_size(WasmBin) >= 4 ->
            WasmMsg = #{ <<"device">> => <<"wasm-64@1.0">>, <<"body">> => WasmBin },
            case hb_ao:resolve(WasmMsg, <<"init">>, NodeMsg) of
                {ok, Base} -> wrap_fast_wasm(Base, NodeMsg);
                _ -> init_wasm_from_id(NodeMsg)
            end;
        _ ->
            init_wasm_from_id(NodeMsg)
    end.

init_wasm_from_id(NodeMsg) ->
    WasmIdFile = "/home/weslur/database/hydb/out/wasm_id.txt",
    case file:read_file(WasmIdFile) of
        {ok, RawId} ->
            WasmID = string:trim(RawId),
            WasmMsg = #{ <<"device">> => <<"wasm-64@1.0">>, <<"image">> => WasmID },
            case hb_ao:resolve(WasmMsg, <<"init">>, NodeMsg) of
                {ok, Base} -> wrap_fast_wasm(Base, NodeMsg);
                _ -> undefined
            end;
        _ -> undefined
    end.

node_auth_secret() ->
    case ets:info(?TABLE) =/= undefined andalso ets:lookup(?TABLE, cached_auth_secret) of
        [{cached_auth_secret, Sec}] ->
            Sec;
        _ ->
            Sec = case file:read_file("/home/weslur/database/secrets/wallet.json") of
                {ok, Bin} -> crypto:hash(sha256, Bin);
                _ -> crypto:hash(sha256, <<"HYDB_DEFAULT_CLUSTER_SECRET">>)
            end,
            case ets:info(?TABLE) of
                undefined -> ok;
                _ -> ets:insert(?TABLE, {cached_auth_secret, Sec})
            end,
            Sec
    end.

start_stream_hub() ->
    case whereis(hymessage_stream_hub) of
        undefined ->
            Pid = spawn_link(fun() -> stream_hub_listen(8736) end),
            register(hymessage_stream_hub, Pid);
        _ -> ok
    end.

stream_hub_listen(Port) ->
    case gen_tcp:listen(Port, [binary, {packet, 4}, {active, true}, {reuseaddr, true}, {nodelay, true}]) of
        {ok, ListenSock} ->
            stream_hub_accept(ListenSock);
        {error, _Reason} ->
            ok
    end.

stream_hub_accept(ListenSock) ->
    case gen_tcp:accept(ListenSock) of
        {ok, Socket} ->
            inet:setopts(Socket, [{nodelay, true}]),
            Nonce = binary:encode_hex(crypto:strong_rand_bytes(16)),
            Worker = spawn(fun() -> stream_socket_authenticate(Socket, Nonce) end),
            gen_tcp:controlling_process(Socket, Worker),
            ChallengePayload = iolist_to_binary(json:encode(#{
                <<"type">> => <<"AUTH_CHALLENGE">>,
                <<"nonce">> => Nonce
            })),
            _ = gen_tcp:send(Socket, ChallengePayload),
            stream_hub_accept(ListenSock);
        {error, closed} ->
            ok;
        {error, _} ->
            timer:sleep(50),
            stream_hub_accept(ListenSock)
    end.

stream_socket_authenticate(Socket, Nonce) ->
    receive
        {tcp, Socket, Bin} when is_binary(Bin) ->
            case verify_su_handshake(Bin, Nonce) of
                {ok, _NodeId} ->
                    ets:insert(hymessage_subscribers, {Socket, os:system_time(millisecond), 0}),
                    AckPayload = iolist_to_binary(json:encode(#{
                        <<"type">> => <<"AUTH_OK">>,
                        <<"status">> => <<"AUTHENTICATED">>
                    })),
                    _ = gen_tcp:send(Socket, AckPayload),
                    stream_socket_worker(Socket);
                error ->
                    ets:update_counter(?TABLE, unauthorized_tcp_rejections, 1, {unauthorized_tcp_rejections, 0}),
                    RejPayload = iolist_to_binary(json:encode(#{
                        <<"type">> => <<"AUTH_REJECTED">>,
                        <<"error_code">> => <<"ERR_NODE_AUTH_FAILED">>
                    })),
                    _ = gen_tcp:send(Socket, RejPayload),
                    gen_tcp:close(Socket)
            end;
        {tcp_closed, Socket} ->
            ok;
        {tcp_error, Socket, _} ->
            gen_tcp:close(Socket)
    after 3000 ->
        ets:update_counter(?TABLE, unauthorized_tcp_rejections, 1, {unauthorized_tcp_rejections, 0}),
        gen_tcp:close(Socket)
    end.

verify_su_handshake(Bin, Nonce) ->
    try json:decode(Bin) of
        #{ <<"type">> := <<"AUTH_RESPONSE">>, <<"node_id">> := NodeId, <<"sig">> := GivenSig }
          when is_binary(NodeId), is_binary(GivenSig) ->
            Msg = <<Nonce/binary, ":", NodeId/binary>>,
            ExpectedSig = binary:encode_hex(crypto:mac(hmac, sha256, node_auth_secret(), Msg)),
            case string:uppercase(GivenSig) =:= string:uppercase(ExpectedSig) of
                true -> {ok, NodeId};
                false -> error
            end;
        _ ->
            error
    catch _:_ ->
        error
    end.

stream_socket_worker(Socket) ->
    receive
        {tcp, Socket, <<AckSeq:32/big>>} ->
            ets:insert(?TABLE, {last_su_ack, AckSeq}),
            update_subscriber_ack(Socket, AckSeq),
            case whereis(?SERVER) of
                undefined -> ok;
                ServerPid -> ServerPid ! {su_ack, AckSeq}
            end,
            stream_socket_worker(Socket);
        {tcp, Socket, Bin} when is_binary(Bin) ->
            try json:decode(Bin) of
                #{<<"ack_seq">> := AckSeq0} ->
                    AckSeq = to_integer(AckSeq0, 0),
                    ets:insert(?TABLE, {last_su_ack, AckSeq}),
                    update_subscriber_ack(Socket, AckSeq),
                    case whereis(?SERVER) of
                        undefined -> ok;
                        ServerPid -> ServerPid ! {su_ack, AckSeq}
                    end;
                _ -> ok
            catch _:_ -> ok
            end,
            stream_socket_worker(Socket);
        {tcp_closed, Socket} ->
            ets:delete(hymessage_subscribers, Socket);
        {tcp_error, Socket, _} ->
            ets:delete(hymessage_subscribers, Socket)
    end.

update_subscriber_ack(Socket, AckSeq) ->
    case ets:lookup(hymessage_subscribers, Socket) of
        [{Socket, ConnAt, _OldAck}] ->
            ets:insert(hymessage_subscribers, {Socket, ConnAt, AckSeq});
        _ ->
            ets:insert(hymessage_subscribers, {Socket, os:system_time(millisecond), AckSeq})
    end.

broadcast_and_await_su(Event, Seq) ->
    case ets:tab2list(hymessage_subscribers) of
        [] ->
            {false, 0};
        Subscribers ->
            Payload = try json:encode(Event) catch _:_ -> <<>> end,
            ValidSubscribers = lists:filter(fun(SubTuple) ->
                Sock = element(1, SubTuple),
                case catch gen_tcp:send(Sock, Payload) of
                    ok -> true;
                    _ ->
                        ets:delete(hymessage_subscribers, Sock),
                        false
                end
            end, Subscribers),
            if ValidSubscribers == [] ->
                {false, 0};
               true ->
                Awaited = await_su_ack(Seq, 50),
                {Awaited, length(ValidSubscribers)}
            end
    end.

await_su_ack(Seq, TimeoutMs) ->
    T0 = os:perf_counter(millisecond),
    await_su_ack_loop(Seq, T0, TimeoutMs).

await_su_ack_loop(Seq, T0, TimeoutMs) ->
    case ets:lookup(?TABLE, last_su_ack) of
        [{last_su_ack, AckSeq}] when AckSeq >= Seq ->
            true;
        _ ->
            receive
                {su_ack, AckSeq} when AckSeq >= Seq ->
                    true;
                {su_ack, _OldAckSeq} ->
                    await_su_ack_loop(Seq, T0, TimeoutMs)
            after 1 ->
                Now = os:perf_counter(millisecond),
                if (Now - T0) >= TimeoutMs -> false;
                   true -> await_su_ack_loop(Seq, T0, TimeoutMs)
                end
            end
    end.

fast_beamr_call(Pid, Func, Params, Default) ->
    FuncList = if is_binary(Func) -> binary_to_list(Func); is_list(Func) -> Func end,
    Pid ! {wasm_send, self(), {command, term_to_binary({call, FuncList, Params})}},
    receive
        {execution_result, [Out | _]} -> Out;
        {execution_result, Out} when is_integer(Out) -> Out;
        {error, _} -> Default
    after 2000 ->
        Default
    end.

wasm_call(undefined, _Func, _Params, _NodeMsg, Default) ->
    Default;
wasm_call({fast_wasm, Pid, _Base}, Func, Params, _NodeMsg, Default) when is_pid(Pid) ->
    fast_beamr_call(Pid, Func, Params, Default);
wasm_call(WasmBase, Func, Params, NodeMsg, Default) when is_map(WasmBase) ->
    case wrap_fast_wasm(WasmBase, NodeMsg) of
        {fast_wasm, Pid, _} ->
            fast_beamr_call(Pid, Func, Params, Default);
        _ ->
            Req = hb_maps:merge(WasmBase, #{
                <<"function">> => Func,
                <<"parameters">> => Params
            }, NodeMsg),
            case hb_ao:resolve(Req, <<"compute">>, NodeMsg) of
                {ok, WasmRes} ->
                    case hb_ao:get(<<"results/output">>, WasmRes, NodeMsg) of
                        [Out] -> Out;
                        Out when is_integer(Out) -> Out;
                        _ -> Default
                    end;
                _ -> Default
            end
    end.

check_epoch(undefined, CurrentEpoch) -> {ok, CurrentEpoch};
check_epoch(ReqEpoch, CurrentEpoch) when is_integer(ReqEpoch) ->
    if ReqEpoch < CurrentEpoch -> {error, fenced_stale_epoch};
       true -> {ok, CurrentEpoch}
    end.

combine_roots(WasmRoot, DocRoot) ->
    Mask64 = 16#FFFFFFFFFFFFFFFF,
    CombinedU = ((WasmRoot band Mask64) bxor (DocRoot band Mask64)) band Mask64,
    if CombinedU >= 16#8000000000000000 -> CombinedU - 16#10000000000000000;
       true -> CombinedU
    end.

boot_arweave_wasm_for_namespace(Namespace, Cfg, NodeMsg) ->
    MaybeBin = case maps:get(<<"wasm_bin">>, Cfg, undefined) of
        Bin when is_binary(Bin), byte_size(Bin) >= 4 ->
            Bin;
        _ ->
            case maps:get(<<"wasm_tx_id">>, Cfg, undefined) of
                TxId when is_binary(TxId), byte_size(TxId) > 0 ->
                    Gw = maps:get(<<"arweave_gateway">>, Cfg, <<"http://127.0.0.1:1984">>),
                    SafeGw = case is_trusted_arweave_gateway(Gw) of
                        true -> Gw;
                        false -> <<"http://127.0.0.1:1984">>
                    end,
                    _ = inets:start(),
                    WasmUrl = binary_to_list(<<SafeGw/binary, "/", TxId/binary>>),
                    case httpc:request(get, {WasmUrl, []}, [{timeout, 5000}], [{body_format, binary}]) of
                        {ok, {{_, 200, _}, _H, DownloadedBin}} when byte_size(DownloadedBin) >= 4 ->
                            DownloadedBin;
                        _ ->
                            undefined
                    end;
                _ ->
                    undefined
            end
    end,
    case MaybeBin of
        WasmBin when is_binary(WasmBin) ->
            WasmMsg = #{ <<"device">> => <<"wasm-64@1.0">>, <<"body">> => WasmBin },
            case hb_ao:resolve(WasmMsg, <<"init">>, NodeMsg) of
                {ok, NsWasmBase0} ->
                    NsWasmBase = wrap_fast_wasm(NsWasmBase0, NodeMsg),
                    ets:insert(?TABLE, {{ns_wasm_base, Namespace}, NsWasmBase}),
                    ets:insert(?TABLE, {{ns_wasm_root, Namespace}, 0}),
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end.

resolve_ns_wasm(Namespace, DefaultWasmBase) ->
    case ets:lookup(?TABLE, {ns_wasm_base, Namespace}) of
        [{{ns_wasm_base, Namespace}, CustomBase}] when CustomBase =/= undefined ->
            {CustomBase, true};
        _ ->
            {DefaultWasmBase, false}
    end.

apply_wasm_root_delta(Namespace, true, NewInstanceRoot, TotalWasmRoot0) ->
    OldNsRoot = case ets:lookup(?TABLE, {ns_wasm_root, Namespace}) of
        [{{ns_wasm_root, Namespace}, R}] -> R;
        _ -> 0
    end,
    ets:insert(?TABLE, {{ns_wasm_root, Namespace}, NewInstanceRoot}),
    combine_roots(combine_roots(TotalWasmRoot0, OldNsRoot), NewInstanceRoot);
apply_wasm_root_delta(_Namespace, false, NewInstanceRoot, TotalWasmRoot0) ->
    OldDefRoot = case ets:lookup(?TABLE, default_wasm_root) of
        [{default_wasm_root, R}] -> R;
        _ -> TotalWasmRoot0
    end,
    ets:insert(?TABLE, {default_wasm_root, NewInstanceRoot}),
    combine_roots(combine_roots(TotalWasmRoot0, OldDefRoot), NewInstanceRoot).

build_event_hash(PrevHash, Seq, OpCode, Namespace, Key, ValType, CanonicalValBin) ->
    KeyBin = if is_integer(Key) -> integer_to_binary(Key);
                is_binary(Key) -> Key
             end,
    Payload = case {Namespace, OpCode, ValType} of
        {_, <<"24">>, _} ->
            [PrevHash, integer_to_binary(Seq), <<"24">>, Namespace, <<":">>, CanonicalValBin];
        {<<"default">>, <<"20">>, <<"int64">>} ->
            [PrevHash, integer_to_binary(Seq), <<"20">>, KeyBin, CanonicalValBin];
        {<<"default">>, <<"22">>, _} ->
            [PrevHash, integer_to_binary(Seq), <<"22">>, KeyBin];
        {_, <<"22">>, _} ->
            [PrevHash, integer_to_binary(Seq), <<"22">>, Namespace, <<":">>, KeyBin];
        {_, <<"20">>, _} ->
            [PrevHash, integer_to_binary(Seq), <<"20">>, Namespace, <<":">>, KeyBin, <<":">>, CanonicalValBin]
    end,
    binary:encode_hex(crypto:hash(sha256, Payload)).

%% ============================================================================
%% GenServer Callbacks
%% ============================================================================

handle_call({contract_register_schema, Namespace, Cfg, ReqEpoch}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    CurrentEpoch = case ets:lookup(?TABLE, epoch) of [{epoch, Ep}] -> Ep; _ -> 1 end,
    case check_epoch(ReqEpoch, CurrentEpoch) of
        {error, fenced_stale_epoch} ->
            {reply, {error, #{
                <<"ok">> => false,
                <<"status">> => 409,
                <<"error_code">> => <<"FENCED_STALE_EPOCH">>,
                <<"verdict">> => <<"FENCED_STALE_EPOCH">>,
                <<"error">> => <<"fenced_stale_epoch">>
            }}, State};
        {ok, Epoch} ->
            CleanCfg = maps:remove(<<"wasm_bin">>, Cfg),
            case hydb_contract:validate_schema_definition(Namespace, CleanCfg) of
                {error, SchemaErrCode, SchemaStatus, SchemaReason} ->
                    {reply, {error, #{
                        <<"ok">> => false,
                        <<"status">> => SchemaStatus,
                        <<"error_code">> => SchemaErrCode,
                        <<"error">> => SchemaReason,
                        <<"namespace">> => Namespace,
                        <<"contract_version">> => hydb_contract:contract_version()
                    }}, State};
                ok ->
                    WasmBooted = boot_arweave_wasm_for_namespace(Namespace, Cfg, NodeMsg),
                    {ok, Entry} = hydb_contract:register_namespace(Namespace, CleanCfg),
                    {ActiveWasmBase, _IsCustomWasm} = resolve_ns_wasm(Namespace, WasmBase),
                    case hydb_contract:configure_wasm_contract(Namespace, Entry, {fun wasm_call/5, ActiveWasmBase, NodeMsg}) of
                        {error, WasmErrCode, WasmStatus, WasmReason} ->
                            hydb_contract:unregister_namespace(Namespace),
                            {reply, {error, #{
                                <<"ok">> => false,
                                <<"status">> => WasmStatus,
                                <<"error_code">> => WasmErrCode,
                                <<"error">> => WasmReason,
                                <<"namespace">> => Namespace,
                                <<"contract_version">> => hydb_contract:contract_version()
                            }}, State};
                        ok ->
                            CanonicalSchemaBin = hydb_contract:canonical_encode_schema(Entry),
                            Seq = ets:update_counter(?TABLE, seq, 1),
                            [{latest_hash, PrevHash}] = ets:lookup(?TABLE, latest_hash),
                            [{state_root, StateRoot}] = ets:lookup(?TABLE, state_root),
                            NewHash = build_event_hash(PrevHash, Seq, <<"24">>, Namespace, <<"__schema__">>, <<"schema">>, CanonicalSchemaBin),
                            ets:insert(?TABLE, {latest_hash, NewHash}),

                            Backend = maps:get(<<"backend">>, Entry),
                            SchemaVsn = maps:get(<<"schema_version">>, Entry),
                            [{history, Hist}] = ets:lookup(?TABLE, history),
                            Event = #{
                                <<"contract_version">> => hydb_contract:contract_version(),
                                <<"namespace">> => Namespace,
                                <<"schema_version">> => SchemaVsn,
                                <<"backend">> => Backend,
                                <<"epoch">> => Epoch,
                                <<"seq">> => Seq,
                                <<"action">> => <<"register_schema">>,
                                <<"key">> => <<"__schema__">>,
                                <<"val">> => Entry,
                                <<"val_type">> => <<"schema">>,
                                <<"canonical_val">> => CanonicalSchemaBin,
                                <<"ver">> => 1,
                                <<"prev_hash">> => PrevHash,
                                <<"hash">> => NewHash,
                                <<"state_root">> => StateRoot
                            },
                            NewHist = [Event | lists:sublist(Hist, 49)],
                            ets:insert(?TABLE, {history, NewHist}),
                            {Replicated, SuCount} = broadcast_and_await_su(Event, Seq),
                            case Replicated of
                                true -> hydb_contract:update_namespace_status(Namespace, <<"replicated">>);
                                false -> ok
                            end,
                            Result = #{
                                <<"ok">> => true,
                                <<"error_code">> => <<"OK">>,
                                <<"device">> => <<"hymessage@1.0">>,
                                <<"contract_version">> => hydb_contract:contract_version(),
                                <<"epoch">> => Epoch,
                                <<"seq">> => Seq,
                                <<"op">> => <<"register_schema">>,
                                <<"namespace">> => Namespace,
                                <<"status">> => case Replicated of true -> <<"replicated">>; false -> <<"configured">> end,
                                <<"contract_status">> => case Replicated of true -> <<"replicated">>; false -> <<"configured">> end,
                                <<"registered_schema">> => Entry,
                                <<"canonical_schema">> => CanonicalSchemaBin,
                                <<"wasm_vm_booted_from_arweave">> => WasmBooted,
                                <<"wasm_contract_configured">> => (Backend =:= <<"wasm_slot">>),
                                <<"capabilities">> => hydb_contract:backend_capabilities(Backend),
                                <<"hash">> => NewHash,
                                <<"state_root">> => StateRoot,
                                <<"replicated">> => Replicated,
                                <<"su_count">> => SuCount
                            },
                            {reply, {ok, Result}, State}
                    end
            end
    end;

handle_call({promote_epoch, NewEpoch}, _From, State) ->
    CurrentEpoch = case ets:lookup(?TABLE, epoch) of
        [{epoch, E}] -> E;
        _ -> 1
    end,
    if NewEpoch =< CurrentEpoch ->
        {reply, {error, invalid_epoch_transition}, State};
       true ->
        ets:insert(?TABLE, {epoch, NewEpoch}),
        [{seq, Seq}] = ets:lookup(?TABLE, seq),
        [{state_root, Root}] = ets:lookup(?TABLE, state_root),
        Result = #{
            <<"ok">> => true,
            <<"device">> => <<"hymessage@1.0">>,
            <<"previous_epoch">> => CurrentEpoch,
            <<"epoch">> => NewEpoch,
            <<"seq">> => Seq,
            <<"state_root">> => Root,
            <<"status">> => <<"PROMOTED">>
        },
        {reply, {ok, Result}, State}
    end;

handle_call({join, PlayerId}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    Token = binary:encode_hex(crypto:strong_rand_bytes(16)),
    ets:insert(?SESSIONS_TABLE, {Token, PlayerId, os:system_time(second)}),
    WasmRes = wasm_call(WasmBase, <<"db_count">>, [], NodeMsg, 0),
    Result = #{
        <<"ok">> => true,
        <<"device">> => <<"hymessage@1.0">>,
        <<"session_token">> => Token,
        <<"player_id">> => PlayerId,
        <<"records_count">> => WasmRes
    },
    {reply, {ok, Result}, State};

handle_call({contract_put, Namespace, RawKey, RawVal, Opts}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    T0 = os:perf_counter(nanosecond),
    T_start = os:perf_counter(microsecond),
    ReqEpoch = maps:get(<<"epoch">>, Opts, undefined),
    Token = maps:get(<<"token">>, Opts, undefined),
    CurrentEpoch = case ets:lookup(?TABLE, epoch) of [{epoch, Ep}] -> Ep; _ -> 1 end,

    case check_epoch(ReqEpoch, CurrentEpoch) of
        {error, fenced_stale_epoch} ->
            {reply, {error, #{
                <<"ok">> => false,
                <<"status">> => 409,
                <<"error_code">> => <<"FENCED_STALE_EPOCH">>,
                <<"verdict">> => <<"FENCED_STALE_EPOCH">>,
                <<"error">> => <<"fenced_stale_epoch">>
            }}, State};
        {ok, Epoch} ->
            SessionCheck = case Token of
                undefined -> {ok, false};
                _ ->
                    case ets:lookup(?SESSIONS_TABLE, Token) of
                        [{Token, _, _}] -> {ok, true};
                        [] -> invalid_token
                    end
            end,
            case SessionCheck of
                invalid_token ->
                    {reply, {error, #{
                        <<"ok">> => false,
                        <<"status">> => 403,
                        <<"error_code">> => <<"ERR_UNAUTHORIZED">>,
                        <<"error">> => <<"invalid_session_token">>
                    }}, State};
                {ok, HasValidSession} ->
                    {ActiveWasmBase, IsCustomWasm} = resolve_ns_wasm(Namespace, WasmBase),
                    WasmCtx = {fun wasm_call/5, ActiveWasmBase, NodeMsg},
                    OptsWithSession = Opts#{ <<"has_valid_session">> => HasValidSession },
                    %% GUARDRAIL A: Pre-Sequence Contract & Schema Validation BEFORE allocating Seq!
                    case hydb_contract:validate_pre_sequence(Namespace, <<"put">>, RawKey, RawVal, OptsWithSession, WasmCtx) of
                        {error, ErrCode, HttpStatus, ReasonBin} ->
                            {reply, {error, #{
                                <<"ok">> => false,
                                <<"status">> => HttpStatus,
                                <<"error_code">> => ErrCode,
                                <<"error">> => ReasonBin,
                                <<"namespace">> => Namespace,
                                <<"contract_version">> => hydb_contract:contract_version()
                            }}, State};
                        {ok, Validated} ->
                            T_val = os:perf_counter(microsecond),
                            Backend = maps:get(<<"backend">>, Validated),
                            SchemaVsn = maps:get(<<"schema_version">>, Validated),
                            NormKey = maps:get(<<"key">>, Validated),
                            NormVal = maps:get(<<"val">>, Validated),
                            ValType = maps:get(<<"val_type">>, Validated),
                            CanonicalValBin = maps:get(<<"canonical_val">>, Validated),

                            %% Allocate sequence number ONLY after passing contract validation
                            Seq = ets:update_counter(?TABLE, seq, 1),
                            [{latest_hash, PrevHash}] = ets:lookup(?TABLE, latest_hash),
                            WasmRoot0 = case ets:lookup(?TABLE, wasm_root) of [{wasm_root, WR}] -> WR; _ -> 0 end,
                            DocRoot0 = case ets:lookup(?TABLE, doc_root) of [{doc_root, DR}] -> DR; _ -> 0 end,

                            NewHash = build_event_hash(PrevHash, Seq, <<"20">>, Namespace, NormKey, ValType, CanonicalValBin),

                            {WasmRoot1, DocRoot1, Ver} = case Backend of
                                <<"wasm_slot">> ->
                                    {ok, NewInstanceWR, V} = hydb_contract:execute_backend_put(<<"wasm_slot">>, Namespace, NormKey, NormVal, Seq, WasmRoot0, WasmCtx),
                                    CombinedWR = apply_wasm_root_delta(Namespace, IsCustomWasm, NewInstanceWR, WasmRoot0),
                                    ets:insert(?TABLE, {wasm_root, CombinedWR}),
                                    {CombinedWR, DocRoot0, V};
                                <<"ets_doc">> ->
                                    {ok, NewDR, V} = hydb_contract:execute_backend_put(<<"ets_doc">>, Namespace, NormKey, NormVal, Seq, DocRoot0, WasmCtx),
                                    ets:insert(?TABLE, {doc_root, NewDR}),
                                    {WasmRoot0, NewDR, V}
                            end,
                            T_wasm = os:perf_counter(microsecond),
                            StateRoot = combine_roots(WasmRoot1, DocRoot1),
                            ets:insert(?TABLE, {latest_hash, NewHash}),
                            ets:insert(?TABLE, {state_root, StateRoot}),

                            [{history, Hist}] = ets:lookup(?TABLE, history),
                            Event = #{
                                <<"contract_version">> => hydb_contract:contract_version(),
                                <<"namespace">> => Namespace,
                                <<"schema_version">> => SchemaVsn,
                                <<"backend">> => Backend,
                                <<"epoch">> => Epoch,
                                <<"seq">> => Seq,
                                <<"action">> => <<"put">>,
                                <<"key">> => NormKey,
                                <<"val">> => NormVal,
                                <<"val_type">> => ValType,
                                <<"canonical_val">> => CanonicalValBin,
                                <<"ver">> => Ver,
                                <<"prev_hash">> => PrevHash,
                                <<"hash">> => NewHash,
                                <<"state_root">> => StateRoot
                            },
                            NewHist = [Event | lists:sublist(Hist, 49)],
                            ets:insert(?TABLE, {history, NewHist}),
                            T_hash = os:perf_counter(microsecond),

                            {Replicated, SuCount} = broadcast_and_await_su(Event, Seq),
                            T_repl = os:perf_counter(microsecond),
                            T1 = os:perf_counter(nanosecond),
                            DurationMs = ((T1 - T0) / 1000.0) / 1000.0,
                            WasmSource = case IsCustomWasm of true -> <<"arweave_tx">>; false -> <<"node_default">> end,
                            WasmInstanceCount = case {Backend, IsCustomWasm} of
                                {<<"wasm_slot">>, true} -> wasm_call(ActiveWasmBase, <<"db_count">>, [], NodeMsg, 0);
                                _ -> 0
                            end,

                            Result = #{
                                <<"ok">> => true,
                                <<"error_code">> => <<"OK">>,
                                <<"device">> => <<"hymessage@1.0">>,
                                <<"contract_version">> => hydb_contract:contract_version(),
                                <<"namespace">> => Namespace,
                                <<"schema_version">> => SchemaVsn,
                                <<"backend">> => Backend,
                                <<"wasm_source">> => WasmSource,
                                <<"wasm_instance_records">> => WasmInstanceCount,
                                <<"epoch">> => Epoch,
                                <<"seq">> => Seq,
                                <<"op">> => <<"put">>,
                                <<"key">> => NormKey,
                                <<"val">> => NormVal,
                                <<"val_type">> => ValType,
                                <<"ver">> => Ver,
                                <<"state_root">> => StateRoot,
                                <<"hash">> => NewHash,
                                <<"replicated">> => Replicated,
                                <<"su_count">> => SuCount,
                                <<"duration_ms">> => DurationMs,
                                <<"t_val_us">> => (T_val - T_start),
                                <<"t_wasm_us">> => (T_wasm - T_val),
                                <<"t_hash_us">> => (T_hash - T_wasm),
                                <<"t_repl_us">> => (T_repl - T_hash)
                            },
                            {reply, {ok, Result}, State}
                    end
            end
    end;

handle_call({contract_get, Namespace, RawKey}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    T0 = os:perf_counter(nanosecond),
    case hydb_contract:get_namespace_config(Namespace) of
        {error, not_found} ->
            {reply, {error, #{
                <<"ok">> => false,
                <<"status">> => 404,
                <<"error_code">> => <<"ERR_NAMESPACE_UNKNOWN">>,
                <<"namespace">> => Namespace,
                <<"error">> => iolist_to_binary([<<"Namespace '">>, Namespace, <<"' is not registered">>])
            }}, State};
        {ok, Cfg} ->
            Backend = maps:get(<<"backend">>, Cfg, <<"wasm_slot">>),
            Epoch = case ets:lookup(?TABLE, epoch) of [{epoch, Ep}] -> Ep; _ -> 1 end,
            {ActiveWasmBase, IsCustomWasm} = resolve_ns_wasm(Namespace, WasmBase),
            WasmCtx = {fun wasm_call/5, ActiveWasmBase, NodeMsg},
            WasmSource = case IsCustomWasm of true -> <<"arweave_tx">>; false -> <<"node_default">> end,

            case Backend of
                <<"wasm_slot">> ->
                    NormKey = to_integer(RawKey, undefined),
                    if NormKey =:= undefined ->
                        {reply, {error, #{
                            <<"ok">> => false,
                            <<"status">> => 400,
                            <<"error_code">> => <<"ERR_INVALID_INPUT">>,
                            <<"namespace">> => Namespace,
                            <<"error">> => <<"Key must be int64 for wasm_slot backend">>
                        }}, State};
                       true ->
                        case hydb_contract:execute_backend_get(<<"wasm_slot">>, Namespace, NormKey, WasmCtx) of
                            {ok, #{ <<"val">> := Val, <<"ver">> := Ver }} ->
                                T1 = os:perf_counter(nanosecond),
                                {reply, {ok, #{
                                    <<"ok">> => true,
                                    <<"error_code">> => <<"OK">>,
                                    <<"device">> => <<"hymessage@1.0">>,
                                    <<"contract_version">> => hydb_contract:contract_version(),
                                    <<"namespace">> => Namespace,
                                    <<"backend">> => Backend,
                                    <<"wasm_source">> => WasmSource,
                                    <<"epoch">> => Epoch,
                                    <<"key">> => NormKey,
                                    <<"val">> => Val,
                                    <<"ver">> => Ver,
                                    <<"found">> => true,
                                    <<"duration_ms">> => ((T1 - T0) / 1000.0) / 1000.0
                                }}, State};
                            {error, ErrCode, _Status} ->
                                T1 = os:perf_counter(nanosecond),
                                {reply, {ok, #{
                                    <<"ok">> => true,
                                    <<"error_code">> => ErrCode,
                                    <<"device">> => <<"hymessage@1.0">>,
                                    <<"contract_version">> => hydb_contract:contract_version(),
                                    <<"namespace">> => Namespace,
                                    <<"backend">> => Backend,
                                    <<"wasm_source">> => WasmSource,
                                    <<"epoch">> => Epoch,
                                    <<"key">> => NormKey,
                                    <<"val">> => -1,
                                    <<"ver">> => 0,
                                    <<"found">> => false,
                                    <<"duration_ms">> => ((T1 - T0) / 1000.0) / 1000.0
                                }}, State}
                        end
                    end;
                _ ->
                    case hydb_contract:execute_backend_get(Backend, Namespace, RawKey, WasmCtx) of
                        {ok, #{ <<"val">> := Val, <<"ver">> := Ver }} ->
                            T1 = os:perf_counter(nanosecond),
                            {reply, {ok, #{
                                <<"ok">> => true,
                                <<"error_code">> => <<"OK">>,
                                <<"device">> => <<"hymessage@1.0">>,
                                <<"contract_version">> => hydb_contract:contract_version(),
                                <<"namespace">> => Namespace,
                                <<"backend">> => Backend,
                                <<"epoch">> => Epoch,
                                <<"key">> => RawKey,
                                <<"val">> => Val,
                                <<"ver">> => Ver,
                                <<"found">> => true,
                                <<"duration_ms">> => ((T1 - T0) / 1000.0) / 1000.0
                            }}, State};
                        {error, ErrCode, _Status} ->
                            T1 = os:perf_counter(nanosecond),
                            {reply, {ok, #{
                                <<"ok">> => true,
                                <<"error_code">> => ErrCode,
                                <<"device">> => <<"hymessage@1.0">>,
                                <<"contract_version">> => hydb_contract:contract_version(),
                                <<"namespace">> => Namespace,
                                <<"backend">> => Backend,
                                <<"epoch">> => Epoch,
                                <<"key">> => RawKey,
                                <<"val">> => null,
                                <<"ver">> => 0,
                                <<"found">> => false,
                                <<"duration_ms">> => ((T1 - T0) / 1000.0) / 1000.0
                            }}, State}
                    end
            end
    end;

handle_call({contract_exists, Namespace, RawKey}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    case hydb_contract:get_namespace_config(Namespace) of
        {error, not_found} ->
            {reply, {error, #{
                <<"ok">> => false,
                <<"status">> => 404,
                <<"error_code">> => <<"ERR_NAMESPACE_UNKNOWN">>,
                <<"namespace">> => Namespace,
                <<"error">> => iolist_to_binary([<<"Namespace '">>, Namespace, <<"' is not registered">>])
            }}, State};
        {ok, Cfg} ->
            Backend = maps:get(<<"backend">>, Cfg, <<"wasm_slot">>),
            {ActiveWasmBase, _IsCustomWasm} = resolve_ns_wasm(Namespace, WasmBase),
            WasmCtx = {fun wasm_call/5, ActiveWasmBase, NodeMsg},
            NormKey = case Backend of <<"wasm_slot">> -> to_integer(RawKey, 0); _ -> RawKey end,
            {ok, #{ <<"exists">> := Exists, <<"ver">> := Ver }} = hydb_contract:execute_backend_exists(Backend, Namespace, NormKey, WasmCtx),
            {reply, {ok, #{
                <<"ok">> => true,
                <<"error_code">> => <<"OK">>,
                <<"contract_version">> => hydb_contract:contract_version(),
                <<"namespace">> => Namespace,
                <<"backend">> => Backend,
                <<"key">> => NormKey,
                <<"exists">> => Exists,
                <<"ver">> => Ver
            }}, State}
    end;

handle_call({contract_scan, Namespace, Prefix}, _From, State) ->
    case hydb_contract:get_namespace_config(Namespace) of
        {error, not_found} ->
            {reply, {error, #{
                <<"ok">> => false,
                <<"status">> => 404,
                <<"error_code">> => <<"ERR_NAMESPACE_UNKNOWN">>,
                <<"namespace">> => Namespace,
                <<"error">> => iolist_to_binary([<<"Namespace '">>, Namespace, <<"' is not registered">>])
            }}, State};
        {ok, Cfg} ->
            Backend = maps:get(<<"backend">>, Cfg, <<"wasm_slot">>),
            case hydb_contract:execute_backend_scan(Backend, Namespace, Prefix) of
                {ok, Items} ->
                    {reply, {ok, #{
                        <<"ok">> => true,
                        <<"error_code">> => <<"OK">>,
                        <<"contract_version">> => hydb_contract:contract_version(),
                        <<"namespace">> => Namespace,
                        <<"backend">> => Backend,
                        <<"prefix">> => Prefix,
                        <<"count">> => length(Items),
                        <<"items">> => Items
                    }}, State};
                {error, ErrCode, HttpStatus, ReasonBin} ->
                    {reply, {error, #{
                        <<"ok">> => false,
                        <<"status">> => HttpStatus,
                        <<"error_code">> => ErrCode,
                        <<"namespace">> => Namespace,
                        <<"backend">> => Backend,
                        <<"error">> => ReasonBin
                    }}, State}
            end
    end;

handle_call({contract_del, Namespace, RawKey, Opts}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    T0 = os:perf_counter(nanosecond),
    ReqEpoch = maps:get(<<"epoch">>, Opts, undefined),
    Token = maps:get(<<"token">>, Opts, undefined),
    CurrentEpoch = case ets:lookup(?TABLE, epoch) of [{epoch, Ep}] -> Ep; _ -> 1 end,

    case check_epoch(ReqEpoch, CurrentEpoch) of
        {error, fenced_stale_epoch} ->
            {reply, {error, #{
                <<"ok">> => false,
                <<"status">> => 409,
                <<"error_code">> => <<"FENCED_STALE_EPOCH">>,
                <<"verdict">> => <<"FENCED_STALE_EPOCH">>,
                <<"error">> => <<"fenced_stale_epoch">>
            }}, State};
        {ok, Epoch} ->
            SessionCheck = case Token of
                undefined -> {ok, false};
                _ ->
                    case ets:lookup(?SESSIONS_TABLE, Token) of
                        [{Token, _, _}] -> {ok, true};
                        [] -> invalid_token
                    end
            end,
            case SessionCheck of
                invalid_token ->
                    {reply, {error, #{
                        <<"ok">> => false,
                        <<"status">> => 403,
                        <<"error_code">> => <<"ERR_UNAUTHORIZED">>,
                        <<"error">> => <<"invalid_session_token">>
                    }}, State};
                {ok, HasValidSession} ->
                    {ActiveWasmBase, IsCustomWasm} = resolve_ns_wasm(Namespace, WasmBase),
                    WasmCtx = {fun wasm_call/5, ActiveWasmBase, NodeMsg},
                    OptsWithSession = Opts#{ <<"has_valid_session">> => HasValidSession },
                    case hydb_contract:validate_pre_sequence(Namespace, <<"del">>, RawKey, 0, OptsWithSession, WasmCtx) of
                        {error, ErrCode, HttpStatus, ReasonBin} ->
                            {reply, {error, #{
                                <<"ok">> => false,
                                <<"status">> => HttpStatus,
                                <<"error_code">> => ErrCode,
                                <<"error">> => ReasonBin,
                                <<"namespace">> => Namespace,
                                <<"contract_version">> => hydb_contract:contract_version()
                            }}, State};
                        {ok, Validated} ->
                            Backend = maps:get(<<"backend">>, Validated),
                            SchemaVsn = maps:get(<<"schema_version">>, Validated),
                            NormKey = maps:get(<<"key">>, Validated),

                            Seq = ets:update_counter(?TABLE, seq, 1),
                            [{latest_hash, PrevHash}] = ets:lookup(?TABLE, latest_hash),
                            WasmRoot0 = case ets:lookup(?TABLE, wasm_root) of [{wasm_root, WR}] -> WR; _ -> 0 end,
                            DocRoot0 = case ets:lookup(?TABLE, doc_root) of [{doc_root, DR}] -> DR; _ -> 0 end,

                            NewHash = build_event_hash(PrevHash, Seq, <<"22">>, Namespace, NormKey, <<"int64">>, <<"0">>),

                            {WasmRoot1, DocRoot1, DelVer} = case Backend of
                                <<"wasm_slot">> ->
                                    {ok, NewInstanceWR, DVer} = hydb_contract:execute_backend_del(<<"wasm_slot">>, Namespace, NormKey, Seq, WasmCtx),
                                    CombinedWR = apply_wasm_root_delta(Namespace, IsCustomWasm, NewInstanceWR, WasmRoot0),
                                    ets:insert(?TABLE, {wasm_root, CombinedWR}),
                                    {CombinedWR, DocRoot0, DVer};
                                <<"ets_doc">> ->
                                    {ok, NewDR, DVer} = hydb_contract:execute_backend_del(<<"ets_doc">>, Namespace, NormKey, Seq, {DocRoot0}),
                                    ets:insert(?TABLE, {doc_root, NewDR}),
                                    {WasmRoot0, NewDR, DVer}
                            end,
                            StateRoot = combine_roots(WasmRoot1, DocRoot1),
                            ets:insert(?TABLE, {latest_hash, NewHash}),
                            ets:insert(?TABLE, {state_root, StateRoot}),

                            [{history, Hist}] = ets:lookup(?TABLE, history),
                            Event = #{
                                <<"contract_version">> => hydb_contract:contract_version(),
                                <<"namespace">> => Namespace,
                                <<"schema_version">> => SchemaVsn,
                                <<"backend">> => Backend,
                                <<"epoch">> => Epoch,
                                <<"seq">> => Seq,
                                <<"action">> => <<"del">>,
                                <<"key">> => NormKey,
                                <<"val">> => 0,
                                <<"ver">> => DelVer,
                                <<"prev_hash">> => PrevHash,
                                <<"hash">> => NewHash,
                                <<"state_root">> => StateRoot
                            },
                            NewHist = [Event | lists:sublist(Hist, 49)],
                            ets:insert(?TABLE, {history, NewHist}),

                            {Replicated, SuCount} = broadcast_and_await_su(Event, Seq),
                            T1 = os:perf_counter(nanosecond),
                            DurationMs = ((T1 - T0) / 1000.0) / 1000.0,
                            Result = #{
                                <<"ok">> => true,
                                <<"error_code">> => <<"OK">>,
                                <<"device">> => <<"hymessage@1.0">>,
                                <<"contract_version">> => hydb_contract:contract_version(),
                                <<"namespace">> => Namespace,
                                <<"schema_version">> => SchemaVsn,
                                <<"backend">> => Backend,
                                <<"epoch">> => Epoch,
                                <<"seq">> => Seq,
                                <<"op">> => <<"del">>,
                                <<"key">> => NormKey,
                                <<"ver">> => DelVer,
                                <<"state_root">> => StateRoot,
                                <<"hash">> => NewHash,
                                <<"replicated">> => Replicated,
                                <<"su_count">> => SuCount,
                                <<"duration_ms">> => DurationMs
                            },
                            {reply, {ok, Result}, State}
                    end
            end
    end;

handle_call(db_snapshot, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    BytesWritten = wasm_call(WasmBase, <<"db_snapshot_export">>, [], NodeMsg, 0),
    ets:insert(?TABLE, {last_snapshot_bytes, BytesWritten}),
    [{seq, Seq}] = ets:lookup(?TABLE, seq),
    [{state_root, StateRoot}] = ets:lookup(?TABLE, state_root),
    Result = #{
        <<"ok">> => true,
        <<"device">> => <<"hymessage@1.0">>,
        <<"seq">> => Seq,
        <<"state_root">> => StateRoot,
        <<"bytes_exported">> => BytesWritten,
        <<"message">> => <<"WASM linear memory snapshot ready">>
    },
    {reply, {ok, Result}, State};

handle_call(db_restore, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    [{last_snapshot_bytes, Bytes}] = ets:lookup(?TABLE, last_snapshot_bytes),
    ImportStatus = wasm_call(WasmBase, <<"db_snapshot_import">>, [Bytes], NodeMsg, -1),
    Result = #{
        <<"ok">> => (ImportStatus == 1),
        <<"device">> => <<"hymessage@1.0">>,
        <<"import_status">> => ImportStatus,
        <<"verified">> => (ImportStatus == 1)
    },
    {reply, {ok, Result}, State};

handle_call({submit, Action, Value, Token}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    T0 = os:perf_counter(nanosecond),
    SessionInfo = case Token of
        undefined -> anonymous;
        _ ->
            case ets:lookup(?SESSIONS_TABLE, Token) of
                [{Token, PlayerId, _}] -> {player, PlayerId};
                [] -> invalid_token
            end
    end,
    case SessionInfo of
        invalid_token ->
            {reply, {error, invalid_session_token}, State};
        _ ->
            Seq = ets:update_counter(?TABLE, seq, 1),
            [{latest_hash, PrevHash}] = ets:lookup(?TABLE, latest_hash),
            Payload = iolist_to_binary([
                PrevHash,
                integer_to_binary(Seq),
                integer_to_binary(Action),
                integer_to_binary(Value)
            ]),
            NewHash = binary:encode_hex(crypto:hash(sha256, Payload)),
            WasmState = wasm_call(WasmBase, <<"compute">>, [Action, Value], NodeMsg, 0),
            ets:insert(?TABLE, {latest_hash, NewHash}),
            [{history, Hist}] = ets:lookup(?TABLE, history),
            Event = #{
                <<"seq">> => Seq,
                <<"action">> => Action,
                <<"value">> => Value,
                <<"prev_hash">> => PrevHash,
                <<"hash">> => NewHash,
                <<"wasm_state">> => WasmState
            },
            NewHist = [Event | lists:sublist(Hist, 99)],
            ets:insert(?TABLE, {history, NewHist}),
            T1 = os:perf_counter(nanosecond),
            DurationMs = ((T1 - T0) / 1000.0) / 1000.0,
            Result = #{
                <<"ok">> => true,
                <<"device">> => <<"hymessage@1.0">>,
                <<"seq">> => Seq,
                <<"action">> => Action,
                <<"value">> => Value,
                <<"prev_hash">> => PrevHash,
                <<"hash">> => NewHash,
                <<"wasm_state">> => WasmState,
                <<"duration_ms">> => DurationMs
            },
            {reply, {ok, Result}, State}
    end;

handle_call(get_state, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    Epoch = case ets:lookup(?TABLE, epoch) of [{epoch, Ep}] -> Ep; _ -> 1 end,
    [{seq, Seq}] = ets:lookup(?TABLE, seq),
    [{latest_hash, LatestHash}] = ets:lookup(?TABLE, latest_hash),
    [{history, Hist}] = ets:lookup(?TABLE, history),
    ActiveSessions = ets:info(?SESSIONS_TABLE, size),

    WasmRoot = case ets:lookup(?TABLE, wasm_root) of
        [{wasm_root, WR}] when WR =/= 0 -> WR;
        _ -> wasm_call(WasmBase, <<"db_root_hash">>, [], NodeMsg, 0)
    end,
    DocRoot = case ets:lookup(?TABLE, doc_root) of [{doc_root, DR}] -> DR; _ -> 0 end,
    RootHash = combine_roots(WasmRoot, DocRoot),
    Count = wasm_call(WasmBase, <<"db_count">>, [], NodeMsg, 0),

    ActiveSubscribers = case ets:info(hymessage_subscribers, size) of
        undefined -> 0;
        Sz -> Sz
    end,
    LastSuAck = case ets:lookup(?TABLE, last_su_ack) of
        [{last_su_ack, AckS}] -> AckS;
        _ -> 0
    end,
    UnauthTcpRej = case ets:lookup(?TABLE, unauthorized_tcp_rejections) of
        [{unauthorized_tcp_rejections, URej}] -> URej;
        _ -> 0
    end,
    Result = #{
        <<"ok">> => true,
        <<"device">> => <<"hymessage@1.0">>,
        <<"contract_version">> => hydb_contract:contract_version(),
        <<"epoch">> => Epoch,
        <<"seq">> => Seq,
        <<"latest_hash">> => LatestHash,
        <<"db_root_hash">> => RootHash,
        <<"db_records_count">> => Count,
        <<"history_length">> => length(Hist),
        <<"active_sessions">> => ActiveSessions,
        <<"active_su_subscribers">> => ActiveSubscribers,
        <<"last_su_ack">> => LastSuAck,
        <<"unauthorized_tcp_rejections">> => UnauthTcpRej
    },
    {reply, {ok, Result}, State};

handle_call({sync_events, SinceSeq}, _From, State) ->
    [{history, Hist}] = ets:lookup(?TABLE, history),
    Filtered = [E || E <- Hist, maps:get(<<"seq">>, E, 0) > SinceSeq],
    Sorted = lists:sort(fun(A, B) -> maps:get(<<"seq">>, A) =< maps:get(<<"seq">>, B) end, Filtered),
    EventsBin = try iolist_to_binary(json:encode(Sorted)) catch _:_ -> <<"[]">> end,
    Result = #{
        <<"ok">> => true,
        <<"device">> => <<"hymessage@1.0">>,
        <<"since_seq">> => SinceSeq,
        <<"events_count">> => length(Sorted),
        <<"events_json">> => EventsBin
    },
    {reply, {ok, Result}, State};

handle_call({replay, Events}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    T0 = os:perf_counter(nanosecond),
    SortedEvents = lists:sort(fun(A, B) ->
        to_integer(maps:get(<<"seq">>, A, 0), 0) =< to_integer(maps:get(<<"seq">>, B, 0), 0)
    end, Events),
    ApplyFun = fun(Ev, Acc) ->
        Seq = to_integer(maps:get(<<"seq">>, Ev, 0), 0),
        EvEpoch = to_integer(maps:get(<<"epoch">>, Ev, 1), 1),
        Namespace = maps:get(<<"namespace">>, Ev, <<"default">>),
        Backend = maps:get(<<"backend">>, Ev, <<"wasm_slot">>),
        ValType = maps:get(<<"val_type">>, Ev, <<"int64">>),
        Op = maps:get(<<"action">>, Ev, maps:get(<<"op">>, Ev, <<"put">>)),
        RawKey = maps:get(<<"key">>, Ev, 1),
        RawVal = maps:get(<<"val">>, Ev, 0),
        Key = case {Op, Backend} of
            {<<"register_schema">>, _} -> <<"__schema__">>;
            {_, <<"wasm_slot">>} -> to_integer(RawKey, 1);
            _ -> RawKey
        end,
        Val = case {Op, Backend} of
            {<<"register_schema">>, _} -> RawVal;
            {_, <<"wasm_slot">>} -> to_integer(RawVal, 0);
            _ -> RawVal
        end,
        CanonicalValBin = maps:get(<<"canonical_val">>, Ev,
            if Op =:= <<"register_schema">> andalso is_map(Val) -> hydb_contract:canonical_encode_schema(Val);
               is_integer(Val) -> integer_to_binary(Val);
               is_binary(Val) -> Val;
               is_map(Val) -> hydb_contract:canonical_encode_value(<<"json">>, Val);
               true -> <<"0">>
            end),
        PrevHash = case Acc of
            [] ->
                case ets:lookup(?TABLE, latest_hash) of
                    [{latest_hash, H}] -> H;
                    _ -> ?GENESIS_HASH
                end;
            [LastEv | _] -> maps:get(<<"hash">>, LastEv)
        end,
        ActionCode = case Op of
            <<"register_schema">> -> <<"24">>;
            <<"del">> -> <<"22">>;
            _ -> <<"20">>
        end,
        NewHash = build_event_hash(PrevHash, Seq, ActionCode, Namespace, Key, ValType, CanonicalValBin),

        WasmRoot0 = case ets:lookup(?TABLE, wasm_root) of [{wasm_root, WR}] -> WR; _ -> 0 end,
        DocRoot0 = case ets:lookup(?TABLE, doc_root) of [{doc_root, DR}] -> DR; _ -> 0 end,

        {WasmRoot1, DocRoot1, Ver} = case {Op, Backend} of
            {<<"register_schema">>, _} ->
                SchemaCfg = if is_map(Val) -> Val;
                               is_binary(CanonicalValBin) ->
                                   try json:decode(CanonicalValBin) catch _:_ -> #{} end;
                               true -> #{}
                            end,
                _ = boot_arweave_wasm_for_namespace(Namespace, SchemaCfg, NodeMsg),
                {ok, Entry} = hydb_contract:register_namespace(Namespace, SchemaCfg),
                {ActiveWasmBase, _IsCustomWasm} = resolve_ns_wasm(Namespace, WasmBase),
                _ = hydb_contract:configure_wasm_contract(Namespace, Entry, {fun wasm_call/5, ActiveWasmBase, NodeMsg}),
                hydb_contract:update_namespace_status(Namespace, <<"replicated">>),
                {WasmRoot0, DocRoot0, 1};
            {<<"del">>, <<"wasm_slot">>} ->
                {ActiveWasmBase, IsCustomWasm} = resolve_ns_wasm(Namespace, WasmBase),
                WasmCtx = {fun wasm_call/5, ActiveWasmBase, NodeMsg},
                {ok, NewInstanceWR, _} = hydb_contract:execute_backend_del(<<"wasm_slot">>, Namespace, Key, Seq, WasmCtx),
                CombinedWR = apply_wasm_root_delta(Namespace, IsCustomWasm, NewInstanceWR, WasmRoot0),
                ets:insert(?TABLE, {wasm_root, CombinedWR}),
                {CombinedWR, DocRoot0, 0};
            {_, <<"wasm_slot">>} ->
                {ActiveWasmBase, IsCustomWasm} = resolve_ns_wasm(Namespace, WasmBase),
                WasmCtx = {fun wasm_call/5, ActiveWasmBase, NodeMsg},
                {ok, NewInstanceWR, V} = hydb_contract:execute_backend_put(<<"wasm_slot">>, Namespace, Key, Val, Seq, WasmRoot0, WasmCtx),
                CombinedWR = apply_wasm_root_delta(Namespace, IsCustomWasm, NewInstanceWR, WasmRoot0),
                ets:insert(?TABLE, {wasm_root, CombinedWR}),
                {CombinedWR, DocRoot0, V};
            {<<"del">>, <<"ets_doc">>} ->
                {ok, NewDR, _} = hydb_contract:execute_backend_del(<<"ets_doc">>, Namespace, Key, Seq, {DocRoot0}),
                ets:insert(?TABLE, {doc_root, NewDR}),
                {WasmRoot0, NewDR, 0};
            {_, <<"ets_doc">>} ->
                WasmCtx = {fun wasm_call/5, WasmBase, NodeMsg},
                {ok, NewDR, V} = hydb_contract:execute_backend_put(<<"ets_doc">>, Namespace, Key, Val, Seq, DocRoot0, WasmCtx),
                ets:insert(?TABLE, {doc_root, NewDR}),
                {WasmRoot0, NewDR, V}
        end,
        StateRoot = combine_roots(WasmRoot1, DocRoot1),
        CurrEpoch = case ets:lookup(?TABLE, epoch) of [{epoch, CE}] -> CE; _ -> 1 end,
        if EvEpoch > CurrEpoch -> ets:insert(?TABLE, {epoch, EvEpoch}); true -> ok end,
        ets:insert(?TABLE, {seq, Seq}),
        ets:insert(?TABLE, {latest_hash, NewHash}),
        ets:insert(?TABLE, {state_root, StateRoot}),
        NewEv = #{
            <<"contract_version">> => hydb_contract:contract_version(),
            <<"namespace">> => Namespace,
            <<"backend">> => Backend,
            <<"epoch">> => max(CurrEpoch, EvEpoch),
            <<"seq">> => Seq,
            <<"action">> => Op,
            <<"key">> => Key,
            <<"val">> => Val,
            <<"val_type">> => ValType,
            <<"canonical_val">> => CanonicalValBin,
            <<"ver">> => Ver,
            <<"prev_hash">> => PrevHash,
            <<"hash">> => NewHash,
            <<"state_root">> => StateRoot
        },
        [NewEv | Acc]
    end,
    AppliedReversed = lists:foldl(ApplyFun, [], SortedEvents),
    [{history, OldHist}] = ets:lookup(?TABLE, history),
    NewHistory = lists:sublist(AppliedReversed ++ OldHist, 500),
    ets:insert(?TABLE, {history, NewHistory}),
    [{epoch, FinalEpoch}] = ets:lookup(?TABLE, epoch),
    [{seq, FinalSeq}] = ets:lookup(?TABLE, seq),
    [{state_root, FinalRoot}] = ets:lookup(?TABLE, state_root),
    [{latest_hash, FinalHash}] = ets:lookup(?TABLE, latest_hash),
    T1 = os:perf_counter(nanosecond),
    DurationMs = ((T1 - T0) / 1000.0) / 1000.0,
    Result = #{
        <<"ok">> => true,
        <<"device">> => <<"hymessage@1.0">>,
        <<"contract_version">> => hydb_contract:contract_version(),
        <<"replayed_count">> => length(SortedEvents),
        <<"epoch">> => FinalEpoch,
        <<"seq">> => FinalSeq,
        <<"state_root">> => FinalRoot,
        <<"latest_hash">> => FinalHash,
        <<"duration_ms">> => DurationMs
    },
    {reply, {ok, Result}, State};

handle_call(reset, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    ets:insert(?TABLE, {epoch, 1}),
    ets:insert(?TABLE, {seq, 0}),
    ets:insert(?TABLE, {latest_hash, ?GENESIS_HASH}),
    ets:insert(?TABLE, {state_root, 0}),
    ets:insert(?TABLE, {wasm_root, 0}),
    ets:insert(?TABLE, {default_wasm_root, 0}),
    ets:insert(?TABLE, {admin_locked, false}),
    ets:match_delete(?TABLE, {{ns_wasm_base, '_'}, '_'}),
    ets:match_delete(?TABLE, {{ns_wasm_root, '_'}, '_'}),
    ets:match_delete(?TABLE, {{ver, '_', '_'}, '_'}),
    ets:insert(?TABLE, {doc_root, 0}),
    ets:insert(?TABLE, {history, []}),
    ets:insert(?TABLE, {last_su_ack, 0}),
    ets:delete_all_objects(?SESSIONS_TABLE),
    hydb_contract:reset_tables(),
    ActiveBase = case WasmBase of
        undefined -> init_wasm(NodeMsg);
        _ ->
            case wasm_call(WasmBase, <<"db_contract_violations_ns">>, [0], NodeMsg, -999) of
                -999 -> init_wasm(NodeMsg);
                _ -> wrap_fast_wasm(WasmBase, NodeMsg)
            end
    end,
    wasm_call(ActiveBase, <<"db_reset">>, [], NodeMsg, 0),
    {ok, FinCfg} = hydb_contract:get_namespace_config(<<"finance.balances">>),
    _ = hydb_contract:configure_wasm_contract(<<"finance.balances">>, FinCfg, {fun wasm_call/5, ActiveBase, NodeMsg}),
    hydb_contract:update_namespace_status(<<"finance.balances">>, <<"replicated">>),
    {reply, ok, State#state{wasm_base = ActiveBase}};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({su_ack, AckSeq}, State) ->
    ets:insert(?TABLE, {last_su_ack, AckSeq}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
