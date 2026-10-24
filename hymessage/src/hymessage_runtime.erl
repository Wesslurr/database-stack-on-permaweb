-module(hymessage_runtime).
-behaviour(gen_server).

-export([start_link/0, ensure_started/0, submit/2, submit/3, get_state/0, reset/0]).
-export([join/1, sync_events/1, replay/1, promote_epoch/1]).
-export([db_put/3, db_put/4, db_get/1, db_del/2, db_del/3, db_snapshot/0, db_restore/0]).
-export([submit_from_req/3, get_state_from_req/3, reset_from_req/3, join_from_req/3, sync_from_req/3, replay_from_req/3, promote_from_req/3]).
-export([put_from_req/3, get_from_req/3, del_from_req/3, exists_from_req/3, schema_from_req/3, scan_from_req/3, snapshot_from_req/3, restore_from_req/3, cluster_membership_from_req/3, request_status_from_req/3]).
-export([compute_event_hash/2, compute_canonical_event_hash/1, compute_canonical_event_hash/2, compute_legacy_event_hash/2]).
-export([get_cluster_membership/0, set_cluster_membership/1, set_cluster_membership/2, get_configured_cluster_size/0, get_configured_cluster_members/0, is_quorum_satisfied/2, is_quorum_satisfied/3, count_acked_subscribers/1, verify_su_handshake/2, get_request_status/1]).
-export([is_test_profile/0, validate_startup_security/0, constant_time_equals/2, verify_admin_req/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    wasm_base,
    node_msg
}).

-define(TABLE, hymessage_store).
-define(SESSIONS_TABLE, hymessage_sessions).
-define(IDEMPOTENCY_TABLE, hymessage_idempotency_cache).
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

get_cluster_membership() ->
    ensure_started(),
    gen_server:call(?SERVER, get_cluster_membership, 5000).

set_cluster_membership(Size) when is_integer(Size), Size > 0 ->
    set_cluster_membership(get_configured_cluster_members(), Size);
set_cluster_membership(Members) when is_list(Members) ->
    set_cluster_membership(Members, max(1, length(Members))).

set_cluster_membership(Members, Size) when is_list(Members), is_integer(Size), Size > 0 ->
    ensure_started(),
    gen_server:call(?SERVER, {set_cluster_membership, Members, Size}, 5000).

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
    ReqQuorum = parse_bin_field(<<"quorum">>, Req, <<"single">>),
    RequireDurability = case parse_bin_field(<<"require_durability">>, Req, undefined) of
        <<"true">> -> true;
        <<"false">> -> false;
        _ -> false
    end,
    BaseOpts = #{
        <<"token">> => Token,
        <<"epoch">> => ReqEpoch,
        <<"expected_ver">> => ExpectedVer,
        <<"quorum">> => ReqQuorum,
        <<"require_durability">> => RequireDurability,
        <<"timeout_ms">> => parse_opt_int_field(<<"timeout_ms">>, Req),
        <<"request_id">> => extract_request_id(Req)
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
    ReqQuorum = parse_bin_field(<<"quorum">>, Req, <<"single">>),
    RequireDurability = case parse_bin_field(<<"require_durability">>, Req, undefined) of
        <<"true">> -> true;
        <<"false">> -> false;
        _ -> false
    end,
    Opts = #{
        <<"token">> => Token,
        <<"epoch">> => ReqEpoch,
        <<"expected_ver">> => ExpectedVer,
        <<"quorum">> => ReqQuorum,
        <<"require_durability">> => RequireDurability,
        <<"timeout_ms">> => parse_opt_int_field(<<"timeout_ms">>, Req),
        <<"request_id">> => extract_request_id(Req)
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
                        <<"rules">> => Rules4,
                        <<"request_id">> => extract_request_id(Req)
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

is_trusted_arweave_gateway(GatewayBin) when is_binary(GatewayBin) ->
    Defaults = [
        <<"http://127.0.0.1:1984">>,
        <<"http://localhost:1984">>,
        <<"https://arweave.net">>,
        <<"https://ar-io.dev">>,
        <<"https://permaweb.io">>,
        <<"https://arweave.dev">>
    ],
    case lists:member(GatewayBin, Defaults) of
        true -> true;
        false ->
            case os:getenv("TRUSTED_ARWEAVE_GATEWAYS") of
                false ->
                    case os:getenv("ARWEAVE_GATEWAY") of
                        false -> false;
                        EnvGw ->
                            GatewayBin =:= iolist_to_binary(EnvGw)
                    end;
                EnvList ->
                    Configured = [iolist_to_binary(string:trim(G)) || G <- string:tokens(EnvList, ",")],
                    lists:member(GatewayBin, Configured)
            end
    end;
is_trusted_arweave_gateway(_) -> false.

constant_time_equals(A, B) when is_binary(A), is_binary(B) ->
    case byte_size(A) =:= byte_size(B) of
        false -> false;
        true ->
            crypto:hash_equals(crypto:hash(sha256, A), crypto:hash(sha256, B))
    end;
constant_time_equals(_, _) ->
    false.

extract_admin_token(Req) ->
    HeadersMap = case maps:get(<<"headers">>, Req, undefined) of
        M when is_map(M) -> M;
        _ -> #{}
    end,
    case parse_bin_field(<<"headers/x-admin-token">>, Req, undefined) of
        undefined ->
            case parse_bin_field(<<"x-admin-token">>, Req, maps:get(<<"x-admin-token">>, HeadersMap, undefined)) of
                undefined ->
                    case parse_bin_field(<<"headers/authorization">>, Req, undefined) of
                        undefined ->
                            case parse_bin_field(<<"authorization">>, Req, maps:get(<<"authorization">>, HeadersMap, undefined)) of
                                undefined ->
                                    parse_bin_field(<<"admin_token">>, Req, undefined);
                                AuthVal -> parse_bearer_token(AuthVal)
                            end;
                        AuthHdr -> parse_bearer_token(AuthHdr)
                    end;
                XTok -> string:trim(XTok)
            end;
        XHdrTok -> string:trim(XHdrTok)
    end.

parse_bearer_token(<<"Bearer ", Token/binary>>) -> string:trim(Token);
parse_bearer_token(<<"bearer ", Token/binary>>) -> string:trim(Token);
parse_bearer_token(Token) when is_binary(Token) -> string:trim(Token);
parse_bearer_token(_) -> undefined.

is_test_profile() ->
    case os:getenv("HYDB_PROFILE") of
        "prod" -> false;
        "production" -> false;
        "test" -> true;
        false ->
            case os:getenv("HYDB_ENV") of
                "prod" -> false;
                "production" -> false;
                "test" -> true;
                false ->
                    case os:getenv("NODE_ENV") of
                        "test" -> true;
                        _ -> false
                    end;
                _ -> false
            end;
        "" ->
            case os:getenv("HYDB_ENV") of
                "test" -> true;
                _ ->
                    case os:getenv("NODE_ENV") of
                        "test" -> true;
                        _ -> false
                    end
            end;
        _ -> false
    end.

verify_admin_req(Req, ForceRequire) ->
    GivenToken = extract_admin_token(Req),
    IsLocked = case ets:lookup(?TABLE, admin_locked) of
        [{admin_locked, true}] -> true;
        _ -> false
    end,
    MustAuthenticate = ForceRequire orelse IsLocked orelse (not is_test_profile()),
    case {GivenToken, MustAuthenticate} of
        {undefined, false} ->
            ok;
        {undefined, true} ->
            {error, #{
                <<"ok">> => false,
                <<"status">> => 401,
                <<"error_code">> => <<"ERR_ADMIN_UNAUTHORIZED">>,
                <<"error">> => <<"Missing operator admin_token for protected administrative action">>
            }};
        {TokenBin, _} when is_binary(TokenBin) ->
            ExpectedAdminToken = admin_auth_secret(),
            ExpectedAdminHex = string:uppercase(binary:encode_hex(crypto:hash(sha256, ExpectedAdminToken))),
            ExactMatch = constant_time_equals(TokenBin, ExpectedAdminToken),
            CaseInsensitiveMatch = constant_time_equals(string:uppercase(TokenBin), string:uppercase(ExpectedAdminToken)),
            ShaHexMatch = constant_time_equals(string:uppercase(TokenBin), ExpectedAdminHex),
            case ExactMatch orelse CaseInsensitiveMatch orelse ShaHexMatch of
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
                    #{ <<"ok">> => false, <<"status">> => 409, <<"error">> => to_bin_spec(Reason) }
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
    case verify_admin_req(Req, true) of
        {error, ErrMap} ->
            ErrMap;
        ok ->
            case is_test_profile() of
                false ->
                    #{
                        <<"ok">> => false,
                        <<"status">> => 403,
                        <<"error_code">> => <<"ERR_RESET_DISABLED">>,
                        <<"error">> => <<"Reset endpoint is strictly disabled outside test profile">>
                    };
                true ->
                    ok = reset(),
                    #{
                        <<"ok">> => true,
                        <<"device">> => <<"hymessage@1.0">>,
                        <<"contract_version">> => hydb_contract:contract_version(),
                        <<"message">> => <<"state reset successfully">>
                    }
            end
    end.

cluster_membership_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    case parse_bin_field(<<"action">>, Req, <<"get">>) of
        <<"set">> ->
            case verify_admin_req(Req, true) of
                ok ->
                    NewSize = parse_opt_int_field(<<"cluster_size">>, Req),
                    RawMembers = parse_raw_field(<<"cluster_members">>, Req, []),
                    MembersList = if is_list(RawMembers) -> [to_bin_spec(M) || M <- RawMembers];
                                     is_binary(RawMembers) -> [to_bin_spec(string:trim(binary_to_list(M))) || M <- binary:split(RawMembers, [<<",">>, <<" ">>], [global, trim_all])];
                                     true -> []
                                  end,
                    FinalSize = case NewSize of
                        undefined -> max(1, length(MembersList));
                        S when is_integer(S), S > 0 -> S;
                        _ -> max(1, length(MembersList))
                    end,
                    case set_cluster_membership(MembersList, FinalSize) of
                        {ok, Res} -> Res;
                        {error, Err} -> #{ <<"ok">> => false, <<"status">> => 400, <<"error">> => hb_util:bin(Err) }
                    end;
                {error, ErrMap} ->
                    ErrMap
            end;
        _ ->
            case get_cluster_membership() of
                {ok, Res} -> Res;
                {error, Err} -> #{ <<"ok">> => false, <<"status">> => 500, <<"error">> => hb_util:bin(Err) }
            end
    end.

request_status_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    case extract_request_id(Req) of
        undefined ->
            #{
                <<"ok">> => false,
                <<"status">> => 400,
                <<"error_code">> => <<"ERR_INVALID_INPUT">>,
                <<"error">> => <<"Missing request_id parameter">>
            };
        ReqId ->
            {ok, StatusMap} = get_request_status(ReqId),
            StatusMap
    end.

get_request_status(ReqId) when is_binary(ReqId) ->
    ensure_started(),
    case ets:lookup(?IDEMPOTENCY_TABLE, ReqId) of
        [] ->
            {ok, #{
                <<"ok">> => true,
                <<"found">> => false,
                <<"request_id">> => ReqId,
                <<"commit_status">> => <<"unknown">>,
                <<"write_status">> => <<"unknown">>
            }};
        [{ReqId, Entry}] ->
            Seq = maps:get(<<"seq">>, Entry),
            QuorumMode = maps:get(<<"quorum">>, Entry, <<"single">>),
            WasDurable = maps:get(<<"replicated">>, Entry, false),
            NowDurable = WasDurable orelse is_quorum_satisfied(Seq, QuorumMode),
            CommitStatus = case NowDurable of true -> <<"durable">>; false -> <<"provisional">> end,
            WriteStatus = case NowDurable of true -> <<"durable">>; false -> <<"accepted">> end,
            Res0 = maps:get(<<"result">>, Entry),
            Res = Res0#{
                <<"replicated">> => NowDurable,
                <<"commit_status">> => CommitStatus,
                <<"write_status">> => WriteStatus
            },
            ets:insert(?IDEMPOTENCY_TABLE, {ReqId, Entry#{
                <<"replicated">> => NowDurable,
                <<"commit_status">> => CommitStatus,
                <<"write_status">> => WriteStatus,
                <<"result">> => Res
            }}),
            {ok, #{
                <<"ok">> => true,
                <<"found">> => true,
                <<"request_id">> => ReqId,
                <<"seq">> => Seq,
                <<"op">> => maps:get(<<"op">>, Entry),
                <<"namespace">> => maps:get(<<"namespace">>, Entry),
                <<"replicated">> => NowDurable,
                <<"commit_status">> => CommitStatus,
                <<"write_status">> => WriteStatus,
                <<"result">> => Res
            }}
    end.

extract_request_id(ReqOrOpts) when is_map(ReqOrOpts) ->
    case parse_bin_field(<<"headers/x-request-id">>, ReqOrOpts, undefined) of
        undefined ->
            case parse_bin_field(<<"x-request-id">>, ReqOrOpts, undefined) of
                undefined ->
                    case parse_bin_field(<<"request_id">>, ReqOrOpts, undefined) of
                        undefined -> parse_bin_field(<<"idempotency_key">>, ReqOrOpts, undefined);
                        <<>> -> parse_bin_field(<<"idempotency_key">>, ReqOrOpts, undefined);
                        RId -> string:trim(RId)
                    end;
                XRId -> string:trim(XRId)
            end;
        HdrRId -> string:trim(HdrRId)
    end;
extract_request_id(_) -> undefined.

is_request_id_required() ->
    case os:getenv("HYDB_REQUIRE_REQUEST_ID") of
        "true" -> true;
        "false" -> false;
        _ -> not is_test_profile()
    end.

get_max_idempotency_entries() ->
    case os:getenv("HYDB_IDEMPOTENCY_MAX_ENTRIES") of
        false -> 10000;
        "" -> 10000;
        Str -> max(1, to_integer(Str, 10000))
    end.

evict_oldest_idempotency_if_needed() ->
    MaxEntries = get_max_idempotency_entries(),
    case ets:info(?IDEMPOTENCY_TABLE, size) of
        Sz when is_integer(Sz), Sz >= MaxEntries ->
            All = ets:tab2list(?IDEMPOTENCY_TABLE),
            case All of
                [] -> ok;
                _ ->
                    {OldestKey, _} = lists:foldl(fun({K, V}, {MinK, MinSeq}) ->
                        S = maps:get(<<"seq">>, V, 0),
                        if S < MinSeq -> {K, S}; true -> {MinK, MinSeq} end
                    end, hd([ {K0, maps:get(<<"seq">>, V0, 0)} || {K0, V0} <- [hd(All)] ]), tl(All)),
                    ets:delete(?IDEMPOTENCY_TABLE, OldestKey),
                    ok
            end;
        _ -> ok
    end.

check_idempotency(undefined, _Op, _Namespace, _RawKey, _RawVal, _QuorumMode, _RequireDurability) ->
    case is_request_id_required() of
        true ->
            {conflict, #{
                <<"ok">> => false,
                <<"status">> => 400,
                <<"error_code">> => <<"ERR_MISSING_REQUEST_ID">>,
                <<"error">> => <<"request_id or idempotency_key is mandatory in production profile">>
            }};
        false ->
            not_cached
    end;
check_idempotency(<<>>, Op, Namespace, RawKey, RawVal, QuorumMode, RequireDurability) ->
    check_idempotency(undefined, Op, Namespace, RawKey, RawVal, QuorumMode, RequireDurability);
check_idempotency(ReqId, Op, Namespace, RawKey, RawVal, QuorumMode, RequireDurability) when is_binary(ReqId) ->
    case ets:lookup(?IDEMPOTENCY_TABLE, ReqId) of
        [] ->
            not_cached;
        [{ReqId, CachedEntry}] ->
            CachedOp = maps:get(<<"op">>, CachedEntry),
            CachedNs = maps:get(<<"namespace">>, CachedEntry),
            CachedKey = maps:get(<<"raw_key">>, CachedEntry),
            CachedVal = maps:get(<<"raw_val">>, CachedEntry),
            KeyMatch = (to_bin_spec(RawKey) =:= to_bin_spec(CachedKey)) orelse (RawKey =:= CachedKey),
            ValMatch = (Op =:= <<"del">>) orelse (to_bin_spec(RawVal) =:= to_bin_spec(CachedVal)) orelse (RawVal =:= CachedVal),
            case (CachedOp =:= Op) andalso (CachedNs =:= Namespace) andalso KeyMatch andalso ValMatch of
                false ->
                    {conflict, #{
                        <<"ok">> => false,
                        <<"status">> => 409,
                        <<"error_code">> => <<"ERR_IDEMPOTENCY_KEY_CONFLICT">>,
                        <<"error">> => <<"request_id already used with different operation parameters">>,
                        <<"request_id">> => ReqId
                    }};
                true ->
                    Seq = maps:get(<<"seq">>, CachedEntry),
                    BaseResult = maps:get(<<"result">>, CachedEntry),
                    WasDurable = maps:get(<<"replicated">>, BaseResult, false),
                    NowDurable = WasDurable orelse is_quorum_satisfied(Seq, QuorumMode),
                    CommitStatus = case NowDurable of true -> <<"durable">>; false -> <<"provisional">> end,
                    WriteStatus = case NowDurable of true -> <<"durable">>; false -> <<"accepted">> end,
                    UpdatedRes = BaseResult#{
                        <<"replicated">> => NowDurable,
                        <<"commit_status">> => CommitStatus,
                        <<"write_status">> => WriteStatus,
                        <<"request_id">> => ReqId,
                        <<"idempotent_replay">> => true
                    },
                    ets:insert(?IDEMPOTENCY_TABLE, {ReqId, CachedEntry#{
                        <<"replicated">> => NowDurable,
                        <<"commit_status">> => CommitStatus,
                        <<"write_status">> => WriteStatus,
                        <<"result">> => UpdatedRes
                    }}),
                    if
                        RequireDurability andalso not NowDurable ->
                            {cached_error, #{
                                <<"ok">> => false,
                                <<"status">> => 504,
                                <<"error_code">> => <<"ERR_DURABILITY_TIMEOUT">>,
                                <<"error">> => <<"Required durability replication ACK not satisfied by SU quorum">>,
                                <<"commit_status">> => CommitStatus,
                                <<"write_status">> => WriteStatus,
                                <<"replicated">> => false,
                                <<"seq">> => Seq,
                                <<"request_id">> => ReqId,
                                <<"idempotent_replay">> => true
                            }};
                        true ->
                            {cached_ok, UpdatedRes}
                    end
            end
    end.

store_idempotency(undefined, _Op, _Namespace, _RawKey, _RawVal, _Seq, _Quorum, Result) ->
    Result;
store_idempotency(<<>>, _Op, _Namespace, _RawKey, _RawVal, _Seq, _Quorum, Result) ->
    Result;
store_idempotency(ReqId, Op, Namespace, RawKey, RawVal, Seq, QuorumMode, Result) when is_binary(ReqId) ->
    evict_oldest_idempotency_if_needed(),
    Replicated = maps:get(<<"replicated">>, Result, false),
    CommitStatus = maps:get(<<"commit_status">>, Result, <<"provisional">>),
    WriteStatus = case Replicated of true -> <<"durable">>; false -> <<"accepted">> end,
    EnrichedResult = Result#{
        <<"request_id">> => ReqId,
        <<"write_status">> => WriteStatus
    },
    Entry = #{
        <<"request_id">> => ReqId,
        <<"op">> => Op,
        <<"namespace">> => Namespace,
        <<"raw_key">> => RawKey,
        <<"raw_val">> => RawVal,
        <<"seq">> => Seq,
        <<"quorum">> => QuorumMode,
        <<"replicated">> => Replicated,
        <<"commit_status">> => CommitStatus,
        <<"write_status">> => WriteStatus,
        <<"result">> => EnrichedResult,
        <<"timestamp">> => os:system_time(millisecond)
    },
    ets:insert(?IDEMPOTENCY_TABLE, {ReqId, Entry}),
    EnrichedResult.

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
                                    case hb_safe_get(Key, Req) of
                                        not_found ->
                                            case hb_safe_get(QueryKey, Req) of
                                                not_found ->
                                                    case hb_safe_get(BodyKey, Req) of
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

hb_safe_get(Key, Req) ->
    case erlang:function_exported(hb_ao, get, 3) of
        true ->
            try hb_ao:get(Key, Req, #{}) catch _:_ -> not_found end;
        false ->
            not_found
    end.

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
    case parse_raw_field(<<"events">>, Req, not_found) of
        not_found -> [];
        EvList when is_list(EvList) -> EvList;
        Bin when is_binary(Bin), byte_size(Bin) > 0 ->
            try json:decode(Bin) of
                L when is_list(L) -> L;
                #{<<"events">> := L2} when is_list(L2) -> L2;
                _ -> []
            catch _:_ -> []
            end;
        _ -> []
    end;
parse_events_field(_) -> [].

%% ============================================================================
%% GenServer Lifecycle, Startup Security, & Production WAL/Epoch Reconciliation
%% ============================================================================

hot_wal_path() ->
    case os:getenv("HYDB_HOT_WAL_PATH") of
        P1 when is_list(P1), length(P1) > 0 -> P1;
        _ ->
            case os:getenv("HYDB_RECONCILE_WAL_PATH") of
                P2 when is_list(P2), length(P2) > 0 -> P2;
                _ -> filename:join([get_db_dir(), "hymessage/hot_wal.log"])
            end
    end.

hot_wal_enabled() ->
    case os:getenv("HYDB_HOT_WAL_PATH") of
        P1 when is_list(P1), length(P1) > 0 -> true;
        _ ->
            case os:getenv("HYDB_ENABLE_HOT_WAL") of
                "true" -> true;
                "false" -> false;
                _ -> not is_test_profile()
            end
    end.

append_hot_wal_event(Event) ->
    case hot_wal_enabled() of
        false -> ok;
        true ->
            Path = hot_wal_path(),
            _ = filelib:ensure_dir(Path),
            Line = iolist_to_binary([json:encode(Event), <<"\n">>]),
            case file:open(Path, [append, raw, binary]) of
                {ok, Fd} ->
                    _ = file:write(Fd, Line),
                    _ = file:datasync(Fd),
                    _ = file:close(Fd),
                    ok;
                _ -> ok
            end
    end.

hot_epoch_path() ->
    case os:getenv("HYDB_HOT_EPOCH_PATH") of
        P when is_list(P), length(P) > 0 -> P;
        _ -> hot_wal_path() ++ ".epoch"
    end.

persist_hot_epoch(Epoch) when is_integer(Epoch) ->
    Path = hot_epoch_path(),
    Tmp = Path ++ ".tmp",
    _ = filelib:ensure_dir(Path),
    case file:open(Tmp, [write, raw, binary]) of
        {ok, Fd} ->
            _ = file:write(Fd, integer_to_binary(Epoch)),
            _ = file:datasync(Fd),
            _ = file:close(Fd),
            _ = file:rename(Tmp, Path),
            ok;
        _ -> ok
    end.

read_persisted_hot_epoch() ->
    EnvEpoch = case os:getenv("HYDB_RECONCILE_EPOCH") of
        false -> 1;
        "" -> 1;
        EStr -> to_integer(EStr, 1)
    end,
    DiskEpoch = case file:read_file(hot_epoch_path()) of
        {ok, Bin} -> to_integer(string:trim(Bin), 1);
        _ -> 1
    end,
    max(1, max(EnvEpoch, DiskEpoch)).

membership_meta_path() ->
    case os:getenv("HYDB_MEMBERSHIP_PATH") of
        P when is_list(P), length(P) > 0 -> P;
        _ -> hot_wal_path() ++ ".membership.json"
    end.

persist_membership_to_disk(Members, Size) ->
    Path = membership_meta_path(),
    Tmp = Path ++ ".tmp",
    _ = filelib:ensure_dir(Path),
    Payload = iolist_to_binary(json:encode(#{
        <<"cluster_members">> => Members,
        <<"cluster_size">> => Size
    })),
    case file:open(Tmp, [write, raw, binary]) of
        {ok, Fd} ->
            _ = file:write(Fd, Payload),
            _ = file:datasync(Fd),
            _ = file:close(Fd),
            _ = file:rename(Tmp, Path),
            ok;
        _ -> ok
    end.

read_persisted_membership() ->
    case file:read_file(membership_meta_path()) of
        {ok, Bin} when byte_size(Bin) > 0 ->
            try json:decode(Bin) of
                #{ <<"cluster_members">> := M0, <<"cluster_size">> := S0 } when is_list(M0), is_integer(S0), S0 > 0 ->
                    {[to_bin_spec(M) || M <- M0], S0};
                _ -> undefined
            catch _:_ -> undefined
            end;
        _ -> undefined
    end.

validate_startup_security() ->
    case ets:info(?TABLE) of
        undefined -> ok;
        _ ->
            ets:delete(?TABLE, cached_auth_secret),
            ets:delete(?TABLE, cached_admin_secret)
    end,
    _ = node_auth_secret(),
    _ = admin_auth_secret(),
    case is_test_profile() of
        true -> ok;
        false ->
            case env_cluster_members() =/= [] orelse read_persisted_membership() =/= undefined of
                true -> ok;
                false -> throw({error, missing_cluster_membership_in_production})
            end
    end.

startup_reconcile_from_disk(WasmBase, NodeMsg) ->
    PersistedEpoch0 = read_persisted_hot_epoch(),
    EnvEpoch = case os:getenv("HYDB_RECONCILE_EPOCH") of
        false -> 1;
        "" -> 1;
        EpStr -> try list_to_integer(EpStr) catch _:_ -> 1 end
    end,
    PersistedEpoch = max(PersistedEpoch0, EnvEpoch),
    CurrEp = case ets:lookup(?TABLE, epoch) of [{epoch, E}] -> E; _ -> 1 end,
    ets:insert(?TABLE, {epoch, max(CurrEp, PersistedEpoch)}),
    CandidatePaths = case {os:getenv("HYDB_RECONCILE_WAL_PATH"), os:getenv("HYDB_HOT_WAL_PATH")} of
        {P1, _} when is_list(P1), length(P1) > 0 -> [P1];
        {_, P2} when is_list(P2), length(P2) > 0 -> [P2];
        _ ->
            case hot_wal_enabled() of
                true -> [hot_wal_path(), filename:join([get_db_dir(), "sd/wal.log"])];
                false -> []
            end
    end,
    Events = read_first_available_wal(CandidatePaths),
    case Events of
        [] -> ok;
        _ ->
            _ = replay_events_internal(Events, WasmBase, NodeMsg),
            FinalEp = case ets:lookup(?TABLE, epoch) of [{epoch, FE}] -> FE; _ -> 1 end,
            ets:insert(?TABLE, {epoch, max(FinalEp, PersistedEpoch)}),
            ok
    end.

read_first_available_wal([]) -> [];
read_first_available_wal([Path | Rest]) ->
    case file:read_file(Path) of
        {ok, Content} when byte_size(Content) > 0 ->
            Lines = binary:split(Content, <<"\n">>, [global, trim_all]),
            Decoded = lists:filtermap(fun(L) ->
                try {true, json:decode(L)} catch _:_ -> false end
            end, Lines),
            case Decoded of
                [] -> read_first_available_wal(Rest);
                _ -> Decoded
            end;
        _ ->
            read_first_available_wal(Rest)
    end.

init([]) ->
    case catch validate_startup_security() of
        ok ->
            init_ets(),
            hydb_contract:init_tables(),
            start_stream_hub(),
            NodeMsg = get_node_msg(),
            WasmBase = init_wasm(NodeMsg),
            startup_reconcile_from_disk(WasmBase, NodeMsg),
            {ok, #state{wasm_base = WasmBase, node_msg = NodeMsg}};
        {error, Reason} ->
            {stop, {security_configuration_error, Reason}};
        OtherErr ->
            {stop, {security_configuration_error, OtherErr}}
    end.

init_ets() ->
    case ets:info(?TABLE) of
        undefined ->
            ets:new(?TABLE, [public, named_table, set]),
            ets:insert(?TABLE, {epoch, read_persisted_hot_epoch()}),
            ets:insert(?TABLE, {seq, 0}),
            ets:insert(?TABLE, {latest_hash, ?GENESIS_HASH}),
            ets:insert(?TABLE, {state_root, 0}),
            ets:insert(?TABLE, {wasm_root, 0}),
            ets:insert(?TABLE, {doc_root, 0}),
            ets:insert(?TABLE, {history, []}),
            ets:insert(?TABLE, {last_su_ack, 0}),
            ets:insert(?TABLE, {unauthorized_tcp_rejections, 0}),
            ets:insert(?TABLE, {last_snapshot_bytes, 0}),
            init_cluster_membership_ets();
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
    end,
    case ets:info(?IDEMPOTENCY_TABLE) of
        undefined ->
            ets:new(?IDEMPOTENCY_TABLE, [public, named_table, set]);
        _ -> ok
    end.

init_cluster_membership_ets() ->
    case read_persisted_membership() of
        {PersistedMembers, PersistedSize} ->
            ets:insert(?TABLE, {cluster_members, PersistedMembers}),
            ets:insert(?TABLE, {cluster_size, PersistedSize});
        undefined ->
            InitialMembers = env_cluster_members(),
            InitialSize = env_cluster_size(InitialMembers),
            ets:insert(?TABLE, {cluster_members, InitialMembers}),
            ets:insert(?TABLE, {cluster_size, InitialSize})
    end.

env_cluster_members() ->
    case os:getenv("HYDB_CLUSTER_MEMBERS") of
        false -> [];
        "" -> [];
        MStr -> [list_to_binary(string:trim(M)) || M <- string:lexemes(MStr, ", ")]
    end.

env_cluster_size(Members) ->
    case os:getenv("HYDB_CLUSTER_SIZE") of
        false -> max(1, length(Members));
        "" -> max(1, length(Members));
        SStr ->
            case string:to_integer(string:trim(SStr)) of
                {Int, _} when Int > 0 -> Int;
                _ -> max(1, length(Members))
            end
    end.

get_configured_cluster_size() ->
    case ets:info(?TABLE) of
        undefined -> env_cluster_size(env_cluster_members());
        _ ->
            case ets:lookup(?TABLE, cluster_size) of
                [{cluster_size, Size}] when is_integer(Size), Size > 0 -> Size;
                _ -> env_cluster_size(env_cluster_members())
            end
    end.

get_configured_cluster_members() ->
    case ets:info(?TABLE) of
        undefined -> env_cluster_members();
        _ ->
            case ets:lookup(?TABLE, cluster_members) of
                [{cluster_members, Members}] when is_list(Members) -> Members;
                _ -> env_cluster_members()
            end
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

get_db_dir() ->
    case os:getenv("DB_DIR") of
        false -> ".";
        "" -> ".";
        Dir -> Dir
    end.

init_wasm(NodeMsg) ->
    case erlang:function_exported(hb_ao, resolve, 3) of
        false ->
            fallback_wasm_slot;
        true ->
            DbDir = get_db_dir(),
            WasmBinFile = filename:join([DbDir, "hydb/out/hydb.wasm"]),
            case file:read_file(WasmBinFile) of
                {ok, WasmBin} when byte_size(WasmBin) >= 4 ->
                    WasmMsg = #{ <<"device">> => <<"wasm-64@1.0">>, <<"body">> => WasmBin },
                    case hb_ao:resolve(WasmMsg, <<"init">>, NodeMsg) of
                        {ok, Base} -> wrap_fast_wasm(Base, NodeMsg);
                        _ -> init_wasm_from_id(NodeMsg)
                    end;
                _ ->
                    init_wasm_from_id(NodeMsg)
            end
    end.

init_wasm_from_id(NodeMsg) ->
    case erlang:function_exported(hb_ao, resolve, 3) of
        false ->
            fallback_wasm_slot;
        true ->
            DbDir = get_db_dir(),
            WasmIdFile = filename:join([DbDir, "hydb/out/wasm_id.txt"]),
            case file:read_file(WasmIdFile) of
                {ok, RawId} ->
                    WasmID = string:trim(RawId),
                    WasmMsg = #{ <<"device">> => <<"wasm-64@1.0">>, <<"image">> => WasmID },
                    case hb_ao:resolve(WasmMsg, <<"init">>, NodeMsg) of
                        {ok, Base} -> wrap_fast_wasm(Base, NodeMsg);
                        _ -> undefined
                    end;
                _ -> undefined
            end
    end.

node_auth_secret() ->
    case ets:info(?TABLE) =/= undefined andalso ets:lookup(?TABLE, cached_auth_secret) of
        [{cached_auth_secret, Sec}] ->
            Sec;
        _ ->
            Sec = case os:getenv("HYDB_CLUSTER_SECRET") of
                EnvSec when is_list(EnvSec), length(EnvSec) >= 16 ->
                    crypto:hash(sha256, list_to_binary(EnvSec));
                EnvSec when is_list(EnvSec), length(EnvSec) > 0 ->
                    throw({error, cluster_secret_too_short});
                _ ->
                    case is_test_profile() of
                        true ->
                            WalletPath = case os:getenv("WALLET_PATH") of
                                false -> filename:join([get_db_dir(), "secrets/wallet.json"]);
                                "" -> filename:join([get_db_dir(), "secrets/wallet.json"]);
                                WP -> WP
                            end,
                            case file:read_file(WalletPath) of
                                {ok, Bin} when byte_size(Bin) >= 16 ->
                                    crypto:hash(sha256, Bin);
                                _ ->
                                    throw({error, missing_cluster_auth_secret})
                            end;
                        false ->
                            throw({error, missing_cluster_auth_secret})
                    end
            end,
            case ets:info(?TABLE) of
                undefined -> ok;
                _ -> ets:insert(?TABLE, {cached_auth_secret, Sec})
            end,
            Sec
    end.

admin_auth_secret() ->
    case os:getenv("HYDB_ADMIN_TOKEN") of
        AdminEnv when is_list(AdminEnv), length(AdminEnv) >= 16 ->
            case not is_test_profile() andalso os:getenv("HYDB_CLUSTER_SECRET") =:= AdminEnv of
                true -> throw({error, admin_token_must_differ_from_cluster_secret});
                false -> list_to_binary(AdminEnv)
            end;
        AdminEnv when is_list(AdminEnv), length(AdminEnv) > 0 ->
            throw({error, admin_token_too_short});
        _ ->
            case is_test_profile() of
                true ->
                    string:uppercase(binary:encode_hex(node_auth_secret()));
                false ->
                    throw({error, missing_admin_auth_token})
            end
    end.

start_stream_hub() ->
    case whereis(hymessage_stream_hub) of
        undefined ->
            TcpPort = case os:getenv("HOT_TCP_PORT") of
                false -> 8736;
                "" -> 8736;
                PStr -> list_to_integer(PStr)
            end,
            Pid = spawn_link(fun() -> stream_hub_listen(TcpPort) end),
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
                {ok, NodeId, SubLastTick} ->
                    evict_duplicate_node_sockets(NodeId, Socket),
                    ets:insert(hymessage_subscribers, {Socket, NodeId, os:system_time(millisecond), SubLastTick}),
                    AckPayload = iolist_to_binary(json:encode(#{
                        <<"type">> => <<"AUTH_OK">>,
                        <<"status">> => <<"AUTHENTICATED">>,
                        <<"node_id">> => NodeId
                    })),
                    _ = gen_tcp:send(Socket, AckPayload),
                    catch_up_subscriber(Socket, SubLastTick),
                    stream_socket_worker(Socket);
                {error, not_in_cluster_members} ->
                    ets:update_counter(?TABLE, unauthorized_tcp_rejections, 1, {unauthorized_tcp_rejections, 0}),
                    RejPayload = iolist_to_binary(json:encode(#{
                        <<"type">> => <<"AUTH_REJECTED">>,
                        <<"error_code">> => <<"ERR_NODE_NOT_IN_CLUSTER_MEMBERS">>
                    })),
                    _ = gen_tcp:send(Socket, RejPayload),
                    gen_tcp:close(Socket);
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

evict_duplicate_node_sockets(NodeId, NewSocket) ->
    case ets:info(hymessage_subscribers) of
        undefined -> ok;
        _ ->
            Subs = ets:tab2list(hymessage_subscribers),
            lists:foreach(fun
                ({OldSock, ExistingNodeId, _ConnAt, _AckSeq}) when ExistingNodeId =:= NodeId andalso OldSock =/= NewSocket ->
                    ets:delete(hymessage_subscribers, OldSock),
                    catch gen_tcp:close(OldSock);
                (_) -> ok
            end, Subs)
    end.

catch_up_subscriber(Socket, SubLastTick) when is_integer(SubLastTick) ->
    case ets:lookup(?TABLE, history) of
        [{history, Hist}] when is_list(Hist), Hist =/= [] ->
            Missed = [E || E <- Hist, to_integer(maps:get(<<"seq">>, E, 0), 0) > SubLastTick],
            Sorted = lists:sort(fun(A, B) ->
                to_integer(maps:get(<<"seq">>, A, 0), 0) =< to_integer(maps:get(<<"seq">>, B, 0), 0)
            end, Missed),
            lists:foreach(fun(Ev) ->
                Payload = try iolist_to_binary(json:encode(Ev)) catch _:_ -> <<>> end,
                if byte_size(Payload) > 0 ->
                    _ = gen_tcp:send(Socket, Payload);
                   true -> ok
                end
            end, Sorted);
        _ ->
            ok
    end;
catch_up_subscriber(_Socket, _SubLastTick) ->
    ok.

verify_su_handshake(Bin, Nonce) ->
    try json:decode(Bin) of
        #{ <<"type">> := <<"AUTH_RESPONSE">>, <<"node_id">> := NodeId, <<"sig">> := GivenSig } = Decoded
          when is_binary(NodeId), byte_size(NodeId) > 0, is_binary(GivenSig) ->
            Msg = <<Nonce/binary, ":", NodeId/binary>>,
            ExpectedSig = binary:encode_hex(crypto:mac(hmac, sha256, node_auth_secret(), Msg)),
            case constant_time_equals(string:uppercase(GivenSig), string:uppercase(ExpectedSig)) of
                true ->
                    Members = get_configured_cluster_members(),
                    case lists:member(NodeId, Members) of
                        true ->
                            SubLastTick = to_integer(maps:get(<<"last_tick">>, Decoded, 0), 0),
                            {ok, NodeId, SubLastTick};
                        false ->
                            {error, not_in_cluster_members}
                    end;
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
        [{Socket, NodeId, ConnAt, _OldAck}] ->
            ets:insert(hymessage_subscribers, {Socket, NodeId, ConnAt, AckSeq});
        _ ->
            ok
    end.

broadcast_and_await_su(Event, Seq) ->
    broadcast_and_await_su(Event, Seq, <<"single">>, 1500).

broadcast_and_await_su(Event, Seq, QuorumMode) ->
    broadcast_and_await_su(Event, Seq, QuorumMode, 1500).

broadcast_and_await_su(Event, Seq, QuorumMode, TimeoutMs) ->
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
            TotalSubs = length(ValidSubscribers),
            if TotalSubs == 0 ->
                {false, 0};
               true ->
                ActualTimeout = if is_integer(TimeoutMs) andalso TimeoutMs > 0 -> TimeoutMs; true -> 1500 end,
                Awaited = await_su_ack(Seq, QuorumMode, TotalSubs, ActualTimeout),
                {Awaited, TotalSubs}
            end
    end.

count_acked_subscribers(Seq) ->
    Subs = ets:tab2list(hymessage_subscribers),
    Members = get_configured_cluster_members(),
    AckedNodeIds = lists:foldl(fun
        ({_Sock, NodeId, _ConnAt, AckSeq}, Acc) when is_binary(NodeId), AckSeq >= Seq ->
            case lists:member(NodeId, Members) of
                true ->
                    case lists:member(NodeId, Acc) of
                        true -> Acc;
                        false -> [NodeId | Acc]
                    end;
                false -> Acc
            end;
        (_, Acc) -> Acc
    end, [], Subs),
    length(AckedNodeIds).

check_targeted_su_acked(Seq, TargetNodeId) when is_binary(TargetNodeId) ->
    Members = get_configured_cluster_members(),
    case lists:member(TargetNodeId, Members) of
        false -> false;
        true ->
            Subs = ets:tab2list(hymessage_subscribers),
            lists:any(fun
                ({_Sock, NodeId, _ConnAt, AckSeq}) when NodeId =:= TargetNodeId andalso AckSeq >= Seq -> true;
                (_) -> false
            end, Subs)
    end.

is_quorum_satisfied(Seq, QuorumMode) ->
    ClusterSize = get_configured_cluster_size(),
    AckedCount = count_acked_subscribers(Seq),
    case QuorumMode of
        <<"all">> ->
            AckedCount >= ClusterSize andalso ClusterSize > 0;
        all ->
            AckedCount >= ClusterSize andalso ClusterSize > 0;
        <<"majority">> ->
            Required = (ClusterSize div 2) + 1,
            AckedCount >= Required andalso AckedCount > 0;
        majority ->
            Required = (ClusterSize div 2) + 1,
            AckedCount >= Required andalso AckedCount > 0;
        {target, TargetId} ->
            check_targeted_su_acked(Seq, TargetId);
        <<"target:", Target/binary>> ->
            check_targeted_su_acked(Seq, Target);
        _ ->
            %% Default "single": at least 1 SU ACK
            AckedCount >= 1
    end.

is_quorum_satisfied(Seq, QuorumMode, _LegacyTotalSubs) ->
    %% Quorum is strictly anchored to configured cluster membership (get_configured_cluster_size()),
    %% never dynamically shrinking when subscriber sockets close or drop.
    is_quorum_satisfied(Seq, QuorumMode).

await_su_ack(Seq, QuorumMode, TotalSubs, TimeoutMs) ->
    T0 = os:perf_counter(millisecond),
    await_su_ack_loop(Seq, QuorumMode, TotalSubs, T0, TimeoutMs).

await_su_ack_loop(Seq, QuorumMode, TotalSubs, T0, TimeoutMs) ->
    case is_quorum_satisfied(Seq, QuorumMode, TotalSubs) of
        true ->
            true;
        false ->
            receive
                {su_ack, AckSeq} when AckSeq >= Seq ->
                    await_su_ack_loop(Seq, QuorumMode, TotalSubs, T0, TimeoutMs);
                {su_ack, _OldAckSeq} ->
                    await_su_ack_loop(Seq, QuorumMode, TotalSubs, T0, TimeoutMs)
            after 5 ->
                Now = os:perf_counter(millisecond),
                if (Now - T0) >= TimeoutMs -> false;
                   true -> await_su_ack_loop(Seq, QuorumMode, TotalSubs, T0, TimeoutMs)
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
wasm_call(fallback_wasm_slot, Func, Params, _NodeMsg, Default) ->
    fallback_wasm_slot_call(Func, Params, Default);
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

fallback_wasm_root() ->
    case ets:lookup(?TABLE, fallback_wasm_root) of
        [{fallback_wasm_root, R}] -> R;
        _ -> 0
    end.

fallback_comp_key(0, Key) -> Key;
fallback_comp_key(Ns, Key) ->
    Mask64 = 16#FFFFFFFFFFFFFFFF,
    ((Ns band Mask64) bxor (Key band Mask64)) band Mask64.

fallback_wasm_slot_call(<<"db_reset">>, [], _Default) ->
    ets:match_delete(?TABLE, {{wasm_slot_rec, '_', '_'}, '_'}),
    ets:match_delete(?TABLE, {{wasm_slot_contract, '_'}, '_'}),
    ets:insert(?TABLE, {fallback_wasm_root, 0}),
    0;
fallback_wasm_slot_call(<<"db_root_hash">>, [], _Default) ->
    fallback_wasm_root();
fallback_wasm_slot_call(<<"db_count">>, [], _Default) ->
    ets:foldl(fun
        ({{wasm_slot_rec, _, _}, #{active := active}}, Acc) -> Acc + 1;
        (_, Acc) -> Acc
    end, 0, ?TABLE);
fallback_wasm_slot_call(<<"db_count_ns">>, [Ns], _Default) ->
    ets:foldl(fun
        ({{wasm_slot_rec, RecNs, _}, #{active := active}}, Acc) when RecNs =:= Ns -> Acc + 1;
        (_, Acc) -> Acc
    end, 0, ?TABLE);
fallback_wasm_slot_call(<<"db_set_contract_ns">>, [Ns, HasMin, MinVal, HasMax, MaxVal, MaxRec], _Default) ->
    ets:insert(?TABLE, {{wasm_slot_contract, Ns}, #{
        has_min => (HasMin =/= 0),
        min_val => MinVal,
        has_max => (HasMax =/= 0),
        max_val => MaxVal,
        max_records => if MaxRec > 0 -> MaxRec; true -> 1024 end
    }}),
    1;
fallback_wasm_slot_call(<<"db_contract_violations_ns">>, [_Ns], _Default) ->
    0;
fallback_wasm_slot_call(<<"db_check_put_ns">>, [Ns, Key, Val], _Default) ->
    case ets:lookup(?TABLE, {wasm_slot_contract, Ns}) of
        [{{wasm_slot_contract, Ns}, #{has_min := HasMin, min_val := MinV, has_max := HasMax, max_val := MaxV, max_records := MaxRec}}] ->
            if HasMin andalso Val < MinV -> -422;
               HasMax andalso Val > MaxV -> -422;
               true ->
                Exists = fallback_wasm_slot_call(<<"db_has_ns">>, [Ns, Key], 0) =:= 1,
                case Exists of
                    true -> 1;
                    false ->
                        Cnt = fallback_wasm_slot_call(<<"db_count_ns">>, [Ns], 0),
                        if Cnt >= MaxRec -> -507; true -> 1 end
                end
            end;
        _ -> 1
    end;
fallback_wasm_slot_call(<<"db_put">>, [Key, Val, Seq], Default) ->
    fallback_wasm_slot_call(<<"db_put_ns">>, [0, Key, Val, Seq], Default);
fallback_wasm_slot_call(<<"db_put_ns">>, [Ns, Key, Val, Seq], _Default) ->
    CompKey = fallback_comp_key(Ns, Key),
    CurrRoot = fallback_wasm_root(),
    case ets:lookup(?TABLE, {wasm_slot_rec, Ns, Key}) of
        [{{wasm_slot_rec, Ns, Key}, #{active := active, val := OldVal, ver := OldVer}}] ->
            OldH = hydb_contract:canonical_hash_64(CompKey, OldVal, OldVer),
            NewVer = OldVer + 1,
            NewH = hydb_contract:canonical_hash_64(CompKey, Val, NewVer),
            NewRoot = combine_roots(combine_roots(CurrRoot, OldH), NewH),
            ets:insert(?TABLE, {{wasm_slot_rec, Ns, Key}, #{active => active, val => Val, ver => NewVer, seq => Seq}}),
            ets:insert(?TABLE, {fallback_wasm_root, NewRoot}),
            NewRoot;
        [{{wasm_slot_rec, Ns, Key}, #{active := tombstone, ver := OldVer}}] ->
            NewVer = OldVer + 1,
            NewH = hydb_contract:canonical_hash_64(CompKey, Val, NewVer),
            NewRoot = combine_roots(CurrRoot, NewH),
            ets:insert(?TABLE, {{wasm_slot_rec, Ns, Key}, #{active => active, val => Val, ver => NewVer, seq => Seq}}),
            ets:insert(?TABLE, {fallback_wasm_root, NewRoot}),
            NewRoot;
        [] ->
            NewH = hydb_contract:canonical_hash_64(CompKey, Val, 1),
            NewRoot = combine_roots(CurrRoot, NewH),
            ets:insert(?TABLE, {{wasm_slot_rec, Ns, Key}, #{active => active, val => Val, ver => 1, seq => Seq}}),
            ets:insert(?TABLE, {fallback_wasm_root, NewRoot}),
            NewRoot
    end;
fallback_wasm_slot_call(<<"db_del">>, [Key, Seq], Default) ->
    fallback_wasm_slot_call(<<"db_del_ns">>, [0, Key, Seq], Default);
fallback_wasm_slot_call(<<"db_del_ns">>, [Ns, Key, Seq], _Default) ->
    CompKey = fallback_comp_key(Ns, Key),
    CurrRoot = fallback_wasm_root(),
    case ets:lookup(?TABLE, {wasm_slot_rec, Ns, Key}) of
        [{{wasm_slot_rec, Ns, Key}, #{active := active, val := OldVal, ver := OldVer}}] ->
            OldH = hydb_contract:canonical_hash_64(CompKey, OldVal, OldVer),
            NewVer = OldVer + 1,
            NewRoot = combine_roots(CurrRoot, OldH),
            ets:insert(?TABLE, {{wasm_slot_rec, Ns, Key}, #{active => tombstone, val => 0, ver => NewVer, seq => Seq}}),
            ets:insert(?TABLE, {fallback_wasm_root, NewRoot}),
            NewRoot;
        _ ->
            CurrRoot
    end;
fallback_wasm_slot_call(<<"db_has">>, [Key], Default) ->
    fallback_wasm_slot_call(<<"db_has_ns">>, [0, Key], Default);
fallback_wasm_slot_call(<<"db_has_ns">>, [Ns, Key], _Default) ->
    case ets:lookup(?TABLE, {wasm_slot_rec, Ns, Key}) of
        [{{wasm_slot_rec, Ns, Key}, #{active := active}}] -> 1;
        _ -> 0
    end;
fallback_wasm_slot_call(<<"db_get">>, [Key], Default) ->
    fallback_wasm_slot_call(<<"db_get_ns">>, [0, Key], Default);
fallback_wasm_slot_call(<<"db_get_ns">>, [Ns, Key], Default) ->
    case ets:lookup(?TABLE, {wasm_slot_rec, Ns, Key}) of
        [{{wasm_slot_rec, Ns, Key}, #{active := active, val := Val}}] -> Val;
        _ -> Default
    end;
fallback_wasm_slot_call(<<"db_version">>, [Key], Default) ->
    fallback_wasm_slot_call(<<"db_version_ns">>, [0, Key], Default);
fallback_wasm_slot_call(<<"db_version_ns">>, [Ns, Key], _Default) ->
    case ets:lookup(?TABLE, {wasm_slot_rec, Ns, Key}) of
        [{{wasm_slot_rec, Ns, Key}, #{ver := Ver}}] -> Ver;
        _ -> 0
    end;
fallback_wasm_slot_call(_Func, _Params, Default) ->
    Default.

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

compute_event_hash(Event, LastHash) ->
    GivenHash = maps:get(<<"hash">>, Event, maps:get(hash, Event, <<>>)),
    CanonicalHash = compute_canonical_event_hash(Event, LastHash),
    case GivenHash of
        CanonicalHash ->
            CanonicalHash;
        _ ->
            LegacyHash = compute_legacy_event_hash(Event, LastHash),
            case GivenHash of
                LegacyHash ->
                    LegacyHash;
                _ ->
                    CanonicalHash
            end
    end.

compute_canonical_event_hash(Event) when is_map(Event) ->
    LastHash = maps:get(prev_hash, Event, maps:get(<<"prev_hash">>, Event, ?GENESIS_HASH)),
    compute_canonical_event_hash(Event, LastHash).

compute_canonical_event_hash(Event, LastHash) when is_map(Event) ->
    ContractVsn = maps:get(contract_version, Event, maps:get(<<"contract_version">>, Event, <<"hydb.core@1.0">>)),
    PrevHashHex = maps:get(prev_hash, Event, maps:get(<<"prev_hash">>, Event, LastHash)),
    PrevHashBin = hex_to_bin_32(PrevHashHex),
    Epoch = to_int_spec(maps:get(epoch, Event, maps:get(<<"epoch">>, Event, 1))),
    Seq = to_int_spec(maps:get(seq, Event, maps:get(<<"seq">>, Event, 1))),
    RawOp = maps:get(op, Event, maps:get(<<"op">>, Event, maps:get(action, Event, maps:get(<<"action">>, Event, <<"put">>)))),
    Op = case RawOp of
        <<"register_schema">> -> <<"register">>;
        register_schema -> <<"register">>;
        OtherOp -> to_bin_spec(OtherOp)
    end,
    Ns = to_bin_spec(maps:get(namespace, Event, maps:get(<<"namespace">>, Event, <<"default">>))),
    SchemaVsn = to_bin_spec(maps:get(schema_version, Event, maps:get(<<"schema_version">>, Event, <<"1.0.0">>))),
    RawKey = maps:get(key, Event, maps:get(<<"key">>, Event, absent)),
    NormKey = normalize_key_spec(RawKey, Op),
    ValType = to_bin_spec(maps:get(value_type, Event, maps:get(<<"val_type">>, Event, maps:get(<<"value_type">>, Event, maps:get(val_type, Event, <<"int64">>))))),
    RawVal = maps:get(value, Event, maps:get(<<"val">>, Event, maps:get(val, Event, maps:get(<<"value">>, Event, absent)))),
    NormVal = normalize_val_spec(RawVal, ValType, Op),
    Ver = to_int_spec(maps:get(ver, Event, maps:get(<<"ver">>, Event, 1))),

    FrameHeader = <<"HYDBE", 1>>,
    Bytes = iolist_to_binary([
        FrameHeader,
        encode_spec_val({string, to_bin_spec(ContractVsn)}),
        encode_spec_val({bytes, PrevHashBin}),
        encode_spec_val({int64, Epoch}),
        encode_spec_val({int64, Seq}),
        encode_spec_val({string, Op}),
        encode_spec_val({string, Ns}),
        encode_spec_val({string, SchemaVsn}),
        encode_spec_val(NormKey),
        encode_spec_val({string, ValType}),
        encode_spec_val(NormVal),
        encode_spec_val({int64, Ver})
    ]),
    binary:encode_hex(crypto:hash(sha256, Bytes)).

hex_to_bin_32(Hex) when is_binary(Hex), byte_size(Hex) =:= 64 ->
    try binary:decode_hex(Hex) catch _:_ -> <<0:256>> end;
hex_to_bin_32(Bin) when is_binary(Bin), byte_size(Bin) =:= 32 ->
    Bin;
hex_to_bin_32(_) ->
    <<0:256>>.

to_int_spec(I) when is_integer(I) -> I;
to_int_spec(B) when is_binary(B) ->
    try binary_to_integer(B) catch _:_ -> 0 end;
to_int_spec(L) when is_list(L) ->
    try list_to_integer(L) catch _:_ -> 0 end;
to_int_spec(_) -> 0.

to_bin_spec(B) when is_binary(B) -> B;
to_bin_spec(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin_spec(L) when is_list(L) -> list_to_binary(L);
to_bin_spec(I) when is_integer(I) -> integer_to_binary(I);
to_bin_spec(_) -> <<>>.

normalize_key_spec(absent, _) -> absent;
normalize_key_spec(undefined, _) -> absent;
normalize_key_spec(_, <<"register">>) -> absent;
normalize_key_spec({int64, I}, _) -> {int64, I};
normalize_key_spec({string, S}, _) -> {string, S};
normalize_key_spec(I, _) when is_integer(I) -> {int64, I};
normalize_key_spec(S, _) when is_binary(S) ->
    case S of
        <<"__schema__">> -> absent;
        _ -> {string, S}
    end;
normalize_key_spec(Other, _) -> Other.

normalize_val_spec(absent, _, _) -> absent;
normalize_val_spec(undefined, _, _) -> absent;
normalize_val_spec(_, _, <<"del">>) -> absent;
normalize_val_spec({int64, I}, _, _) -> {int64, I};
normalize_val_spec({bool, B}, _, _) -> {bool, B};
normalize_val_spec({string, S}, _, _) -> {string, S};
normalize_val_spec({object, O}, _, _) -> {object, O};
normalize_val_spec({array, A}, _, _) -> {array, A};
normalize_val_spec(I, <<"int64">>, _) when is_integer(I) -> {int64, I};
normalize_val_spec(B, <<"bool">>, _) when is_boolean(B) -> {bool, B};
normalize_val_spec(S, <<"string">>, _) when is_binary(S) -> {string, S};
normalize_val_spec(Map, <<"schema">>, _) when is_map(Map) ->
    schema_map_to_h(Map);
normalize_val_spec(Other, _, _) -> Other.

schema_map_to_h(Schema) when is_map(Schema) ->
    case code:ensure_loaded(hydb_spec) of
        {module, hydb_spec} ->
            hydb_spec:schema_to_h(Schema);
        _ ->
            Backend = maps:get(<<"backend">>, Schema, maps:get(backend, Schema, <<"ets_doc">>)),
            KeyType = maps:get(<<"key_type">>, Schema, maps:get(key_type, Schema, <<"string">>)),
            Ns = maps:get(<<"namespace">>, Schema, maps:get(namespace, Schema, <<"default">>)),
            RequireAuth = maps:get(<<"require_auth">>, Schema, maps:get(require_auth, Schema, false)),
            Requires = maps:get(<<"requires">>, Schema, maps:get(requires, Schema, [])),
            Rules = maps:get(<<"rules">>, Schema, maps:get(rules, Schema, #{})),
            SchemaVsn = maps:get(<<"schema_version">>, Schema, maps:get(schema_version, Schema, <<"1.0.0">>)),
            ValType = maps:get(<<"value_type">>, Schema, maps:get(value_type, Schema, <<"int64">>)),
            ReqSorted = lists:sort([to_bin_spec(R) || R <- Requires]),
            RulesEntries = lists:map(fun({RK, RV}) ->
                RKBin = to_bin_spec(RK),
                RVEnc = if is_integer(RV) -> {int64, RV};
                           is_boolean(RV) -> {bool, RV};
                           is_binary(RV) -> {string, RV};
                           true -> RV
                        end,
                {RKBin, RVEnc}
            end, maps:to_list(Rules)),
            {object, [
                {<<"backend">>, {string, to_bin_spec(Backend)}},
                {<<"key_type">>, {string, to_bin_spec(KeyType)}},
                {<<"namespace">>, {string, to_bin_spec(Ns)}},
                {<<"require_auth">>, {bool, RequireAuth}},
                {<<"requires">>, {array, [{string, R} || R <- ReqSorted]}},
                {<<"rules">>, {object, RulesEntries}},
                {<<"schema_version">>, {string, to_bin_spec(SchemaVsn)}},
                {<<"value_type">>, {string, to_bin_spec(ValType)}}
            ]}
    end.

encode_spec_val(absent) -> <<8>>;
encode_spec_val(undefined) -> <<8>>;
encode_spec_val(null) -> <<5>>;
encode_spec_val({null}) -> <<5>>;
encode_spec_val({int64, Val}) when is_integer(Val) ->
    <<1, Val:64/big-signed-integer>>;
encode_spec_val({bool, true}) ->
    <<2, 1>>;
encode_spec_val({bool, false}) ->
    <<2, 0>>;
encode_spec_val({string, Str}) when is_binary(Str) ->
    Len = byte_size(Str),
    <<3, Len:32/big-unsigned-integer, Str/binary>>;
encode_spec_val({bytes, Bin}) when is_binary(Bin) ->
    Len = byte_size(Bin),
    <<4, Len:32/big-unsigned-integer, Bin/binary>>;
encode_spec_val({array, Elements}) when is_list(Elements) ->
    Count = length(Elements),
    EncodedElems = [encode_spec_val(El) || El <- Elements],
    iolist_to_binary([<<6, Count:32/big-unsigned-integer>>, EncodedElems]);
encode_spec_val({object, Pairs}) when is_list(Pairs) ->
    Sorted = lists:sort(fun({K1, _}, {K2, _}) ->
        to_bin_spec(K1) =< to_bin_spec(K2)
    end, Pairs),
    Count = length(Sorted),
    EncodedEntries = lists:map(fun({K, V}) ->
        KBin = to_bin_spec(K),
        KLen = byte_size(KBin),
        VEnc = encode_spec_val(V),
        <<KLen:32/big-unsigned-integer, KBin/binary, VEnc/binary>>
    end, Sorted),
    iolist_to_binary([<<7, Count:32/big-unsigned-integer>>, EncodedEntries]);
encode_spec_val(Map) when is_map(Map) ->
    encode_spec_val({object, maps:to_list(Map)});
encode_spec_val(I) when is_integer(I) ->
    encode_spec_val({int64, I});
encode_spec_val(B) when is_boolean(B) ->
    encode_spec_val({bool, B});
encode_spec_val(S) when is_binary(S) ->
    encode_spec_val({string, S}).

compute_legacy_event_hash(Event, LastHash) ->
    Seq = to_integer(maps:get(<<"seq">>, Event, maps:get(seq, Event, 0)), 0),
    Op = maps:get(<<"action">>, Event, maps:get(<<"op">>, Event, maps:get(action, Event, maps:get(op, Event, <<"put">>)))),
    Namespace = to_bin_spec(maps:get(<<"namespace">>, Event, maps:get(namespace, Event, <<"default">>))),
    ValType = to_bin_spec(maps:get(<<"val_type">>, Event, maps:get(val_type, Event, maps:get(value_type, Event, <<"int64">>)))),
    RawKey = maps:get(<<"key">>, Event, maps:get(key, Event, 1)),
    RawVal = maps:get(<<"val">>, Event, maps:get(val, Event, maps:get(value, Event, 0))),
    CanonicalValBin = maps:get(<<"canonical_val">>, Event,
        if is_integer(RawVal) -> integer_to_binary(RawVal);
           is_binary(RawVal) -> RawVal;
           true -> <<"0">>
        end),
    OpCode = case Op of
        <<"register_schema">> -> <<"24">>;
        <<"register">> -> <<"24">>;
        register -> <<"24">>;
        <<"del">> -> <<"22">>;
        del -> <<"22">>;
        _ -> <<"20">>
    end,
    build_event_hash(LastHash, Seq, OpCode, Namespace, RawKey, ValType, CanonicalValBin).

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
            ReqId = maps:get(<<"request_id">>, Cfg, undefined),
            CleanCfg0 = maps:remove(<<"wasm_bin">>, Cfg),
            CleanCfg = maps:remove(<<"request_id">>, CleanCfg0),
            case check_idempotency(ReqId, <<"register_schema">>, Namespace, <<"__schema__">>, CleanCfg, <<"single">>, false) of
                {conflict, ConflictMap} ->
                    {reply, {error, ConflictMap}, State};
                {cached_ok, CachedRes} ->
                    {reply, {ok, CachedRes}, State};
                not_cached ->
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
                                        <<"state_root">> => StateRoot,
                                        <<"state_root_str">> => integer_to_binary(StateRoot),
                                        <<"request_id">> => case ReqId of undefined -> <<"">>; _ -> ReqId end
                                    },
                                    append_hot_wal_event(Event),
                                    NewHist = [Event | lists:sublist(Hist, 49)],
                                    ets:insert(?TABLE, {history, NewHist}),
                                    {Replicated, SuCount} = broadcast_and_await_su(Event, Seq),
                                    case Replicated of
                                        true -> hydb_contract:update_namespace_status(Namespace, <<"replicated">>);
                                        false -> ok
                                    end,
                                    CommitStatus = case Replicated of true -> <<"durable">>; false -> <<"provisional">> end,
                                    WriteStatus = case Replicated of true -> <<"durable">>; false -> <<"accepted">> end,
                                    Result0 = #{
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
                                        <<"commit_status">> => CommitStatus,
                                        <<"write_status">> => WriteStatus,
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
                                    Result = store_idempotency(ReqId, <<"register_schema">>, Namespace, <<"__schema__">>, CleanCfg, Seq, <<"single">>, Result0),
                                    {reply, {ok, Result}, State}
                            end
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
        persist_hot_epoch(NewEpoch),
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
                    ReqId = maps:get(<<"request_id">>, Opts, undefined),
                    ReqQuorum = maps:get(<<"quorum">>, Opts, <<"single">>),
                    RequireDurability = maps:get(<<"require_durability">>, Opts, false),
                    case check_idempotency(ReqId, <<"put">>, Namespace, RawKey, RawVal, ReqQuorum, RequireDurability) of
                        {conflict, ConflictMap} ->
                            {reply, {error, ConflictMap}, State};
                        {cached_ok, CachedRes} ->
                            {reply, {ok, CachedRes}, State};
                        {cached_error, CachedErr} ->
                            {reply, {error, CachedErr}, State};
                        not_cached ->
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
                                        <<"state_root">> => StateRoot,
                                        <<"state_root_str">> => integer_to_binary(StateRoot),
                                        <<"request_id">> => case ReqId of undefined -> <<"">>; _ -> ReqId end
                                    },
                                    append_hot_wal_event(Event),
                                    NewHist = [Event | lists:sublist(Hist, 49)],
                                    ets:insert(?TABLE, {history, NewHist}),
                                    T_hash = os:perf_counter(microsecond),

                                    TimeoutMs = maps:get(<<"timeout_ms">>, Opts, 1500),
                                    {Replicated, SuCount} = broadcast_and_await_su(Event, Seq, ReqQuorum, TimeoutMs),
                                    T_repl = os:perf_counter(microsecond),
                                    T1 = os:perf_counter(nanosecond),
                                    DurationMs = ((T1 - T0) / 1000.0) / 1000.0,
                                    WasmSource = case IsCustomWasm of true -> <<"arweave_tx">>; false -> <<"node_default">> end,
                                    WasmInstanceCount = case {Backend, IsCustomWasm} of
                                        {<<"wasm_slot">>, true} -> wasm_call(ActiveWasmBase, <<"db_count">>, [], NodeMsg, 0);
                                        _ -> 0
                                    end,
                                    CommitStatus = case Replicated of true -> <<"durable">>; false -> <<"provisional">> end,
                                    WriteStatus = case Replicated of true -> <<"durable">>; false -> <<"accepted">> end,
                                    Result0 = #{
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
                                        <<"commit_status">> => CommitStatus,
                                        <<"write_status">> => WriteStatus,
                                        <<"replicated">> => Replicated,
                                        <<"su_count">> => SuCount,
                                        <<"duration_ms">> => DurationMs,
                                        <<"t_val_us">> => (T_val - T_start),
                                        <<"t_wasm_us">> => (T_wasm - T_val),
                                        <<"t_hash_us">> => (T_hash - T_wasm),
                                        <<"t_repl_us">> => (T_repl - T_hash)
                                    },
                                    Result = store_idempotency(ReqId, <<"put">>, Namespace, RawKey, RawVal, Seq, ReqQuorum, Result0),
                                    if
                                        RequireDurability andalso not Replicated ->
                                            {reply, {error, #{
                                                <<"ok">> => false,
                                                <<"status">> => 504,
                                                <<"error_code">> => <<"ERR_DURABILITY_TIMEOUT">>,
                                                <<"error">> => <<"Required durability replication ACK not satisfied by SU quorum">>,
                                                <<"commit_status">> => CommitStatus,
                                                <<"write_status">> => WriteStatus,
                                                <<"replicated">> => false,
                                                <<"seq">> => Seq,
                                                <<"request_id">> => ReqId
                                            }}, State};
                                        true ->
                                            {reply, {ok, Result}, State}
                                    end
                            end
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
                    ReqId = maps:get(<<"request_id">>, Opts, undefined),
                    ReqQuorum = maps:get(<<"quorum">>, Opts, <<"single">>),
                    RequireDurability = maps:get(<<"require_durability">>, Opts, false),
                    case check_idempotency(ReqId, <<"del">>, Namespace, RawKey, 0, ReqQuorum, RequireDurability) of
                        {conflict, ConflictMap} ->
                            {reply, {error, ConflictMap}, State};
                        {cached_ok, CachedRes} ->
                            {reply, {ok, CachedRes}, State};
                        {cached_error, CachedErr} ->
                            {reply, {error, CachedErr}, State};
                        not_cached ->
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
                                        <<"state_root">> => StateRoot,
                                        <<"state_root_str">> => integer_to_binary(StateRoot),
                                        <<"request_id">> => case ReqId of undefined -> <<"">>; _ -> ReqId end
                                    },
                                    append_hot_wal_event(Event),
                                    NewHist = [Event | lists:sublist(Hist, 49)],
                                    ets:insert(?TABLE, {history, NewHist}),

                                    TimeoutMs = maps:get(<<"timeout_ms">>, Opts, 1500),
                                    {Replicated, SuCount} = broadcast_and_await_su(Event, Seq, ReqQuorum, TimeoutMs),
                                    T1 = os:perf_counter(nanosecond),
                                    DurationMs = ((T1 - T0) / 1000.0) / 1000.0,
                                    CommitStatus = case Replicated of true -> <<"durable">>; false -> <<"provisional">> end,
                                    WriteStatus = case Replicated of true -> <<"durable">>; false -> <<"accepted">> end,
                                    Result0 = #{
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
                                        <<"commit_status">> => CommitStatus,
                                        <<"write_status">> => WriteStatus,
                                        <<"replicated">> => Replicated,
                                        <<"su_count">> => SuCount,
                                        <<"duration_ms">> => DurationMs
                                    },
                                    Result = store_idempotency(ReqId, <<"del">>, Namespace, RawKey, 0, Seq, ReqQuorum, Result0),
                                    if
                                        RequireDurability andalso not Replicated ->
                                            {reply, {error, #{
                                                <<"ok">> => false,
                                                <<"status">> => 504,
                                                <<"error_code">> => <<"ERR_DURABILITY_TIMEOUT">>,
                                                <<"error">> => <<"Required durability replication ACK not satisfied by SU quorum">>,
                                                <<"commit_status">> => CommitStatus,
                                                <<"write_status">> => WriteStatus,
                                                <<"replicated">> => false,
                                                <<"seq">> => Seq,
                                                <<"request_id">> => ReqId
                                            }}, State};
                                        true ->
                                            {reply, {ok, Result}, State}
                                    end
                            end
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
        <<"db_root_hash_str">> => integer_to_binary(RootHash),
        <<"state_root_str">> => integer_to_binary(RootHash),
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
    {ok, Result} = replay_events_internal(Events, WasmBase, NodeMsg),
    {reply, {ok, Result}, State};

handle_call(get_cluster_membership, _From, State) ->
    Members = get_configured_cluster_members(),
    Size = get_configured_cluster_size(),
    ActiveSubscribers = case ets:info(hymessage_subscribers) of
        undefined -> 0;
        _ -> length(ets:tab2list(hymessage_subscribers))
    end,
    Result = #{
        <<"ok">> => true,
        <<"cluster_size">> => Size,
        <<"cluster_members">> => Members,
        <<"active_subscribers">> => ActiveSubscribers,
        <<"majority_threshold">> => (Size div 2) + 1
    },
    {reply, {ok, Result}, State};

handle_call({set_cluster_membership, Members, Size}, _From, State) ->
    ets:insert(?TABLE, {cluster_members, Members}),
    ets:insert(?TABLE, {cluster_size, Size}),
    persist_membership_to_disk(Members, Size),
    ActiveSubscribers = case ets:info(hymessage_subscribers) of
        undefined -> 0;
        _ -> length(ets:tab2list(hymessage_subscribers))
    end,
    Result = #{
        <<"ok">> => true,
        <<"cluster_size">> => Size,
        <<"cluster_members">> => Members,
        <<"active_subscribers">> => ActiveSubscribers,
        <<"majority_threshold">> => (Size div 2) + 1
    },
    {reply, {ok, Result}, State};

handle_call(reset, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    _ = file:delete(hot_wal_path()),
    _ = file:delete(hot_epoch_path()),
    _ = file:delete(membership_meta_path()),
    ets:insert(?TABLE, {epoch, 1}),
    ets:insert(?TABLE, {seq, 0}),
    ets:insert(?TABLE, {latest_hash, ?GENESIS_HASH}),
    ets:insert(?TABLE, {state_root, 0}),
    ets:insert(?TABLE, {wasm_root, 0}),
    ets:insert(?TABLE, {default_wasm_root, 0}),
    ets:insert(?TABLE, {admin_locked, false}),
    init_cluster_membership_ets(),
    ets:match_delete(?TABLE, {{ns_wasm_base, '_'}, '_'}),
    ets:match_delete(?TABLE, {{ns_wasm_root, '_'}, '_'}),
    ets:match_delete(?TABLE, {{ver, '_', '_'}, '_'}),
    ets:insert(?TABLE, {doc_root, 0}),
    ets:insert(?TABLE, {history, []}),
    ets:insert(?TABLE, {last_su_ack, 0}),
    ets:delete_all_objects(?SESSIONS_TABLE),
    case ets:info(?IDEMPOTENCY_TABLE) of
        undefined -> ok;
        _ -> ets:delete_all_objects(?IDEMPOTENCY_TABLE)
    end,
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

replay_events_internal(Events, WasmBase, NodeMsg) ->
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
        ReqId = maps:get(<<"request_id">>, Ev, undefined),
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
        MaxEpoch = max(CurrEpoch, EvEpoch),
        if EvEpoch > CurrEpoch -> ets:insert(?TABLE, {epoch, EvEpoch}); true -> ok end,
        ets:insert(?TABLE, {seq, Seq}),
        ets:insert(?TABLE, {latest_hash, NewHash}),
        ets:insert(?TABLE, {state_root, StateRoot}),
        NewEv = #{
            <<"contract_version">> => hydb_contract:contract_version(),
            <<"namespace">> => Namespace,
            <<"backend">> => Backend,
            <<"epoch">> => MaxEpoch,
            <<"seq">> => Seq,
            <<"action">> => Op,
            <<"key">> => Key,
            <<"val">> => Val,
            <<"val_type">> => ValType,
            <<"canonical_val">> => CanonicalValBin,
            <<"ver">> => Ver,
            <<"prev_hash">> => PrevHash,
            <<"hash">> => NewHash,
            <<"state_root">> => StateRoot,
            <<"request_id">> => case ReqId of undefined -> <<"">>; _ -> ReqId end
        },
        case is_binary(ReqId) andalso byte_size(ReqId) > 0 of
            true ->
                ReplayedRes = #{
                    <<"ok">> => true,
                    <<"error_code">> => <<"OK">>,
                    <<"device">> => <<"hymessage@1.0">>,
                    <<"contract_version">> => hydb_contract:contract_version(),
                    <<"namespace">> => Namespace,
                    <<"backend">> => Backend,
                    <<"epoch">> => MaxEpoch,
                    <<"seq">> => Seq,
                    <<"op">> => Op,
                    <<"key">> => Key,
                    <<"val">> => Val,
                    <<"val_type">> => ValType,
                    <<"ver">> => Ver,
                    <<"state_root">> => StateRoot,
                    <<"hash">> => NewHash,
                    <<"commit_status">> => <<"durable">>,
                    <<"write_status">> => <<"durable">>,
                    <<"replicated">> => true
                },
                _ = store_idempotency(ReqId, Op, Namespace, Key, Val, Seq, <<"single">>, ReplayedRes);
            false ->
                ok
        end,
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
    {ok, Result}.
