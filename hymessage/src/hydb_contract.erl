-module(hydb_contract).

%% ============================================================================
%% HyDB Core Contract Library & Pluggable Backend Adapter Layer
%% ============================================================================
%% Responsibilities (Architecture Specification Section 2):
%% A. Core Contract:
%%    - Operations: get, put, delete, exists
%%    - Data Model: Key-Value foundation + Structured Document model
%%    - Data Types: int64, string, bytes, bool, json
%%    - Namespace: Explicit data space isolation
%%    - Versioned Schema: Type & business invariant validation
%%    - Explicit Error Semantics:
%%        OK (200), ERR_KEY_NOT_FOUND (404), ERR_INVALID_INPUT (400),
%%        ERR_SCHEMA_VIOLATION (422), ERR_VERSION_CONFLICT (409),
%%        ERR_CAPABILITY_UNSUPPORTED (501), ERR_BACKEND_FAILURE (500)
%%    - Canonical Encoding Contract for deterministic hashing & commitment
%% B. Capability & Extension:
%%    - cas (Compare-And-Swap via expected_ver)
%%    - prefix_scan (Range/Prefix query)
%%    - structured_doc (Non-integer keys/values, JSON documents)
%%    - fast_fingerprint_64 (64-bit Merkle-XOR accumulator)
%%    - snapshot (WASM linear memory snapshot)
%% C. Backend Integration:
%%    - wasm_slot: Rust WASM O(1) slot table backend
%%    - ets_doc:   BEAM ETS multi-type & structured document backend
%% ============================================================================

-export([
    contract_version/0,
    init_tables/0,
    reset_tables/0,
    register_namespace/2,
    unregister_namespace/1,
    update_namespace_status/2,
    is_namespace_registered/1,
    is_namespace_active/1,
    validate_schema_definition/2,
    check_requires_list/3,
    check_rules_coherence/2,
    check_record_exists/4,
    configure_wasm_contract/3,
    get_namespace_config/1,
    list_namespaces/0,
    backend_capabilities/1,
    check_capability/2,
    validate_pre_sequence/6,
    namespace_id_64/1,
    canonical_encode_value/2,
    canonical_encode_schema/1,
    canonical_hash_64/3,
    execute_backend_put/7,
    execute_backend_del/5,
    execute_backend_get/4,
    execute_backend_exists/4,
    execute_backend_scan/3
]).

-define(CONTRACT_VERSION, <<"hydb.core@1.0">>).
-define(SCHEMA_TABLE, hydb_schemas).
-define(DOC_BACKEND_TABLE, hydb_ets_doc_store).

contract_version() ->
    ?CONTRACT_VERSION.

init_tables() ->
    case ets:info(?SCHEMA_TABLE) of
        undefined ->
            ets:new(?SCHEMA_TABLE, [public, named_table, set]),
            register_default_namespaces();
        _ -> ok
    end,
    case ets:info(?DOC_BACKEND_TABLE) of
        undefined ->
            ets:new(?DOC_BACKEND_TABLE, [public, named_table, ordered_set]);
        _ -> ok
    end.

reset_tables() ->
    init_tables(),
    ets:delete_all_objects(?SCHEMA_TABLE),
    ets:delete_all_objects(?DOC_BACKEND_TABLE),
    register_default_namespaces().

register_default_namespaces() ->
    %% 1. Default namespace: backed by ultra-fast Rust WASM slot engine
    ets:insert(?SCHEMA_TABLE, {<<"default">>, #{
        <<"namespace">> => <<"default">>,
        <<"backend">> => <<"wasm_slot">>,
        <<"schema_version">> => <<"1.0.0">>,
        <<"key_type">> => <<"int64">>,
        <<"value_type">> => <<"int64">>,
        <<"status">> => <<"replicated">>,
        <<"rules">> => #{}
    }}),
    %% 2. Financial balances namespace: int64 with non-negative invariant (min_value: 0)
    ets:insert(?SCHEMA_TABLE, {<<"finance.balances">>, #{
        <<"namespace">> => <<"finance.balances">>,
        <<"backend">> => <<"wasm_slot">>,
        <<"schema_version">> => <<"1.0.0">>,
        <<"key_type">> => <<"int64">>,
        <<"value_type">> => <<"int64">>,
        <<"status">> => <<"replicated">>,
        <<"rules">> => #{ <<"min_value">> => 0 }
    }}),
    %% 3. Document / Universal KV namespace: backed by ets_doc supporting JSON, strings, CAS, prefix_scan
    ets:insert(?SCHEMA_TABLE, {<<"app.docs">>, #{
        <<"namespace">> => <<"app.docs">>,
        <<"backend">> => <<"ets_doc">>,
        <<"schema_version">> => <<"1.0.0">>,
        <<"key_type">> => <<"string">>,
        <<"value_type">> => <<"any">>,
        <<"status">> => <<"replicated">>,
        <<"rules">> => #{}
    }}),
    ok.

validate_schema_definition(Namespace, ConfigMap) when is_binary(Namespace), is_map(ConfigMap) ->
    case re:run(Namespace, "^[a-z0-9][a-z0-9._-]{0,63}$") of
        nomatch ->
            {error, <<"ERR_INVALID_SCHEMA">>, 422, <<"Invalid namespace format (must be lowercase alphanumeric + ._-)">>};
        {match, _} ->
            Backend = maps:get(<<"backend">>, ConfigMap, <<"ets_doc">>),
            KeyType = maps:get(<<"key_type">>, ConfigMap, <<"string">>),
            ValType = maps:get(<<"value_type">>, ConfigMap, <<"any">>),
            SchemaVsn = maps:get(<<"schema_version">>, ConfigMap, <<"1.0.0">>),
            Requires = maps:get(<<"requires">>, ConfigMap, []),
            Rules = maps:get(<<"rules">>, ConfigMap, #{}),

            %% Reject unsupported complex/unimplemented features
            UnsupportedKeys = [<<"indexes">>, <<"secondary_indexes">>, <<"query_language">>, <<"migrations">>],
            case [K || K <- UnsupportedKeys, maps:is_key(K, ConfigMap)] of
                [UnsupKey | _] ->
                    {error, <<"ERR_UNSUPPORTED_FEATURE">>, 501,
                     iolist_to_binary([<<"Feature '">>, UnsupKey, <<"' is unsupported in hydb.core@1.0. Complex query languages and secondary indexes are deferred until formal cross-runtime semantics are proven.">>])};
                [] ->
                    %% Validate backend
                    case lists:member(Backend, [<<"wasm_slot">>, <<"ets_doc">>]) of
                false ->
                    {error, <<"ERR_INVALID_SCHEMA">>, 422, iolist_to_binary([<<"Unknown backend '">>, Backend, <<"'">>])};
                true ->
                    %% Validate Schema Version semver
                    case re:run(SchemaVsn, "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)$") of
                        nomatch ->
                            {error, <<"ERR_INVALID_SCHEMA">>, 422, <<"schema_version must be MAJOR.MINOR.PATCH">>};
                        {match, _} ->
                            %% Validate backend capability vs key_type & value_type
                            case Backend of
                                <<"wasm_slot">> ->
                                    if KeyType =/= <<"int64">> ->
                                        {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, 501, <<"wasm_slot only supports int64 keys">>};
                                       ValType =/= <<"int64">> ->
                                        {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, 501, <<"wasm_slot only supports int64 values">>};
                                       true ->
                                        check_schema_requires_and_rules(Backend, KeyType, Requires, Rules)
                                    end;
                                <<"ets_doc">> ->
                                    ValidKeys = [<<"int64">>, <<"string">>],
                                    ValidVals = [<<"int64">>, <<"bool">>, <<"string">>, <<"bytes">>, <<"json">>, <<"any">>],
                                    case lists:member(KeyType, ValidKeys) of
                                        false -> {error, <<"ERR_INVALID_SCHEMA">>, 422, <<"Invalid key_type">>};
                                        true ->
                                            case lists:member(ValType, ValidVals) of
                                                false -> {error, <<"ERR_INVALID_SCHEMA">>, 422, <<"Invalid value_type">>};
                                                true ->
                                                    check_schema_requires_and_rules(Backend, KeyType, Requires, Rules)
                                            end
                                    end
                            end
                    end
            end
        end
    end.

check_schema_requires_and_rules(Backend, KeyType, Requires, Rules) ->
    case check_requires_list(Backend, KeyType, Requires) of
        {error, Code, Reason} ->
            {error, Code, 501, Reason};
        ok ->
            case check_rules_coherence(Backend, Rules) of
                {error, Reason} ->
                    {error, <<"ERR_INVALID_SCHEMA">>, 422, Reason};
                ok ->
                    ok
            end
    end.

check_requires_list(<<"wasm_slot">>, _KeyType, Requires) when is_list(Requires) ->
    Allowed = [<<"get">>, <<"put">>, <<"del">>, <<"exists">>, <<"cas">>, <<"kv_core">>, <<"fast_fingerprint_64">>, <<"snapshot">>],
    case lists:all(fun(R) -> lists:member(R, Allowed) end, Requires) of
        false ->
            {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, <<"wasm_slot lacks required capability">>};
        true ->
            ok
    end;
check_requires_list(<<"ets_doc">>, KeyType, Requires) when is_list(Requires) ->
    Allowed = [<<"get">>, <<"put">>, <<"del">>, <<"exists">>, <<"cas">>, <<"kv_core">>, <<"prefix_scan">>, <<"structured_doc">>, <<"fast_fingerprint_64">>],
    case lists:all(fun(R) -> lists:member(R, Allowed) end, Requires) of
        false ->
            {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, <<"ets_doc lacks capability">>};
        true ->
            case lists:member(<<"prefix_scan">>, Requires) of
                true when KeyType =/= <<"string">> ->
                    {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, <<"prefix_scan requires string keys">>};
                _ ->
                    ok
            end
    end;
check_requires_list(_Backend, _KeyType, _Requires) ->
    ok.

check_rules_coherence(_Backend, Rules) when is_map(Rules) ->
    MinVal = maps:get(<<"min_value">>, Rules, undefined),
    MaxVal = maps:get(<<"max_value">>, Rules, undefined),
    if is_integer(MinVal), is_integer(MaxVal), MinVal > MaxVal ->
        {error, <<"min_value > max_value">>};
       true ->
        MinLen = maps:get(<<"min_length">>, Rules, undefined),
        MaxLen = maps:get(<<"max_length">>, Rules, undefined),
        if is_integer(MinLen), is_integer(MaxLen), MinLen > MaxLen ->
            {error, <<"min_length > max_length">>};
           true ->
            case maps:get(<<"fields">>, Rules, undefined) of
                undefined -> ok;
                Fields when is_map(Fields) ->
                    check_declared_fields_definition(maps:to_list(Fields));
                _ ->
                    {error, <<"fields rule must be an object">>}
            end
        end
    end;
check_rules_coherence(_Backend, _Rules) ->
    ok.

check_declared_fields_definition([]) ->
    ok;
check_declared_fields_definition([{FName, FSpec} | Rest]) when is_map(FSpec) ->
    AllowedTypes = [<<"int64">>, <<"string">>, <<"bool">>, <<"null">>, <<"object">>, <<"array">>],
    FType = maps:get(<<"type">>, FSpec, undefined),
    case lists:member(FType, AllowedTypes) of
        false ->
            {error, iolist_to_binary([<<"field '">>, FName, <<"' has invalid type (allowed: int64, string, bool, null, object, array)">>])};
        true ->
            FMin = maps:get(<<"min_value">>, FSpec, undefined),
            FMax = maps:get(<<"max_value">>, FSpec, undefined),
            if is_integer(FMin), is_integer(FMax), FMin > FMax ->
                {error, iolist_to_binary([<<"field '">>, FName, <<"': min_value > max_value">>])};
               true ->
                FLMin = maps:get(<<"min_length">>, FSpec, undefined),
                FLMax = maps:get(<<"max_length">>, FSpec, undefined),
                if is_integer(FLMin), is_integer(FLMax), FLMin > FLMax ->
                    {error, iolist_to_binary([<<"field '">>, FName, <<"': min_length > max_length">>])};
                   true ->
                    check_declared_fields_definition(Rest)
                end
            end
    end;
check_declared_fields_definition([{FName, _} | _]) ->
    {error, iolist_to_binary([<<"field '">>, FName, <<"' spec must be an object">>])}.

register_namespace(Namespace, ConfigMap) when is_binary(Namespace), is_map(ConfigMap) ->
    init_tables(),
    case validate_schema_definition(Namespace, ConfigMap) of
        {error, Code, Status, Reason} ->
            {error, Code, Status, Reason};
        ok ->
            Backend = maps:get(<<"backend">>, ConfigMap, <<"ets_doc">>),
            SchemaVsn = maps:get(<<"schema_version">>, ConfigMap, <<"1.0.0">>),
            KeyType = maps:get(<<"key_type">>, ConfigMap, <<"string">>),
            ValType = maps:get(<<"value_type">>, ConfigMap, <<"any">>),
            Rules = maps:get(<<"rules">>, ConfigMap, #{}),
            BaseEntry = #{
                <<"namespace">> => Namespace,
                <<"backend">> => Backend,
                <<"schema_version">> => SchemaVsn,
                <<"key_type">> => KeyType,
                <<"value_type">> => ValType,
                <<"rules">> => Rules,
                <<"status">> => <<"registered">>
            },
            Entry1 = case maps:get(<<"contract_tx_id">>, ConfigMap, undefined) of
                undefined -> BaseEntry;
                CTx when is_binary(CTx) -> BaseEntry#{ <<"contract_tx_id">> => CTx }
            end,
            Entry2 = case maps:get(<<"wasm_tx_id">>, ConfigMap, undefined) of
                undefined -> Entry1;
                WTx when is_binary(WTx) -> Entry1#{ <<"wasm_tx_id">> => WTx }
            end,
            WasmSha = case maps:get(<<"wasm_sha256">>, ConfigMap, undefined) of
                S when is_binary(S), byte_size(S) > 0 -> S;
                _ ->
                    case maps:get(<<"connector">>, ConfigMap, undefined) of
                        #{ <<"wasm_sha256">> := CS } when is_binary(CS), byte_size(CS) > 0 -> CS;
                        _ -> undefined
                    end
            end,
            Entry3 = case WasmSha of
                undefined -> Entry2;
                _ -> Entry2#{ <<"wasm_sha256">> => WasmSha }
            end,
            Entry4 = case maps:get(<<"policy">>, ConfigMap, undefined) of
                undefined -> Entry3;
                Pol when is_map(Pol) -> Entry3#{ <<"policy">> => Pol }
            end,
            Entry5 = case maps:get(<<"connector">>, ConfigMap, undefined) of
                undefined -> Entry4;
                Conn when is_map(Conn) -> Entry4#{ <<"connector">> => Conn }
            end,
            ets:insert(?SCHEMA_TABLE, {Namespace, Entry5}),
            {ok, Entry5}
    end.

unregister_namespace(Namespace) when is_binary(Namespace) ->
    init_tables(),
    ets:delete(?SCHEMA_TABLE, Namespace),
    ok.

update_namespace_status(Namespace, NewStatus) when is_binary(Namespace), is_binary(NewStatus) ->
    init_tables(),
    case ets:lookup(?SCHEMA_TABLE, Namespace) of
        [{Namespace, Entry}] ->
            Updated = Entry#{ <<"status">> => NewStatus },
            ets:insert(?SCHEMA_TABLE, {Namespace, Updated}),
            {ok, Updated};
        [] ->
            {error, not_found}
    end.

is_namespace_registered(Namespace) when is_binary(Namespace) ->
    init_tables(),
    case ets:lookup(?SCHEMA_TABLE, Namespace) of
        [{Namespace, _}] -> true;
        [] -> false
    end.

is_namespace_active(Namespace) when is_binary(Namespace) ->
    init_tables(),
    case ets:lookup(?SCHEMA_TABLE, Namespace) of
        [{Namespace, #{ <<"status">> := Status }}] ->
            Status =:= <<"replicated">> orelse Status =:= <<"committed">>;
        [{Namespace, _}] ->
            true;
        [] ->
            false
    end.

configure_wasm_contract(Namespace, Entry, {WasmCallFun, WasmBase, NodeMsg}) when is_map(Entry) ->
    Backend = maps:get(<<"backend">>, Entry, <<"wasm_slot">>),
    case Backend of
        <<"wasm_slot">> ->
            case WasmBase of
                undefined ->
                    {error, <<"ERR_BACKEND_UNAVAILABLE">>, 503, <<"WASM runtime not available">>};
                _ ->
                    Rules = maps:get(<<"rules">>, Entry, #{}),
                    NsId = namespace_id_64(Namespace),
                    {HasMin, MinVal} = case maps:get(<<"min_value">>, Rules, undefined) of
                        Min when is_integer(Min) -> {1, Min};
                        _ -> {0, 0}
                    end,
                    {HasMax, MaxVal} = case maps:get(<<"max_value">>, Rules, undefined) of
                        Max when is_integer(Max) -> {1, Max};
                        _ -> {0, 0}
                    end,
                    MaxRecords = case maps:get(<<"max_records">>, Rules, undefined) of
                        MR when is_integer(MR), MR > 0 -> MR;
                        _ -> 0
                    end,
                    Res = WasmCallFun(WasmBase, <<"db_set_contract_ns">>, [NsId, HasMin, MinVal, HasMax, MaxVal, MaxRecords], NodeMsg, 0),
                    case Res of
                        1 ->
                            update_namespace_status(Namespace, <<"configured">>),
                            ok;
                        _ ->
                            {error, <<"ERR_BACKEND_FAILURE">>, 500, <<"WASM contract configuration failed inside WASM linear memory">>}
                    end
            end;
        _ ->
            update_namespace_status(Namespace, <<"configured">>),
            ok
    end.

get_namespace_config(Namespace) ->
    init_tables(),
    case ets:lookup(?SCHEMA_TABLE, Namespace) of
        [{Namespace, Cfg}] ->
            {ok, Cfg};
        [] ->
            {error, not_found}
    end.

list_namespaces() ->
    init_tables(),
    [Cfg || {_Ns, Cfg} <- ets:tab2list(?SCHEMA_TABLE)].

%% ============================================================================
%% Capability Declarations per Backend
%% ============================================================================

backend_capabilities(<<"wasm_slot">>) ->
    [
        <<"kv_core">>,
        <<"exists">>,
        <<"cas">>,
        <<"fast_fingerprint_64">>,
        <<"snapshot">>
    ];
backend_capabilities(<<"ets_doc">>) ->
    [
        <<"kv_core">>,
        <<"exists">>,
        <<"cas">>,
        <<"structured_doc">>,
        <<"prefix_scan">>,
        <<"fast_fingerprint_64">>
    ];
backend_capabilities(_) ->
    [<<"kv_core">>].

check_capability(Backend, RequiredCap) ->
    Caps = backend_capabilities(Backend),
    lists:member(RequiredCap, Caps).

%% ============================================================================
%% Pre-Sequence Contract Gate (Guardrail A: Validate BEFORE allocating Seq!)
%% ============================================================================
%% Returns:
%%   {ok, ValidatedMap}
%%   | {error, ErrorCode, HttpStatus, ReasonBin}
validate_pre_sequence(Namespace, Op, RawKey, RawVal, Opts, WasmCtx) ->
    case get_namespace_config(Namespace) of
        {error, not_found} ->
            {error, <<"ERR_NAMESPACE_UNKNOWN">>, 404,
             iolist_to_binary([<<"Namespace '">>, Namespace, <<"' is not registered">>])};
        {ok, Cfg} ->
            Status = maps:get(<<"status">>, Cfg, <<"replicated">>),
            case (Status =:= <<"replicated">> orelse Status =:= <<"committed">>) of
                false ->
                    {error, <<"ERR_NAMESPACE_NOT_ACTIVE">>, 409,
                     iolist_to_binary([<<"Namespace '">>, Namespace, <<"' is not active (status=">>, Status, <<")">>])};
                true ->
                    validate_pre_sequence_cfg(Namespace, Op, RawKey, RawVal, Opts, WasmCtx, Cfg)
            end
    end.

validate_pre_sequence_cfg(Namespace, Op, RawKey, RawVal, Opts, WasmCtx, Cfg) ->
    Backend = maps:get(<<"backend">>, Cfg, <<"wasm_slot">>),
    SchemaVsn = maps:get(<<"schema_version">>, Cfg, <<"1.0.0">>),
    KeyType = maps:get(<<"key_type">>, Cfg, <<"int64">>),
    ValType = maps:get(<<"value_type">>, Cfg, <<"int64">>),
    Rules = maps:get(<<"rules">>, Cfg, #{}),
    ReqType = maps:get(<<"type">>, Opts, ValType),
    ExpectedVer = maps:get(<<"expected_ver">>, Opts, undefined),

    %% 0. Check Backend Availability
    BackendAvail = case Backend of
        <<"wasm_slot">> ->
            {_WasmCallFun, WasmBase, _NodeMsg} = WasmCtx,
            case WasmBase of
                undefined -> {error, <<"ERR_BACKEND_UNAVAILABLE">>, 503, <<"WASM backend runtime is unavailable">>};
                _ -> ok
            end;
        _ -> ok
    end,

    case BackendAvail of
        {error, ErrBackendCode, ErrBackendStatus, ErrBackendMsg} ->
            {error, ErrBackendCode, ErrBackendStatus, ErrBackendMsg};
        ok ->
            %% 1. Check Backend Capability for Operation & Data Type
            CapCheck = case Op of
                <<"prefix_scan">> ->
                    check_capability(Backend, <<"prefix_scan">>);
                _ ->
                    case (KeyType =/= <<"int64">>) orelse (ReqType =/= <<"int64">> andalso ReqType =/= <<"any">>) of
                        true -> check_capability(Backend, <<"structured_doc">>);
                        false -> check_capability(Backend, <<"kv_core">>)
                    end
            end,

            case CapCheck of
                false ->
                    {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, 501,
                     iolist_to_binary([<<"Backend '">>, Backend, <<"' in namespace '">>, Namespace,
                                       <<"' does not support required capability for op='">>, Op,
                                       <<"' type='">>, ReqType, <<"'">>])};
                true ->
                    %% 2. Validate Key Type
                    case validate_key(KeyType, RawKey) of
                        {error, KeyReason} ->
                            {error, <<"ERR_INVALID_KEY">>, 400, KeyReason};
                        {ok, NormKey} ->
                            %% 3. Validate Value Type & Business Schema Rules (or existence on del)
                            ValCheck = case Op of
                                <<"put">> ->
                                    EffectiveType = if ValType == <<"any">> -> ReqType; true -> ValType end,
                                    validate_value_and_rules(EffectiveType, RawVal, Rules);
                                <<"del">> ->
                                    case check_record_exists(Backend, Namespace, NormKey, WasmCtx) of
                                        false ->
                                            {error, <<"ERR_NOT_FOUND">>, 404, <<"Record does not exist">>};
                                        true ->
                                            {ok, ValType, 0, <<"0">>}
                                    end;
                                _ ->
                                    {ok, <<"int64">>, 0, <<"0">>}
                            end,
                            case ValCheck of
                                {error, ErrCode, Status, ValReason} ->
                                    {error, ErrCode, Status, ValReason};
                                {ok, FinalType, NormVal, CanonicalValBin} ->
                                    %% 3b. Check Contract-Level Authentication Requirement (require_auth: true)
                                    case check_auth_requirement(Op, Rules, Opts) of
                                        {error, AuthReason} ->
                                            {error, <<"ERR_UNAUTHORIZED">>, 403, AuthReason};
                                        ok ->
                                            %% 3c. Check WASM Slot Capacity Limit & In-WASM Contract Gate (db_check_put_ns)
                                            case check_wasm_capacity(Op, Backend, Namespace, NormKey, NormVal, Rules, WasmCtx) of
                                                {error, CapCode, CapStatus, CapReason} ->
                                                    {error, CapCode, CapStatus, CapReason};
                                                ok ->
                                                    %% 4. Check Optimistic Concurrency Control / CAS (if expected_ver specified)
                                                    case check_cas_version(Backend, Namespace, NormKey, ExpectedVer, WasmCtx) of
                                                        {error, CasReason, CurrentVer} ->
                                                            {error, <<"ERR_VERSION_CONFLICT">>, 409,
                                                             iolist_to_binary([CasReason, <<" (current_ver=">>, integer_to_binary(CurrentVer), <<")">>])};
                                                        ok ->
                                                            {ok, #{
                                                                <<"contract_version">> => ?CONTRACT_VERSION,
                                                                <<"namespace">> => Namespace,
                                                                <<"schema_version">> => SchemaVsn,
                                                                <<"backend">> => Backend,
                                                                <<"key">> => NormKey,
                                                                <<"val">> => NormVal,
                                                                <<"val_type">> => FinalType,
                                                                <<"canonical_val">> => CanonicalValBin
                                                            }}
                                                    end
                                            end
                                    end
                            end
                    end
            end
    end.

check_auth_requirement(Op, Rules, Opts) when Op =:= <<"put">>; Op =:= <<"del">> ->
    RequireAuth = maps:get(<<"require_auth">>, Rules, false),
    HasValidSession = maps:get(<<"has_valid_session">>, Opts, false),
    case (RequireAuth =:= true orelse RequireAuth =:= <<"true">>) andalso (HasValidSession =/= true) of
        true ->
            {error, <<"Namespace contract requires a valid session token (require_auth=true)">>};
        false ->
            ok
    end;
check_auth_requirement(_Op, _Rules, _Opts) ->
    ok.

check_wasm_capacity(<<"put">>, <<"wasm_slot">>, Namespace, Key, Val, Rules, {WasmCallFun, WasmBase, NodeMsg}) when is_integer(Key), is_integer(Val) ->
    case maps:get(<<"max_records">>, Rules, undefined) of
        undefined ->
            ok;
        RawMax ->
            NsId = namespace_id_64(Namespace),
            WasmCheck = WasmCallFun(WasmBase, <<"db_check_put_ns">>, [NsId, Key, Val], NodeMsg, 1),
            case WasmCheck of
                Code when Code =:= -422; Code =:= -42201; Code =:= -42202 ->
                    {error, <<"ERR_SCHEMA_VIOLATION">>, 422, <<"WASM Engine Contract Violation: value violates min/max invariant">>};
                Code when Code =:= -507; Code =:= -50701 ->
                    MaxRecords = if is_integer(RawMax), RawMax > 0 -> RawMax; true -> 1024 end,
                    {error, <<"ERR_WASM_CAPACITY_EXCEEDED">>, 507, iolist_to_binary([
                        <<"WASM Engine Contract Violation: slot capacity exceeded for namespace '">>, Namespace,
                        <<"' (max_records=">>, integer_to_binary(MaxRecords), <<")">>
                    ])};
                _ ->
                    ok
            end
    end;
check_wasm_capacity(_Op, _Backend, _Namespace, _Key, _Val, _Rules, _WasmCtx) ->
    ok.

validate_key(<<"int64">>, K) when is_integer(K) -> {ok, K};
validate_key(<<"int64">>, K) when is_binary(K) ->
    try {ok, binary_to_integer(K)}
    catch _:_ -> {error, <<"Key must be a valid int64">>}
    end;
validate_key(<<"string">>, K) when is_binary(K), byte_size(K) > 0 -> {ok, K};
validate_key(<<"string">>, K) when is_integer(K) -> {ok, integer_to_binary(K)};
validate_key(_, _) -> {error, <<"Invalid or empty key">>}.

validate_value_and_rules(<<"int64">>, V, Rules) ->
    case to_int_strict(V) of
        error ->
            {error, <<"ERR_INVALID_INPUT">>, 400, <<"Value must be a valid int64">>};
        {ok, IntVal} ->
            case check_numeric_rules(IntVal, Rules) of
                ok ->
                    {ok, <<"int64">>, IntVal, integer_to_binary(IntVal)};
                {error, RuleReason} ->
                    {error, <<"ERR_SCHEMA_VIOLATION">>, 422, RuleReason}
            end
    end;
validate_value_and_rules(<<"bool">>, V, _Rules) ->
    case V of
        true -> {ok, <<"bool">>, true, <<"true">>};
        false -> {ok, <<"bool">>, false, <<"false">>};
        <<"true">> -> {ok, <<"bool">>, true, <<"true">>};
        <<"false">> -> {ok, <<"bool">>, false, <<"false">>};
        _ -> {error, <<"ERR_INVALID_INPUT">>, 400, <<"Value must be boolean (true/false)">>}
    end;
validate_value_and_rules(<<"string">>, V, Rules) when is_binary(V) ->
    MinLen = maps:get(<<"min_length">>, Rules, 0),
    MaxLen = maps:get(<<"max_length">>, Rules, undefined),
    ByteSize = byte_size(V),
    if ByteSize < MinLen ->
        {error, <<"ERR_SCHEMA_VIOLATION">>, 422, <<"String value shorter than schema min_length">>};
       is_integer(MaxLen) andalso ByteSize > MaxLen ->
        {error, <<"ERR_SCHEMA_VIOLATION">>, 422, <<"String value longer than schema max_length">>};
       true ->
        {ok, <<"string">>, V, V}
    end;
validate_value_and_rules(<<"bytes">>, V, Rules) when is_binary(V) ->
    MinLen = maps:get(<<"min_length">>, Rules, 0),
    MaxLen = maps:get(<<"max_length">>, Rules, undefined),
    ByteSize = byte_size(V),
    if ByteSize < MinLen ->
        {error, <<"ERR_SCHEMA_VIOLATION">>, 422, <<"Bytes value shorter than schema min_length">>};
       is_integer(MaxLen) andalso ByteSize > MaxLen ->
        {error, <<"ERR_SCHEMA_VIOLATION">>, 422, <<"Bytes value longer than schema max_length">>};
       true ->
        Hex = binary:encode_hex(V),
        {ok, <<"bytes">>, Hex, Hex}
    end;
validate_value_and_rules(<<"json">>, V, Rules) ->
    Decoded = if
        is_map(V) -> {ok, V};
        is_binary(V) ->
            try {ok, json:decode(V)}
            catch _:_ -> error
            end;
        true -> error
    end,
    case Decoded of
        {ok, MapVal} when is_map(MapVal) ->
            case check_json_rules(MapVal, Rules) of
                ok ->
                    CanonicalJson = canonical_encode_value(<<"json">>, MapVal),
                    {ok, <<"json">>, MapVal, CanonicalJson};
                {error, RuleReason} ->
                    {error, <<"ERR_SCHEMA_VIOLATION">>, 422, RuleReason}
            end;
        _ ->
            {error, <<"ERR_INVALID_INPUT">>, 400, <<"Value must be a valid JSON object">>}
    end;
validate_value_and_rules(<<"any">>, V, Rules) ->
    if
        is_integer(V) -> validate_value_and_rules(<<"int64">>, V, Rules);
        is_boolean(V) -> validate_value_and_rules(<<"bool">>, V, Rules);
        is_map(V) -> validate_value_and_rules(<<"json">>, V, Rules);
        is_binary(V) -> validate_value_and_rules(<<"string">>, V, Rules);
        true -> {error, <<"ERR_INVALID_INPUT">>, 400, <<"Unsupported value type">>}
    end;
validate_value_and_rules(_, _, _) ->
    {error, <<"ERR_INVALID_INPUT">>, 400, <<"Value does not match declared schema type">>}.

check_numeric_rules(IntVal, Rules) ->
    MinCheck = case maps:get(<<"min_value">>, Rules, undefined) of
        undefined -> ok;
        Min when IntVal < Min ->
            {error, iolist_to_binary([<<"Schema violation: value ">>, integer_to_binary(IntVal),
                                      <<" is less than min_value ">>, integer_to_binary(Min)])};
        _ -> ok
    end,
    case MinCheck of
        {error, _} = E -> E;
        ok ->
            case maps:get(<<"max_value">>, Rules, undefined) of
                undefined -> ok;
                Max when IntVal > Max ->
                    {error, iolist_to_binary([<<"Schema violation: value ">>, integer_to_binary(IntVal),
                                              <<" exceeds max_value ">>, integer_to_binary(Max)])};
                _ -> ok
            end
    end.

check_json_rules(MapVal, Rules) ->
    ReqFields = maps:get(<<"required_fields">>, Rules, []),
    Missing = [F || F <- ReqFields, not maps:is_key(F, MapVal)],
    case Missing of
        [FirstMissing | _] ->
            {error, iolist_to_binary([<<"Schema violation: missing required JSON field '">>, FirstMissing, <<"'">>])};
        [] ->
            FieldsSpec = maps:get(<<"fields">>, Rules, #{}),
            case check_fields_spec(MapVal, FieldsSpec) of
                {error, _} = Err -> Err;
                ok ->
                    AdditionalFields = maps:get(<<"additional_fields">>, Rules, true),
                    case AdditionalFields of
                        false ->
                            Unexpected = [K || K <- maps:keys(MapVal), not maps:is_key(K, FieldsSpec)],
                            case Unexpected of
                                [FirstUnexp | _] ->
                                    {error, iolist_to_binary([<<"Schema violation: unexpected field '">>, FirstUnexp, <<"' (additional_fields=false)">>])};
                                [] -> ok
                            end;
                        _ -> ok
                    end
            end
    end.

check_fields_spec(MapVal, FieldsSpec) when is_map(FieldsSpec) ->
    check_fields_iter(maps:keys(FieldsSpec), MapVal, FieldsSpec);
check_fields_spec(_MapVal, _FieldsSpec) ->
    ok.

check_fields_iter([], _MapVal, _FieldsSpec) ->
    ok;
check_fields_iter([FName | Rest], MapVal, FieldsSpec) ->
    Spec = maps:get(FName, FieldsSpec),
    IsReq = maps:get(<<"required">>, Spec, false),
    case maps:is_key(FName, MapVal) of
        false ->
            if IsReq ->
                {error, iolist_to_binary([<<"Schema violation: missing required field '">>, FName, <<"'">>])};
               true ->
                check_fields_iter(Rest, MapVal, FieldsSpec)
            end;
        true ->
            FVal = maps:get(FName, MapVal),
            ExpectedType = maps:get(<<"type">>, Spec, <<"any">>),
            case validate_field_type_and_bounds(FName, ExpectedType, FVal, Spec) of
                ok -> check_fields_iter(Rest, MapVal, FieldsSpec);
                {error, _} = E -> E
            end
    end.

validate_field_type_and_bounds(FName, ExpectedType, FVal, Spec) ->
    TypeMatch = case ExpectedType of
        <<"int64">> -> is_integer(FVal);
        <<"string">> -> is_binary(FVal);
        <<"bool">> -> is_boolean(FVal);
        <<"null">> -> FVal =:= null orelse FVal =:= undefined;
        <<"object">> -> is_map(FVal);
        <<"array">> -> is_list(FVal);
        <<"any">> -> true;
        _ -> false
    end,
    case TypeMatch of
        false ->
            {error, iolist_to_binary([<<"Schema violation: field '">>, FName, <<"' must be ">>, ExpectedType])};
        true ->
            case ExpectedType of
                <<"int64">> ->
                    case check_numeric_rules(FVal, Spec) of
                        ok -> ok;
                        {error, Reason} ->
                            {error, iolist_to_binary([<<"Schema violation: field '">>, FName, <<"' ">>, Reason])}
                    end;
                <<"string">> ->
                    MinL = maps:get(<<"min_length">>, Spec, 0),
                    MaxL = maps:get(<<"max_length">>, Spec, undefined),
                    ByteSize = byte_size(FVal),
                    if ByteSize < MinL ->
                        {error, iolist_to_binary([<<"Schema violation: field '">>, FName, <<"' shorter than min_length">>])};
                       is_integer(MaxL), ByteSize > MaxL ->
                        {error, iolist_to_binary([<<"Schema violation: field '">>, FName, <<"' longer than max_length">>])};
                       true -> ok
                    end;
                _ -> ok
            end
    end.

check_cas_version(_Backend, _Ns, _Key, undefined, _WasmCtx) ->
    ok;
check_cas_version(<<"wasm_slot">>, Ns, Key, ExpectedVer, {WasmCallFun, WasmBase, NodeMsg}) when is_integer(Key) ->
    HasKey = case Ns of
        <<"default">> -> WasmCallFun(WasmBase, <<"db_has">>, [Key], NodeMsg, 0);
        _ -> WasmCallFun(WasmBase, <<"db_has_ns">>, [namespace_id_64(Ns), Key], NodeMsg, 0)
    end,
    CurrVer = case {HasKey, Ns} of
        {1, <<"default">>} -> WasmCallFun(WasmBase, <<"db_version">>, [Key], NodeMsg, 0);
        {1, _} -> WasmCallFun(WasmBase, <<"db_version_ns">>, [namespace_id_64(Ns), Key], NodeMsg, 0);
        _ -> 0
    end,
    if CurrVer =:= ExpectedVer -> ok;
       true -> {error, <<"CAS expected_ver mismatch">>, CurrVer}
    end;
check_cas_version(<<"ets_doc">>, Ns, Key, ExpectedVer, _WasmCtx) ->
    KeyBin = to_key_bin(Key),
    CurrVer = case ets:lookup(?DOC_BACKEND_TABLE, {Ns, KeyBin}) of
        [{{Ns, KeyBin}, #{ <<"active">> := true, <<"ver">> := V }}] -> V;
        _ -> 0
    end,
    if CurrVer =:= ExpectedVer -> ok;
       true -> {error, <<"CAS expected_ver mismatch">>, CurrVer}
    end.

check_record_exists(<<"wasm_slot">>, <<"default">>, Key, {WasmCallFun, WasmBase, NodeMsg}) when is_integer(Key) ->
    WasmCallFun(WasmBase, <<"db_has">>, [Key], NodeMsg, 0) =:= 1;
check_record_exists(<<"wasm_slot">>, Ns, Key, {WasmCallFun, WasmBase, NodeMsg}) when is_integer(Key) ->
    NsId = namespace_id_64(Ns),
    HasFun = wasm_export_name(Ns, <<"has">>, <<"db_has_ns">>),
    WasmCallFun(WasmBase, HasFun, [NsId, Key], NodeMsg, 0) =:= 1;
check_record_exists(<<"ets_doc">>, Ns, Key, _WasmCtx) ->
    init_tables(),
    KeyBin = to_key_bin(Key),
    case ets:lookup(?DOC_BACKEND_TABLE, {Ns, KeyBin}) of
        [{{Ns, KeyBin}, #{ <<"active">> := true }}] -> true;
        _ -> false
    end;
check_record_exists(_Backend, _Ns, _Key, _WasmCtx) ->
    false.

%% ============================================================================
%% Canonical Encoding & 64-bit Merkle-XOR Hashing
%% ============================================================================

namespace_id_64(<<"default">>) ->
    0;
namespace_id_64(Ns) when is_binary(Ns) ->
    <<Int32:32/big-unsigned-integer, _/binary>> = crypto:hash(sha256, Ns),
    1000 + (Int32 rem 100000000).

canonical_encode_schema(Entry) when is_map(Entry) ->
    Ns = maps:get(<<"namespace">>, Entry, <<"default">>),
    Backend = maps:get(<<"backend">>, Entry, <<"ets_doc">>),
    SchemaVsn = maps:get(<<"schema_version">>, Entry, <<"1.0.0">>),
    KeyType = maps:get(<<"key_type">>, Entry, <<"string">>),
    ValType = maps:get(<<"value_type">>, Entry, <<"any">>),
    Rules = maps:get(<<"rules">>, Entry, #{}),
    RulesJson = canonical_encode_value(<<"json">>, Rules),
    ContractTxPart = case maps:get(<<"contract_tx_id">>, Entry, undefined) of
        CTx when is_binary(CTx), byte_size(CTx) > 0 ->
            [<<"\"contract_tx_id\":\"">>, CTx, <<"\",">>];
        _ -> []
    end,
    WasmShaPart = case maps:get(<<"wasm_sha256">>, Entry, undefined) of
        WSha when is_binary(WSha), byte_size(WSha) > 0 ->
            [<<",\"wasm_sha256\":\"">>, WSha, <<"\"">>];
        _ -> []
    end,
    WasmTxPart = case maps:get(<<"wasm_tx_id">>, Entry, undefined) of
        WTx when is_binary(WTx), byte_size(WTx) > 0 ->
            [<<",\"wasm_tx_id\":\"">>, WTx, <<"\"">>];
        _ -> []
    end,
    iolist_to_binary([
        <<"{\"backend\":\"">>, Backend, <<"\",">>,
        ContractTxPart,
        <<"\"key_type\":\"">>, KeyType,
        <<"\",\"namespace\":\"">>, Ns,
        <<"\",\"rules\":">>, RulesJson,
        <<",\"schema_version\":\"">>, SchemaVsn,
        <<"\",\"value_type\":\"">>, ValType, <<"\"">>,
        WasmShaPart,
        WasmTxPart,
        <<"}">>
    ]).

canonical_encode_value(<<"int64">>, V) when is_integer(V) ->
    integer_to_binary(V);
canonical_encode_value(<<"bool">>, true) ->
    <<"true">>;
canonical_encode_value(<<"bool">>, false) ->
    <<"false">>;
canonical_encode_value(<<"string">>, V) when is_binary(V) ->
    V;
canonical_encode_value(<<"bytes">>, V) when is_binary(V) ->
    V;
canonical_encode_value(<<"json">>, Map) when is_map(Map) ->
    Keys = lists:sort(maps:keys(Map)),
    Pairs = lists:map(fun(K) ->
        KBin = to_key_bin(K),
        V = maps:get(K, Map),
        VBin = iolist_to_binary(json:encode(V)),
        iolist_to_binary([<<"\"">>, KBin, <<"\":">>, VBin])
    end, Keys),
    iolist_to_binary([<<"{">>, lists:join(<<",">>, Pairs), <<"}">>]);
canonical_encode_value(_, V) ->
    iolist_to_binary(json:encode(V)).

%% Deterministic 64-bit FNV-1a + SplitMix64 matching Rust hydb/src/lib.rs
canonical_hash_64(KeyInt, ValInt, VerInt) when is_integer(KeyInt), is_integer(ValInt), is_integer(VerInt) ->
    Mask64 = 16#FFFFFFFFFFFFFFFF,
    Prime = 16#100000001B3,
    UKey = KeyInt band Mask64,
    UVal = ValInt band Mask64,
    UVer = VerInt band Mask64,

    H0 = 16#CBF29CE484222325,
    H1 = ((H0 bxor UKey) * Prime) band Mask64,
    H2 = ((H1 bxor UVal) * Prime) band Mask64,
    H3 = ((H2 bxor UVer) * Prime) band Mask64,
    H4 = (H3 bxor (H3 bsr 30)) band Mask64,
    H5 = (H4 * 16#BF58476D1CE4E5B9) band Mask64,
    H6 = (H5 bxor (H5 bsr 27)) band Mask64,
    H7 = (H6 * 16#94D049BB133111EB) band Mask64,
    H8 = (H7 bxor (H7 bsr 31)) band Mask64,
    to_signed_64(H8).

to_signed_64(U) when U >= 16#8000000000000000 ->
    U - 16#10000000000000000;
to_signed_64(U) ->
    U.

bytes_to_u64(Bin) when is_binary(Bin) ->
    <<Int64:64/big-unsigned-integer, _/binary>> = crypto:hash(sha256, Bin),
    Int64.

%% ============================================================================
%% Backend Execution Adapters (wasm_slot & ets_doc)
%% ============================================================================

wasm_export_name(Ns, OpKey, DefaultFun) ->
    case get_namespace_config(Ns) of
        {ok, Cfg} ->
            case maps:get(<<"connector">>, Cfg, undefined) of
                #{ <<"exports">> := Exports } when is_map(Exports) ->
                    maps:get(OpKey, Exports, DefaultFun);
                _ ->
                    DefaultFun
            end;
        _ ->
            DefaultFun
    end.

execute_backend_put(<<"wasm_slot">>, <<"default">>, Key, Val, Seq, CurrentRoot, {WasmCallFun, WasmBase, NodeMsg}) ->
    NewRoot = WasmCallFun(WasmBase, <<"db_put">>, [Key, Val, Seq], NodeMsg, CurrentRoot),
    Ver = WasmCallFun(WasmBase, <<"db_version">>, [Key], NodeMsg, 1),
    {ok, NewRoot, Ver};
execute_backend_put(<<"wasm_slot">>, Ns, Key, Val, Seq, CurrentRoot, {WasmCallFun, WasmBase, NodeMsg}) ->
    NsId = namespace_id_64(Ns),
    PutFun = wasm_export_name(Ns, <<"put">>, <<"db_put_ns">>),
    VerFun = wasm_export_name(Ns, <<"version">>, <<"db_version_ns">>),
    NewRoot = WasmCallFun(WasmBase, PutFun, [NsId, Key, Val, Seq], NodeMsg, CurrentRoot),
    Ver = WasmCallFun(WasmBase, VerFun, [NsId, Key], NodeMsg, 1),
    {ok, NewRoot, Ver};
execute_backend_put(<<"ets_doc">>, Ns, Key, Val, Seq, CurrentRoot, _WasmCtx) ->
    init_tables(),
    KeyBin = to_key_bin(Key),
    KeyHashInt = bytes_to_u64(iolist_to_binary([Ns, <<":">>, KeyBin])),
    ValBin = if is_map(Val) -> canonical_encode_value(<<"json">>, Val);
                is_binary(Val) -> Val;
                is_integer(Val) -> integer_to_binary(Val);
                is_boolean(Val) -> atom_to_binary(Val, utf8)
             end,
    ValHashInt = bytes_to_u64(ValBin),
    {OldRecordHash, NewVer} = case ets:lookup(?DOC_BACKEND_TABLE, {Ns, KeyBin}) of
        [{{Ns, KeyBin}, #{ <<"active">> := true, <<"val_hash">> := OldVH, <<"ver">> := OldV }}] ->
            {canonical_hash_64(KeyHashInt, OldVH, OldV), OldV + 1};
        [{{Ns, KeyBin}, #{ <<"active">> := false, <<"ver">> := TombstoneV }}] ->
            {0, TombstoneV + 1};
        [] ->
            {0, 1}
    end,
    NewRecordHash = canonical_hash_64(KeyHashInt, ValHashInt, NewVer),
    NewRoot = to_signed_64(((CurrentRoot bxor OldRecordHash) bxor NewRecordHash) band 16#FFFFFFFFFFFFFFFF),
    ets:insert(?DOC_BACKEND_TABLE, {{Ns, KeyBin}, #{
        <<"active">> => true,
        <<"key">> => KeyBin,
        <<"val">> => ValBin,
        <<"val_hash">> => ValHashInt,
        <<"ver">> => NewVer,
        <<"last_seq">> => Seq
    }}),
    {ok, NewRoot, NewVer}.

execute_backend_del(<<"wasm_slot">>, <<"default">>, Key, Seq, {WasmCallFun, WasmBase, NodeMsg}) ->
    NewRoot = WasmCallFun(WasmBase, <<"db_del">>, [Key, Seq], NodeMsg, 0),
    Ver = WasmCallFun(WasmBase, <<"db_version">>, [Key], NodeMsg, 1),
    {ok, NewRoot, Ver};
execute_backend_del(<<"wasm_slot">>, Ns, Key, Seq, {WasmCallFun, WasmBase, NodeMsg}) ->
    NsId = namespace_id_64(Ns),
    DelFun = wasm_export_name(Ns, <<"del">>, <<"db_del_ns">>),
    VerFun = wasm_export_name(Ns, <<"version">>, <<"db_version_ns">>),
    NewRoot = WasmCallFun(WasmBase, DelFun, [NsId, Key, Seq], NodeMsg, 0),
    Ver = WasmCallFun(WasmBase, VerFun, [NsId, Key], NodeMsg, 1),
    {ok, NewRoot, Ver};
execute_backend_del(<<"ets_doc">>, Ns, Key, Seq, {CurrentRoot}) ->
    init_tables(),
    KeyBin = to_key_bin(Key),
    KeyHashInt = bytes_to_u64(iolist_to_binary([Ns, <<":">>, KeyBin])),
    {NewRoot, TombstoneVer} = case ets:lookup(?DOC_BACKEND_TABLE, {Ns, KeyBin}) of
        [{{Ns, KeyBin}, Rec = #{ <<"active">> := true, <<"val_hash">> := OldVH, <<"ver">> := OldV }}] ->
            OldH = canonical_hash_64(KeyHashInt, OldVH, OldV),
            NewV = OldV + 1,
            ets:insert(?DOC_BACKEND_TABLE, {{Ns, KeyBin}, Rec#{ <<"active">> => false, <<"ver">> => NewV, <<"last_seq">> => Seq }}),
            {to_signed_64((CurrentRoot bxor OldH) band 16#FFFFFFFFFFFFFFFF), NewV};
        _ ->
            {CurrentRoot, 0}
    end,
    {ok, NewRoot, TombstoneVer}.

execute_backend_get(<<"wasm_slot">>, <<"default">>, Key, {WasmCallFun, WasmBase, NodeMsg}) ->
    HasKey = WasmCallFun(WasmBase, <<"db_has">>, [Key], NodeMsg, 0),
    case HasKey of
        1 ->
            Val = WasmCallFun(WasmBase, <<"db_get">>, [Key], NodeMsg, -1),
            Ver = WasmCallFun(WasmBase, <<"db_version">>, [Key], NodeMsg, 1),
            {ok, #{ <<"found">> => true, <<"val">> => Val, <<"ver">> => Ver, <<"val_type">> => <<"int64">> }};
        _ ->
            {error, <<"ERR_KEY_NOT_FOUND">>, 404}
    end;
execute_backend_get(<<"wasm_slot">>, Ns, Key, {WasmCallFun, WasmBase, NodeMsg}) ->
    NsId = namespace_id_64(Ns),
    HasFun = wasm_export_name(Ns, <<"has">>, <<"db_has_ns">>),
    GetFun = wasm_export_name(Ns, <<"get">>, <<"db_get_ns">>),
    VerFun = wasm_export_name(Ns, <<"version">>, <<"db_version_ns">>),
    HasKey = WasmCallFun(WasmBase, HasFun, [NsId, Key], NodeMsg, 0),
    case HasKey of
        1 ->
            Val = WasmCallFun(WasmBase, GetFun, [NsId, Key], NodeMsg, -1),
            Ver = WasmCallFun(WasmBase, VerFun, [NsId, Key], NodeMsg, 1),
            {ok, #{ <<"found">> => true, <<"val">> => Val, <<"ver">> => Ver, <<"val_type">> => <<"int64">> }};
        _ ->
            {error, <<"ERR_KEY_NOT_FOUND">>, 404}
    end;
execute_backend_get(<<"ets_doc">>, Ns, Key, _WasmCtx) ->
    init_tables(),
    KeyBin = to_key_bin(Key),
    case ets:lookup(?DOC_BACKEND_TABLE, {Ns, KeyBin}) of
        [{{Ns, KeyBin}, #{ <<"active">> := true, <<"val">> := Val, <<"ver">> := Ver }}] ->
            {ok, #{ <<"found">> => true, <<"val">> => Val, <<"ver">> => Ver }};
        _ ->
            {error, <<"ERR_KEY_NOT_FOUND">>, 404}
    end.

execute_backend_exists(<<"wasm_slot">>, <<"default">>, Key, {WasmCallFun, WasmBase, NodeMsg}) ->
    HasKey = WasmCallFun(WasmBase, <<"db_has">>, [Key], NodeMsg, 0),
    Ver = WasmCallFun(WasmBase, <<"db_version">>, [Key], NodeMsg, 0),
    {ok, #{ <<"exists">> => (HasKey == 1), <<"ver">> => Ver }};
execute_backend_exists(<<"wasm_slot">>, Ns, Key, {WasmCallFun, WasmBase, NodeMsg}) ->
    NsId = namespace_id_64(Ns),
    HasFun = wasm_export_name(Ns, <<"has">>, <<"db_has_ns">>),
    VerFun = wasm_export_name(Ns, <<"version">>, <<"db_version_ns">>),
    HasKey = WasmCallFun(WasmBase, HasFun, [NsId, Key], NodeMsg, 0),
    Ver = WasmCallFun(WasmBase, VerFun, [NsId, Key], NodeMsg, 0),
    {ok, #{ <<"exists">> => (HasKey == 1), <<"ver">> => Ver }};
execute_backend_exists(<<"ets_doc">>, Ns, Key, _WasmCtx) ->
    init_tables(),
    KeyBin = to_key_bin(Key),
    case ets:lookup(?DOC_BACKEND_TABLE, {Ns, KeyBin}) of
        [{{Ns, KeyBin}, #{ <<"active">> := true, <<"ver">> := Ver }}] ->
            {ok, #{ <<"exists">> => true, <<"ver">> => Ver }};
        [{{Ns, KeyBin}, #{ <<"active">> := false, <<"ver">> := TombstoneVer }}] ->
            {ok, #{ <<"exists">> => false, <<"ver">> => TombstoneVer }};
        _ ->
            {ok, #{ <<"exists">> => false, <<"ver">> => 0 }}
    end.

execute_backend_scan(<<"ets_doc">>, Ns, PrefixBin) ->
    init_tables(),
    PrefSize = byte_size(PrefixBin),
    Matches = ets:foldl(fun
        ({{EntryNs, KeyBin}, #{ <<"active">> := true, <<"val">> := Val, <<"ver">> := Ver }}, Acc)
          when EntryNs =:= Ns ->
            case KeyBin of
                <<PrefixBin:PrefSize/binary, _/binary>> ->
                    [#{ <<"key">> => KeyBin, <<"val">> => Val, <<"ver">> => Ver } | Acc];
                _ -> Acc
            end;
        (_, Acc) -> Acc
    end, [], ?DOC_BACKEND_TABLE),
    Sorted = lists:sort(fun(A, B) -> maps:get(<<"key">>, A) =< maps:get(<<"key">>, B) end, Matches),
    {ok, Sorted};
execute_backend_scan(Backend, Ns, _PrefixBin) ->
    {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, 501,
     iolist_to_binary([<<"Backend '">>, Backend, <<"' for namespace '">>, Ns, <<"' does not support prefix_scan">>])}.

%% Helpers

to_int_strict(V) when is_integer(V) -> {ok, V};
to_int_strict(V) when is_binary(V) ->
    try {ok, binary_to_integer(V)}
    catch _:_ -> error
    end;
to_int_strict(_) -> error.

to_key_bin(K) when is_binary(K) -> K;
to_key_bin(K) when is_integer(K) -> integer_to_binary(K);
to_key_bin(K) when is_atom(K) -> atom_to_binary(K, utf8).
