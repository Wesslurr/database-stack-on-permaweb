-module(hydb_spec).

%% ============================================================================
%% HyDB Canonical Contract Specification Reference — hydb.spec/1 in Erlang
%% ============================================================================
%% Byte-for-byte canonical TLV encoding, SHA-256 event hashing, manifest hashing,
%% schema validation, record version semantics, and state commitment.
%% Aligns with spec/hydb_contract_spec.mjs.
%% ============================================================================

-export([
    spec_version/0,
    contract_version/0,
    genesis_hash/0,
    encode_value/1,
    decode_value/1,
    encode_event/1,
    event_hash/1,
    encode_manifest/1,
    manifest_hash/1,
    encode_state/2,
    state_commitment/2,
    validate_schema/1,
    check_rules/2,
    schema_to_h/1
]).

-define(SPEC_VERSION, <<"hydb.spec/1">>).
-define(CONTRACT_VERSION, <<"hydb.core@1.0">>).
-define(MAGIC, <<"HYDB">>).
-define(ENCODING_VERSION, 1).
-define(GENESIS_HASH, <<"0000000000000000000000000000000000000000000000000000000000000000">>).

-define(TAG_INT64, 1).
-define(TAG_BOOL, 2).
-define(TAG_STRING, 3).
-define(TAG_BYTES, 4).
-define(TAG_NULL, 5).
-define(TAG_ARRAY, 6).
-define(TAG_OBJECT, 7).
-define(TAG_ABSENT, 8).

spec_version() -> ?SPEC_VERSION.
contract_version() -> ?CONTRACT_VERSION.
genesis_hash() -> ?GENESIS_HASH.

%% ============================================================================
%% Canonical TLV Value Encoding
%% ============================================================================

encode_value(undefined) -> <<?TAG_ABSENT>>;
encode_value(absent) -> <<?TAG_ABSENT>>;
encode_value(null) -> <<?TAG_NULL>>;
encode_value({int64, Val}) when is_integer(Val) ->
    <<?TAG_INT64, Val:64/big-signed-integer>>;
encode_value({bool, true}) ->
    <<?TAG_BOOL, 1>>;
encode_value({bool, false}) ->
    <<?TAG_BOOL, 0>>;
encode_value({string, Str}) when is_binary(Str) ->
    Len = byte_size(Str),
    <<?TAG_STRING, Len:32/big-unsigned-integer, Str/binary>>;
encode_value({bytes, Bin}) when is_binary(Bin) ->
    Len = byte_size(Bin),
    <<?TAG_BYTES, Len:32/big-unsigned-integer, Bin/binary>>;
encode_value({null}) ->
    <<?TAG_NULL>>;
encode_value({array, Elements}) when is_list(Elements) ->
    Count = length(Elements),
    EncodedElems = [encode_value(El) || El <- Elements],
    iolist_to_binary([<<?TAG_ARRAY, Count:32/big-unsigned-integer>>, EncodedElems]);
encode_value({object, Pairs}) when is_list(Pairs) ->
    %% Sort entries bytewise by UTF-8 key bytes
    Sorted = lists:sort(fun({K1, _}, {K2, _}) ->
        K1Bin = to_bin(K1),
        K2Bin = to_bin(K2),
        K1Bin =< K2Bin
    end, Pairs),
    check_no_duplicate_keys(Sorted),
    Count = length(Sorted),
    EncodedEntries = lists:map(fun({K, V}) ->
        KBin = to_bin(K),
        KLen = byte_size(KBin),
        VEnc = encode_value(V),
        <<KLen:32/big-unsigned-integer, KBin/binary, VEnc/binary>>
    end, Sorted),
    iolist_to_binary([<<?TAG_OBJECT, Count:32/big-unsigned-integer>>, EncodedEntries]);
encode_value(Map) when is_map(Map) ->
    %% Convert map to object pairs
    Pairs = maps:to_list(Map),
    encode_value({object, Pairs}).

to_bin(B) when is_binary(B) -> B;
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(L) when is_list(L) -> list_to_binary(L).

check_no_duplicate_keys([]) -> ok;
check_no_duplicate_keys([_]) -> ok;
check_no_duplicate_keys([{K1, _}, {K2, _} | _]) when K1 =:= K2 ->
    error({duplicate_object_key, K1});
check_no_duplicate_keys([_ | Rest]) ->
    check_no_duplicate_keys(Rest).

%% ============================================================================
%% Decode TLV Value (minimal reader for tests)
%% ============================================================================

decode_value(<<?TAG_ABSENT, Rest/binary>>) -> {absent, Rest};
decode_value(<<?TAG_NULL, Rest/binary>>) -> {null, Rest};
decode_value(<<?TAG_INT64, Val:64/big-signed-integer, Rest/binary>>) -> {{int64, Val}, Rest};
decode_value(<<?TAG_BOOL, 1, Rest/binary>>) -> {{bool, true}, Rest};
decode_value(<<?TAG_BOOL, 0, Rest/binary>>) -> {{bool, false}, Rest};
decode_value(<<?TAG_STRING, Len:32/big-unsigned-integer, Str:Len/binary, Rest/binary>>) ->
    {{string, Str}, Rest};
decode_value(<<?TAG_BYTES, Len:32/big-unsigned-integer, Bytes:Len/binary, Rest/binary>>) ->
    {{bytes, Bytes}, Rest};
decode_value(<<?TAG_ARRAY, Count:32/big-unsigned-integer, Rest/binary>>) ->
    {Elems, Remaining} = decode_array_elements(Count, Rest, []),
    {{array, Elems}, Remaining};
decode_value(<<?TAG_OBJECT, Count:32/big-unsigned-integer, Rest/binary>>) ->
    {Pairs, Remaining} = decode_object_pairs(Count, Rest, []),
    {{object, Pairs}, Remaining};
decode_value(_) ->
    error(err_encoding).

decode_array_elements(0, Rest, Acc) -> {lists:reverse(Acc), Rest};
decode_array_elements(N, Rest, Acc) ->
    {Elem, Rest1} = decode_value(Rest),
    decode_array_elements(N - 1, Rest1, [Elem | Acc]).

decode_object_pairs(0, Rest, Acc) -> {lists:reverse(Acc), Rest};
decode_object_pairs(N, <<KLen:32/big-unsigned-integer, Key:KLen/binary, Rest/binary>>, Acc) ->
    {Val, Rest1} = decode_value(Rest),
    decode_object_pairs(N - 1, Rest1, [{Key, Val} | Acc]).

%% ============================================================================
%% Canonical Event Encoding & Hashing
%% ============================================================================

encode_event(Ev) when is_map(Ev) ->
    ContractVsn = maps:get(contract_version, Ev, ?CONTRACT_VERSION),
    PrevHashHex = maps:get(prev_hash, Ev, ?GENESIS_HASH),
    PrevHashBin = hex_to_bin_32(PrevHashHex),
    Epoch = maps:get(epoch, Ev),
    Seq = maps:get(seq, Ev),
    Op = maps:get(op, Ev),
    Ns = maps:get(namespace, Ev),
    SchemaVsn = maps:get(schema_version, Ev),
    Key = maps:get(key, Ev, absent),
    ValType = maps:get(value_type, Ev),
    Val = maps:get(value, Ev, absent),
    Ver = maps:get(ver, Ev),

    FrameHeader = <<?MAGIC/binary, $E, ?ENCODING_VERSION>>,
    iolist_to_binary([
        FrameHeader,
        encode_value({string, to_bin(ContractVsn)}),
        encode_value({bytes, PrevHashBin}),
        encode_value({int64, Epoch}),
        encode_value({int64, Seq}),
        encode_value({string, to_bin(Op)}),
        encode_value({string, to_bin(Ns)}),
        encode_value({string, to_bin(SchemaVsn)}),
        encode_value(Key),
        encode_value({string, to_bin(ValType)}),
        encode_value(Val),
        encode_value({int64, Ver})
    ]).

event_hash(Ev) ->
    Bytes = encode_event(Ev),
    binary:encode_hex(crypto:hash(sha256, Bytes)).

hex_to_bin_32(Hex) when is_binary(Hex), byte_size(Hex) =:= 64 ->
    binary:decode_hex(Hex);
hex_to_bin_32(Bin) when is_binary(Bin), byte_size(Bin) =:= 32 ->
    Bin.

%% ============================================================================
%% Canonical Manifest Encoding & Hashing
%% ============================================================================

schema_to_h(Schema) when is_map(Schema) ->
    Backend = maps:get(<<"backend">>, Schema, maps:get(backend, Schema, <<"ets_doc">>)),
    KeyType = maps:get(<<"key_type">>, Schema, maps:get(key_type, Schema, <<"string">>)),
    Ns = maps:get(<<"namespace">>, Schema, maps:get(namespace, Schema, <<"default">>)),
    RequireAuth = maps:get(<<"require_auth">>, Schema, maps:get(require_auth, Schema, false)),
    Requires = maps:get(<<"requires">>, Schema, maps:get(requires, Schema, [])),
    Rules = maps:get(<<"rules">>, Schema, maps:get(rules, Schema, #{})),
    SchemaVsn = maps:get(<<"schema_version">>, Schema, maps:get(schema_version, Schema, <<"1.0.0">>)),
    ValType = maps:get(<<"value_type">>, Schema, maps:get(value_type, Schema, <<"int64">>)),

    ReqSorted = lists:sort([to_bin(R) || R <- Requires]),
    RulesEntries = encode_rules_entries(Rules),

    {object, [
        {<<"backend">>, {string, to_bin(Backend)}},
        {<<"key_type">>, {string, to_bin(KeyType)}},
        {<<"namespace">>, {string, to_bin(Ns)}},
        {<<"require_auth">>, {bool, RequireAuth}},
        {<<"requires">>, {array, [{string, R} || R <- ReqSorted]}},
        {<<"rules">>, {object, RulesEntries}},
        {<<"schema_version">>, {string, to_bin(SchemaVsn)}},
        {<<"value_type">>, {string, to_bin(ValType)}}
    ]}.

encode_rules_entries(Rules) when is_map(Rules) ->
    Pairs = maps:to_list(Rules),
    lists:map(fun({K, V}) ->
        KBin = to_bin(K),
        VEnc = case KBin of
            <<"fields">> when is_map(V) ->
                FieldEntries = lists:map(fun({FName, FSpec}) ->
                    FNBin = to_bin(FName),
                    FSorted = lists:map(fun({FK, FV}) ->
                        FKB = to_bin(FK),
                        FE = if is_integer(FV) -> {int64, FV};
                                is_boolean(FV) -> {bool, FV};
                                true -> {string, to_bin(FV)}
                             end,
                        {FKB, FE}
                    end, maps:to_list(FSpec)),
                    {FNBin, {object, FSorted}}
                end, maps:to_list(V)),
                {object, FieldEntries};
            <<"additional_fields">> when is_boolean(V) ->
                {bool, V};
            _ when is_integer(V) ->
                {int64, V};
            _ when is_boolean(V) ->
                {bool, V};
            _ ->
                {string, to_bin(V)}
        end,
        {KBin, VEnc}
    end, Pairs).

encode_manifest(Schema) when is_map(Schema) ->
    FrameHeader = <<?MAGIC/binary, $M, ?ENCODING_VERSION>>,
    HVal = schema_to_h(Schema),
    iolist_to_binary([FrameHeader, encode_value(HVal)]).

manifest_hash(Schema) ->
    Bytes = encode_manifest(Schema),
    binary:encode_hex(crypto:hash(sha256, Bytes)).

%% ============================================================================
%% Canonical State Commitment
%% ============================================================================

encode_state(SchemasList, RecordsList) when is_list(SchemasList), is_list(RecordsList) ->
    %% Schemas: list of {NamespaceBin, SchemaVsnBin, RegVerInt, ManifestHashHexOrBin}
    %% Sorted bytewise by NamespaceBin
    SortedSchemas = lists:sort(fun({Ns1, _, _, _}, {Ns2, _, _, _}) ->
        to_bin(Ns1) =< to_bin(Ns2)
    end, SchemasList),

    %% Records: list of {NamespaceBin, KeyHVal, VerInt, ValHValOrAbsent}
    %% Sorted bytewise by (NamespaceBin, then canonical Key TLV bytes)
    SortedRecords = lists:sort(fun({Ns1, K1, _, _}, {Ns2, K2, _, _}) ->
        Ns1B = to_bin(Ns1),
        Ns2B = to_bin(Ns2),
        if Ns1B =:= Ns2B ->
            K1Enc = encode_value(K1),
            K2Enc = encode_value(K2),
            K1Enc =< K2Enc;
           true ->
            Ns1B < Ns2B
        end
    end, RecordsList),

    FrameHeader = <<?MAGIC/binary, $S, ?ENCODING_VERSION>>,
    SchemaCount = length(SortedSchemas),
    RecordCount = length(SortedRecords),

    EncodedSchemas = lists:map(fun({Ns, SchemaVsn, RegVer, MHash}) ->
        MHashBin = hex_to_bin_32(MHash),
        [
            encode_value({string, to_bin(Ns)}),
            encode_value({string, to_bin(SchemaVsn)}),
            encode_value({int64, RegVer}),
            encode_value({bytes, MHashBin})
        ]
    end, SortedSchemas),

    EncodedRecords = lists:map(fun({Ns, KeyH, Ver, ValH}) ->
        [
            encode_value({string, to_bin(Ns)}),
            encode_value(KeyH),
            encode_value({int64, Ver}),
            encode_value(ValH)
        ]
    end, SortedRecords),

    iolist_to_binary([
        FrameHeader,
        <<SchemaCount:32/big-unsigned-integer>>,
        EncodedSchemas,
        <<RecordCount:32/big-unsigned-integer>>,
        EncodedRecords
    ]).

state_commitment(SchemasList, RecordsList) ->
    Bytes = encode_state(SchemasList, RecordsList),
    binary:encode_hex(crypto:hash(sha256, Bytes)).

%% ============================================================================
%% Schema Validation & Capabilities
%% ============================================================================

validate_schema(Schema) when is_map(Schema) ->
    Ns = maps:get(<<"namespace">>, Schema, maps:get(namespace, Schema, undefined)),
    SchemaVsn = maps:get(<<"schema_version">>, Schema, maps:get(schema_version, Schema, undefined)),
    Backend = maps:get(<<"backend">>, Schema, maps:get(backend, Schema, undefined)),
    KeyType = maps:get(<<"key_type">>, Schema, maps:get(key_type, Schema, undefined)),
    ValType = maps:get(<<"value_type">>, Schema, maps:get(value_type, Schema, undefined)),

    case is_valid_namespace(Ns) of
        false -> {error, <<"ERR_INVALID_SCHEMA">>, <<"invalid namespace">>};
        true ->
            case is_valid_semver(SchemaVsn) of
                false -> {error, <<"ERR_INVALID_SCHEMA">>, <<"schema_version must be MAJOR.MINOR.PATCH">>};
                true ->
                    case check_backend_capability(Backend, KeyType, ValType) of
                        {error, Code, Detail} -> {error, Code, Detail};
                        ok ->
                            Requires = maps:get(<<"requires">>, Schema, maps:get(requires, Schema, [])),
                            case check_requires(Backend, KeyType, Requires) of
                                {error, Code2, Detail2} -> {error, Code2, Detail2};
                                ok ->
                                    Rules = maps:get(<<"rules">>, Schema, maps:get(rules, Schema, #{})),
                                    check_rules_definition(Backend, ValType, Rules)
                            end
                    end
            end
    end.

is_valid_namespace(Ns) when is_binary(Ns) ->
    case re:run(Ns, "^[a-z0-9][a-z0-9._-]{0,63}$") of
        {match, _} -> true;
        _ -> false
    end;
is_valid_namespace(_) -> false.

is_valid_semver(Vsn) when is_binary(Vsn) ->
    case re:run(Vsn, "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)$") of
        {match, _} -> true;
        _ -> false
    end;
is_valid_semver(_) -> false.

check_backend_capability(<<"wasm_slot">>, <<"int64">>, <<"int64">>) -> ok;
check_backend_capability(<<"wasm_slot">>, KeyType, ValType) ->
    if KeyType =/= <<"int64">> -> {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, <<"wasm_slot only supports int64 key">>};
       ValType =/= <<"int64">> -> {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, <<"wasm_slot only supports int64 value">>};
       true -> ok
    end;
check_backend_capability(<<"ets_doc">>, KeyType, ValType) ->
    ValidKeys = [<<"int64">>, <<"string">>],
    ValidVals = [<<"int64">>, <<"bool">>, <<"string">>, <<"bytes">>, <<"json">>],
    case lists:member(KeyType, ValidKeys) of
        false -> {error, <<"ERR_INVALID_SCHEMA">>, <<"invalid key_type">>};
        true ->
            case lists:member(ValType, ValidVals) of
                false -> {error, <<"ERR_INVALID_SCHEMA">>, <<"invalid value_type">>};
                true -> ok
            end
    end;
check_backend_capability(_, _, _) ->
    {error, <<"ERR_INVALID_SCHEMA">>, <<"unknown backend">>}.

check_requires(_, _, []) -> ok;
check_requires(<<"wasm_slot">>, _, ReqList) ->
    Allowed = [<<"get">>, <<"put">>, <<"del">>, <<"exists">>, <<"cas">>],
    case lists:all(fun(R) -> lists:member(R, Allowed) end, ReqList) of
        true -> ok;
        false -> {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, <<"wasm_slot lacks required capability">>}
    end;
check_requires(<<"ets_doc">>, KeyType, ReqList) ->
    Allowed = [<<"get">>, <<"put">>, <<"del">>, <<"exists">>, <<"cas">>, <<"prefix_scan">>],
    case lists:all(fun(R) -> lists:member(R, Allowed) end, ReqList) of
        false -> {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, <<"ets_doc lacks capability">>};
        true ->
            case lists:member(<<"prefix_scan">>, ReqList) of
                true when KeyType =/= <<"string">> ->
                    {error, <<"ERR_CAPABILITY_UNSUPPORTED">>, <<"prefix_scan requires string keys">>};
                _ -> ok
            end
    end.

check_rules_definition(<<"wasm_slot">>, <<"int64">>, Rules) ->
    Min = maps:get(<<"min_value">>, Rules, maps:get(min_value, Rules, undefined)),
    Max = maps:get(<<"max_value">>, Rules, maps:get(max_value, Rules, undefined)),
    if Min =/= undefined andalso Max =/= undefined andalso Min > Max ->
        {error, <<"ERR_INVALID_SCHEMA">>, <<"min_value > max_value">>};
       true -> ok
    end;
check_rules_definition(<<"ets_doc">>, _, Rules) ->
    MinL = maps:get(<<"min_length">>, Rules, maps:get(min_length, Rules, undefined)),
    MaxL = maps:get(<<"max_length">>, Rules, maps:get(max_length, Rules, undefined)),
    if MinL =/= undefined andalso MaxL =/= undefined andalso MinL > MaxL ->
        {error, <<"ERR_INVALID_SCHEMA">>, <<"min_length > max_length">>};
       true -> ok
    end;
check_rules_definition(_, _, _) -> ok.

%% ============================================================================
%% Value Rule Checks (throws error or returns ok / {error, ...})
%% ============================================================================

check_rules(Schema, {int64, V}) ->
    Rules = maps:get(<<"rules">>, Schema, maps:get(rules, Schema, #{})),
    Min = maps:get(<<"min_value">>, Rules, maps:get(min_value, Rules, undefined)),
    Max = maps:get(<<"max_value">>, Rules, maps:get(max_value, Rules, undefined)),
    if Min =/= undefined andalso V < Min ->
        {error, <<"ERR_SCHEMA_VIOLATION">>, <<"below min_value">>};
       Max =/= undefined andalso V > Max ->
        {error, <<"ERR_SCHEMA_VIOLATION">>, <<"above max_value">>};
       true -> ok
    end;
check_rules(Schema, {string, Str}) ->
    Rules = maps:get(<<"rules">>, Schema, maps:get(rules, Schema, #{})),
    MinL = maps:get(<<"min_length">>, Rules, maps:get(min_length, Rules, undefined)),
    MaxL = maps:get(<<"max_length">>, Rules, maps:get(max_length, Rules, undefined)),
    Len = byte_size(Str),
    if MinL =/= undefined andalso Len < MinL ->
        {error, <<"ERR_SCHEMA_VIOLATION">>, <<"shorter than min_length">>};
       MaxL =/= undefined andalso Len > MaxL ->
        {error, <<"ERR_SCHEMA_VIOLATION">>, <<"longer than max_length">>};
       true -> ok
    end;
check_rules(_Schema, _) ->
    ok.
