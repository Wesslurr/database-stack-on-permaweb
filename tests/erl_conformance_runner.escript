#!/usr/bin/env escript
%% -*- coding: utf-8 -*-
%% Erlang Conformance Vector Runner for HyDB hydb.spec/1

main([Action]) ->
    run_action(list_to_binary(Action));
main(_) ->
    run_action(<<"all">>).

run_action(<<"all">>) ->
    DbDir = case os:getenv("DB_DIR") of
        false -> filename:dirname(filename:dirname(escript:script_name()));
        "" -> filename:dirname(filename:dirname(escript:script_name()));
        D -> D
    end,
    OutDir = filename:join([DbDir, "hymessage/out/ebin"]),
    filelib:ensure_dir(filename:join([OutDir, "dummy"])),
    code:add_path(OutDir),
    code:add_path(filename:join([DbDir, "hymessage/build/dev/erlang/hymessage/ebin"])),
    code:add_path(filename:join([DbDir, "vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib/hymessage_runtime-1.0.0/ebin"])),
    case code:ensure_loaded(hydb_spec) of
        {module, hydb_spec} -> ok;
        _ ->
            Src = filename:join([DbDir, "hymessage/src/hydb_spec.erl"]),
            case compile:file(Src, [{outdir, OutDir}, return_errors]) of
                {ok, hydb_spec} -> code:load_file(hydb_spec);
                _ -> ok
            end
    end,

    %% 1. Value encodings
    Int64Enc = binary:encode_hex(hydb_spec:encode_value({int64, 42})),
    Int64MinEnc = binary:encode_hex(hydb_spec:encode_value({int64, -9223372036854775808})),
    Int64MaxEnc = binary:encode_hex(hydb_spec:encode_value({int64, 9223372036854775807})),
    BoolTrueEnc = binary:encode_hex(hydb_spec:encode_value({bool, true})),
    BoolFalseEnc = binary:encode_hex(hydb_spec:encode_value({bool, false})),
    StrSimpleEnc = binary:encode_hex(hydb_spec:encode_value({string, <<"hello world">>})),
    StrUnicodeEnc = binary:encode_hex(hydb_spec:encode_value({string, <<"Käse 🚀 日本語"/utf8>>})),
    BytesEnc = binary:encode_hex(hydb_spec:encode_value({bytes, <<1, 2, 3, 255>>})),
    AbsentEnc = binary:encode_hex(hydb_spec:encode_value(absent)),
    NullEnc = binary:encode_hex(hydb_spec:encode_value(null)),

    %% Nested JSON / Object with deterministic sorted keys
    JsonEnc = binary:encode_hex(hydb_spec:encode_value({object, [
        {<<"zeta">>, {int64, 100}},
        {<<"alpha">>, {string, <<"first">>}},
        {<<"mid">>, {array, [{int64, 1}, {int64, 2}]}}
    ]})),

    %% 2. Schemas and Manifests
    ValidSchema = #{
        namespace => <<"finance.balances">>,
        schema_version => <<"1.0.0">>,
        backend => <<"wasm_slot">>,
        key_type => <<"int64">>,
        value_type => <<"int64">>,
        require_auth => true,
        requires => [<<"cas">>],
        rules => #{
            <<"min_value">> => 0,
            <<"max_records">> => 1000
        }
    },
    ValidManifestHash = hydb_spec:manifest_hash(ValidSchema),

    DocSchema = #{
        namespace => <<"app.docs">>,
        schema_version => <<"1.0.0">>,
        backend => <<"ets_doc">>,
        key_type => <<"string">>,
        value_type => <<"json">>,
        require_auth => false,
        requires => [<<"prefix_scan">>],
        rules => #{
            <<"fields">> => #{
                <<"title">> => #{ <<"type">> => <<"string">>, <<"required">> => true, <<"min_length">> => 3 },
                <<"views">> => #{ <<"type">> => <<"int64">>, <<"required">> => false, <<"min_value">> => 0 }
            },
            <<"additional_fields">> => true
        }
    },
    DocManifestHash = hydb_spec:manifest_hash(DocSchema),

    %% Schema Rejection Tests
    BadNsRes = case hydb_spec:validate_schema(#{
        namespace => <<"BAD_UPPERCASE">>,
        schema_version => <<"1.0.0">>,
        backend => <<"wasm_slot">>,
        key_type => <<"int64">>,
        value_type => <<"int64">>
    }) of
        {error, Code1, _} -> Code1;
        _ -> <<"OK">>
    end,

    BadCapRes = case hydb_spec:validate_schema(#{
        namespace => <<"test.cap">>,
        schema_version => <<"1.0.0">>,
        backend => <<"wasm_slot">>,
        key_type => <<"int64">>,
        value_type => <<"int64">>,
        requires => [<<"prefix_scan">>]
    }) of
        {error, Code2, _} -> Code2;
        _ -> <<"OK">>
    end,

    BadTypeRes = case hydb_spec:validate_schema(#{
        namespace => <<"test.type">>,
        schema_version => <<"1.0.0">>,
        backend => <<"wasm_slot">>,
        key_type => <<"string">>,
        value_type => <<"json">>
    }) of
        {error, Code3, _} -> Code3;
        _ -> <<"OK">>
    end,

    %% 3. Events and Hash Chain
    Ev1 = #{
        contract_version => <<"hydb.core@1.0">>,
        prev_hash => <<"0000000000000000000000000000000000000000000000000000000000000000">>,
        epoch => 1,
        seq => 1,
        op => <<"register">>,
        namespace => <<"finance.balances">>,
        schema_version => <<"1.0.0">>,
        key => absent,
        value_type => <<"schema">>,
        value => hydb_spec:schema_to_h(ValidSchema),
        ver => 1
    },
    Ev1Hash = hydb_spec:event_hash(Ev1),

    Ev2 = #{
        contract_version => <<"hydb.core@1.0">>,
        prev_hash => Ev1Hash,
        epoch => 1,
        seq => 2,
        op => <<"put">>,
        namespace => <<"finance.balances">>,
        schema_version => <<"1.0.0">>,
        key => {int64, 1001},
        value_type => <<"int64">>,
        value => {int64, 50000},
        ver => 1
    },
    Ev2Hash = hydb_spec:event_hash(Ev2),

    Ev3 = #{
        contract_version => <<"hydb.core@1.0">>,
        prev_hash => Ev2Hash,
        epoch => 1,
        seq => 3,
        op => <<"del">>,
        namespace => <<"finance.balances">>,
        schema_version => <<"1.0.0">>,
        key => {int64, 1001},
        value_type => <<"int64">>,
        value => absent,
        ver => 2
    },
    Ev3Hash = hydb_spec:event_hash(Ev3),

    Ev4 = #{
        contract_version => <<"hydb.core@1.0">>,
        prev_hash => Ev3Hash,
        epoch => 1,
        seq => 4,
        op => <<"put">>,
        namespace => <<"finance.balances">>,
        schema_version => <<"1.0.0">>,
        key => {int64, 1001},
        value_type => <<"int64">>,
        value => {int64, 75000},
        ver => 3
    },
    Ev4Hash = hydb_spec:event_hash(Ev4),

    %% 4. State Commitment
    SchemasState = [
        {<<"finance.balances">>, <<"1.0.0">>, 1, ValidManifestHash},
        {<<"app.docs">>, <<"1.0.0">>, 1, DocManifestHash}
    ],
    RecordsState = [
        {<<"finance.balances">>, {int64, 1001}, 3, {int64, 75000}},
        {<<"finance.balances">>, {int64, 1002}, 1, {int64, 25000}},
        {<<"app.docs">>, {string, <<"doc:readme">>}, 1, {object, [
            {<<"title">>, {string, <<"HyDB Guide">>}},
            {<<"views">>, {int64, 10}}
        ]}}
    ],
    CommitmentHex = hydb_spec:state_commitment(SchemasState, RecordsState),

    %% Output structured JSON result for test controller
    Output = io_lib:format(
        "{\"int64_42\":\"~s\",\"int64_min\":\"~s\",\"int64_max\":\"~s\","
        "\"bool_true\":\"~s\",\"bool_false\":\"~s\",\"str_simple\":\"~s\",\"str_unicode\":\"~s\","
        "\"bytes\":\"~s\",\"absent\":\"~s\",\"null\":\"~s\",\"json\":\"~s\","
        "\"valid_manifest_hash\":\"~s\",\"doc_manifest_hash\":\"~s\","
        "\"bad_ns_error\":\"~s\",\"bad_cap_error\":\"~s\",\"bad_type_error\":\"~s\","
        "\"ev1_hash\":\"~s\",\"ev2_hash\":\"~s\",\"ev3_hash\":\"~s\",\"ev4_hash\":\"~s\","
        "\"state_commitment\":\"~s\"}",
        [
            Int64Enc, Int64MinEnc, Int64MaxEnc,
            BoolTrueEnc, BoolFalseEnc, StrSimpleEnc, StrUnicodeEnc,
            BytesEnc, AbsentEnc, NullEnc, JsonEnc,
            ValidManifestHash, DocManifestHash,
            BadNsRes, BadCapRes, BadTypeRes,
            Ev1Hash, Ev2Hash, Ev3Hash, Ev4Hash,
            CommitmentHex
        ]
    ),
    io:format("~s~n", [lists:flatten(Output)]).
