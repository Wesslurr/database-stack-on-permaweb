%%% @doc Fast external Forge WASM execution & JSON-Iface device (`hs-wasm-output@1.0`).
%%% Acts directly as the process `execution-device', collapsing upstream `stack@1.0' +
%%% `multipass@1.0' + `json-iface@1.0' + `wasm-64@1.0' into a single fast pass with:
%%%   - ETS + `hb_private' caching for serialized `Process' JSON (avoiding per-slot ProcDef hashing)
%%%   - Fast JSON encoding for messages & chunked (`4096' B) WASM string reads (replacing 8 B IPC loops)
%%%   - Persistent `hb_beamr' instance pooling (preventing C port leaks and cold deserializations)
%%%   - Structured `Output' map attachment at `results/output'
-module(dev_hs_wasm_output).
-implements(<<"hs-wasm-output@1.0">>).
-export([init/3, compute/3, normalize/3, snapshot/3, to/3, from/3]).
-include_lib("hb/include/hb.hrl").

-define(MAX_IDLE_INSTANCES, 16).
-define(MAX_PROC_CACHE, 1024).
-define(READ_CHUNK_BYTES, 16384).

fast_opts(Opts) ->
    Opts#{
        <<"hashpath">> => ignore,
        <<"resolve-mode">> => raw,
        <<"spawn-worker">> => false,
        <<"await-inprogress">> => false
    }.

%% @doc Initialize the WASM VM directly via `wasm-64@1.0' and set default `handle' function.
-spec init(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) -> {ok, #{ _ => _ }}.
init(M1, _M2, Opts) ->
    FastOpts = fast_opts(Opts),
    case delegate_wasm(M1, <<"init">>, FastOpts) of
        {ok, WasmM1} ->
            {ok, hb_maps:put(<<"function">>, <<"handle">>, WasmM1, FastOpts)};
        Other ->
            Other
    end.

%% @doc Reuse a warm pooled `hb_beamr' instance when `ProcID' and `at-slot' match,
%% falling back to cold `dev_wasm:normalize/3' deserialization when missing.
-spec normalize(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) -> {ok, #{ _ => _ }}.
normalize(RawM1, M2, Opts) ->
    FastOpts = fast_opts(Opts),
    Prefix = output_prefix(RawM1, FastOpts),
    normalize_with_prefix(RawM1, M2, Prefix, FastOpts).

normalize_with_prefix(RawM1, M2, Prefix, FastOpts) ->
    case hb_private:get(<<Prefix/binary, "/instance">>, RawM1, FastOpts) of
        Inst when is_pid(Inst) ->
            case erlang:is_process_alive(Inst) of
                true ->
                    {ok, strip_snapshot(RawM1, FastOpts)};
                false ->
                    checkout_or_cold_normalize(RawM1, M2, Prefix, FastOpts)
            end;
        _ ->
            checkout_or_cold_normalize(RawM1, M2, Prefix, FastOpts)
    end.

checkout_or_cold_normalize(RawM1, M2, Prefix, FastOpts) ->
    case {get_or_extract_pool_key(RawM1, Prefix, FastOpts), get_slot(RawM1, FastOpts)} of
        {PoolKey, AtSlot} when PoolKey =/= undefined, is_integer(AtSlot) ->
            case pool_checkout(PoolKey, AtSlot, self()) of
                {ok, #{ write := WriteFn, read := ReadFn,
                        instance := Inst, import_resolver := ImportRes }} ->
                    CleanM1 = strip_snapshot(RawM1, FastOpts),
                    {ok,
                        hb_private:set(
                            CleanM1#{ <<"cache-control">> => [<<"no-store">>] },
                            #{
                                <<Prefix/binary, "/pool-key">> => PoolKey,
                                <<Prefix/binary, "/write">> => WriteFn,
                                <<Prefix/binary, "/read">> => ReadFn,
                                <<Prefix/binary, "/instance">> => Inst,
                                <<Prefix/binary, "/import-resolver">> => ImportRes
                            },
                            FastOpts
                        )
                    };
                miss ->
                    cold_normalize(RawM1, M2, Prefix, PoolKey, FastOpts)
            end;
        {PoolKey, _} ->
            cold_normalize(RawM1, M2, Prefix, PoolKey, FastOpts)
    end.

cold_normalize(RawM1, _M2, Prefix, PoolKey, FastOpts) ->
    case delegate_wasm(RawM1, <<"normalize">>, FastOpts) of
        {ok, NormM1} when PoolKey =/= undefined ->
            {ok,
                hb_private:set(
                    NormM1,
                    #{ <<Prefix/binary, "/pool-key">> => PoolKey },
                    FastOpts
                )
            };
        Other ->
            Other
    end.

strip_snapshot(Msg, Opts) when is_map(Msg) ->
    case is_map_key(<<"snapshot">>, Msg) of
        true -> hb_maps:without([<<"snapshot">>], Msg, Opts);
        false -> Msg
    end;
strip_snapshot(Msg, _Opts) ->
    Msg.

%% @doc Confirm the slot in the warm pool and produce the WASM memory snapshot via `hb_beamr:serialize/1'.
-spec snapshot(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ body := binary() }} | {error, _}.
snapshot(M1, _Req, Opts) ->
    FastOpts = fast_opts(Opts),
    case resolve_pool_context(M1, FastOpts) of
        {ok, PoolKey, Prefix, Inst, WriteFn, ReadFn, ImportRes} ->
            case get_slot(M1, FastOpts) of
                Slot when is_integer(Slot) ->
                    pool_confirm_slot(self(), PoolKey, Slot, Prefix, Inst, WriteFn, ReadFn, ImportRes);
                _ ->
                    ok
            end,
            {ok, Serialized} = hb_beamr:serialize(Inst),
            {ok, #{ <<"body">> => Serialized }};
        none ->
            Prefix = output_prefix(M1, FastOpts),
            case hb_private:get(<<Prefix/binary, "/instance">>, M1, FastOpts) of
                Inst when is_pid(Inst) ->
                    {ok, Serialized} = hb_beamr:serialize(Inst),
                    {ok, #{ <<"body">> => Serialized }};
                _ ->
                    delegate_wasm(M1, <<"snapshot">>, FastOpts)
            end
    end.

delegate_wasm(M1, PathBin, Opts) ->
    OrigDev = hb_maps:get(<<"device">>, M1, undefined, Opts),
    SetOpts = Opts#{ <<"hashpath">> => ignore },
    DelegateM1 = hb_maps:put(<<"device">>, <<"wasm-64@1.0">>, M1, SetOpts),
    case hb_ao:resolve(DelegateM1, PathBin, SetOpts) of
        {ok, Res} when is_map(Res) andalso PathBin =/= <<"snapshot">> ->
            Restored =
                case OrigDev of
                    undefined -> hb_maps:without([<<"device">>], Res, SetOpts);
                    _ -> hb_maps:put(<<"device">>, OrigDev, Res, SetOpts)
                end,
            {ok, Restored};
        Other ->
            Other
    end.

%% @doc Execute a single unified prep -> WASM call -> JSON decode + outbox/output pass.
-spec compute(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ _ => _ }} | {error, _} | {failure, _}.
compute(RawM1, M2, Opts) ->
    FastOpts = fast_opts(Opts),
    Prefix = output_prefix(RawM1, FastOpts),
    case normalize_with_prefix(RawM1, M2, Prefix, FastOpts) of
        {ok, M1Inst} ->
            track_active_instance(M1Inst, FastOpts),
            {M1WithProc, ProcID, Image, ProcessJson} =
                get_or_cache_process(M1Inst, Prefix, FastOpts),
            MsgJson = prepare_msg_json(M1WithProc, M2, ProcID, Image, FastOpts),
            execute_wasm(M1WithProc, Prefix, ProcID, Image, MsgJson, ProcessJson, FastOpts);
        Other ->
            Other
    end.

%% @doc Delegate standalone `to' conversion to upstream `json-iface@1.0'.
to(M1, M2, Opts) ->
    delegate_to_json_iface(M1, M2, Opts).

%% @doc Delegate standalone `from' conversion to upstream `json-iface@1.0'.
from(M1, M2, Opts) ->
    delegate_to_json_iface(M1, M2, Opts).

delegate_to_json_iface(M1, M2, Opts) ->
    OrigDev = hb_maps:get(<<"device">>, M1, <<"hs-wasm-output@1.0">>, Opts),
    SetOpts = Opts#{ <<"hashpath">> => ignore },
    DelegateM1 = hb_maps:put(<<"device">>, <<"json-iface@1.0">>, M1, SetOpts),
    case hb_ao:resolve(DelegateM1, M2, Opts) of
        {ok, Res} when is_map(Res) ->
            {ok, hb_maps:put(<<"device">>, OrigDev, Res, SetOpts)};
        Other ->
            Other
    end.

%%% Fast Process JSON & ETS Cache

get_or_cache_process(M1, Prefix, Opts) ->
    CachePrivKey = <<Prefix/binary, "/cached-process">>,
    case hb_private:get(CachePrivKey, M1, Opts) of
        {CachedProcID, CachedImg, CachedProcJson} ->
            {M1, CachedProcID, CachedImg, CachedProcJson};
        not_found ->
            RawProc = hb_maps:get(<<"process">>, M1, not_found, Opts),
            case lookup_proc_cache(RawProc) of
                {ok, CachedPID, CachedImg, CachedProcJson} ->
                    M1Updated =
                        hb_private:set(
                            M1,
                            #{
                                CachePrivKey => {CachedPID, CachedImg, CachedProcJson},
                                <<Prefix/binary, "/pool-key">> => {CachedPID, img_cache_key(CachedImg)}
                            },
                            Opts
                        ),
                    {M1Updated, CachedPID, CachedImg, CachedProcJson};
                miss ->
                    LoadedProc =
                        case RawProc of
                            not_found -> hb_ao:get(<<"process">>, M1, #{}, Opts);
                            _ -> hb_cache:ensure_all_loaded(RawProc, Opts)
                        end,
                    ProcID = hb_util:human_id(hb_message:id(LoadedProc, all, Opts)),
                    Img = extract_image_value(M1, LoadedProc, Opts),
                    ProcJson = encode_process_json(ProcID, LoadedProc, Opts),
                    store_proc_cache(RawProc, ProcID, Img, ProcJson),
                    M1Updated =
                        hb_private:set(
                            M1,
                            #{
                                CachePrivKey => {ProcID, Img, ProcJson},
                                <<Prefix/binary, "/pool-key">> => {ProcID, img_cache_key(Img)}
                            },
                            Opts
                        ),
                    {M1Updated, ProcID, Img, ProcJson}
            end
    end.

extract_image_value(M1, LoadedProc, Opts) ->
    InPrefix = input_prefix(M1, Opts),
    case hb_ao:get(<<InPrefix/binary, "/image">>, M1, undefined, Opts) of
        Bin when is_binary(Bin) -> Bin;
        _ ->
            case hb_ao:get(<<"image">>, LoadedProc, <<>>, Opts) of
                Bin2 when is_binary(Bin2) -> Bin2;
                _ -> <<>>
            end
    end.

encode_process_json(ProcID, Process, Opts) when is_map(Process) ->
    CleanProc = hb_maps:without(
        [<<"snapshot">>, <<"workspace">>, <<"device">>, <<"priv">>, <<"commitments">>],
        Process,
        Opts
    ),
    ProcTags = fast_tags(CleanProc, [
        <<"id">>, <<"anchor">>, <<"owner">>, <<"data">>, <<"target">>, <<"signature">>
    ]),
    Owner =
        case hb_maps:get(<<"owner">>, Process, <<>>, Opts) of
            O when is_binary(O), byte_size(O) > 0 -> O;
            _ ->
                case hb_message:signers(Process, Opts) of
                    [PrimarySigner | _] -> hb_util:raw_address(PrimarySigner);
                    [] -> <<>>
                end
        end,
    BaseProcMap = #{
        <<"Id">> => safe_to_id(ProcID),
        <<"Owner">> => Owner,
        <<"Tags">> => ProcTags
    },
    WithMeta =
        lists:foldl(
            fun(Key, Acc) ->
                case hb_maps:find(Key, Process, Opts) of
                    {ok, Val} when is_binary(Val) -> Acc#{ Key => Val };
                    _ -> Acc
                end
            end,
            BaseProcMap,
            [<<"node-role">>, <<"genesis-url">>, <<"primary-url">>]
        ),
    hb_json:encode(#{ <<"Process">> => WithMeta });
encode_process_json(ProcID, _Process, _Opts) ->
    hb_json:encode(#{ <<"Process">> => #{ <<"Id">> => safe_to_id(ProcID), <<"Tags">> => [] } }).

ensure_proc_cache_table() ->
    case ets:info(hs_wasm_proc_cache) of
        undefined ->
            hb_name:singleton(
                hs_wasm_proc_cache_owner,
                fun() ->
                    try ets:new(
                        hs_wasm_proc_cache,
                        [named_table, set, public, {read_concurrency, true}, {write_concurrency, true}]
                    )
                    catch _:_ -> ok
                    end,
                    receive stop -> ok end
                end
            );
        _ ->
            ok
    end.

lookup_proc_cache(RawProc) when is_map(RawProc) ->
    try
        case ets:info(hs_wasm_proc_cache) of
            undefined -> miss;
            _ ->
                Key = erlang:phash2(RawProc, 16#7FFFFFFF),
                case ets:lookup(hs_wasm_proc_cache, Key) of
                    [{Key, StoredProc, PID, Img, ProcJson}] when StoredProc =:= RawProc ->
                        {ok, PID, Img, ProcJson};
                    _ ->
                        miss
                end
        end
    catch _:_ ->
        miss
    end;
lookup_proc_cache(_) ->
    miss.

store_proc_cache(RawProc, PID, Img, ProcJson) when is_map(RawProc) ->
    try
        ensure_proc_cache_table(),
        case ets:info(hs_wasm_proc_cache, size) of
            N when is_integer(N), N > ?MAX_PROC_CACHE ->
                ets:delete_all_objects(hs_wasm_proc_cache);
            _ ->
                ok
        end,
        Key = erlang:phash2(RawProc, 16#7FFFFFFF),
        ets:insert(hs_wasm_proc_cache, {Key, RawProc, PID, Img, ProcJson})
    catch _:_ ->
        ok
    end;
store_proc_cache(_, _, _, _) ->
    ok.

%%% Fast Message JSON Serialization

prepare_msg_json(M1, M2, ProcID, Image, Opts) ->
    RawBody = hb_maps:get(<<"body">>, M2, #{}, Opts),
    LoadedBody =
        case RawBody of
            Map when is_map(Map) -> hb_cache:ensure_all_loaded(Map, Opts);
            {link, _, _} -> hb_cache:ensure_all_loaded(RawBody, Opts);
            _ -> #{}
        end,
    case is_map(LoadedBody) andalso not is_map_key(<<"commitments">>, LoadedBody) of
        true ->
            try fast_msg_json(LoadedBody, M1, M2, ProcID, Image, Opts)
            catch _:_ -> slow_msg_json(LoadedBody, M1, M2, ProcID, Image, Opts)
            end;
        false ->
            slow_msg_json(LoadedBody, M1, M2, ProcID, Image, Opts)
    end.

fast_msg_json(RawMessage, M1, M2, ProcID, Image, Opts) ->
    CleanRaw = hb_maps:without([<<"priv">>, <<"commitments">>], RawMessage, Opts),
    MsgData =
        case hb_maps:find(<<"data">>, CleanRaw, Opts) of
            {ok, ExistingData} when is_binary(ExistingData), byte_size(ExistingData) > 0 ->
                case hb_util:is_printable_string(ExistingData) of
                    true -> ExistingData;
                    false -> throw(non_printable_data)
                end;
            {ok, ExistingMap} when is_map(ExistingMap); is_list(ExistingMap) ->
                hb_json:encode(ExistingMap);
            _ ->
                hb_json:encode(CleanRaw)
        end,
    BlockHeight = parse_int_field(hb_maps:get(<<"block-height">>, M2, 0, Opts), 0),
    Timestamp = resolve_timestamp(RawMessage, M1, M2, Opts),
    MsgTags0 = fast_tags(CleanRaw, [
        <<"id">>, <<"anchor">>, <<"owner">>, <<"data">>,
        <<"target">>, <<"signature">>, <<"commitments">>,
        <<"module">>, <<"block-height">>, <<"publickey">>
    ]),
    MsgTags =
        case hb_maps:find(<<"id">>, CleanRaw, Opts) of
            {ok, AppId} when is_binary(AppId); is_number(AppId) ->
                [#{ <<"name">> => <<"Id">>, <<"value">> => hb_util:bin(AppId) } | MsgTags0];
            _ ->
                MsgTags0
        end,
    Target = first_binary(
        [hb_maps:get(<<"target">>, RawMessage, undefined, Opts)],
        ProcID
    ),
    Owner = first_binary(
        [hb_maps:get(<<"owner">>, RawMessage, undefined, Opts)],
        <<>>
    ),
    From = first_binary(
        [
            hb_maps:get(<<"from-process">>, RawMessage, undefined, Opts),
            hb_maps:get(<<"from">>, RawMessage, undefined, Opts),
            Owner
        ],
        Owner
    ),
    Anchor = first_binary(
        [hb_maps:get(<<"anchor">>, RawMessage, undefined, Opts)],
        <<>>
    ),
    MsgId =
        case hb_maps:get(<<"hash-chain">>, M2, undefined, Opts) of
            HC when is_binary(HC) -> safe_to_id(HC);
            _ -> <<"unsigned-fast-msg">>
        end,
    hb_json:encode(#{
        <<"Id">> => MsgId,
        <<"Anchor">> => Anchor,
        <<"Owner">> => Owner,
        <<"Signature">> => <<>>,
        <<"PublicKey">> => <<>>,
        <<"Target">> => safe_to_id(Target),
        <<"From">> => From,
        <<"Data">> => MsgData,
        <<"Tags">> => MsgTags,
        <<"Block-Height">> => BlockHeight,
        <<"Timestamp">> => Timestamp,
        <<"Module">> => Image,
        <<"Cron">> => false
    }).

slow_msg_json(RawMessage, M1, M2, _ProcID, Image, Opts) when is_map(RawMessage) ->
    CleanBody = hb_maps:without([<<"priv">>, <<"commitments">>], RawMessage, Opts),
    MsgWithData =
        case hb_maps:find(<<"data">>, RawMessage, Opts) of
            {ok, D} when is_binary(D), byte_size(D) > 0 -> RawMessage;
            _ -> RawMessage#{ <<"data">> => hb_json:encode(CleanBody) }
        end,
    MsgWithTs =
        case is_epoch_ms(hb_maps:get(<<"timestamp">>, MsgWithData, undefined, Opts))
             orelse is_epoch_ms(hb_maps:get(<<"Timestamp">>, MsgWithData, undefined, Opts)) of
            true -> MsgWithData;
            false -> MsgWithData#{ <<"timestamp">> => resolve_timestamp(RawMessage, M1, M2, Opts) }
        end,
    Denorm = denormalize_signed_message(MsgWithTs, Opts),
    Converted =
        hb_message:convert(
            Denorm,
            tabm,
            #{
                <<"device">> => <<"structured@1.0">>,
                <<"encode-types">> => [<<"binary">>],
                <<"bundle">> => true
            },
            Opts
        ),
    WithoutComms = hb_maps:without([<<"commitments">>], Converted, Opts),
    BlockHeight = parse_int_field(hb_maps:get(<<"block-height">>, M2, 0, Opts), 0),
    Tags = fast_tags(hb_maps:without([<<"priv">>, <<"commitments">>], MsgWithTs, Opts), [
        <<"module">>, <<"block-height">>, <<"id">>, <<"signature">>, <<"publickey">>
    ]),
    Struct =
        WithoutComms#{
            <<"Module">> => Image,
            <<"Block-Height">> => BlockHeight,
            <<"Tags">> => Tags
        },
    hb_json:encode(Struct);
slow_msg_json(_Other, _M1, M2, ProcID, Image, Opts) ->
    fast_msg_json(#{}, #{}, M2, ProcID, Image, Opts).

denormalize_signed_message(Message, Opts) ->
    NormOwner =
        case hb_message:signers(Message, Opts) of
            [] -> Message;
            [PrimarySigner | _] ->
                Commitments = hb_message:commitments(PrimarySigner, Message, Opts),
                Commit =
                    case lists:sort(hb_maps:keys(Commitments, Opts)) of
                        [] -> #{};
                        [CID | _] -> hb_maps:get(CID, Commitments, #{}, Opts)
                    end,
                Message#{
                    <<"owner">> => hb_util:raw_address(PrimarySigner),
                    <<"signature">> => hb_maps:get(<<"signature">>, Commit, <<>>, Opts)
                }
        end,
    NormOwner#{ <<"Id">> => hb_message:id(Message, all, Opts) }.

resolve_timestamp(RawMessage, M1, M2, Opts) ->
    Candidates = [
        hb_maps:get(<<"timestamp">>, RawMessage, undefined, Opts),
        hb_maps:get(<<"Timestamp">>, RawMessage, undefined, Opts),
        hb_maps:get(<<"timestamp">>, M2, undefined, Opts),
        hb_maps:get(<<"timestamp">>, M1, undefined, Opts)
    ],
    case find_valid_epoch_ms(Candidates) of
        {ok, Ts} -> Ts;
        none -> erlang:system_time(millisecond)
    end.

find_valid_epoch_ms([]) ->
    none;
find_valid_epoch_ms([H | T]) ->
    case parse_epoch_ms(H) of
        {ok, Ts} -> {ok, Ts};
        none -> find_valid_epoch_ms(T)
    end.

is_epoch_ms(V) ->
    parse_epoch_ms(V) =/= none.

parse_epoch_ms(V) when is_integer(V), V > 1000000000000 ->
    {ok, V};
parse_epoch_ms(V) when is_binary(V) ->
    try binary_to_integer(string:trim(V)) of
        I when I > 1000000000000 -> {ok, I};
        _ -> none
    catch _:_ ->
        none
    end;
parse_epoch_ms(_) ->
    none.

parse_int_field(V, _Default) when is_integer(V) -> V;
parse_int_field(V, Default) when is_binary(V) ->
    case hb_util:safe_int(V) of
        {ok, I} -> I;
        _ -> Default
    end;
parse_int_field(_, Default) -> Default.

first_binary([], Default) -> Default;
first_binary([H | _], _Default) when is_binary(H) -> H;
first_binary([_ | T], Default) -> first_binary(T, Default).

fast_tags(Map, SkipKeys) when is_map(Map) ->
    maps:fold(
        fun(K, V, Acc) when is_binary(K) ->
            NormK = hb_ao:normalize_key(K),
            case lists:member(NormK, SkipKeys) of
                true -> Acc;
                false ->
                    case fast_tag_val(V) of
                        {ok, ValBin} ->
                            [#{ <<"name">> => title_case_key(NormK), <<"value">> => ValBin } | Acc];
                        skip ->
                            Acc
                    end
            end;
           (_, _, Acc) ->
            Acc
        end,
        [],
        Map
    ).

fast_tag_val(V) when is_binary(V) ->
    case unicode:characters_to_binary(V) =:= V andalso hb_util:is_printable_string(V) of
        true -> {ok, V};
        false -> throw(non_printable_tag)
    end;
fast_tag_val(V) when is_integer(V) -> {ok, integer_to_binary(V)};
fast_tag_val(V) when is_float(V) -> {ok, float_to_binary(V, [short])};
fast_tag_val(true) -> {ok, <<"true">>};
fast_tag_val(false) -> {ok, <<"false">>};
fast_tag_val(V) when is_atom(V) -> {ok, atom_to_binary(V, utf8)};
fast_tag_val(_) -> skip.

title_case_key(NormKey) ->
    case unicode:characters_to_binary(NormKey) =:= NormKey of
        true ->
            Words = string:lexemes(NormKey, "-"),
            TitleWords = [binary_to_list(string:titlecase(W)) || W <- Words],
            list_to_binary(string:join(TitleWords, "-"));
        false ->
            NormKey
    end.

safe_to_id(<<>>) -> <<>>;
safe_to_id(ID) -> hb_util:human_id(ID).

%%% Direct WASM Execution & Fast Chunked Output Decode

execute_wasm(M1, Prefix, ProcID, Image, MsgJson, ProcessJson, Opts) ->
    case resolve_exec_handles(M1, Prefix, Opts) of
        {ok, Inst, ImportRes} ->
            case write_wasm_inputs(Inst, MsgJson, ProcessJson) of
                {ok, MsgPtr, ProcPtr} ->
                    case hb_beamr:call(Inst, "handle", [MsgPtr, ProcPtr], ImportRes, M1, Opts) of
                        {trap, Err, _} ->
                            hb_beamr:stop(Inst),
                            {failure, Err};
                        {error, Err, _} ->
                            {error, Err};
                        {_ResType, Res, MsgAfterExec} ->
                            decode_wasm_response(Inst, Res, MsgAfterExec, ProcID, Image, Opts)
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

resolve_exec_handles(M1, Prefix, Opts) ->
    case hb_private:get(<<Prefix/binary, "/instance">>, M1, Opts) of
        Inst when is_pid(Inst) ->
            ImportRes = hb_private:get(<<Prefix/binary, "/import-resolver">>, M1, Opts),
            {ok, Inst, ImportRes};
        _ ->
            {error, no_wasm_instance}
    end.

write_wasm_inputs(Inst, MsgJson, ProcessJson) ->
    case hb_beamr_io:write_string(Inst, MsgJson) of
        {ok, MsgPtr} ->
            case hb_beamr_io:write_string(Inst, ProcessJson) of
                {ok, ProcPtr} -> {ok, MsgPtr, ProcPtr};
                Err -> Err
            end;
        Err ->
            Err
    end.

decode_wasm_response(Inst, Res, MsgAfterExec, ProcID, Image, Opts) ->
    Ptr =
        case Res of
            [P] when is_integer(P) -> P;
            P when is_integer(P) -> P
        end,
    case read_c_string(Inst, Ptr) of
        {ok, JsonBin} ->
            try hb_json:decode(JsonBin) of
                #{ <<"ok">> := true, <<"response">> := Resp } ->
                    Results = build_results_map(Resp, ProcID, Image, Opts),
                    {ok, hb_maps:put(<<"results">>, Results, MsgAfterExec, Opts)};
                #{ <<"ok">> := false } = Reply ->
                    {error, maps:get(<<"response">>, Reply, <<"AOS execution error.">>)};
                _Other ->
                    {error,
                        hb_maps:put(
                            <<"results">>,
                            #{ <<"outbox">> => undefined, <<"body">> => <<"Invalid JSON message input.">> },
                            MsgAfterExec,
                            Opts
                        )}
            catch _:_ ->
                {error,
                    hb_maps:put(
                        <<"results">>,
                        #{ <<"outbox">> => undefined, <<"body">> => <<"JSON error parsing result output.">> },
                        MsgAfterExec,
                        Opts
                    )}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

read_c_string(Inst, Ptr) ->
    case hb_beamr_io:size(Inst) of
        {ok, MemSize} when is_integer(MemSize), Ptr >= 0, Ptr < MemSize ->
            read_c_string_loop(Inst, Ptr, MemSize, []);
        {ok, _} ->
            {error, read_out_of_bounds};
        Err ->
            Err
    end.

read_c_string_loop(_Inst, Offset, MemSize, _Acc) when Offset >= MemSize ->
    {error, unterminated_string};
read_c_string_loop(Inst, Offset, MemSize, Acc) ->
    ChunkSize = min(?READ_CHUNK_BYTES, MemSize - Offset),
    case hb_beamr_io:read(Inst, Offset, ChunkSize) of
        {ok, Chunk} ->
            case binary:split(Chunk, <<0>>) of
                [Final, _Rest] ->
                    {ok, iolist_to_binary(lists:reverse([Final | Acc]))};
                [_NoNull] ->
                    read_c_string_loop(Inst, Offset + ChunkSize, MemSize, [Chunk | Acc])
            end;
        Err ->
            Err
    end.

build_results_map(#{ <<"Error">> := Error }, _ProcID, _Image, _Opts) ->
    #{
        <<"outbox">> => #{},
        <<"patches">> => [],
        <<"data">> => Error,
        <<"output">> => #{ <<"data">> => Error, <<"body">> => Error }
    };
build_results_map(Resp, ProcID, Image, Opts) when is_map(Resp) ->
    OutputMap =
        case maps:find(<<"Output">>, Resp) of
            {ok, Out} when is_map(Out) -> Out;
            _ ->
                case maps:find(<<"output">>, Resp) of
                    {ok, Out2} when is_map(Out2) -> Out2;
                    _ -> #{}
                end
        end,
    Data = maps:get(<<"data">>, OutputMap,
        maps:get(<<"Data">>, Resp,
            maps:get(<<"body">>, OutputMap, <<>>))),
    RawMessages = maps:get(<<"Messages">>, Resp, maps:get(<<"messages">>, Resp, [])),
    RawPatches = maps:get(<<"patches">>, Resp, maps:get(<<"Patches">>, Resp, [])),
    Outbox = build_outbox_map(RawMessages, ProcID, Image, Opts),
    Patches = [convert_unset_values(tags_to_map(P, Opts)) || P <- RawPatches],
    NormalizedOutput =
        case map_size(OutputMap) > 0 of
            true -> OutputMap;
            false -> #{ <<"data">> => Data, <<"body">> => Data }
        end,
    #{
        <<"outbox">> => Outbox,
        <<"patches">> => Patches,
        <<"data">> => Data,
        <<"output">> => NormalizedOutput
    }.

build_outbox_map([], _ProcID, _Image, _Opts) ->
    #{};
build_outbox_map(Messages, ProcID, Image, Opts) ->
    {OutMap, _} =
        lists:foldl(
            fun(Msg, {Acc, Idx}) ->
                Pre = preprocess_outbox_msg(Msg, ProcID, Image, Opts),
                {Acc#{ Idx => Pre }, Idx + 1}
            end,
            {#{}, 1},
            Messages
        ),
    OutMap.

preprocess_outbox_msg(Msg, ProcID, Image, Opts) ->
    Tags = tags_to_map(Msg, Opts),
    NormalizedBase =
        maps:fold(
            fun(K, _V, Acc) when K =:= <<"from-process">>; K =:= <<"from-image">>;
                                 K =:= <<"anchor">>; K =:= <<"Tags">> ->
                Acc;
               (K, V, Acc) ->
                Acc#{ hb_ao:normalize_key(K) => V }
            end,
            #{},
            Msg
        ),
    Merged = convert_unset_values(maps:merge(NormalizedBase, Tags)),
    Merged#{
        <<"from-process">> => ProcID,
        <<"from-image">> => Image
    }.

tags_to_map(Msg, _Opts) ->
    RawTags = maps:get(<<"Tags">>, Msg, []),
    lists:foldl(
        fun(Tag, Acc) when is_map(Tag) ->
            case {maps:find(<<"name">>, Tag), maps:find(<<"value">>, Tag)} of
                {{ok, Name}, {ok, Val}} -> Acc#{ Name => Val };
                _ -> Acc
            end;
           (_, Acc) ->
            Acc
        end,
        #{},
        RawTags
    ).

convert_unset_values(Map) when is_map(Map) ->
    maps:map(
        fun(_Key, <<"__ao-unset__">>) -> unset;
           (_Key, Value) when is_map(Value) -> convert_unset_values(Value);
           (_Key, Value) -> Value
        end,
        Map
    );
convert_unset_values(Other) ->
    Other.

%%% Persistent WASM Instance Pool (prevents `hb_beamr` C port leaks and cold reloads)

output_prefix(Msg, Opts) ->
    hb_maps:get(<<"output-prefix">>, Msg, <<"">>, Opts).

input_prefix(Msg, Opts) ->
    hb_maps:get(<<"input-prefix">>, Msg, <<"">>, Opts).

get_slot(Msg, Opts) ->
    case hb_maps:get(<<"at-slot">>, Msg, undefined, Opts) of
        I when is_integer(I) -> I;
        B when is_binary(B) ->
            case hb_util:safe_int(B) of
                {ok, I} -> I;
                _ -> undefined
            end;
        _ ->
            undefined
    end.

img_cache_key(Img) when is_binary(Img), byte_size(Img) =< 128 -> Img;
img_cache_key(Img) -> erlang:phash2(Img).

extract_pool_key(Msg, Opts) ->
    case hb_maps:get(<<"process">>, Msg, not_found, Opts) of
        ProcMsg when is_map(ProcMsg) ->
            ProcID = hb_util:human_id(hb_message:id(ProcMsg, all, Opts)),
            InPrefix = input_prefix(Msg, Opts),
            ImgKey = img_cache_key(hb_ao:get(<<InPrefix/binary, "/image">>, Msg, undefined, Opts)),
            {ProcID, ImgKey};
        _ ->
            undefined
    end.

get_or_extract_pool_key(M1, Prefix, Opts) ->
    case hb_private:get(<<Prefix/binary, "/pool-key">>, M1, Opts) of
        {_, _} = PoolKey ->
            PoolKey;
        _ ->
            extract_pool_key(M1, Opts)
    end.

resolve_pool_context(M1, Opts) ->
    Prefix = output_prefix(M1, Opts),
    case {
        hb_private:get(<<Prefix/binary, "/instance">>, M1, Opts),
        get_or_extract_pool_key(M1, Prefix, Opts)
    } of
        {Inst, PoolKey} when is_pid(Inst), PoolKey =/= undefined ->
            WriteFn = hb_private:get(<<Prefix/binary, "/write">>, M1, Opts),
            ReadFn = hb_private:get(<<Prefix/binary, "/read">>, M1, Opts),
            ImportRes = hb_private:get(<<Prefix/binary, "/import-resolver">>, M1, Opts),
            {ok, PoolKey, Prefix, Inst, WriteFn, ReadFn, ImportRes};
        _ ->
            none
    end.

track_active_instance(M1, Opts) ->
    case resolve_pool_context(M1, Opts) of
        {ok, PoolKey, Prefix, Inst, WriteFn, ReadFn, ImportRes} ->
            pool_mark_active(self(), PoolKey, Prefix, Inst, WriteFn, ReadFn, ImportRes);
        none ->
            ok
    end.

pool_pid() ->
    case whereis(hs_wasm_pool) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            hb_name:singleton(
                hs_wasm_pool,
                fun() ->
                    pool_loop(#{ idle => #{}, active => #{}, refs => #{}, lru => [] })
                end
            )
    end.

pool_call(Msg) ->
    Pid = pool_pid(),
    Ref = erlang:monitor(process, Pid),
    Pid ! {Msg, self(), Ref},
    receive
        {Ref, Reply} ->
            erlang:demonitor(Ref, [flush]),
            Reply;
        {'DOWN', Ref, process, Pid, _Reason} ->
            miss
    after 5000 ->
        erlang:demonitor(Ref, [flush]),
        miss
    end.

pool_checkout(PoolKey, AtSlot, CallerPid) ->
    pool_call({checkout, PoolKey, AtSlot, CallerPid}).

pool_mark_active(CallerPid, PoolKey, Prefix, Inst, WriteFn, ReadFn, ImportRes) ->
    _ = pool_call({mark_active, CallerPid, PoolKey, Prefix, Inst, WriteFn, ReadFn, ImportRes}),
    ok.

pool_confirm_slot(CallerPid, PoolKey, Slot, Prefix, Inst, WriteFn, ReadFn, ImportRes) ->
    _ = pool_call({confirm_slot, CallerPid, PoolKey, Slot, Prefix, Inst, WriteFn, ReadFn, ImportRes}),
    ok.

pool_loop(State) ->
    receive
        {{checkout, PoolKey, AtSlot, CallerPid}, From, Ref} ->
            {Reply, NextState} = handle_checkout(PoolKey, AtSlot, CallerPid, State),
            From ! {Ref, Reply},
            pool_loop(NextState);
        {{mark_active, CallerPid, PoolKey, Prefix, Inst, WriteFn, ReadFn, ImportRes}, From, Ref} ->
            NextState = handle_mark_active(CallerPid, PoolKey, Prefix, Inst, WriteFn, ReadFn, ImportRes, State),
            From ! {Ref, ok},
            pool_loop(NextState);
        {{confirm_slot, CallerPid, PoolKey, Slot, Prefix, Inst, WriteFn, ReadFn, ImportRes}, From, Ref} ->
            NextState = handle_confirm_slot(CallerPid, PoolKey, Slot, Prefix, Inst, WriteFn, ReadFn, ImportRes, State),
            From ! {Ref, ok},
            pool_loop(NextState);
        {release_caller, CallerPid, From, Ref} ->
            NextState = handle_release_caller(CallerPid, State),
            From ! {Ref, ok},
            pool_loop(NextState);
        {release_caller, CallerPid} ->
            NextState = handle_release_caller(CallerPid, State),
            pool_loop(NextState);
        {'DOWN', MonRef, process, CallerPid, _Reason} ->
            NextState =
                case maps:get(MonRef, maps:get(refs, State), undefined) of
                    CallerPid -> handle_release_caller(CallerPid, State);
                    _ -> State
                end,
            pool_loop(NextState);
        _Other ->
            pool_loop(State)
    end.

handle_checkout(PoolKey, AtSlot, CallerPid, State = #{ idle := Idle, lru := Lru }) ->
    case maps:find(PoolKey, Idle) of
        {ok, Entry = #{ slot := AtSlot, instance := Inst }} ->
            Idle1 = maps:remove(PoolKey, Idle),
            Lru1 = lists:delete(PoolKey, Lru),
            State1 = State#{ idle => Idle1, lru => Lru1 },
            case is_pid(Inst) andalso erlang:is_process_alive(Inst) of
                true ->
                    State2 = set_active_caller(
                        CallerPid,
                        Entry#{ confirmed_slot => undefined },
                        State1
                    ),
                    {{ok, Entry}, State2};
                false ->
                    {miss, State1}
            end;
        {ok, #{ instance := OldInst }} ->
            safe_stop_beamr(OldInst),
            Idle1 = maps:remove(PoolKey, Idle),
            Lru1 = lists:delete(PoolKey, Lru),
            {miss, State#{ idle => Idle1, lru => Lru1 }};
        error ->
            {miss, State}
    end.

handle_mark_active(CallerPid, PoolKey, Prefix, Inst, WriteFn, ReadFn, ImportRes, State) ->
    Entry = #{
        key => PoolKey,
        prefix => Prefix,
        instance => Inst,
        write => WriteFn,
        read => ReadFn,
        import_resolver => ImportRes,
        confirmed_slot => undefined
    },
    set_active_caller(CallerPid, Entry, State).

handle_confirm_slot(CallerPid, PoolKey, Slot, Prefix, Inst, WriteFn, ReadFn, ImportRes, State) ->
    Entry = #{
        key => PoolKey,
        prefix => Prefix,
        instance => Inst,
        write => WriteFn,
        read => ReadFn,
        import_resolver => ImportRes,
        confirmed_slot => Slot
    },
    set_active_caller(CallerPid, Entry, State).

set_active_caller(CallerPid, Entry = #{ instance := Inst }, State = #{ active := Active, refs := Refs }) ->
    MonRef =
        case maps:find(CallerPid, Active) of
            {ok, #{ mon := ExistingRef, instance := PrevInst }} ->
                if
                    PrevInst =/= Inst -> safe_stop_beamr(PrevInst);
                    true -> ok
                end,
                ExistingRef;
            error ->
                erlang:monitor(process, CallerPid)
        end,
    Active1 = maps:put(CallerPid, Entry#{ mon => MonRef }, Active),
    Refs1 = maps:put(MonRef, CallerPid, Refs),
    State#{ active => Active1, refs => Refs1 }.

handle_release_caller(CallerPid, State = #{ active := Active, refs := Refs }) ->
    case maps:take(CallerPid, Active) of
        {Entry = #{ mon := MonRef }, Active1} ->
            erlang:demonitor(MonRef, [flush]),
            Refs1 = maps:remove(MonRef, Refs),
            promote_or_stop(Entry, State#{ active => Active1, refs => Refs1 });
        error ->
            State
    end.

promote_or_stop(#{ key := PoolKey, instance := Inst, confirmed_slot := Slot } = Entry,
                State = #{ idle := Idle, lru := Lru })
        when is_integer(Slot), is_pid(Inst) ->
    case erlang:is_process_alive(Inst) of
        true ->
            case maps:find(PoolKey, Idle) of
                {ok, #{ instance := OldInst }} when OldInst =/= Inst ->
                    safe_stop_beamr(OldInst);
                _ ->
                    ok
            end,
            IdleEntry = #{
                slot => Slot,
                prefix => maps:get(prefix, Entry),
                instance => Inst,
                write => maps:get(write, Entry),
                read => maps:get(read, Entry),
                import_resolver => maps:get(import_resolver, Entry)
            },
            Idle1 = maps:put(PoolKey, IdleEntry, Idle),
            Lru1 = [PoolKey | lists:delete(PoolKey, Lru)],
            evict_overflow_idle(State#{ idle => Idle1, lru => Lru1 });
        false ->
            State
    end;
promote_or_stop(#{ instance := Inst }, State) ->
    safe_stop_beamr(Inst),
    State.

evict_overflow_idle(State = #{ idle := Idle, lru := Lru }) when length(Lru) > ?MAX_IDLE_INSTANCES ->
    Keep = lists:sublist(Lru, ?MAX_IDLE_INSTANCES),
    Evict = lists:nthtail(?MAX_IDLE_INSTANCES, Lru),
    Idle1 =
        lists:foldl(
            fun(K, Acc) ->
                case maps:take(K, Acc) of
                    {#{ instance := OldInst }, Acc1} ->
                        safe_stop_beamr(OldInst),
                        Acc1;
                    error ->
                        Acc
                end
            end,
            Idle,
            Evict
        ),
    State#{ idle => Idle1, lru => Keep };
evict_overflow_idle(State) ->
    State.

safe_stop_beamr(Inst) when is_pid(Inst) ->
    catch hb_beamr:stop(Inst),
    ok;
safe_stop_beamr(_) ->
    ok.
