%%% @doc Fast external Forge WASM execution & JSON-Iface device (`hs-wasm-output@1.0`).
%%% Acts directly as the process `execution-device', collapsing upstream `stack@1.0' +
%%% `multipass@1.0' + `json-iface@1.0' + `wasm-64@1.0' into a single fast pass with:
%%%   - ETS + `hb_private' caching for serialized `Process' JSON (avoiding per-slot ProcDef hashing)
%%%   - Fast JSON encoding for messages & chunked (`4096' B) WASM string reads (replacing 8 B IPC loops)
%%%   - Persistent `hb_beamr' instance pooling (preventing C port leaks and cold deserializations)
%%%   - Structured `Output' map attachment at `results/output'
-module(dev_hs_wasm_output).
-implements(<<"hs-wasm-output@1.0">>).
-export([init/3, compute/3, normalize/3, snapshot/3, to/3, from/3, latest/3]).
-include_lib("hb/include/hb.hrl").

-define(MAX_IDLE_INSTANCES, 16).
-define(MAX_PROC_CACHE, 1024).
-define(READ_CHUNK_BYTES, 16384).
-define(DEFAULT_CKPT_SLOTS, 25).
-define(ZLIB_MAGIC, <<"HSZ1:">>).
-define(CKPT_MAGIC, <<"HSCK1:">>).
-define(ACTOR_CKPT_MAGIC, <<"HSCKPT2:">>).
-define(WAL_FRAME_MAGIC, 16#48535731).
-define(ACTOR_CKPT_EVERY, 50).
-define(ACTOR_IDLE_CKPT_MS, 400).
-define(ACTOR_MAX_COALESCE_REQS, 16).
-define(ACTOR_MAX_COALESCE_ITEMS, 200).

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

cold_normalize(RawM1, M2, Prefix, PoolKey, FastOpts) ->
    MemBin = extract_snapshot_body(RawM1, FastOpts),
    AtSlot = get_slot(RawM1, FastOpts),
    Role = extract_node_role(RawM1, FastOpts),
    ActorCkpt =
        case {is_fast_actor_role(Role), PoolKey} of
            {true, {ProcID0, _}} -> load_actor_ckpt(Role, ProcID0, AtSlot, FastOpts);
            _ -> none
        end,
    Res =
        case ActorCkpt of
            {ok, CkptBaseSlot, CkptWalSeq, CkptUncompressed} ->
                CleanCkptM1 = replace_snapshot_body(RawM1, CkptUncompressed, FastOpts),
                case delegate_wasm(CleanCkptM1, <<"normalize">>, FastOpts) of
                    {ok, NormCkptM1} ->
                        replay_cold_wal_if_any(NormCkptM1, Prefix, Role, CkptBaseSlot, CkptWalSeq, FastOpts);
                    ErrCkpt ->
                        ErrCkpt
                end;
            none ->
                BaseRes =
                    case MemBin of
                        <<"HSZ1:", ZlibBin/binary>> ->
                            Uncompressed = zlib:uncompress(ZlibBin),
                            CleanM1 = replace_snapshot_body(RawM1, Uncompressed, FastOpts),
                            case delegate_wasm(CleanM1, <<"normalize">>, FastOpts) of
                                {ok, NormM1} ->
                                    case {PoolKey, AtSlot} of
                                        {{ProcID, _}, Slot} when is_integer(Slot) ->
                                            store_last_ckpt(ProcID, Slot);
                                        _ ->
                                            ok
                                    end,
                                    {ok, NormM1};
                                Err ->
                                    Err
                            end;
                        <<"HSCK1:", CkptSlot:64/integer>> ->
                            restore_from_checkpoint_and_replay(
                                RawM1, M2, Prefix, PoolKey, CkptSlot, FastOpts
                            );
                        _ ->
                            delegate_wasm(RawM1, <<"normalize">>, FastOpts)
                    end,
                case {BaseRes, is_fast_actor_role(Role)} of
                    {{ok, NormBaseM1}, true} ->
                        ReplayBase = case is_integer(AtSlot) of true -> AtSlot; false -> 0 end,
                        replay_cold_wal_if_any(NormBaseM1, Prefix, Role, ReplayBase, 0, FastOpts);
                    _ ->
                        BaseRes
                end
        end,
    case Res of
        {ok, NormOut} when PoolKey =/= undefined ->
            {ok,
                hb_private:set(
                    NormOut,
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

%% @doc Confirm the slot in the warm pool and emit either a zlib-compressed full
%% WASM snapshot on checkpoint slots or a lightweight checkpoint reference marker
%% (`HSCK1:...`) on intermediate slots to avoid per-slot 1.31 MB LMDB bloat.
-spec snapshot(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ body := binary() }} | {error, _}.
snapshot(M1, _Req, Opts) ->
    FastOpts = fast_opts(Opts),
    Slot = get_slot(M1, FastOpts),
    Interval = snapshot_interval(FastOpts),
    case resolve_pool_context(M1, FastOpts) of
        {ok, PoolKey = {ProcID, _}, Prefix, Inst, WriteFn, ReadFn, ImportRes} ->
            case is_integer(Slot) of
                true ->
                    pool_confirm_slot(self(), PoolKey, Slot, Prefix, Inst, WriteFn, ReadFn, ImportRes);
                false ->
                    ok
            end,
            snapshot_with_policy(ProcID, Slot, Interval, Inst);
        none ->
            Prefix = output_prefix(M1, FastOpts),
            case hb_private:get(<<Prefix/binary, "/instance">>, M1, FastOpts) of
                Inst when is_pid(Inst) ->
                    case get_or_extract_pool_key(M1, Prefix, FastOpts) of
                        {ProcID, _} ->
                            snapshot_with_policy(ProcID, Slot, Interval, Inst);
                        _ ->
                            {ok, Serialized} = hb_beamr:serialize(Inst),
                            Compressed = zlib:compress(Serialized),
                            {ok, #{ <<"body">> => <<?ZLIB_MAGIC/binary, Compressed/binary>> }}
                    end;
                _ ->
                    delegate_wasm(M1, <<"snapshot">>, FastOpts)
            end
    end.

snapshot_with_policy(ProcID, Slot, Interval, Inst) ->
    NeedFull =
        (not is_integer(Slot))
        orelse (Slot =< 0)
        orelse (Interval =< 1)
        orelse ((Slot rem Interval) =:= 0),
    case NeedFull of
        true ->
            {ok, Serialized} = hb_beamr:serialize(Inst),
            Compressed = zlib:compress(Serialized),
            store_last_ckpt(ProcID, Slot),
            {ok, #{ <<"body">> => <<?ZLIB_MAGIC/binary, Compressed/binary>> }};
        false ->
            case lookup_last_ckpt(ProcID, Slot) of
                {ok, CkptSlot} ->
                    {ok, #{ <<"body">> => <<?CKPT_MAGIC/binary, CkptSlot:64/integer>> }};
                none ->
                    {ok, Serialized} = hb_beamr:serialize(Inst),
                    Compressed = zlib:compress(Serialized),
                    store_last_ckpt(ProcID, Slot),
                    {ok, #{ <<"body">> => <<?ZLIB_MAGIC/binary, Compressed/binary>> }}
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
            maybe_register_actor_meta(M1WithProc, Prefix, ProcID, Image, ProcessJson, Opts, FastOpts),
            {MsgJson, IsMutating} = prepare_msg_json_with_flag(M1WithProc, M2, ProcID, Image, FastOpts),
            execute_wasm(M1WithProc, Prefix, ProcID, Image, MsgJson, ProcessJson, IsMutating, FastOpts);
        Other ->
            Other
    end.

%% @doc Delegate standalone `to' conversion to upstream `json-iface@1.0'.
to(M1, M2, Opts) ->
    delegate_to_json_iface(M1, M2, Opts).

%% @doc Delegate standalone `from' conversion to upstream `json-iface@1.0'.
from(M1, M2, Opts) ->
    delegate_to_json_iface(M1, M2, Opts).

%% @doc Return the latest cached JSON output from RAM (`hs_wasm_latest_cache`)
%% by `role' or `process' ID in sub-millisecond time without scanning LMDB.
-spec latest(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ _ => _ }} | {error, _}.
latest(M1, M2, Opts) ->
    FastOpts = fast_opts(Opts),
    Key =
        case hb_ao:get(<<"process">>, M2, undefined, FastOpts) of
            P when is_binary(P), byte_size(P) > 0 ->
                P;
            _ ->
                case hb_ao:get(<<"role">>, M2, undefined, FastOpts) of
                    R when is_binary(R), byte_size(R) > 0 ->
                        R;
                    _ ->
                        hb_ao:get(<<"node-role">>, M1, <<"primary">>, FastOpts)
                end
        end,
    ensure_proc_cache_table(),
    case lookup_latest_cache(Key) of
        {ok, BodyBin} ->
            {ok, #{
                <<"status">> => 200,
                <<"content-type">> => <<"application/json">>,
                <<"cache-control">> => <<"no-store">>,
                <<"body">> => BodyBin
            }};
        miss ->
            FallbackRes =
                hb_ao:resolve(
                    #{ <<"device">> => <<"node-process@1.0">> },
                    #{
                        <<"path">> => <<Key/binary, "/now/results~message@1.0/output/body">>
                    },
                    Opts
                ),
            case FallbackRes of
                {ok, FallbackBody} when is_binary(FallbackBody) ->
                    store_latest_cache(Key, undefined, FallbackBody),
                    {ok, #{
                        <<"status">> => 200,
                        <<"content-type">> => <<"application/json">>,
                        <<"cache-control">> => <<"no-store">>,
                        <<"body">> => FallbackBody
                    }};
                {ok, #{ <<"body">> := FallbackBody }} when is_binary(FallbackBody) ->
                    store_latest_cache(Key, undefined, FallbackBody),
                    {ok, #{
                        <<"status">> => 200,
                        <<"content-type">> => <<"application/json">>,
                        <<"cache-control">> => <<"no-store">>,
                        <<"body">> => FallbackBody
                    }};
                Other ->
                    Other
            end
    end.

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
    case {
        ets:info(hs_wasm_proc_cache),
        ets:info(hs_wasm_ckpt_cache),
        ets:info(hs_wasm_latest_cache),
        ets:info(hs_wasm_actors)
    } of
        {P, C, L, A} when P =/= undefined, C =/= undefined, L =/= undefined, A =/= undefined ->
            ok;
        _ ->
            hb_name:singleton(
                hs_wasm_proc_cache_owner,
                fun() ->
                    TableOpts = [named_table, set, public, {read_concurrency, true}, {write_concurrency, true}],
                    lists:foreach(
                        fun(Tbl) ->
                            try ets:new(Tbl, TableOpts)
                            catch _:_ -> ok
                            end
                        end,
                        [hs_wasm_proc_cache, hs_wasm_ckpt_cache, hs_wasm_latest_cache, hs_wasm_actors]
                    ),
                    receive stop -> ok end
                end
            )
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
    {MsgJson, _IsMutating} = prepare_msg_json_with_flag(M1, M2, ProcID, Image, Opts),
    MsgJson.

prepare_msg_json_with_flag(M1, M2, ProcID, Image, Opts) ->
    RawBody = hb_maps:get(<<"body">>, M2, #{}, Opts),
    LoadedBody =
        case RawBody of
            Map when is_map(Map) ->
                case contains_links(Map) of
                    true -> hb_cache:ensure_all_loaded(Map, Opts);
                    false -> Map
                end;
            {link, _, _} -> hb_cache:ensure_all_loaded(RawBody, Opts);
            _ -> #{}
        end,
    MutSource =
        case is_map(LoadedBody) andalso
             (hb_ao:get(<<"action">>, LoadedBody, hb_ao:get(<<"Action">>, LoadedBody, undefined, Opts), Opts) =/= undefined) of
            true -> LoadedBody;
            false -> M2
        end,
    IsMutating = is_mutating_req(MutSource, Opts),
    MsgJson =
        case is_map(LoadedBody) andalso not is_map_key(<<"commitments">>, LoadedBody) of
            true ->
                try fast_msg_json(LoadedBody, M1, M2, ProcID, Image, Opts)
                catch _:_ -> slow_msg_json(LoadedBody, M1, M2, ProcID, Image, Opts)
                end;
            false ->
                slow_msg_json(LoadedBody, M1, M2, ProcID, Image, Opts)
        end,
    {MsgJson, IsMutating}.

contains_links(Map) when is_map(Map) ->
    maps:fold(
        fun(_K, {link, _, _}, _Acc) -> true;
           (_K, V, false) when is_map(V) -> contains_links(V);
           (_K, _V, Acc) -> Acc
        end,
        false,
        Map
    );
contains_links(_) ->
    false.

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

execute_wasm(M1, Prefix, ProcID, Image, MsgJson, ProcessJson, IsMutating, Opts) ->
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
                            case decode_wasm_response(Inst, Res, MsgAfterExec, ProcID, Image, Opts) of
                                {ok, MsgWithResults} ->
                                    case IsMutating of
                                        true ->
                                            Results = hb_maps:get(<<"results">>, MsgWithResults, #{}, Opts),
                                            case extract_http_status(Results) < 400 of
                                                true -> persist_cold_wal_if_actor(MsgJson);
                                                false -> ok
                                            end;
                                        false ->
                                            ok
                                    end,
                                    {ok, MsgWithResults};
                                OtherRes ->
                                    OtherRes
                            end
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
                    record_latest_result(MsgAfterExec, ProcID, Results, Opts),
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

pool_set_actor_meta(CallerPid, Meta) ->
    _ = pool_call({set_actor_meta, CallerPid, Meta}),
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
        {{set_actor_meta, CallerPid, Meta}, From, Ref} ->
            NextState = handle_set_actor_meta(CallerPid, Meta, State),
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
    ExistingMeta =
        case maps:find(CallerPid, maps:get(active, State)) of
            {ok, Prev} -> maps:get(actor_meta, Prev, undefined);
            error -> undefined
        end,
    Entry = #{
        key => PoolKey,
        prefix => Prefix,
        instance => Inst,
        write => WriteFn,
        read => ReadFn,
        import_resolver => ImportRes,
        confirmed_slot => undefined,
        actor_meta => ExistingMeta
    },
    set_active_caller(CallerPid, Entry, State).

handle_confirm_slot(CallerPid, PoolKey, Slot, Prefix, Inst, WriteFn, ReadFn, ImportRes, State) ->
    ExistingMeta =
        case maps:find(CallerPid, maps:get(active, State)) of
            {ok, Prev} -> maps:get(actor_meta, Prev, undefined);
            error -> undefined
        end,
    Entry = #{
        key => PoolKey,
        prefix => Prefix,
        instance => Inst,
        write => WriteFn,
        read => ReadFn,
        import_resolver => ImportRes,
        confirmed_slot => Slot,
        actor_meta => ExistingMeta
    },
    set_active_caller(CallerPid, Entry, State).

handle_set_actor_meta(CallerPid, Meta, State = #{ active := Active }) ->
    case maps:find(CallerPid, Active) of
        {ok, Entry} ->
            State#{ active => maps:put(CallerPid, Entry#{ actor_meta => Meta }, Active) };
        error ->
            State
    end.

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
            case maybe_promote_to_actor(Entry, State) of
                {promoted, State1} ->
                    State1;
                not_actor ->
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
                    evict_overflow_idle(State#{ idle => Idle1, lru => Lru1 })
            end;
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

%%% Compressed Checkpoint Snapshot & Bounded Replay Helpers

snapshot_interval(Opts) ->
    case hb_util:safe_int(hb_opts:get(process_snapshot_slots, ?DEFAULT_CKPT_SLOTS, Opts)) of
        {ok, N} when N > 0 -> N;
        _ -> ?DEFAULT_CKPT_SLOTS
    end.

store_last_ckpt(ProcID, Slot) when is_binary(ProcID), is_integer(Slot), Slot >= 0 ->
    try
        ensure_proc_cache_table(),
        ets:insert(hs_wasm_ckpt_cache, {ProcID, Slot})
    catch _:_ ->
        ok
    end;
store_last_ckpt(_, _) ->
    ok.

lookup_last_ckpt(ProcID, CurSlot) when is_binary(ProcID), is_integer(CurSlot) ->
    try
        case ets:info(hs_wasm_ckpt_cache) of
            undefined -> none;
            _ ->
                case ets:lookup(hs_wasm_ckpt_cache, ProcID) of
                    [{ProcID, CkptSlot}] when is_integer(CkptSlot), CkptSlot >= 0, CkptSlot < CurSlot ->
                        {ok, CkptSlot};
                    _ ->
                        none
                end
        end
    catch _:_ ->
        none
    end;
lookup_last_ckpt(_, _) ->
    none.

record_latest_result(MsgAfterExec, ProcID, Results, Opts) when is_map(Results) ->
    BodyBin =
        case maps:get(<<"data">>, Results, undefined) of
            B when is_binary(B), byte_size(B) > 0 -> B;
            _ ->
                Out = maps:get(<<"output">>, Results, #{}),
                case maps:get(<<"body">>, Out, undefined) of
                    B2 when is_binary(B2), byte_size(B2) > 0 -> B2;
                    _ -> <<>>
                end
        end,
    case byte_size(BodyBin) > 0 of
        true ->
            Role = extract_node_role(MsgAfterExec, Opts),
            store_latest_cache(ProcID, Role, BodyBin);
        false ->
            ok
    end;
record_latest_result(_, _, _, _) ->
    ok.

extract_node_role(Msg, Opts) ->
    Proc = hb_maps:get(<<"process">>, Msg, #{}, Opts),
    case hb_ao:get(<<"node-role">>, Proc, undefined, Opts) of
        R when is_binary(R), byte_size(R) > 0 ->
            R;
        _ ->
            case hb_ao:get(<<"node-role">>, Msg, undefined, Opts) of
                R2 when is_binary(R2), byte_size(R2) > 0 -> R2;
                _ -> undefined
            end
    end.

store_latest_cache(ProcID, Role, BodyBin) ->
    try
        ensure_proc_cache_table(),
        Ts = erlang:system_time(millisecond),
        if is_binary(ProcID), byte_size(ProcID) > 0 ->
            ets:insert(hs_wasm_latest_cache, {ProcID, BodyBin, Ts});
           true -> ok
        end,
        if is_binary(Role), byte_size(Role) > 0 ->
            ets:insert(hs_wasm_latest_cache, {Role, BodyBin, Ts});
           true -> ok
        end
    catch _:_ ->
        ok
    end.

lookup_latest_cache(Key) when is_binary(Key) ->
    try
        case ets:info(hs_wasm_latest_cache) of
            undefined -> miss;
            _ ->
                case ets:lookup(hs_wasm_latest_cache, Key) of
                    [{Key, BodyBin, _Ts}] when is_binary(BodyBin) -> {ok, BodyBin};
                    _ -> miss
                end
        end
    catch _:_ ->
        miss
    end;
lookup_latest_cache(_) ->
    miss.

snapshot_path(RawM1, Opts) ->
    case hb_ao:get(<<"device-key">>, RawM1, Opts) of
        not_found -> [<<"snapshot">>, <<"body">>];
        Key -> [<<"snapshot">>, Key, <<"body">>]
    end.

extract_snapshot_body(RawM1, Opts) ->
    hb_ao:get(snapshot_path(RawM1, Opts), {as, <<"message@1.0">>, RawM1}, not_found, Opts).

replace_snapshot_body(RawM1, NewBody, Opts) ->
    Snap0 = hb_maps:get(<<"snapshot">>, RawM1, #{}, Opts),
    Snap0Map = if is_map(Snap0) -> Snap0; true -> #{} end,
    Snap1 =
        case hb_ao:get(<<"device-key">>, RawM1, Opts) of
            not_found ->
                Snap0Map#{ <<"body">> => NewBody };
            Key ->
                Sub0 = hb_maps:get(Key, Snap0Map, #{}, Opts),
                Sub0Map = if is_map(Sub0) -> Sub0; true -> #{} end,
                Snap0Map#{ Key => Sub0Map#{ <<"body">> => NewBody } }
        end,
    RawM1#{ <<"snapshot">> => Snap1 }.

restore_from_checkpoint_and_replay(RawM1, _M2, Prefix, PoolKey, CkptSlot, FastOpts) ->
    AtSlot =
        case get_slot(RawM1, FastOpts) of
            S when is_integer(S) -> S;
            _ -> CkptSlot
        end,
    ProcID =
        case PoolKey of
            {PID, _} when is_binary(PID) -> PID;
            _ ->
                Proc = hb_ao:get(<<"process">>, RawM1, #{}, FastOpts),
                hb_util:human_id(hb_message:id(Proc, all, FastOpts))
        end,
    Interval = snapshot_interval(FastOpts),
    {BaseSlot, InitRes} =
        case find_and_normalize_checkpoint(ProcID, CkptSlot, Interval, RawM1, FastOpts) of
            {ok, FoundSlot, NormM1} ->
                {FoundSlot, {ok, NormM1}};
            none ->
                CleanInit = strip_snapshot(RawM1, FastOpts),
                {-1, delegate_wasm(CleanInit, <<"init">>, FastOpts)}
        end,
    case InitRes of
        {ok, InstM1} ->
            InstM1WithProc =
                case hb_maps:get(<<"process">>, InstM1, not_found, FastOpts) of
                    not_found ->
                        hb_maps:put(<<"process">>, hb_maps:get(<<"process">>, RawM1, #{}, FastOpts), InstM1, FastOpts);
                    _ ->
                        InstM1
                end,
            {M1Ready, LoadedProcID, Image, ProcessJson} =
                get_or_cache_process(InstM1WithProc, Prefix, FastOpts),
            case replay_slot_range(M1Ready, Prefix, LoadedProcID, Image, ProcessJson, BaseSlot + 1, AtSlot, FastOpts) of
                ok ->
                    WithSlot = hb_maps:put(<<"at-slot">>, AtSlot, M1Ready, FastOpts),
                    store_last_ckpt(LoadedProcID, max(BaseSlot, 0)),
                    {ok, WithSlot};
                {error, _} = Err ->
                    Err
            end;
        Err ->
            Err
    end.

find_and_normalize_checkpoint(ProcID, CkptSlot, Interval, FallbackM1, Opts) ->
    Candidates = checkpoint_candidates(CkptSlot, Interval),
    try_checkpoint_candidates(ProcID, Candidates, FallbackM1, Opts).

checkpoint_candidates(CkptSlot, Interval) when is_integer(CkptSlot), CkptSlot >= 0, Interval > 0 ->
    Seq = lists:seq(CkptSlot, 0, -Interval),
    case lists:last(Seq) of
        0 -> Seq;
        _ -> Seq ++ [0]
    end;
checkpoint_candidates(_, _) ->
    [0].

try_checkpoint_candidates(_ProcID, [], _FallbackM1, _Opts) ->
    none;
try_checkpoint_candidates(ProcID, [Slot | Rest], FallbackM1, Opts) ->
    case read_computed_slot(ProcID, Slot, Opts) of
        {ok, CachedSlotMsg} ->
            LoadedMsg = hb_cache:ensure_all_loaded(CachedSlotMsg, Opts),
            MsgWithProc =
                case hb_maps:get(<<"process">>, LoadedMsg, not_found, Opts) of
                    not_found ->
                        hb_maps:put(<<"process">>, hb_maps:get(<<"process">>, FallbackM1, #{}, Opts), LoadedMsg, Opts);
                    _ ->
                        LoadedMsg
                end,
            case extract_snapshot_body(MsgWithProc, Opts) of
                <<"HSZ1:", ZlibBin/binary>> ->
                    Uncompressed = zlib:uncompress(ZlibBin),
                    Clean = replace_snapshot_body(MsgWithProc, Uncompressed, Opts),
                    case delegate_wasm(Clean, <<"normalize">>, Opts) of
                        {ok, NormM1} -> {ok, Slot, NormM1};
                        _ -> try_checkpoint_candidates(ProcID, Rest, FallbackM1, Opts)
                    end;
                RawBin when is_binary(RawBin), byte_size(RawBin) > 64 ->
                    case RawBin of
                        <<"HSCK1:", _/binary>> ->
                            try_checkpoint_candidates(ProcID, Rest, FallbackM1, Opts);
                        _ ->
                            case delegate_wasm(MsgWithProc, <<"normalize">>, Opts) of
                                {ok, NormM1} -> {ok, Slot, NormM1};
                                _ -> try_checkpoint_candidates(ProcID, Rest, FallbackM1, Opts)
                            end
                    end;
                _ ->
                    try_checkpoint_candidates(ProcID, Rest, FallbackM1, Opts)
            end;
        _ ->
            try_checkpoint_candidates(ProcID, Rest, FallbackM1, Opts)
    end.

replay_slot_range(_M1, _Prefix, _ProcID, _Image, _ProcessJson, FromSlot, ToSlot, _Opts)
        when FromSlot > ToSlot ->
    ok;
replay_slot_range(M1, Prefix, ProcID, Image, ProcessJson, FromSlot, ToSlot, Opts) ->
    case resolve_exec_handles(M1, Prefix, Opts) of
        {ok, Inst, ImportRes} ->
            replay_each_slot(M1, Inst, ImportRes, ProcID, Image, ProcessJson, FromSlot, ToSlot, Opts);
        Err ->
            Err
    end.

replay_each_slot(_M1, _Inst, _ImportRes, _ProcID, _Image, _ProcessJson, Slot, ToSlot, _Opts)
        when Slot > ToSlot ->
    ok;
replay_each_slot(M1, Inst, ImportRes, ProcID, Image, ProcessJson, Slot, ToSlot, Opts) ->
    case read_scheduler_assignment(ProcID, Slot, Opts) of
        {ok, Assignment} ->
            MsgJson = prepare_msg_json(M1, Assignment, ProcID, Image, Opts),
            case write_wasm_inputs(Inst, MsgJson, ProcessJson) of
                {ok, MsgPtr, ProcPtr} ->
                    case hb_beamr:call(Inst, "handle", [MsgPtr, ProcPtr], ImportRes, M1, Opts) of
                        {trap, Err, _} -> {error, {replay_trap, Slot, Err}};
                        {error, Err, _} -> {error, {replay_error, Slot, Err}};
                        {_ResType, Res, _MsgAfterExec} ->
                            case Res of
                                [P] when is_integer(P) -> _ = read_c_string(Inst, P);
                                P when is_integer(P) -> _ = read_c_string(Inst, P);
                                _ -> ok
                            end,
                            replay_each_slot(M1, Inst, ImportRes, ProcID, Image, ProcessJson, Slot + 1, ToSlot, Opts)
                    end;
                Err ->
                    Err
            end;
        not_found ->
            replay_each_slot(M1, Inst, ImportRes, ProcID, Image, ProcessJson, Slot + 1, ToSlot, Opts)
    end.

read_computed_slot(ProcID, Slot, RawOpts) ->
    CacheOpts = RawOpts#{
        <<"store">> => hb_opts:get(
            process_store,
            hb_opts:get(store, no_viable_store, RawOpts),
            RawOpts
        )
    },
    Path = hb_path:to_binary([
        <<"computed">>,
        hb_util:human_id(ProcID),
        <<"slot">>,
        integer_to_binary(Slot)
    ]),
    hb_cache:read(Path, CacheOpts).

read_scheduler_assignment(ProcID, Slot, RawOpts) ->
    SchedOpts = RawOpts#{
        <<"store">> => hb_opts:get(
            scheduler_store,
            hb_opts:get(store, no_viable_store, RawOpts),
            RawOpts
        )
    },
    P1 = hb_path:to_binary([
        <<"~scheduler@1.0">>,
        <<"assignments">>,
        hb_util:human_id(ProcID),
        hb_util:bin(Slot)
    ]),
    case hb_cache:read(P1, SchedOpts) of
        {ok, RawAssignment} ->
            Assignment = hb_message:normalize_commitments(RawAssignment, SchedOpts),
            {ok, hb_cache:ensure_all_loaded(Assignment, SchedOpts)};
        {error, not_found} ->
            not_found
    end.


%%% Persistent Fast L0 WASM Actor + WAL/Checkpoint Recovery (`hs_wasm_actor`)

is_fast_actor_role(<<"primary">>) -> true;
is_fast_actor_role(<<"sd1">>) -> true;
is_fast_actor_role(<<"sd2">>) -> true;
is_fast_actor_role(_) -> false.

maybe_register_actor_meta(M1WithProc, _Prefix, ProcID, Image, ProcessJson, FullOpts, FastOpts) ->
    Role = extract_node_role(M1WithProc, FastOpts),
    case is_fast_actor_role(Role) of
        true ->
            {WalSeq, CkptBaseSlot} =
                case erase(hs_cold_wal_state) of
                    {WS, BS} when is_integer(WS), is_integer(BS) ->
                        {WS, BS};
                    _ ->
                        case get(hs_cold_actor_ctx) of
                            #{ proc_id := ProcID, role := Role, wal_seq := PrevWS, ckpt_base_slot := PrevBS } ->
                                {PrevWS, PrevBS};
                            _ ->
                                {0, 0}
                        end
                end,
            Meta = #{
                proc_id => ProcID,
                role => Role,
                image => Image,
                proc_json => ProcessJson,
                node_opts => FullOpts,
                exec_opts => FastOpts,
                base_template => strip_snapshot(M1WithProc, FastOpts),
                wal_seq => WalSeq,
                ckpt_base_slot => CkptBaseSlot
            },
            put(hs_cold_actor_ctx, Meta),
            pool_set_actor_meta(self(), Meta);
        false ->
            erase(hs_cold_actor_ctx),
            ok
    end.

persist_cold_wal_if_actor(MsgJson) ->
    case get(hs_cold_actor_ctx) of
        Meta = #{ proc_id := ProcID, role := Role, wal_seq := WS, ckpt_base_slot := BS, node_opts := NodeOpts } ->
            case is_fast_actor_role(Role) of
                true ->
                    WalSeqNext = WS + 1,
                    WalDir = actor_wal_dir(Role, NodeOpts),
                    ok = filelib:ensure_dir(filename:join(WalDir, "dummy")),
                    WalPath = filename:join(WalDir, binary_to_list(ProcID) ++ ".wal"),
                    case file:open(WalPath, [append, raw, binary]) of
                        {ok, WalFd} ->
                            try append_actor_wal(WalFd, BS, WalSeqNext, MsgJson)
                            after _ = file:close(WalFd)
                            end,
                            Meta1 = Meta#{ wal_seq => WalSeqNext },
                            put(hs_cold_actor_ctx, Meta1),
                            pool_set_actor_meta(self(), Meta1);
                        _ ->
                            ok
                    end;
                false ->
                    ok
            end;
        _ ->
            ok
    end.

maybe_promote_to_actor(#{ actor_meta := Meta = #{ proc_id := ProcID, role := Role, image := Image },
                          instance := Inst,
                          import_resolver := ImportRes,
                          confirmed_slot := Slot,
                          key := PoolKey },
                       State = #{ idle := Idle, lru := Lru }) when is_map(Meta) ->
    ensure_proc_cache_table(),
    case is_fast_actor_role(Role) of
        false ->
            not_actor;
        true ->
            ExistingAlive =
                case ets:lookup(hs_wasm_actors, {actor, ProcID}) of
                    [{_, ExistingPid}] when is_pid(ExistingPid) ->
                        erlang:is_process_alive(ExistingPid);
                    _ ->
                        false
                end,
            case ExistingAlive of
                true ->
                    safe_stop_beamr(Inst),
                    Idle1 = maps:remove(PoolKey, Idle),
                    Lru1 = lists:delete(PoolKey, Lru),
                    {promoted, State#{ idle => Idle1, lru => Lru1 }};
                false ->
                    ActorPid = spawn(fun() -> actor_init(Inst, ImportRes, Slot, Meta) end),
                    ets:insert(hs_wasm_actors, [
                        {{actor, ProcID}, ActorPid},
                        {{role_img_actor, Role, Image}, {ProcID, ActorPid}},
                        {{role_actor, Role}, {ProcID, ActorPid}}
                    ]),
                    Idle1 = maps:remove(PoolKey, Idle),
                    Lru1 = lists:delete(PoolKey, Lru),
                    {promoted, State#{ idle => Idle1, lru => Lru1 }}
            end
    end;
maybe_promote_to_actor(_, _) ->
    not_actor.

actor_init(Inst, ImportRes, ConfirmedSlot, Meta) ->
    #{
        proc_id := ProcID,
        role := Role,
        image := Image,
        proc_json := ProcessJson,
        node_opts := NodeOpts,
        exec_opts := ExecOpts,
        base_template := BaseTemplate
    } = Meta,
    InitWalSeq = maps:get(wal_seq, Meta, 0),
    BaseSlot =
        case is_integer(ConfirmedSlot) of
            true -> ConfirmedSlot;
            false -> maps:get(ckpt_base_slot, Meta, 0)
        end,
    NowSyncSlot = resolve_now_sync_slot(ProcID, BaseSlot, NodeOpts),
    WalDir = actor_wal_dir(Role, NodeOpts),
    ok = filelib:ensure_dir(filename:join(WalDir, "dummy")),
    WalPath = filename:join(WalDir, binary_to_list(ProcID) ++ ".wal"),
    CkptPath = filename:join(WalDir, binary_to_list(ProcID) ++ ".ckpt"),
    {ok, WalFd} = file:open(WalPath, [append, raw, binary]),
    InstMon = erlang:monitor(process, Inst),
    State = #{
        proc_id => ProcID,
        role => Role,
        image => Image,
        proc_json => ProcessJson,
        node_opts => NodeOpts,
        exec_opts => ExecOpts,
        base_template => BaseTemplate,
        inst => Inst,
        inst_mon => InstMon,
        import_res => ImportRes,
        base_slot => BaseSlot,
        now_sync_slot => NowSyncSlot,
        wal_seq => InitWalSeq,
        outbox_seq => 0,
        wal_path => WalPath,
        ckpt_path => CkptPath,
        wal_fd => WalFd,
        dirty_since_ckpt => 1,
        ckpt_in_flight => false,
        idle_timer => undefined,
        flush_timer => undefined,
        last_queued => 0
    },
    actor_loop(arm_idle_ckpt_timer(State)).

resolve_now_sync_slot(ProcID, BaseSlot, NodeOpts) ->
    try
        CacheOpts = NodeOpts#{
            <<"store">> => hb_opts:get(process_store, hb_opts:get(store, no_viable_store, NodeOpts), NodeOpts)
        },
        HumanID = hb_util:human_id(ProcID),
        SlotRoot = hb_path:to_binary([<<"computed">>, HumanID, <<"slot">>]),
        case hb_cache:list_numbered(SlotRoot, CacheOpts) of
            Slots when is_list(Slots), length(Slots) > 0 ->
                max(BaseSlot, lists:max(Slots));
            _ ->
                BaseSlot
        end
    catch _:_ ->
        BaseSlot
    end.

actor_loop(State = #{ proc_id := ProcID, role := Role, image := Image, inst := Inst, inst_mon := InstMon }) ->
    receive
        {fast_push, Req, ReqOpts, DispatchFn, From, Ref} ->
            cancel_actor_timer(maps:get(idle_timer, State)),
            cancel_actor_timer(maps:get(flush_timer, State, undefined)),
            State0 = State#{ idle_timer => undefined, flush_timer => undefined },
            FirstItem = #{ req => Req, opts => ReqOpts, dispatch_fn => DispatchFn, from => From, ref => Ref },
            ExtraItems = drain_actor_mailbox(?ACTOR_MAX_COALESCE_REQS - 1, ?ACTOR_MAX_COALESCE_ITEMS - 1),
            AllItems = [FirstItem | ExtraItems],
            NextState = process_actor_items(AllItems, State0),
            actor_loop(NextState);

        {fast_push, Req, ReqOpts, From, Ref} ->
            cancel_actor_timer(maps:get(idle_timer, State)),
            cancel_actor_timer(maps:get(flush_timer, State, undefined)),
            State0 = State#{ idle_timer => undefined, flush_timer => undefined },
            FirstItem = #{ req => Req, opts => ReqOpts, dispatch_fn => undefined, from => From, ref => Ref },
            ExtraItems = drain_actor_mailbox(?ACTOR_MAX_COALESCE_REQS - 1, ?ACTOR_MAX_COALESCE_ITEMS - 1),
            AllItems = [FirstItem | ExtraItems],
            NextState = process_actor_items(AllItems, State0),
            actor_loop(NextState);

        {auto_flush_batch, ExpectedWalSeq} ->
            State0 = State#{ flush_timer => undefined },
            case Role =:= <<"primary">>
                 andalso maps:get(wal_seq, State0) =:= ExpectedWalSeq
                 andalso maps:get(last_queued, State0, 0) > 0 of
                true ->
                    BaseTemplate = maps:get(base_template, State0),
                    NodeOpts = maps:get(node_opts, State0),
                    spawn(fun() ->
                        _ = (catch hb_ao:resolve(
                            BaseTemplate#{
                                <<"device">> => <<"hs-push@1.0">>,
                                <<"execution-device">> => <<"hs-wasm-output@1.0">>,
                                <<"node-role">> => Role,
                                <<"image">> => Image
                            },
                            #{ <<"path">> => <<"push">>, <<"action">> => <<"Advance">> },
                            NodeOpts
                        ))
                    end),
                    actor_loop(State0#{ last_queued => 0 });
                false ->
                    actor_loop(State0)
            end;

        {idle_ckpt, ExpectedWalSeq} ->
            State0 = State#{ idle_timer => undefined },
            case maps:get(wal_seq, State0) =:= ExpectedWalSeq
                 andalso maps:get(dirty_since_ckpt, State0) > 0
                 andalso maps:get(ckpt_in_flight, State0) =:= false of
                true ->
                    actor_loop(start_async_ckpt(State0));
                false ->
                    actor_loop(State0)
            end;

        {ckpt_saved, SavedWalSeq} ->
            State0 = State#{ ckpt_in_flight => false },
            NextState =
                case maps:get(wal_seq, State0) =:= SavedWalSeq of
                    true -> rotate_actor_wal(State0);
                    false -> State0
                end,
            actor_loop(NextState);

        {ckpt_failed, _Reason} ->
            actor_loop(State#{ ckpt_in_flight => false });

        {'DOWN', InstMon, process, Inst, Reason} ->
            cleanup_actor_registration(ProcID, Role, Image, State),
            exit({wasm_inst_down, Reason});

        stop ->
            cleanup_actor_registration(ProcID, Role, Image, State),
            ok;

        _Other ->
            actor_loop(State)
    end.

drain_actor_mailbox(MaxReqs, MaxItems) when MaxReqs > 0, MaxItems > 0 ->
    drain_actor_mailbox_loop(MaxReqs, MaxItems, []);
drain_actor_mailbox(_, _) ->
    [].

drain_actor_mailbox_loop(0, _ItemsLeft, Acc) ->
    lists:reverse(Acc);
drain_actor_mailbox_loop(_ReqsLeft, ItemsLeft, Acc) when ItemsLeft =< 0 ->
    lists:reverse(Acc);
drain_actor_mailbox_loop(ReqsLeft, ItemsLeft, Acc) ->
    receive
        {fast_push, Req, ReqOpts, DispatchFn, From, Ref} ->
            Item = #{ req => Req, opts => ReqOpts, dispatch_fn => DispatchFn, from => From, ref => Ref },
            Cost = estimate_item_count(Req, ReqOpts),
            drain_actor_mailbox_loop(ReqsLeft - 1, ItemsLeft - Cost, [Item | Acc]);
        {fast_push, Req, ReqOpts, From, Ref} ->
            Item = #{ req => Req, opts => ReqOpts, dispatch_fn => undefined, from => From, ref => Ref },
            Cost = estimate_item_count(Req, ReqOpts),
            drain_actor_mailbox_loop(ReqsLeft - 1, ItemsLeft - Cost, [Item | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.

estimate_item_count(Req, Opts) ->
    case hb_maps:get(<<"action">>, Req, undefined, Opts) of
        <<"Write-Batch">> ->
            Packed = hb_maps:get(<<"packed_inputs">>, Req, hb_maps:get(<<"packed-inputs">>, Req, <<>>, Opts), Opts),
            case is_binary(Packed) andalso byte_size(Packed) > 0 of
                true -> length(binary:matches(Packed, <<";">>)) + 1;
                false -> 1
            end;
        _ ->
            1
    end.

process_actor_items([], State) ->
    arm_flush_timer(arm_idle_ckpt_timer(State));
process_actor_items([Item | Rest], State) ->
    ExecOpts = maps:get(exec_opts, State),
    case extract_actor_coalesce(Item, ExecOpts) of
        {ok, FirstEntry} ->
            {Coalesced, Tail} = take_coalescible_prefix(Rest, ExecOpts, [FirstEntry], maps:get(count, FirstEntry)),
            NextState = execute_actor_coalesced(lists:reverse(Coalesced), State),
            process_actor_items(Tail, NextState);
        skip ->
            #{ req := Req, dispatch_fn := DispatchFn, from := From, ref := Ref } = Item,
            {Reply, NextState} = actor_exec_one(Req, Req, DispatchFn, State),
            From ! {Ref, Reply},
            process_actor_items(Rest, NextState)
    end.

take_coalescible_prefix([], _Opts, Acc, _TotalCount) ->
    {Acc, []};
take_coalescible_prefix([Item | Rest] = All, Opts, Acc, TotalCount) ->
    case TotalCount >= ?ACTOR_MAX_COALESCE_ITEMS of
        true ->
            {Acc, All};
        false ->
            case extract_actor_coalesce(Item, Opts) of
                {ok, Entry} ->
                    take_coalescible_prefix(Rest, Opts, [Entry | Acc], TotalCount + maps:get(count, Entry));
                skip ->
                    {Acc, All}
            end
    end.

execute_actor_coalesced([#{ item := #{ req := Req, dispatch_fn := DispatchFn, from := From, ref := Ref } }], State) ->
    {Reply, NextState} = actor_exec_one(Req, Req, DispatchFn, State),
    From ! {Ref, Reply},
    NextState;
execute_actor_coalesced([FirstEntry | RestEntries] = Entries, State) ->
    #{ item := #{ req := TemplateReq, dispatch_fn := DispatchFn, from := FirstFrom, ref := FirstRef } } = FirstEntry,
    PackedInputs = iolist_to_binary(lists:join(<<";">>, [maps:get(packed, E) || E <- Entries])),
    FlushOut = lists:any(fun(E) -> maps:get(flush, E) end, Entries),
    TsOut = lists:foldl(fun(E, Acc) -> max(Acc, maps:get(ts, E)) end, 0, Entries),
    DropKeys = [
        <<"id">>, <<"x">>, <<"y">>, <<"target_tick">>, <<"client_tick">>,
        <<"inputs">>, <<"packed-inputs">>, <<"packed_inputs">>,
        <<"client_signer">>, <<"client-signer">>, <<"owner">>,
        <<"Action">>, <<"timestamp">>
    ],
    MergedReq = (maps:without(DropKeys, TemplateReq))#{
        <<"action">> => <<"Write-Batch">>,
        <<"packed_inputs">> => PackedInputs,
        <<"flush">> => FlushOut,
        <<"ts">> => TsOut
    },
    {Reply, NextState} = actor_exec_one(MergedReq, MergedReq, DispatchFn, State),
    FirstFrom ! {FirstRef, Reply},
    SlimFollowerReply = strip_coalesced_outbox(Reply),
    _ = [From ! {Ref, SlimFollowerReply} || #{ item := #{ from := From, ref := Ref } } <- RestEntries],
    NextState.

strip_coalesced_outbox({ok, PushRes}) when is_map(PushRes) ->
    CleanResults =
        case maps:get(<<"results">>, PushRes, undefined) of
            Res when is_map(Res) -> maps:without([<<"outbox">>], Res);
            _ -> #{}
        end,
    {ok, (maps:without([<<"outbox">>, <<"_hs_outbox_order">>], PushRes))#{
        <<"results">> => CleanResults
    }};
strip_coalesced_outbox(Other) ->
    Other.

actor_exec_one(WorkReq, OrigReq, DispatchFn, State) ->
    #{
        proc_id := ProcID,
        role := Role,
        image := Image,
        proc_json := ProcessJson,
        node_opts := NodeOpts,
        exec_opts := ExecOpts,
        base_template := BaseTemplate,
        inst := Inst,
        import_res := ImportRes,
        base_slot := BaseSlot,
        wal_seq := WalSeq,
        wal_fd := WalFd,
        dirty_since_ckpt := Dirty
    } = State,
    NowSyncSlot = maps:get(now_sync_slot, State, BaseSlot),
    WalSeqNext = WalSeq + 1,
    MsgId = <<"fast-", (integer_to_binary(WalSeqNext))/binary>>,
    MsgJson = build_actor_msg_json(WorkReq, OrigReq, MsgId, BaseTemplate, ProcID, Image, ExecOpts),
    case write_wasm_inputs(Inst, MsgJson, ProcessJson) of
        {ok, MsgPtr, ProcPtr} ->
            case hb_beamr:call(Inst, "handle", [MsgPtr, ProcPtr], ImportRes, BaseTemplate, ExecOpts) of
                {trap, Err, _} ->
                    cleanup_actor_registration(ProcID, Role, Image, State),
                    {{error, {wasm_trap, Err}}, State};
                {error, Err, _} ->
                    {{error, Err}, State};
                {_ResType, Res, _MsgAfterExec} ->
                    Ptr = case Res of [P] when is_integer(P) -> P; P when is_integer(P) -> P end,
                    case read_c_string(Inst, Ptr) of
                        {ok, JsonBin} ->
                            try hb_json:decode(JsonBin) of
                                #{ <<"ok">> := true, <<"response">> := Resp } ->
                                    Results = build_results_map(Resp, ProcID, Image, ExecOpts),
                                    record_latest_result(BaseTemplate, ProcID, Results, ExecOpts),
                                    HttpStatus = extract_http_status(Results),
                                    NextState0 =
                                        case is_mutating_req(WorkReq, ExecOpts) andalso HttpStatus < 400 of
                                            true ->
                                                append_actor_wal(WalFd, BaseSlot, WalSeqNext, MsgJson),
                                                State#{ wal_seq => WalSeqNext, dirty_since_ckpt => Dirty + 1 };
                                            false ->
                                                State
                                        end,
                                    NewSyncSlot = maybe_sync_now_cache(WorkReq, Results, HttpStatus, ProcID, NowSyncSlot, NodeOpts),
                                    QueuedInt = actor_int_opt(maps:get(<<"queued">>, maps:get(<<"output">>, Results, #{}), 0), 0),
                                    NextState1 = maybe_trigger_ckpt(NextState0#{ last_queued => QueuedInt, now_sync_slot => NewSyncSlot }),
                                    OutboxMap = maps:get(<<"outbox">>, Results, #{}),
                                    {RemainingOutbox, OutboxOrderTag, NextState2} =
                                        case is_function(DispatchFn, 2) of
                                            true ->
                                                RemOutbox =
                                                    case is_map(OutboxMap) andalso map_size(OutboxMap) > 0 of
                                                        true ->
                                                            try DispatchFn(OutboxMap, ExecOpts)
                                                            catch _:_ -> OutboxMap
                                                            end;
                                                        false ->
                                                            OutboxMap
                                                    end,
                                                {RemOutbox, undefined, NextState1};
                                            false ->
                                                case is_map(OutboxMap) andalso map_size(OutboxMap) > 0 of
                                                    true ->
                                                        OSeqNext = maps:get(outbox_seq, NextState1, 0) + 1,
                                                        {OutboxMap, {self(), OSeqNext}, NextState1#{ outbox_seq => OSeqNext }};
                                                    false ->
                                                        {OutboxMap, undefined, NextState1}
                                                end
                                        end,
                                    CleanResults = Results#{ <<"outbox">> => RemainingOutbox },
                                    PushRes0 = CleanResults#{
                                        <<"results">> => CleanResults,
                                        <<"slot">> => BaseSlot,
                                        <<"process">> => ProcID
                                    },
                                    PushRes =
                                        case OutboxOrderTag of
                                            undefined -> PushRes0;
                                            Tag -> PushRes0#{ <<"_hs_outbox_order">> => Tag }
                                        end,
                                    {{ok, PushRes}, NextState2};
                                #{ <<"ok">> := false } = Reply ->
                                    {{error, maps:get(<<"response">>, Reply, <<"AOS execution error.">>)}, State};
                                _Other ->
                                    {{error, #{ <<"body">> => <<"Invalid JSON message input.">> }}, State}
                            catch _:_ ->
                                {{error, #{ <<"body">> => <<"JSON error parsing result output.">> }}, State}
                            end;
                        Err ->
                            {{error, Err}, State}
                    end
            end;
        {error, Reason} ->
            {{error, Reason}, State}
    end.

build_actor_msg_json(WorkReq, OrigReq, MsgId, BaseTemplate, ProcID, Image, ExecOpts) ->
    CleanReq = strip_http_req_keys(WorkReq),
    M2Stub = #{
        <<"body">> => CleanReq,
        <<"timestamp">> => resolve_req_ts(CleanReq),
        <<"hash-chain">> => MsgId
    },
    case is_map(OrigReq) andalso is_map_key(<<"commitments">>, OrigReq) of
        true ->
            slow_msg_json(OrigReq, BaseTemplate, M2Stub#{ <<"body">> => OrigReq }, ProcID, Image, ExecOpts);
        false ->
            try fast_msg_json(CleanReq, BaseTemplate, M2Stub, ProcID, Image, ExecOpts)
            catch _:_ -> slow_msg_json(CleanReq, BaseTemplate, M2Stub, ProcID, Image, ExecOpts)
            end
    end.

strip_http_req_keys(Req) when is_map(Req) ->
    maps:without([
        <<"priv">>, <<"commitments">>, <<"method">>, <<"path">>,
        <<"host">>, <<"user-agent">>, <<"accept">>, <<"accept-encoding">>,
        <<"content-type">>, <<"content-length">>, <<"connection">>,
        <<"expect">>, <<"transfer-encoding">>, <<"cache-control">>
    ], Req);
strip_http_req_keys(Other) ->
    Other.

resolve_req_ts(Req) when is_map(Req) ->
    case parse_epoch_ms(maps:get(<<"timestamp">>, Req, undefined)) of
        {ok, Ts} -> Ts;
        none -> erlang:system_time(millisecond)
    end;
resolve_req_ts(_) ->
    erlang:system_time(millisecond).

extract_http_status(Results) when is_map(Results) ->
    Out = maps:get(<<"output">>, Results, #{}),
    case maps:get(<<"status">>, Out, 200) of
        I when is_integer(I) -> I;
        B when is_binary(B) ->
            case hb_util:safe_int(B) of
                {ok, I2} -> I2;
                _ -> 200
            end;
        _ -> 200
    end.

is_mutating_req(Req, Opts) when is_map(Req) ->
    case hb_maps:get(<<"action">>, Req, hb_maps:get(<<"Action">>, Req, undefined, Opts), Opts) of
        <<"Batch-Status">> -> false;
        <<"Read-All">> -> false;
        <<"Read-Point">> -> false;
        <<"Read-Nearby">> -> false;
        <<"Snapshot">> -> false;
        <<"Hydrate">> -> false;
        <<"Get-Official-State">> -> false;
        <<"Ping">> -> false;
        _ -> true
    end;
is_mutating_req(_Req, _Opts) ->
    true.

maybe_sync_now_cache(Req, Results, HttpStatus, ProcID, BaseSlot, NodeOpts) when HttpStatus < 400 ->
    Action =
        case is_map(Req) of
            true -> hb_maps:get(<<"action">>, Req, hb_maps:get(<<"Action">>, Req, undefined, NodeOpts), NodeOpts);
            false -> undefined
        end,
    BodyBin = maps:get(<<"data">>, Results, <<>>),
    IsCommitAction =
        Action =:= <<"Commit">>
        orelse Action =:= <<"Ack">>
        orelse Action =:= <<"Ack-Batch">>
        orelse Action =:= <<"Advance">>
        orelse Action =:= <<"Snapshot">>
        orelse Action =:= <<"Topology-Update">>,
    IsReplicatedBody =
        is_binary(BodyBin) andalso binary:match(BodyBin, <<"\"ack\":\"replicated\"">>) =/= nomatch,
    case IsCommitAction orelse IsReplicatedBody of
        true ->
            try
                CacheOpts = NodeOpts#{
                    <<"store">> => hb_opts:get(process_store, hb_opts:get(store, no_viable_store, NodeOpts), NodeOpts)
                },
                HumanID = hb_util:human_id(ProcID),
                SyncSlot = advance_now_sync_slot(HumanID, BaseSlot, CacheOpts),
                SlotNumPath = hb_path:to_binary([<<"computed">>, HumanID, <<"slot">>, integer_to_binary(SyncSlot)]),
                NewResults = #{
                    <<"output">> => maps:get(<<"output">>, Results, #{}),
                    <<"data">> => BodyBin
                },
                Msg =
                    case hb_cache:read(SlotNumPath, CacheOpts) of
                        {ok, PrevMsg} when is_map(PrevMsg) ->
                            PrevMsg#{ <<"results">> => NewResults };
                        _ ->
                            #{ <<"results">> => NewResults }
                    end,
                {ok, Root} = hb_cache:write(hb_private:reset(Msg), CacheOpts),
                ok = hb_cache:link(Root, SlotNumPath, CacheOpts),
                Store = hb_opts:get(store, no_viable_store, CacheOpts),
                RefreshedPath = hb_path:to_binary([<<"computed">>, HumanID, <<"latest">>]),
                RefreshedVal = iolist_to_binary([integer_to_binary(SyncSlot), <<":">>, integer_to_binary(erlang:system_time(second))]),
                _ = hb_store:write(Store, #{ RefreshedPath => RefreshedVal }, CacheOpts),
                SyncSlot
            catch _:_ ->
                BaseSlot
            end;
        false ->
            BaseSlot
    end;
maybe_sync_now_cache(_, _, _, _, BaseSlot, _) ->
    BaseSlot.

advance_now_sync_slot(HumanID, CurSlot, CacheOpts) ->
    NextPath = hb_path:to_binary([<<"computed">>, HumanID, <<"slot">>, integer_to_binary(CurSlot + 1)]),
    case hb_cache:read(NextPath, CacheOpts) of
        {ok, _} ->
            SlotRoot = hb_path:to_binary([<<"computed">>, HumanID, <<"slot">>]),
            case hb_cache:list_numbered(SlotRoot, CacheOpts) of
                Slots when is_list(Slots), length(Slots) > 0 ->
                    max(CurSlot, lists:max(Slots));
                _ ->
                    CurSlot + 1
            end;
        _ ->
            CurSlot
    end.

maybe_trigger_ckpt(State = #{ dirty_since_ckpt := Dirty, ckpt_in_flight := false })
        when Dirty >= ?ACTOR_CKPT_EVERY ->
    start_async_ckpt(State);
maybe_trigger_ckpt(State) ->
    State.

arm_flush_timer(State = #{ role := <<"primary">>, last_queued := Q, wal_seq := WalSeq })
        when Q > 0 ->
    Ref = erlang:send_after(40, self(), {auto_flush_batch, WalSeq}),
    State#{ flush_timer => Ref };
arm_flush_timer(State) ->
    State.

arm_idle_ckpt_timer(State = #{ dirty_since_ckpt := Dirty, ckpt_in_flight := false, wal_seq := WalSeq })
        when Dirty > 0 ->
    Ref = erlang:send_after(?ACTOR_IDLE_CKPT_MS, self(), {idle_ckpt, WalSeq}),
    State#{ idle_timer => Ref };
arm_idle_ckpt_timer(State) ->
    State.

cancel_actor_timer(undefined) -> ok;
cancel_actor_timer(Ref) ->
    _ = erlang:cancel_timer(Ref),
    ok.

start_async_ckpt(State = #{ inst := Inst, wal_seq := WalSeq, base_slot := BaseSlot, ckpt_path := CkptPath }) ->
    ActorPid = self(),
    case catch hb_beamr:serialize(Inst) of
        {ok, Serialized} ->
            spawn(fun() ->
                try
                    Compressed = zlib:compress(Serialized),
                    Blob = <<?ACTOR_CKPT_MAGIC/binary, BaseSlot:64/integer, WalSeq:64/integer, Compressed/binary>>,
                    TmpPath = CkptPath ++ ".tmp",
                    ok = file:write_file(TmpPath, Blob),
                    ok = file:rename(TmpPath, CkptPath),
                    ActorPid ! {ckpt_saved, WalSeq}
                catch C:R ->
                    ActorPid ! {ckpt_failed, {C, R}}
                end
            end),
            State#{ ckpt_in_flight => true, dirty_since_ckpt => 0 };
        _ ->
            State
    end.

rotate_actor_wal(State = #{ wal_fd := WalFd, wal_path := WalPath }) ->
    _ = file:close(WalFd),
    {ok, TruncFd} = file:open(WalPath, [write, raw, binary]),
    _ = file:close(TruncFd),
    {ok, NewFd} = file:open(WalPath, [append, raw, binary]),
    State#{ wal_fd => NewFd }.

append_actor_wal(WalFd, BaseSlot, WalSeq, MsgJson) ->
    Len = byte_size(MsgJson),
    Crc = erlang:crc32(MsgJson),
    Frame = <<?WAL_FRAME_MAGIC:32/integer, BaseSlot:64/integer, WalSeq:64/integer, Len:32/integer, MsgJson:Len/binary, Crc:32/integer>>,
    ok = file:write(WalFd, Frame).

cleanup_actor_registration(ProcID, Role, Image, #{ wal_fd := WalFd }) ->
    catch file:close(WalFd),
    catch ets:delete_object(hs_wasm_actors, {{actor, ProcID}, self()}),
    catch ets:delete_object(hs_wasm_actors, {{role_img_actor, Role, Image}, {ProcID, self()}}),
    catch ets:delete_object(hs_wasm_actors, {{role_actor, Role}, {ProcID, self()}}),
    ok.

actor_wal_dir(Role, Opts) ->
    RoleStr = case is_binary(Role) of true -> binary_to_list(Role); false -> "default" end,
    case hb_opts:get(priv_key_location, undefined, Opts) of
        Path when is_binary(Path), byte_size(Path) > 0 ->
            filename:join(filename:dirname(binary_to_list(Path)), "l0_wal");
        Path when is_list(Path), length(Path) > 0 ->
            filename:join(filename:dirname(Path), "l0_wal");
        _ ->
            filename:join("/tmp/hs_l0_wal", RoleStr)
    end.

load_actor_ckpt(Role, ProcID, _AtSlot, Opts) ->
    CkptPath = filename:join(actor_wal_dir(Role, Opts), binary_to_list(ProcID) ++ ".ckpt"),
    case file:read_file(CkptPath) of
        {ok, <<"HSCKPT2:", CkptBaseSlot:64/integer, CkptWalSeq:64/integer, ZlibBin/binary>>} ->
            try zlib:uncompress(ZlibBin) of
                Uncomp when is_binary(Uncomp) -> {ok, CkptBaseSlot, CkptWalSeq, Uncomp}
            catch _:_ ->
                none
            end;
        _ ->
            none
    end.

replay_cold_wal_if_any(NormM1, Prefix, Role, MinBaseSlot, MinWalSeq, FastOpts) ->
    {M1Ready, ProcID, _Image, ProcessJson} = get_or_cache_process(NormM1, Prefix, FastOpts),
    WalPath = filename:join(actor_wal_dir(Role, FastOpts), binary_to_list(ProcID) ++ ".wal"),
    case file:read_file(WalPath) of
        {ok, WalBin} when byte_size(WalBin) > 0 ->
            case resolve_exec_handles(M1Ready, Prefix, FastOpts) of
                {ok, Inst, ImportRes} ->
                    Frames = parse_wal_frames(WalBin, []),
                    ToReplay = [
                        {WSeq, MsgJson}
                        || {_BSlot, WSeq, MsgJson} <- Frames,
                           WSeq > MinWalSeq
                    ],
                    MaxWalSeq = lists:foldl(fun({_, WS, _}, Acc) -> max(Acc, WS) end, MinWalSeq, Frames),
                    case replay_wal_frames(ToReplay, Inst, ImportRes, ProcessJson, M1Ready, FastOpts) of
                        ok ->
                            put(hs_cold_wal_state, {MaxWalSeq, MinBaseSlot}),
                            {ok, M1Ready};
                        Err ->
                            Err
                    end;
                _ ->
                    {ok, M1Ready}
            end;
        _ ->
            put(hs_cold_wal_state, {MinWalSeq, MinBaseSlot}),
            {ok, M1Ready}
    end.

parse_wal_frames(<<?WAL_FRAME_MAGIC:32/integer, BSlot:64/integer, WSeq:64/integer, Len:32/integer,
                   MsgJson:Len/binary, Crc:32/integer, Rest/binary>>, Acc) ->
    case erlang:crc32(MsgJson) =:= Crc of
        true -> parse_wal_frames(Rest, [{BSlot, WSeq, MsgJson} | Acc]);
        false -> lists:reverse(Acc)
    end;
parse_wal_frames(_, Acc) ->
    lists:reverse(Acc).

replay_wal_frames([], _Inst, _ImportRes, _ProcJson, _M1, _Opts) ->
    ok;
replay_wal_frames([{_WSeq, MsgJson} | Rest], Inst, ImportRes, ProcJson, M1, Opts) ->
    case write_wasm_inputs(Inst, MsgJson, ProcJson) of
        {ok, MsgPtr, ProcPtr} ->
            case hb_beamr:call(Inst, "handle", [MsgPtr, ProcPtr], ImportRes, M1, Opts) of
                {trap, Err, _} -> {error, {wal_replay_trap, Err}};
                {error, Err, _} -> {error, {wal_replay_error, Err}};
                {_ResType, Res, _} ->
                    Ptr = case Res of [P] when is_integer(P) -> P; P when is_integer(P) -> P end,
                    _ = read_c_string(Inst, Ptr),
                    replay_wal_frames(Rest, Inst, ImportRes, ProcJson, M1, Opts)
            end;
        Err ->
            Err
    end.

extract_actor_coalesce(Item = #{ req := Req, opts := Opts }, ActorOpts) when is_map(Req) ->
    UseOpts = maps:merge(ActorOpts, Opts),
    HasCommitments = hb_maps:get(<<"commitments">>, Req, undefined, UseOpts) =/= undefined,
    HasIdemKey =
        hb_maps:get(<<"idempotency_key">>, Req, undefined, UseOpts) =/= undefined orelse
        hb_maps:get(<<"idempotency-key">>, Req, undefined, UseOpts) =/= undefined orelse
        hb_maps:get(<<"input_id">>, Req, undefined, UseOpts) =/= undefined orelse
        hb_maps:get(<<"input-id">>, Req, undefined, UseOpts) =/= undefined,
    case (not HasCommitments) andalso (not HasIdemKey) of
        false ->
            skip;
        true ->
            Action = hb_maps:get(<<"action">>, Req, hb_maps:get(<<"Action">>, Req, undefined, UseOpts), UseOpts),
            case actor_extract_entry(Action, Req, UseOpts) of
                {ok, Entry} -> {ok, Entry#{ item => Item }};
                skip -> skip
            end
    end;
extract_actor_coalesce(_, _) ->
    skip.

actor_extract_entry(<<"Write-Batch">>, Req, Opts) ->
    Packed = hb_maps:get(<<"packed_inputs">>, Req, hb_maps:get(<<"packed-inputs">>, Req, undefined, Opts), Opts),
    case is_binary(Packed) andalso byte_size(Packed) > 0
         andalso binary:match(Packed, [<<"\n">>, <<"\r">>]) =:= nomatch of
        true ->
            Count = length(binary:matches(Packed, <<";">>)) + 1,
            Flush = actor_bool_opt(hb_maps:get(<<"flush">>, Req, true, Opts), true),
            Ts = actor_int_opt(hb_maps:get(<<"ts">>, Req, hb_maps:get(<<"timestamp">>, Req, undefined, Opts), Opts),
                               erlang:system_time(millisecond)),
            {ok, #{ packed => Packed, count => Count, flush => Flush, ts => Ts }};
        false ->
            skip
    end;
actor_extract_entry(<<"Write-Point">>, Req, Opts) ->
    actor_extract_single_point(Req, Opts);
actor_extract_entry(<<"Input">>, Req, Opts) ->
    actor_extract_single_point(Req, Opts);
actor_extract_entry(_, _, _) ->
    skip.

actor_extract_single_point(Req, Opts) ->
    Id = hb_maps:get(<<"id">>, Req, undefined, Opts),
    XVal = actor_num_opt(hb_maps:get(<<"x">>, Req, undefined, Opts)),
    YVal = actor_num_opt(hb_maps:get(<<"y">>, Req, undefined, Opts)),
    Owner = actor_first_non_empty([
        hb_maps:get(<<"client_signer">>, Req, undefined, Opts),
        hb_maps:get(<<"client-signer">>, Req, undefined, Opts),
        hb_maps:get(<<"owner">>, Req, undefined, Opts)
    ], <<"anonymous">>),
    case is_binary(Id) andalso byte_size(Id) >= 1 andalso byte_size(Id) =< 64
         andalso actor_safe_token(Id) andalso actor_safe_token(Owner)
         andalso XVal =/= error andalso YVal =/= error of
        true ->
            {ok, XNum} = XVal,
            {ok, YNum} = YVal,
            case abs(XNum) =< 1000.0 andalso abs(YNum) =< 1000.0 of
                true ->
                    TTick = actor_int_opt(hb_maps:get(<<"target_tick">>, Req, hb_maps:get(<<"client_tick">>, Req, 0, Opts), Opts), 0),
                    Ts = actor_int_opt(hb_maps:get(<<"ts">>, Req, hb_maps:get(<<"timestamp">>, Req, undefined, Opts), Opts),
                                       erlang:system_time(millisecond)),
                    Flush = actor_bool_opt(hb_maps:get(<<"flush">>, Req, false, Opts), false),
                    PackedLine = iolist_to_binary([
                        Id, <<"|">>, float_to_binary(XNum, [{decimals, 6}, compact]),
                        <<"|">>, float_to_binary(YNum, [{decimals, 6}, compact]),
                        <<"|">>, integer_to_binary(TTick),
                        <<"|">>, integer_to_binary(Ts),
                        <<"|">>, Owner
                    ]),
                    {ok, #{ packed => PackedLine, count => 1, flush => Flush, ts => Ts }};
                false ->
                    skip
            end;
        false ->
            skip
    end.

actor_safe_token(Bin) when is_binary(Bin) ->
    binary:match(Bin, [<<"\n">>, <<"\r">>, <<"|">>, <<";">>]) =:= nomatch;
actor_safe_token(_) ->
    false.

actor_first_non_empty([], Default) -> Default;
actor_first_non_empty([B | Rest], Default) ->
    case is_binary(B) andalso byte_size(B) > 0 of
        true -> B;
        false -> actor_first_non_empty(Rest, Default)
    end.

actor_bool_opt(true, _) -> true;
actor_bool_opt(false, _) -> false;
actor_bool_opt(<<"true">>, _) -> true;
actor_bool_opt(<<"false">>, _) -> false;
actor_bool_opt(<<"1">>, _) -> true;
actor_bool_opt(<<"0">>, _) -> false;
actor_bool_opt(_, Default) -> Default.

actor_int_opt(I, _) when is_integer(I) -> I;
actor_int_opt(F, _) when is_float(F) -> trunc(F);
actor_int_opt(B, Default) when is_binary(B) ->
    case hb_util:safe_int(B) of {ok, N} -> N; _ -> Default end;
actor_int_opt(_, Default) -> Default.

actor_num_opt(I) when is_integer(I) -> {ok, float(I)};
actor_num_opt(F) when is_float(F) -> {ok, F};
actor_num_opt(B) when is_binary(B) ->
    S = string:trim(binary_to_list(B)),
    case catch list_to_float(S) of
        F when is_float(F) -> {ok, F};
        _ -> case catch list_to_integer(S) of I when is_integer(I) -> {ok, float(I)}; _ -> error end
    end;
actor_num_opt(_) -> error.
