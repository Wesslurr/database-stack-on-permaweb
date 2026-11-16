%%% @doc External Forge push wrapper device (`hs-push@1.0`).
%%% Delegates scheduling, slot computation, and target-process downstream
%%% delivery to unmodified upstream `push@1.0', while additionally evaluating
%%% outbox messages that carry `resolve` without `target' (such as
%%% `~relay@1.0/cast' replication/ACK broadcasts from WASM processes).
-module(dev_hs_push).
-implements(<<"hs-push@1.0">>).
-export([device_info/1, push/3, schedule/3, pending/3, latest/3]).
-include_lib("hb/include/hb.hrl").

-define(INGRESS_WINDOW_MS, 3).
-define(MAX_COALESCE_REQS, 16).
-define(MAX_COALESCE_ITEMS, 200).

%% @doc Export `push', `schedule', `pending', and `latest'.
device_info(_Opts) ->
    #{ exports => [<<"push">>, <<"schedule">>, <<"pending">>, <<"latest">>] }.

%% @doc Fast read endpoint returning the latest cached WASM output (`output/body')
%% for the initialized process via `hs-wasm-output@1.0/latest'.
latest(Base, Req, Opts) ->
    TargetMsg = Base#{ <<"device">> => <<"hs-wasm-output@1.0">> },
    hb_ao:resolve(TargetMsg, Req#{ <<"path">> => <<"latest">> }, Opts).

%% @doc Coalesce concurrent unsigned `Input'/`Write-Point'/`Write-Batch' pushes
%% arriving within a short (`?INGRESS_WINDOW_MS' ms) window (or while the previous
%% slot push is in flight) into a single packed `Write-Batch' push, then
%% execute any `resolve'-only outbox casts/relays.
-spec push(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ _ => _ }} | {error, _} | {failure, _} | pid().
push(Base, Req, Opts) ->
    case lookup_warm_actor(Base, Opts) of
        {ok, ActorPid} ->
            fast_actor_push(ActorPid, Base, Req, Opts);
        miss ->
            case extract_coalescible_write(Req, Opts) of
                {ok, Entry} ->
                    submit_ingress_coalesced(Base, Req, Opts, Entry);
                skip ->
                    execute_push_once(Base, Req, Opts)
            end
    end.

execute_push_once(Base, Req, Opts) ->
    Role = hb_ao:get(<<"node-role">>, Base, undefined, Opts),
    ExecDev = hb_ao:get(<<"execution-device">>, Base, undefined, Opts),
    case ExecDev =:= <<"hs-wasm-output@1.0">>
         andalso (Role =:= <<"primary">> orelse Role =:= <<"sd1">> orelse Role =:= <<"sd2">>) of
        true ->
            BootRes =
                global:trans(
                    {hs_push_cold_boot, Role},
                    fun() ->
                        case lookup_warm_actor(Base, Opts) of
                            {ok, ActorPid} ->
                                {warm, ActorPid};
                            miss ->
                                try {cold, delegate_to_push(Base, Req, Opts)}
                                after release_wasm_caller()
                                end
                        end
                    end,
                    [node()],
                    20
                ),
            case BootRes of
                {warm, ActorPid} ->
                    fast_actor_push(ActorPid, Base, Req, Opts);
                {cold, {ok, PushRes}} when is_map(PushRes) ->
                    {ok, dispatch_resolve_outbox(PushRes, Opts)};
                {cold, Other} ->
                    Other;
                aborted ->
                    DelegateRes =
                        try delegate_to_push(Base, Req, Opts)
                        after release_wasm_caller()
                        end,
                    case DelegateRes of
                        {ok, PushRes} when is_map(PushRes) ->
                            {ok, dispatch_resolve_outbox(PushRes, Opts)};
                        Other ->
                            Other
                    end
            end;
        false ->
            DelegateRes =
                try delegate_to_push(Base, Req, Opts)
                after release_wasm_caller()
                end,
            case DelegateRes of
                {ok, PushRes} when is_map(PushRes) ->
                    {ok, dispatch_resolve_outbox(PushRes, Opts)};
                Other ->
                    Other
            end
    end.

lookup_warm_actor(Base, Opts) ->
    ExecDev = hb_ao:get(<<"execution-device">>, Base, undefined, Opts),
    Role = hb_ao:get(<<"node-role">>, Base, undefined, Opts),
    case ExecDev =:= <<"hs-wasm-output@1.0">>
         andalso (Role =:= <<"primary">> orelse Role =:= <<"sd1">> orelse Role =:= <<"sd2">>) of
        true ->
            case ets:info(hs_wasm_actors) of
                undefined ->
                    miss;
                _ ->
                    Image = hb_ao:get(<<"image">>, Base, undefined, Opts),
                    Lookup =
                        case is_binary(Image) of
                            true -> ets:lookup(hs_wasm_actors, {role_img_actor, Role, Image});
                            false -> ets:lookup(hs_wasm_actors, {role_actor, Role})
                        end,
                    case Lookup of
                        [{_, {_ProcID, ActorPid}}] when is_pid(ActorPid) ->
                            case erlang:is_process_alive(ActorPid) of
                                true -> {ok, ActorPid};
                                false -> miss
                            end;
                        _ ->
                            miss
                    end
            end;
        false ->
            miss
    end.

fast_actor_push(ActorPid, Base, Req, Opts) ->
    Ref = make_ref(),
    Mon = erlang:monitor(process, ActorPid),
    SelfModDispatch = fun(OutboxMap, ExecOpts) -> dispatch_actor_outbox(OutboxMap, ExecOpts) end,
    ActorPid ! {fast_push, Req, Opts, SelfModDispatch, self(), Ref},
    receive
        {Ref, {ok, PushRes}} when is_map(PushRes) ->
            erlang:demonitor(Mon, [flush]),
            Dispatched = dispatch_resolve_outbox(PushRes, Opts),
            CleanResults =
                case maps:get(<<"results">>, Dispatched, undefined) of
                    ResMap when is_map(ResMap) ->
                        maps:without([<<"outbox">>, <<"patches">>, <<"inbox">>], ResMap);
                    _ ->
                        #{
                            <<"output">> => maps:get(<<"output">>, Dispatched, #{}),
                            <<"data">> => maps:get(<<"data">>, Dispatched, <<>>)
                        }
                end,
            SlimRes = (maps:without([<<"outbox">>, <<"patches">>, <<"inbox">>, <<"_hs_outbox_order">>], Dispatched))#{
                <<"results">> => CleanResults
            },
            {ok, SlimRes};
        {Ref, Other} ->
            erlang:demonitor(Mon, [flush]),
            Other;
        {'DOWN', Mon, process, ActorPid, _Reason} ->
            execute_push_once(Base, Req, Opts)
    after 30000 ->
        erlang:demonitor(Mon, [flush]),
        {error, actor_push_timeout}
    end.

release_wasm_caller() ->
    case whereis(hs_wasm_pool) of
        PoolPid when is_pid(PoolPid) ->
            Ref = erlang:monitor(process, PoolPid),
            PoolPid ! {release_caller, self(), self(), Ref},
            receive
                {Ref, ok} ->
                    erlang:demonitor(Ref, [flush]),
                    ok;
                {'DOWN', Ref, process, PoolPid, _Reason} ->
                    ok
            after 1000 ->
                erlang:demonitor(Ref, [flush]),
                ok
            end;
        undefined ->
            ok
    end.

%% @doc Delegate `schedule' directly to upstream `push@1.0'.
schedule(Base, Req, Opts) ->
    delegate_to_push(Base, Req, Opts).

%% @doc Delegate `pending' directly to upstream `push@1.0'.
pending(Base, Req, Opts) ->
    delegate_to_push(Base, Req, Opts).

delegate_to_push(Base, Req, Opts) ->
    {ok, DelegateBase} =
        hb_ao:raw(
            <<"message@1.0">>,
            <<"set">>,
            Base,
            #{ <<"device">> => <<"push@1.0">> },
            Opts#{ <<"hashpath">> => ignore }
        ),
    hb_ao:resolve(DelegateBase, Req, Opts).

dispatch_actor_outbox(Outbox, Opts) when is_map(Outbox), map_size(Outbox) > 0 ->
    LoadedOutbox = hb_cache:ensure_all_loaded(hb_private:reset(Outbox), Opts),
    Pairs =
        lists:sort(
            fun({KeyA, _}, {KeyB, _}) -> sort_key(KeyA) =< sort_key(KeyB) end,
            hb_maps:to_list(LoadedOutbox, Opts)
        ),
    {AsyncCastEntries, SyncPairs} =
        lists:foldr(
            fun({Key, Msg} = Pair, {AsyncAcc, SyncAcc}) ->
                case prepare_resolve_entry(Key, Msg, Opts) of
                    {ok, async_cast, RemoveKeys, ResolveMsg} ->
                        {[{async_cast, RemoveKeys, ResolveMsg} | AsyncAcc], SyncAcc};
                    _ ->
                        {AsyncAcc, [Pair | SyncAcc]}
                end
            end,
            {[], []},
            Pairs
        ),
    case AsyncCastEntries of
        [] ->
            ok;
        _ ->
            ResolveOpts = Opts#{ <<"force-message">> => true },
            CoalescedItems = coalesce_ack_casts(AsyncCastEntries, Opts),
            relay_fifo_enqueue(undefined, CoalescedItems, ResolveOpts)
    end,
    maps:from_list(SyncPairs);
dispatch_actor_outbox(Outbox, _Opts) ->
    Outbox.

dispatch_resolve_outbox(PushRes, Opts) ->
    case hb_ao:get(<<"outbox">>, PushRes, not_found, Opts) of
        Outbox when is_map(Outbox), map_size(Outbox) > 0 ->
            LoadedOutbox = hb_cache:ensure_all_loaded(hb_private:reset(Outbox), Opts),
            Pairs =
                lists:sort(
                    fun({KeyA, _}, {KeyB, _}) -> sort_key(KeyA) =< sort_key(KeyB) end,
                    hb_maps:to_list(LoadedOutbox, Opts)
                ),
            Entries =
                lists:filtermap(
                    fun({Key, Msg}) ->
                        case prepare_resolve_entry(Key, Msg, Opts) of
                            {ok, Mode, RemoveKeys, ResolveMsg} ->
                                {true, {Mode, RemoveKeys, ResolveMsg}};
                            skip ->
                                false
                        end
                    end,
                    Pairs
                ),
            {AsyncCastEntries, SyncEntries} =
                lists:partition(
                    fun({Mode, _RemoveKeys, _ResolveMsg}) -> Mode =:= async_cast end,
                    Entries
                ),
            AsyncRemoveKeys = dispatch_async_cast(AsyncCastEntries, PushRes, Opts),
            SyncRemoveKeys =
                case [{RK, RM} || {sync, RK, RM} <- SyncEntries] of
                    [] ->
                        [];
                    [{RemoveKeys, ResolveMsg}] ->
                        _ = hb_ao:resolve(ResolveMsg, Opts#{ <<"force-message">> => true }),
                        RemoveKeys;
                    MultiSync ->
                        dispatch_parallel(MultiSync, Opts)
                end,
            case AsyncRemoveKeys ++ SyncRemoveKeys of
                [] -> PushRes;
                AllRemoveKeys -> maps:without(AllRemoveKeys, PushRes)
            end;
        _ ->
            PushRes
    end.

dispatch_async_cast([], PushRes, _Opts) ->
    case maps:get(<<"_hs_outbox_order">>, PushRes, undefined) of
        undefined -> ok;
        OrderTag -> relay_fifo_enqueue(OrderTag, [], #{})
    end,
    [];
dispatch_async_cast(Items, PushRes, Opts) ->
    ResolveOpts = Opts#{ <<"force-message">> => true },
    CoalescedItems = coalesce_ack_casts(Items, Opts),
    OrderTag = maps:get(<<"_hs_outbox_order">>, PushRes, undefined),
    relay_fifo_enqueue(OrderTag, CoalescedItems, ResolveOpts),
    lists:flatmap(
        fun({_Mode, RemoveKeys, _ResolveMsg}) -> RemoveKeys end,
        CoalescedItems
    ).

relay_fifo_enqueue(undefined, CoalescedItems, ResolveOpts) ->
    FifoPid = ensure_relay_fifo(),
    FifoPid ! {direct, CoalescedItems, ResolveOpts},
    ok;
relay_fifo_enqueue({ActorPid, Seq}, CoalescedItems, ResolveOpts) ->
    FifoPid = ensure_relay_fifo(),
    FifoPid ! {ordered, ActorPid, Seq, CoalescedItems, ResolveOpts},
    ok.

ensure_relay_fifo() ->
    case whereis(hs_relay_fifo) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            Self = self(),
            Ref = make_ref(),
            spawn(fun() ->
                try
                    register(hs_relay_fifo, self()),
                    Self ! {Ref, self()},
                    relay_fifo_loop(#{ actors => #{}, workers => #{} })
                catch error:badarg ->
                    Self ! {Ref, whereis(hs_relay_fifo)}
                end
            end),
            receive
                {Ref, Pid2} when is_pid(Pid2) -> Pid2
            after 1000 ->
                ensure_relay_fifo()
            end
    end.

relay_fifo_loop(State = #{ actors := Actors, workers := Workers }) ->
    receive
        {direct, Items, ResolveOpts} ->
            Workers1 = send_items_to_workers(Items, ResolveOpts, Workers, ResolveOpts),
            relay_fifo_loop(State#{ workers => Workers1 });

        {ordered, ActorPid, Seq, Items, ResolveOpts} ->
            ActorEntry0 =
                case maps:find(ActorPid, Actors) of
                    {ok, E} ->
                        E;
                    error ->
                        Mon = erlang:monitor(process, ActorPid),
                        #{ mon => Mon, next => 1, buf => #{}, timer => undefined }
                end,
            #{ next := NextSeq, buf := Buf0, timer := Timer0 } = ActorEntry0,
            if
                Seq < NextSeq ->
                    relay_fifo_loop(State);
                true ->
                    _ = case Timer0 of undefined -> ok; Ref -> erlang:cancel_timer(Ref) end,
                    Buf1 = maps:put(Seq, {Items, ResolveOpts}, Buf0),
                    {NextSeq1, Buf2, Workers1} = drain_ordered_buf(NextSeq, Buf1, Workers),
                    Timer1 =
                        case map_size(Buf2) > 0 of
                            true -> erlang:send_after(200, self(), {flush_gap, ActorPid, NextSeq1});
                            false -> undefined
                        end,
                    ActorEntry1 = ActorEntry0#{ next => NextSeq1, buf => Buf2, timer => Timer1 },
                    relay_fifo_loop(State#{ actors => maps:put(ActorPid, ActorEntry1, Actors), workers => Workers1 })
            end;

        {flush_gap, ActorPid, ExpectedSeq} ->
            case maps:find(ActorPid, Actors) of
                {ok, Entry = #{ next := ExpectedSeq, buf := Buf0 }} when map_size(Buf0) > 0 ->
                    SortedKeys = lists:sort(maps:keys(Buf0)),
                    MinPresent = hd(SortedKeys),
                    {NextSeq1, Buf1, Workers1} = drain_ordered_buf(MinPresent, Buf0, Workers),
                    Timer1 =
                        case map_size(Buf1) > 0 of
                            true -> erlang:send_after(200, self(), {flush_gap, ActorPid, NextSeq1});
                            false -> undefined
                        end,
                    Entry1 = Entry#{ next => NextSeq1, buf => Buf1, timer => Timer1 },
                    relay_fifo_loop(State#{ actors => maps:put(ActorPid, Entry1, Actors), workers => Workers1 });
                _ ->
                    relay_fifo_loop(State)
            end;

        {'DOWN', _Mon, process, ActorPid, _Reason} ->
            case maps:take(ActorPid, Actors) of
                {#{ buf := Buf0, timer := Timer0 }, Actors1} ->
                    _ = case Timer0 of undefined -> ok; Ref -> erlang:cancel_timer(Ref) end,
                    Workers1 =
                        lists:foldl(
                            fun(K, WAcc) ->
                                {Items, ResolveOpts} = maps:get(K, Buf0),
                                send_items_to_workers(Items, ResolveOpts, WAcc, ResolveOpts)
                            end,
                            Workers,
                            lists:sort(maps:keys(Buf0))
                        ),
                    relay_fifo_loop(State#{ actors => Actors1, workers => Workers1 });
                error ->
                    relay_fifo_loop(State)
            end;

        _Other ->
            relay_fifo_loop(State)
    end.

drain_ordered_buf(NextSeq, Buf, Workers) ->
    case maps:take(NextSeq, Buf) of
        {{Items, ResolveOpts}, Buf1} ->
            Workers1 = send_items_to_workers(Items, ResolveOpts, Workers, ResolveOpts),
            drain_ordered_buf(NextSeq + 1, Buf1, Workers1);
        error ->
            {NextSeq, Buf, Workers}
    end.

send_items_to_workers([], _ResolveOpts, Workers, _DefaultOpts) ->
    Workers;
send_items_to_workers([{_Mode, _RemoveKeys, ResolveMsg} | Rest], ResolveOpts, Workers, DefaultOpts) ->
    TargetKey = hb_maps:get(<<"relay-path">>, ResolveMsg, <<"default">>, ResolveOpts),
    PoolSize =
        case is_binary(TargetKey) andalso binary:match(TargetKey, <<"/arbit/">>) =/= nomatch of
            true -> 1;
            false -> 4
        end,
    {WorkerPid, Workers1} = pick_relay_worker(TargetKey, PoolSize, Workers),
    WorkerPid ! {cast, ResolveMsg, ResolveOpts},
    send_items_to_workers(Rest, ResolveOpts, Workers1, DefaultOpts).

pick_relay_worker(TargetKey, 1, Workers) ->
    ensure_relay_worker({TargetKey, 0}, Workers);
pick_relay_worker(TargetKey, PoolSize, Workers) ->
    NextIdx = (maps:get({idx, TargetKey}, Workers, 0)) rem PoolSize,
    {Pid, Workers1} = ensure_relay_worker({TargetKey, NextIdx}, Workers),
    {Pid, maps:put({idx, TargetKey}, (NextIdx + 1) rem PoolSize, Workers1)}.

ensure_relay_worker(LaneKey, Workers) ->
    case maps:find(LaneKey, Workers) of
        {ok, Pid} when is_pid(Pid) ->
            case erlang:is_process_alive(Pid) of
                true -> {Pid, Workers};
                false ->
                    NPid = spawn(fun() -> relay_worker_loop() end),
                    {NPid, maps:put(LaneKey, NPid, Workers)}
            end;
        _ ->
            NPid = spawn(fun() -> relay_worker_loop() end),
            {NPid, maps:put(LaneKey, NPid, Workers)}
    end.

relay_worker_loop() ->
    receive
        {cast, ResolveMsg, ResolveOpts} ->
            CallMsg =
                case is_cast_resolve_path(maps:get(<<"path">>, ResolveMsg, undefined)) of
                    true -> ResolveMsg#{ <<"path">> => <<"~relay@1.0/call">> };
                    false -> ResolveMsg
                end,
            Res = (catch hb_ao:resolve(CallMsg, ResolveOpts)),
            case needs_relay_retry(Res) of
                true ->
                    timer:sleep(20),
                    _ = (catch hb_ao:resolve(CallMsg, ResolveOpts));
                false ->
                    ok
            end,
            relay_worker_loop();
        stop ->
            ok
    end.

needs_relay_retry({error, closed}) -> true;
needs_relay_retry({error, socket_closed_remotely}) -> true;
needs_relay_retry({error, timeout}) -> true;
needs_relay_retry({ok, #{ <<"status">> := St }}) when is_integer(St), St >= 500 -> true;
needs_relay_retry({ok, #{ <<"status">> := St }}) when is_binary(St) ->
    case hb_util:safe_int(St) of
        {ok, N} when N >= 500 -> true;
        _ -> false
    end;
needs_relay_retry(_) -> false.

%% @doc Group multiple pending `action = "Ack"` casts targeting the same
%% `relay-path' within a single outbox flush into one `Ack-Batch' HTTP POST.
coalesce_ack_casts(Items, Opts) ->
    coalesce_ack_casts(Items, Opts, #{}, [], []).

coalesce_ack_casts([], _Opts, AckGroups, GroupOrder, PassthroughRev) ->
    BatchItems =
        lists:map(
            fun(RelayPath) ->
                case maps:get(RelayPath, AckGroups) of
                    [Single] ->
                        {M, Keys, _AckMap, Msg} = Single,
                        {M, Keys, Msg};
                    Group when length(Group) >= 2 ->
                        ChronoGroup = lists:reverse(Group),
                        {Mode, AllKeys, AckMaps, TemplateMsg} =
                            lists:foldr(
                                fun({M, Keys, AckMap, Msg}, {_, KeysAcc, MapsAcc, _}) ->
                                    {M, Keys ++ KeysAcc, [AckMap | MapsAcc], Msg}
                                end,
                                {async_cast, [], [], element(4, hd(ChronoGroup))},
                                ChronoGroup
                            ),
                        BatchJson = hb_json:encode(#{
                            <<"action">> => <<"Ack-Batch">>,
                            <<"acks">> => AckMaps
                        }),
                        CoalescedMsg = maps:without(
                            [<<"content-length">>],
                            TemplateMsg#{ <<"relay-body">> => BatchJson }
                        ),
                        {Mode, AllKeys, CoalescedMsg}
                end
            end,
            lists:reverse(GroupOrder)
        ),
    lists:reverse(PassthroughRev) ++ BatchItems;
coalesce_ack_casts([{Mode, RemoveKeys, ResolveMsg} = Item | Rest], Opts, AckGroups, GroupOrder, PassthroughRev) ->
    case extract_ack_cast(ResolveMsg, Opts) of
        {ok, RelayPath, AckMap} ->
            {NewGroups, NewOrder} =
                case maps:find(RelayPath, AckGroups) of
                    {ok, ExistingRev} ->
                        {maps:put(RelayPath, [{Mode, RemoveKeys, AckMap, ResolveMsg} | ExistingRev], AckGroups), GroupOrder};
                    error ->
                        {maps:put(RelayPath, [{Mode, RemoveKeys, AckMap, ResolveMsg}], AckGroups), [RelayPath | GroupOrder]}
                end,
            coalesce_ack_casts(Rest, Opts, NewGroups, NewOrder, PassthroughRev);
        false ->
            coalesce_ack_casts(Rest, Opts, AckGroups, GroupOrder, [Item | PassthroughRev])
    end.

extract_ack_cast(ResolveMsg, Opts) ->
    Method = hb_ao:get(<<"relay-method">>, ResolveMsg, <<"GET">>, Opts),
    RelayPath = hb_ao:get(<<"relay-path">>, ResolveMsg, undefined, Opts),
    RelayBody = hb_ao:get(<<"relay-body">>, ResolveMsg, undefined, Opts),
    case (Method =:= <<"POST">> orelse Method =:= <<"post">>)
        andalso is_binary(RelayPath) andalso RelayPath =/= <<>>
        andalso is_binary(RelayBody)
        andalso (binary:match(RelayBody, <<"\"Ack\"">>) =/= nomatch)
    of
        true ->
            try hb_json:decode(RelayBody) of
                #{ <<"action">> := <<"Ack">> } = AckMap ->
                    {ok, RelayPath, AckMap};
                _ ->
                    false
            catch _:_ ->
                false
            end;
        false ->
            false
    end.

dispatch_parallel(Items, Opts) ->
    Parent = self(),
    ResolveOpts = Opts#{ <<"force-message">> => true },
    Workers =
        [
            spawn_monitor(fun() ->
                _ = hb_ao:resolve(ResolveMsg, ResolveOpts),
                Parent ! {hs_push_done, self(), RemoveKeys}
            end)
        || {RemoveKeys, ResolveMsg} <- Items
        ],
    lists:flatmap(
        fun({Pid, Ref}) ->
            receive
                {hs_push_done, Pid, RemoveKeys} ->
                    erlang:demonitor(Ref, [flush]),
                    RemoveKeys;
                {'DOWN', Ref, process, Pid, _Reason} ->
                    []
            end
        end,
        Workers
    ).

prepare_resolve_entry(Key, RawMsg, Opts) ->
    LoadedMsg = hb_cache:ensure_loaded(RawMsg, Opts),
    case is_map(LoadedMsg) of
        false ->
            skip;
        true ->
            NormMsg = hb_ao:normalize_keys(hb_private:reset(LoadedMsg), Opts),
            HasTarget = hb_ao:get(<<"target">>, NormMsg, undefined, Opts) =/= undefined,
            ResolvePath = hb_ao:get(<<"resolve">>, NormMsg, undefined, Opts),
            case (not HasTarget) andalso is_binary(ResolvePath) andalso ResolvePath =/= <<>> of
                true ->
                    ReqMsg = maps:without([<<"target">>], NormMsg),
                    KeyBin = hb_util:bin(Key),
                    Mode =
                        case is_cast_resolve_path(ResolvePath) of
                            true -> async_cast;
                            false -> sync
                        end,
                    {ok, Mode, [KeyBin, Key], ReqMsg#{ <<"path">> => ResolvePath }};
                false ->
                    skip
            end
    end.

is_cast_resolve_path(Path) when is_binary(Path) ->
    Trimmed0 = strip_prefix(string:trim(Path), <<"/">>),
    Trimmed1 = strip_prefix(Trimmed0, <<"~">>),
    Trimmed1 =:= <<"relay@1.0/cast">>
        orelse binary:match(Trimmed1, <<"relay@1.0/cast/">>) =:= {0, 15}
        orelse binary:match(Trimmed1, <<"relay@1.0/cast?">>) =:= {0, 15};
is_cast_resolve_path(_) ->
    false.

strip_prefix(Bin, Prefix) ->
    Len = byte_size(Prefix),
    case Bin of
        <<Prefix:Len/binary, Rest/binary>> -> strip_prefix(Rest, Prefix);
        _ -> Bin
    end.

sort_key(Key) ->
    case hb_util:safe_int(Key) of
        {ok, N} -> N;
        {error, invalid} -> Key
    end.

%%% Ingress Micro-Batch Coalescer (`?INGRESS_WINDOW_MS' ms + busy-queue accumulation)

extract_coalescible_write(Req, Opts) when is_map(Req) ->
    HasCommitments = hb_maps:get(<<"commitments">>, Req, undefined, Opts) =/= undefined,
    HasIdemKey =
        hb_maps:get(<<"idempotency_key">>, Req, undefined, Opts) =/= undefined orelse
        hb_maps:get(<<"idempotency-key">>, Req, undefined, Opts) =/= undefined orelse
        hb_maps:get(<<"input_id">>, Req, undefined, Opts) =/= undefined orelse
        hb_maps:get(<<"input-id">>, Req, undefined, Opts) =/= undefined,
    case (not HasCommitments) andalso (not HasIdemKey) of
        false ->
            skip;
        true ->
            Action = hb_maps:get(
                <<"action">>,
                Req,
                hb_maps:get(<<"Action">>, Req, undefined, Opts),
                Opts
            ),
            case Action of
                <<"Write-Batch">> ->
                    extract_write_batch_entry(Req, Opts);
                <<"Write-Point">> ->
                    extract_single_point_entry(Req, Opts);
                <<"Input">> ->
                    extract_single_point_entry(Req, Opts);
                _ ->
                    skip
            end
    end;
extract_coalescible_write(_, _) ->
    skip.

extract_write_batch_entry(Req, Opts) ->
    Packed = hb_maps:get(
        <<"packed_inputs">>,
        Req,
        hb_maps:get(<<"packed-inputs">>, Req, undefined, Opts),
        Opts
    ),
    case is_binary(Packed) andalso byte_size(Packed) > 0 andalso is_safe_packed(Packed) of
        true ->
            Count = length(binary:matches(Packed, <<";">>)) + 1,
            Flush = parse_bool_opt(hb_maps:get(<<"flush">>, Req, true, Opts), true),
            Ts = parse_int_opt(
                hb_maps:get(<<"ts">>, Req, hb_maps:get(<<"timestamp">>, Req, undefined, Opts), Opts),
                erlang:system_time(millisecond)
            ),
            {ok, #{ packed => Packed, count => Count, flush => Flush, ts => Ts }};
        false ->
            skip
    end.

extract_single_point_entry(Req, Opts) ->
    Id = hb_maps:get(<<"id">>, Req, undefined, Opts),
    XVal = parse_num_opt(hb_maps:get(<<"x">>, Req, undefined, Opts)),
    YVal = parse_num_opt(hb_maps:get(<<"y">>, Req, undefined, Opts)),
    Owner = first_non_empty_bin(
        [
            hb_maps:get(<<"client_signer">>, Req, undefined, Opts),
            hb_maps:get(<<"client-signer">>, Req, undefined, Opts),
            hb_maps:get(<<"owner">>, Req, undefined, Opts)
        ],
        <<"anonymous">>
    ),
    case is_binary(Id) andalso byte_size(Id) >= 1 andalso byte_size(Id) =< 64
        andalso is_safe_token(Id) andalso is_safe_token(Owner)
        andalso XVal =/= error andalso YVal =/= error
    of
        true ->
            {ok, XNum} = XVal,
            {ok, YNum} = YVal,
            case abs(XNum) =< 1000.0 andalso abs(YNum) =< 1000.0 of
                true ->
                    TTick = parse_int_opt(
                        hb_maps:get(<<"target_tick">>, Req, hb_maps:get(<<"client_tick">>, Req, 0, Opts), Opts),
                        0
                    ),
                    Ts = parse_int_opt(
                        hb_maps:get(<<"ts">>, Req, hb_maps:get(<<"timestamp">>, Req, undefined, Opts), Opts),
                        erlang:system_time(millisecond)
                    ),
                    Flush = parse_bool_opt(hb_maps:get(<<"flush">>, Req, false, Opts), false),
                    PackedLine = iolist_to_binary([
                        Id, <<"|">>, fmt_num(XNum), <<"|">>, fmt_num(YNum),
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

submit_ingress_coalesced(Base, Req, Opts, Entry) ->
    ProcKey = proc_queue_key(Base, Opts),
    PoolPid = ensure_ingress_pool(),
    Ref = make_ref(),
    Mon = erlang:monitor(process, PoolPid),
    PoolPid ! {submit, ProcKey, #{
        base => Base,
        req => Req,
        opts => Opts,
        entry => Entry,
        from => self(),
        ref => Ref
    }},
    receive
        {Ref, Result} ->
            erlang:demonitor(Mon, [flush]),
            Result;
        {'DOWN', Mon, process, PoolPid, _Reason} ->
            execute_push_once(Base, Req, Opts)
    after 30000 ->
        erlang:demonitor(Mon, [flush]),
        {error, ingress_coalesce_timeout}
    end.

proc_queue_key(Base, Opts) ->
    case hb_ao:get(<<"id">>, Base, undefined, Opts) of
        ID when is_binary(ID), byte_size(ID) > 0 -> ID;
        _ -> hb_ao:get(<<"node-role">>, Base, <<"default">>, Opts)
    end.

ensure_ingress_pool() ->
    case whereis(hs_push_ingress) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            Self = self(),
            Ref = make_ref(),
            Candidate = spawn(fun() ->
                case catch register(hs_push_ingress, self()) of
                    true ->
                        Self ! {Ref, self()},
                        ingress_loop(#{ procs => #{}, mons => #{} });
                    _ ->
                        Self ! {Ref, whereis(hs_push_ingress)}
                end
            end),
            receive
                {Ref, WinnerPid} when is_pid(WinnerPid) -> WinnerPid;
                {Ref, _} -> Candidate
            after 1000 ->
                Candidate
            end
    end.

ingress_loop(State = #{ procs := Procs, mons := Mons }) ->
    receive
        {submit, ProcKey, Item} ->
            EntryCount = maps:get(count, maps:get(entry, Item), 1),
            PState0 = maps:get(ProcKey, Procs, #{ active => false, items => [], timer => undefined, count => 0 }),
            Items1 = [Item | maps:get(items, PState0)],
            Count1 = maps:get(count, PState0) + EntryCount,
            NextState =
                case maps:get(active, PState0) of
                    true ->
                        PState1 = PState0#{ items => Items1, count => Count1 },
                        State#{ procs => maps:put(ProcKey, PState1, Procs) };
                    false ->
                        case length(Items1) >= ?MAX_COALESCE_REQS orelse Count1 >= ?MAX_COALESCE_ITEMS of
                            true ->
                                cancel_ingress_timer(maps:get(timer, PState0)),
                                dispatch_ingress_batch(ProcKey, lists:reverse(Items1), State);
                            false ->
                                Timer1 =
                                    case maps:get(timer, PState0) of
                                        undefined -> erlang:send_after(?INGRESS_WINDOW_MS, self(), {flush, ProcKey});
                                        Existing -> Existing
                                    end,
                                PState1 = PState0#{ items => Items1, count => Count1, timer => Timer1 },
                                State#{ procs => maps:put(ProcKey, PState1, Procs) }
                        end
                end,
            ingress_loop(NextState);

        {flush, ProcKey} ->
            NextState =
                case maps:find(ProcKey, Procs) of
                    {ok, #{ active := false, items := ItemsRev }} when ItemsRev =/= [] ->
                        dispatch_ingress_batch(ProcKey, lists:reverse(ItemsRev), State);
                    _ ->
                        State
                end,
            ingress_loop(NextState);

        {worker_done, ProcKey, WorkerPid} ->
            NextState =
                case maps:find(ProcKey, Procs) of
                    {ok, #{ worker_pid := WorkerPid, worker_mon := MonRef, items := [] } = PState} ->
                        erlang:demonitor(MonRef, [flush]),
                        Mons1 = maps:remove(MonRef, Mons),
                        PState1 = PState#{ active => false, worker_pid => undefined, worker_mon => undefined, active_items => [] },
                        State#{ procs => maps:put(ProcKey, PState1, Procs), mons => Mons1 };
                    {ok, #{ worker_pid := WorkerPid, worker_mon := MonRef, items := ItemsRev }} when ItemsRev =/= [] ->
                        erlang:demonitor(MonRef, [flush]),
                        Mons1 = maps:remove(MonRef, Mons),
                        dispatch_ingress_batch(
                            ProcKey,
                            lists:reverse(ItemsRev),
                            State#{ mons => Mons1 }
                        );
                    _ ->
                        State
                end,
            ingress_loop(NextState);

        {'DOWN', MonRef, process, _Pid, Reason} ->
            NextState =
                case maps:take(MonRef, Mons) of
                    {ProcKey, Mons1} ->
                        case maps:find(ProcKey, Procs) of
                            {ok, #{ worker_mon := MonRef, active_items := ActiveItems, items := ItemsRev } = PState} ->
                                _ = [From ! {Ref, {error, {worker_crash, Reason}}} || #{ from := From, ref := Ref } <- ActiveItems],
                                case ItemsRev of
                                    [] ->
                                        PState1 = PState#{ active => false, worker_pid => undefined, worker_mon => undefined, active_items => [] },
                                        State#{ procs => maps:put(ProcKey, PState1, Procs), mons => Mons1 };
                                    _ ->
                                        dispatch_ingress_batch(
                                            ProcKey,
                                            lists:reverse(ItemsRev),
                                            State#{ mons => Mons1 }
                                        )
                                end;
                            _ ->
                                State#{ mons => Mons1 }
                        end;
                    error ->
                        State
                end,
            ingress_loop(NextState);

        _Other ->
            ingress_loop(State)
    end.

dispatch_ingress_batch(ProcKey, BatchItems, State = #{ procs := Procs, mons := Mons }) ->
    PoolPid = self(),
    {WorkerPid, MonRef} =
        spawn_monitor(fun() ->
            Res =
                try run_coalesced_batch(BatchItems)
                catch C:R -> {error, {C, R}}
                end,
            _ = [From ! {Ref, Res} || #{ from := From, ref := Ref } <- BatchItems],
            PoolPid ! {worker_done, ProcKey, self()}
        end),
    PState1 = #{
        active => true,
        worker_pid => WorkerPid,
        worker_mon => MonRef,
        active_items => BatchItems,
        items => [],
        timer => undefined,
        count => 0
    },
    State#{
        procs => maps:put(ProcKey, PState1, Procs),
        mons => maps:put(MonRef, ProcKey, Mons)
    }.

cancel_ingress_timer(undefined) ->
    ok;
cancel_ingress_timer(TimerRef) ->
    _ = erlang:cancel_timer(TimerRef),
    ok.

run_coalesced_batch([#{ base := Base, req := Req, opts := Opts }]) ->
    execute_push_once(Base, Req, Opts);
run_coalesced_batch([#{ base := Base, req := TemplateReq, opts := Opts } | _] = BatchItems) ->
    PackedInputs = iolist_to_binary(
        lists:join(<<";">>, [maps:get(packed, maps:get(entry, I)) || I <- BatchItems])
    ),
    FlushOut = lists:any(fun(I) -> maps:get(flush, maps:get(entry, I)) end, BatchItems),
    TsOut = lists:foldl(fun(I, Acc) -> max(Acc, maps:get(ts, maps:get(entry, I))) end, 0, BatchItems),
    DropKeys = [
        <<"id">>, <<"x">>, <<"y">>, <<"target_tick">>, <<"client_tick">>,
        <<"inputs">>, <<"packed-inputs">>, <<"packed_inputs">>,
        <<"client_signer">>, <<"client-signer">>, <<"owner">>,
        <<"Action">>
    ],
    CleanReq = maps:without(DropKeys, TemplateReq),
    MergedReq = CleanReq#{
        <<"action">> => <<"Write-Batch">>,
        <<"packed_inputs">> => PackedInputs,
        <<"flush">> => FlushOut,
        <<"ts">> => TsOut
    },
    execute_push_once(Base, MergedReq, Opts).

is_safe_token(Bin) when is_binary(Bin) ->
    binary:match(Bin, [<<"\n">>, <<"\r">>, <<"|">>, <<";">>]) =:= nomatch;
is_safe_token(_) ->
    false.

is_safe_packed(Bin) when is_binary(Bin) ->
    binary:match(Bin, [<<"\n">>, <<"\r">>]) =:= nomatch;
is_safe_packed(_) ->
    false.

first_non_empty_bin([], Default) ->
    Default;
first_non_empty_bin([B | Rest], Default) ->
    case is_binary(B) andalso byte_size(B) > 0 of
        true -> B;
        false -> first_non_empty_bin(Rest, Default)
    end.

parse_bool_opt(true, _) -> true;
parse_bool_opt(false, _) -> false;
parse_bool_opt(<<"true">>, _) -> true;
parse_bool_opt(<<"false">>, _) -> false;
parse_bool_opt(<<"1">>, _) -> true;
parse_bool_opt(<<"0">>, _) -> false;
parse_bool_opt(_, Default) -> Default.

parse_int_opt(I, _) when is_integer(I) -> I;
parse_int_opt(F, _) when is_float(F) -> trunc(F);
parse_int_opt(B, Default) when is_binary(B) ->
    case hb_util:safe_int(B) of
        {ok, N} -> N;
        _ -> Default
    end;
parse_int_opt(_, Default) -> Default.

parse_num_opt(I) when is_integer(I) -> {ok, float(I)};
parse_num_opt(F) when is_float(F) -> {ok, F};
parse_num_opt(B) when is_binary(B) ->
    S = string:trim(binary_to_list(B)),
    case catch list_to_float(S) of
        F when is_float(F) -> {ok, F};
        _ ->
            case catch list_to_integer(S) of
                I when is_integer(I) -> {ok, float(I)};
                _ -> error
            end
    end;
parse_num_opt(_) -> error.

fmt_num(F) ->
    float_to_binary(F, [{decimals, 6}, compact]).
