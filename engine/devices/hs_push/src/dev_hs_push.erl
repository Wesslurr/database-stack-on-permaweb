%%% @doc External Forge push wrapper device (`hs-push@1.0`).
%%% Delegates scheduling, slot computation, and target-process downstream
%%% delivery to unmodified upstream `push@1.0', while additionally evaluating
%%% outbox messages that carry `resolve` without `target' (such as
%%% `~relay@1.0/cast' replication/ACK broadcasts from WASM processes).
-module(dev_hs_push).
-implements(<<"hs-push@1.0">>).
-export([device_info/1, push/3, schedule/3, pending/3]).
-include_lib("hb/include/hb.hrl").

%% @doc Export `push', `schedule', and `pending' matching `push@1.0'.
device_info(_Opts) ->
    #{ exports => [<<"push">>, <<"schedule">>, <<"pending">>] }.

%% @doc Delegate `push' to upstream `push@1.0', then execute any `resolve'-only
%% outbox messages (those lacking a `target') and strip upstream's 404 stub
%% keys for those entries.
-spec push(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ _ => _ }} | {error, _} | {failure, _} | pid().
push(Base, Req, Opts) ->
    DelegateRes =
        try delegate_to_push(Base, Req, Opts)
        after release_wasm_caller()
        end,
    case DelegateRes of
        {ok, PushRes} when is_map(PushRes) ->
            {ok, dispatch_resolve_outbox(PushRes, Opts)};
        Other ->
            Other
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
            AsyncRemoveKeys = dispatch_async_cast(AsyncCastEntries, Opts),
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

dispatch_async_cast([], _Opts) ->
    [];
dispatch_async_cast(Items, Opts) ->
    ResolveOpts = Opts#{ <<"force-message">> => true },
    lists:flatmap(
        fun({_Mode, RemoveKeys, ResolveMsg}) ->
            spawn(fun() ->
                _ = (catch hb_ao:resolve(ResolveMsg, ResolveOpts))
            end),
            RemoveKeys
        end,
        Items
    ).

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
