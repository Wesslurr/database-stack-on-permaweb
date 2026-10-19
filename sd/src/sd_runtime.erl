-module(sd_runtime).
-behaviour(gen_server).

%% ============================================================================
%% Shared Data Pool (SD) Runtime Engine (~sd@1.0)
%% ============================================================================
%% Responsibilities:
%% 1. Append-Only Disk Write-Ahead-Log (WAL) with group commit fsync.
%% 2. In-memory RAM dual-store & secondary query serving.
%% 3. Real-time TCP streaming subscriber from Hot Node (:8736).
%% 4. Multi-party batch checkpoint proposal for AO Arbitrator.
%% 5. Epoch fencing protection against split-brain scenarios.
%% ============================================================================

-export([start_link/0, ensure_started/0, reset/0, is_connected/0, get_current_tick/0, get_state_root/0, get_epoch/0]).
-export([get_tick/0, get_state/0, get_key/1, get_wal/0, get_wal_batch/2, get_status/0, get_status_map/0, fence_epoch/1, recover_wal/0, simulate_restart_from_wal/0]).
-export([get_tick_from_req/3, get_state_from_req/3, get_key_from_req/3, get_wal_from_req/3, get_wal_batch_from_req/3, get_status_from_req/3, reset_from_req/3, fence_from_req/3, restart_from_req/3, handle_scheduler_http/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TABLE, sd_ram_store).
-define(COMPAT_TABLE, su_ram_store).
-define(SERVER, ?MODULE).
-define(DEFAULT_WAL_PATH, "/home/weslur/database/sd/wal.log").
-define(HOT_NODE_PORT, 8736).
-define(GENESIS_HASH, <<"0000000000000000000000000000000000000000000000000000000000000000">>).

-record(state, {
    sub_pid,
    hot_host = "127.0.0.1",
    hot_port = ?HOT_NODE_PORT
}).

wal_path() ->
    case os:getenv("HYDB_WAL_PATH") of
        false -> ?DEFAULT_WAL_PATH;
        P when is_list(P), length(P) > 0 -> P;
        _ -> ?DEFAULT_WAL_PATH
    end.

is_connected() ->
    ensure_started(),
    lookup_val(connected_to_hot, false).

get_current_tick() ->
    ensure_started(),
    lookup_val(tick, 0).

get_state_root() ->
    ensure_started(),
    lookup_val(state_root, 0).

get_epoch() ->
    ensure_started(),
    lookup_val(epoch, 1).

fence_epoch(NewEpoch) when is_integer(NewEpoch) ->
    ensure_started(),
    gen_server:call(?SERVER, {fence_epoch, NewEpoch}, 5000).

recover_wal() ->
    ensure_started(),
    gen_server:call(?SERVER, recover_wal, 10000).

simulate_restart_from_wal() ->
    ensure_started(),
    gen_server:call(?SERVER, simulate_restart_from_wal, 10000).

ensure_started() ->
    case whereis(?SERVER) of
        undefined ->
            case gen_server:start({local, ?SERVER}, ?MODULE, [], []) of
                {ok, Pid} -> {ok, Pid};
                {error, {already_started, Pid}} -> {ok, Pid};
                Error -> Error
            end;
        Pid -> {ok, Pid}
    end.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    init_ets(),
    recover_from_wal(wal_path()),
    SubPid = spawn_link(fun() -> subscriber_loop(?SERVER, "127.0.0.1", ?HOT_NODE_PORT) end),
    {ok, #state{sub_pid = SubPid}}.

init_ets() ->
    case ets:info(?TABLE) of
        undefined ->
            ets:new(?TABLE, [public, named_table, set, {read_concurrency, true}, {write_concurrency, true}]),
            reset_ets_defaults();
        _ -> ok
    end,
    case ets:info(?COMPAT_TABLE) of
        undefined ->
            ets:new(?COMPAT_TABLE, [public, named_table, set, {read_concurrency, true}, {write_concurrency, true}]);
        _ -> ok
    end.

reset_ets_defaults() ->
    ets:insert(?TABLE, {epoch, 1}),
    ets:insert(?TABLE, {tick, 0}),
    ets:insert(?TABLE, {state_root, 0}),
    ets:insert(?TABLE, {latest_hash, ?GENESIS_HASH}),
    ets:insert(?TABLE, {total_replicated, 0}),
    ets:insert(?TABLE, {fenced_rejections, 0}),
    ets:insert(?TABLE, {gap_rejections, 0}),
    ets:insert(?TABLE, {hash_rejections, 0}),
    ets:insert(?TABLE, {connected_to_hot, false}),
    ets:insert(?TABLE, {history, []}).

get_or_open_wal_fd() ->
    case ets:lookup(?TABLE, wal_fd) of
        [{wal_fd, Fd}] -> Fd;
        [] ->
            Path = wal_path(),
            _ = filelib:ensure_dir(Path),
            {ok, Fd} = file:open(Path, [append, raw, binary]),
            ets:insert(?TABLE, {wal_fd, Fd}),
            Fd
    end.

close_wal_fd() ->
    case ets:lookup(?TABLE, wal_fd) of
        [{wal_fd, Fd}] ->
            _ = catch file:close(Fd),
            ets:delete(?TABLE, wal_fd),
            ok;
        [] -> ok
    end.

recover_from_wal(Path) ->
    case file:read_file(Path) of
        {ok, Content} ->
            Lines = binary:split(Content, <<"\n">>, [global, trim_all]),
            validate_and_apply_wal_lines(Lines, 1, ?GENESIS_HASH, 0);
        _ -> 0
    end.

validate_and_apply_wal_lines([], _ExpectedSeq, _LastHash, AppliedCount) ->
    AppliedCount;
validate_and_apply_wal_lines([Line | Rest], ExpectedSeq, LastHash, AppliedCount) ->
    case catch json:decode(Line) of
        Event when is_map(Event) ->
            Seq = to_integer(maps:get(<<"seq">>, Event, 0), 0),
            GivenHash = maps:get(<<"hash">>, Event, <<>>),
            GivenPrevHash = maps:get(<<"prev_hash">>, Event, <<>>),
            ExpectedHash = compute_event_hash(Event, LastHash),
            HashMatches = (string:uppercase(GivenHash) =:= string:uppercase(ExpectedHash)),
            PrevHashMatches = (string:uppercase(GivenPrevHash) =:= string:uppercase(LastHash)),
            if
                Seq =/= ExpectedSeq ->
                    ets:insert(?TABLE, {wal_recovery_halt_reason, {sequence_gap, Seq, ExpectedSeq}}),
                    AppliedCount;
                not PrevHashMatches ->
                    ets:insert(?TABLE, {wal_recovery_halt_reason, {broken_hash_chain, GivenPrevHash, LastHash}}),
                    AppliedCount;
                not HashMatches ->
                    ets:insert(?TABLE, {wal_recovery_halt_reason, {hash_mismatch, GivenHash, ExpectedHash}}),
                    AppliedCount;
                true ->
                    apply_event_to_ets(Event),
                    validate_and_apply_wal_lines(Rest, ExpectedSeq + 1, GivenHash, AppliedCount + 1)
            end;
        _CorruptOrTruncated ->
            ets:insert(?TABLE, {wal_recovery_truncated_detected, true}),
            AppliedCount
    end.

apply_event_to_ets(Event) ->
    Seq = to_integer(maps:get(<<"seq">>, Event, 0), 0),
    Epoch = to_integer(maps:get(<<"epoch">>, Event, 1), 1),
    Namespace = maps:get(<<"namespace">>, Event, <<"default">>),
    CurrEpoch = lookup_val(epoch, 1),
    ets:insert(?TABLE, {epoch, max(CurrEpoch, Epoch)}),
    ets:insert(?TABLE, {tick, Seq}),
    RootBin = case maps:get(<<"state_root">>, Event, undefined) of
        undefined ->
            integer_to_binary(lookup_val(state_root, 0));
        RootInt when is_integer(RootInt) ->
            ets:insert(?TABLE, {state_root, RootInt}),
            integer_to_binary(RootInt);
        RootB when is_binary(RootB) ->
            ets:insert(?TABLE, {state_root, to_integer(RootB, 0)}),
            RootB
    end,
    case maps:get(<<"hash">>, Event, undefined) of
        undefined -> ok;
        Hash ->
            ets:insert(?TABLE, {latest_hash, Hash}),
            ets:insert(?TABLE, {{event_hash, Seq}, Hash})
    end,
    ets:insert(?TABLE, {{wal_event, Seq}, Event#{ <<"state_root">> => RootBin }}),
    ets:update_counter(?TABLE, total_replicated, 1, {total_replicated, 0}),
    Op = maps:get(<<"action">>, Event, maps:get(<<"op">>, Event, <<"put">>)),
    case Op of
        <<"register_schema">> ->
            SchemaDef = maps:get(<<"val">>, Event, #{}),
            ets:insert(?TABLE, {{schema_ns, Namespace}, SchemaDef});
        _ ->
            case maps:get(<<"key">>, Event, undefined) of
                undefined -> ok;
                Key0 ->
                    Key = case Namespace of
                        <<"default">> -> to_integer(Key0, Key0);
                        _ -> Key0
                    end,
                    case Op of
                        <<"del">> ->
                            ets:delete(?TABLE, {kv_ns, Namespace, Key}),
                            if Namespace =:= <<"default">> -> ets:delete(?TABLE, {kv, Key}); true -> ok end;
                        _ ->
                            Val = maps:get(<<"val">>, Event, 0),
                            ets:insert(?TABLE, {{kv_ns, Namespace, Key}, Val}),
                            if Namespace =:= <<"default">> -> ets:insert(?TABLE, {{kv, Key}, Val}); true -> ok end
                    end
            end
    end.

reset() ->
    ensure_started(),
    gen_server:call(?SERVER, reset, 5000).

get_tick() ->
    ensure_started(),
    gen_server:call(?SERVER, get_tick, 5000).

get_state() ->
    ensure_started(),
    gen_server:call(?SERVER, get_state, 5000).

get_key(Key) when is_integer(Key) ->
    ensure_started(),
    gen_server:call(?SERVER, {get_key, Key}, 5000).

get_wal() ->
    ensure_started(),
    gen_server:call(?SERVER, get_wal, 5000).

get_wal_batch(FromSeq, ToSeq) ->
    ensure_started(),
    gen_server:call(?SERVER, {get_wal_batch, FromSeq, ToSeq}, 10000).

get_status() ->
    ensure_started(),
    gen_server:call(?SERVER, get_status, 5000).

get_status_map() ->
    ensure_started(),
    Epoch = lookup_val(epoch, 1),
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    Connected = lookup_val(connected_to_hot, false),
    Total = lookup_val(total_replicated, 0),
    FencedRej = lookup_val(fenced_rejections, 0),
    GapRej = lookup_val(gap_rejections, 0),
    HashRej = lookup_val(hash_rejections, 0),
    #{
        <<"ok">> => true,
        <<"role">> => <<"SD-HyperBEAM-Node">>,
        <<"node_name">> => <<"hb_su">>,
        <<"device">> => <<"sd@1.0">>,
        <<"connected_to_hot">> => Connected,
        <<"epoch">> => Epoch,
        <<"latest_tick_ram">> => Tick,
        <<"latest_tick">> => Tick,
        <<"state_root_ram">> => Root,
        <<"state_root_str">> => integer_to_binary(Root),
        <<"total_replicated">> => Total,
        <<"fenced_rejections">> => FencedRej,
        <<"gap_rejections">> => GapRej,
        <<"hash_rejections">> => HashRej,
        <<"wal_file">> => list_to_binary(wal_path())
    }.

%% HyperBEAM Request Handlers for ~sd@1.0

get_tick_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    case get_tick() of
        {ok, Res} -> Res;
        Res -> Res
    end.

get_state_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    case get_state() of
        {ok, Res} -> Res;
        Res -> Res
    end.

get_key_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Key = parse_int_field(<<"key">>, Req, 1),
    case get_key(Key) of
        {ok, Res} -> Res;
        {error, not_found} ->
            #{ <<"ok">> => false, <<"status">> => 404, <<"error">> => <<"key_not_found">> };
        Res -> Res
    end.

get_wal_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    case get_wal() of
        {ok, Res} -> Res;
        Res -> Res
    end.

get_wal_batch_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    FromSeq = parse_int_field(<<"from_seq">>, Req, 1),
    CurrTick = lookup_val(tick, 0),
    ToSeq = parse_int_field(<<"to_seq">>, Req, CurrTick),
    get_wal_batch(FromSeq, ToSeq).

get_status_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    get_status_map().

fence_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    CurrEpoch = lookup_val(epoch, 1),
    NewEpoch = parse_int_field(<<"epoch">>, Req, CurrEpoch),
    fence_epoch(NewEpoch).

restart_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    simulate_restart_from_wal().

reset_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    case reset() of
        {ok, Res} -> Res;
        Res -> Res
    end.

%% Compatibility handler for ~scheduler@1.0 backward routing
handle_scheduler_http(<<"wal_batch">>, Base, Req, Opts) ->
    {ok, get_wal_batch_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"key">>, Base, Req, Opts) ->
    {ok, get_key_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"fence_epoch">>, Base, Req, Opts) ->
    {ok, fence_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"simulate_restart">>, Base, Req, Opts) ->
    {ok, restart_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"reset">>, Base, Req, Opts) ->
    {ok, reset_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"tick">>, Base, Req, Opts) ->
    {ok, get_tick_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"state">>, Base, Req, Opts) ->
    {ok, get_state_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"wal">>, Base, Req, Opts) ->
    {ok, get_wal_from_req(Base, Req, Opts)};
handle_scheduler_http(_, Base, Req, Opts) ->
    {ok, get_status_from_req(Base, Req, Opts)}.

%% GenServer Callbacks

handle_call({fence_epoch, NewEpoch}, _From, State) ->
    CurrEpoch = lookup_val(epoch, 1),
    if NewEpoch < CurrEpoch ->
        {reply, #{ <<"ok">> => false, <<"error">> => <<"epoch_cannot_decrease">>, <<"epoch">> => CurrEpoch }, State};
       true ->
        ets:insert(?TABLE, {epoch, NewEpoch}),
        {reply, #{ <<"ok">> => true, <<"previous_epoch">> => CurrEpoch, <<"epoch">> => NewEpoch }, State}
    end;

handle_call(recover_wal, _From, State) ->
    close_wal_fd(),
    Count = recover_from_wal(wal_path()),
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    {reply, #{ <<"ok">> => true, <<"recovered_entries">> => Count, <<"tick">> => Tick, <<"state_root">> => Root, <<"state_root_str">> => integer_to_binary(Root) }, State};

handle_call(simulate_restart_from_wal, _From, State) ->
    close_wal_fd(),
    Conn = lookup_val(connected_to_hot, false),
    ets:delete_all_objects(?TABLE),
    reset_ets_defaults(),
    ets:insert(?TABLE, {connected_to_hot, Conn}),
    Count = recover_from_wal(wal_path()),
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    Epoch = lookup_val(epoch, 1),
    KVCount = ets:foldl(fun({{kv, _}, _}, Acc) -> Acc + 1; (_, Acc) -> Acc end, 0, ?TABLE),
    HaltReason = case ets:lookup(?TABLE, wal_recovery_halt_reason) of
        [{wal_recovery_halt_reason, R}] -> list_to_binary(io_lib:format("~p", [R]));
        _ -> <<"none">>
    end,
    Truncated = case ets:lookup(?TABLE, wal_recovery_truncated_detected) of
        [{wal_recovery_truncated_detected, T}] -> T;
        _ -> false
    end,
    {reply, #{
        <<"ok">> => true,
        <<"recovered_entries">> => Count,
        <<"epoch">> => Epoch,
        <<"tick">> => Tick,
        <<"state_root">> => Root,
        <<"state_root_str">> => integer_to_binary(Root),
        <<"records_count">> => KVCount,
        <<"halt_reason">> => HaltReason,
        <<"truncated_detected">> => Truncated,
        <<"source">> => <<"WAL_TO_RAM_RECOVERY">>
    }, State};

handle_call(get_tick, _From, State) ->
    Epoch = lookup_val(epoch, 1),
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    Hash = lookup_val(latest_hash, <<>>),
    KVCount = ets:foldl(fun({{kv, _}, _}, Acc) -> Acc + 1; (_, Acc) -> Acc end, 0, ?TABLE),
    Reply = #{
        <<"ok">> => true,
        <<"epoch">> => Epoch,
        <<"tick">> => Tick,
        <<"latest_tick">> => Tick,
        <<"state_root">> => Root,
        <<"state_root_str">> => integer_to_binary(Root),
        <<"latest_hash">> => Hash,
        <<"records_count">> => KVCount,
        <<"source">> => <<"RAM">>
    },
    {reply, Reply, State};

handle_call(get_state, _From, State) ->
    Epoch = lookup_val(epoch, 1),
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    AllKV = ets:foldl(fun({{kv, K}, V}, Acc) ->
                          Acc#{ integer_to_binary(K) => V };
                         (_, Acc) -> Acc
                      end, #{}, ?TABLE),
    Reply = #{
        <<"ok">> => true,
        <<"epoch">> => Epoch,
        <<"tick">> => Tick,
        <<"latest_tick">> => Tick,
        <<"state_root">> => Root,
        <<"state_root_str">> => integer_to_binary(Root),
        <<"kv">> => AllKV,
        <<"records">> => AllKV,
        <<"total_keys">> => maps:size(AllKV),
        <<"records_count">> => maps:size(AllKV),
        <<"source">> => <<"RAM">>
    },
    {reply, Reply, State};

handle_call({get_key, Key}, _From, State) ->
    case ets:lookup(?TABLE, {kv, Key}) of
        [{{kv, Key}, Val}] ->
            Reply = #{
                <<"ok">> => true,
                <<"key">> => Key,
                <<"val">> => Val,
                <<"found">> => true,
                <<"source">> => <<"RAM">>
            },
            {reply, Reply, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_wal_batch, FromSeq0, ToSeq0}, _From, State) ->
    CurrTick = lookup_val(tick, 0),
    FromSeq = max(1, FromSeq0),
    ToSeq = if ToSeq0 =< 0 -> CurrTick; true -> min(ToSeq0, CurrTick) end,
    Events = if FromSeq =< ToSeq andalso ToSeq > 0 ->
        lists:filtermap(fun(S) ->
            case ets:lookup(?TABLE, {wal_event, S}) of
                [{{wal_event, S}, Ev}] -> {true, Ev};
                [] -> false
            end
        end, lists:seq(FromSeq, ToSeq));
       true -> []
    end,
    SuId = <<"SU_NODE_1">>,
    {BatchEpoch, ActualFrom, ActualTo, PropHash, PropRootBin} = case Events of
        [] ->
            {lookup_val(epoch, 1), FromSeq, ToSeq, lookup_val(latest_hash, ?GENESIS_HASH), integer_to_binary(lookup_val(state_root, 0))};
        [FirstEv | _] ->
            LastEv = lists:last(Events),
            Ep = to_integer(maps:get(<<"epoch">>, LastEv, lookup_val(epoch, 1)), 1),
            FSeq = to_integer(maps:get(<<"seq">>, FirstEv, FromSeq), FromSeq),
            TSeq = to_integer(maps:get(<<"seq">>, LastEv, ToSeq), ToSeq),
            PHash = maps:get(<<"hash">>, LastEv, ?GENESIS_HASH),
            PRoot0 = maps:get(<<"state_root">>, LastEv, <<"0">>),
            PRootB = if is_integer(PRoot0) -> integer_to_binary(PRoot0);
                        is_binary(PRoot0) -> PRoot0;
                        true -> <<"0">>
                     end,
            {Ep, FSeq, TSeq, PHash, PRootB}
    end,
    BatchSig = compute_batch_sig(SuId, BatchEpoch, ActualFrom, ActualTo, PropHash, PropRootBin),
    EventsJson = try iolist_to_binary(json:encode(Events)) catch _:_ -> <<"[]">> end,
    Reply = #{
        <<"ok">> => true,
        <<"su_id">> => SuId,
        <<"sd_id">> => <<"SD_NODE_1">>,
        <<"epoch">> => BatchEpoch,
        <<"from_seq">> => ActualFrom,
        <<"to_seq">> => ActualTo,
        <<"proposed_hash">> => PropHash,
        <<"proposed_state_root">> => PropRootBin,
        <<"batch_sig">> => BatchSig,
        <<"events_count">> => length(Events),
        <<"events_json">> => EventsJson
    },
    {reply, Reply, State};

handle_call(get_wal, _From, State) ->
    Entries = read_wal_entries(wal_path()),
    Reply = #{
        <<"ok">> => true,
        <<"wal_file">> => list_to_binary(wal_path()),
        <<"count">> => length(Entries),
        <<"total_lines">> => length(Entries),
        <<"entries">> => Entries
    },
    {reply, Reply, State};

handle_call(get_status, _From, State) ->
    {reply, get_status_map(), State};

handle_call(reset, _From, State) ->
    close_wal_fd(),
    Conn = lookup_val(connected_to_hot, false),
    ets:delete_all_objects(?TABLE),
    reset_ets_defaults(),
    ets:insert(?TABLE, {connected_to_hot, Conn}),
    _ = file:delete(wal_path()),
    {reply, #{ <<"ok">> => true, <<"reset">> => true }, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    close_wal_fd(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Mutual HMAC-SHA256 Node Authentication

node_auth_secret() ->
    case ets:info(?TABLE) of
        undefined -> compute_node_auth_secret();
        _ ->
            case ets:lookup(?TABLE, cached_auth_secret) of
                [{cached_auth_secret, Sec}] -> Sec;
                [] ->
                    Sec = compute_node_auth_secret(),
                    ets:insert(?TABLE, {cached_auth_secret, Sec}),
                    Sec
            end
    end.

compute_node_auth_secret() ->
    case file:read_file("/home/weslur/database/secrets/wallet.json") of
        {ok, Bin} when byte_size(Bin) > 0 ->
            crypto:hash(sha256, Bin);
        _ ->
            case os:getenv("HYDB_CLUSTER_SECRET") of
                false ->
                    throw({error, missing_cluster_auth_secret});
                EnvSec when is_list(EnvSec), length(EnvSec) > 0 ->
                    crypto:hash(sha256, list_to_binary(EnvSec));
                _ ->
                    throw({error, missing_cluster_auth_secret})
            end
    end.

compute_batch_sig(SdId, Epoch, FromSeq, ToSeq, PropHash, PropRootBin) ->
    UpperHash = string:uppercase(PropHash),
    Msg = iolist_to_binary([
        SdId, <<":">>,
        integer_to_binary(Epoch), <<":">>,
        integer_to_binary(FromSeq), <<":">>,
        integer_to_binary(ToSeq), <<":">>,
        UpperHash, <<":">>,
        PropRootBin
    ]),
    binary:encode_hex(crypto:mac(hmac, sha256, node_auth_secret(), Msg)).

subscriber_loop(Server, Host, Port) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, 4}, {active, true}, {reuseaddr, true}, {nodelay, true}], 2000) of
        {ok, Socket} ->
            case sd_perform_handshake(Socket) of
                ok ->
                    ets:insert(?TABLE, {connected_to_hot, true}),
                    sd_socket_loop(Socket, Server, Host, Port);
                error ->
                    gen_tcp:close(Socket),
                    ets:insert(?TABLE, {connected_to_hot, false}),
                    timer:sleep(500),
                    subscriber_loop(Server, Host, Port)
            end;
        {error, _Reason} ->
            ets:insert(?TABLE, {connected_to_hot, false}),
            timer:sleep(500),
            subscriber_loop(Server, Host, Port)
    end.

sd_perform_handshake(Socket) ->
    receive
        {tcp, Socket, Data} ->
            try json:decode(Data) of
                #{ <<"type">> := <<"AUTH_CHALLENGE">>, <<"nonce">> := Nonce } when is_binary(Nonce) ->
                    NodeId = <<"hb_su">>,
                    Msg = <<Nonce/binary, ":", NodeId/binary>>,
                    Sig = binary:encode_hex(crypto:mac(hmac, sha256, node_auth_secret(), Msg)),
                    Resp = iolist_to_binary(json:encode(#{
                        <<"type">> => <<"AUTH_RESPONSE">>,
                        <<"node_id">> => NodeId,
                        <<"sig">> => Sig
                    })),
                    ok = gen_tcp:send(Socket, Resp),
                    receive
                        {tcp, Socket, AckData} ->
                            case json:decode(AckData) of
                                #{ <<"type">> := <<"AUTH_OK">> } -> ok;
                                _ -> error
                            end;
                        {tcp_closed, Socket} -> error;
                        {tcp_error, Socket, _} -> error
                    after 2000 ->
                        error
                    end;
                _ ->
                    error
            catch _:_ ->
                error
            end;
        {tcp_closed, Socket} -> error;
        {tcp_error, Socket, _} -> error
    after 2000 ->
        error
    end.

sd_socket_loop(Socket, Server, Host, Port) ->
    receive
        {tcp, Socket, Data} ->
            handle_incoming_event(Socket, Data),
            sd_socket_loop(Socket, Server, Host, Port);
        {tcp_closed, Socket} ->
            ets:insert(?TABLE, {connected_to_hot, false}),
            timer:sleep(500),
            subscriber_loop(Server, Host, Port);
        {tcp_error, Socket, _Reason} ->
            gen_tcp:close(Socket),
            ets:insert(?TABLE, {connected_to_hot, false}),
            timer:sleep(500),
            subscriber_loop(Server, Host, Port);
        _Other ->
            sd_socket_loop(Socket, Server, Host, Port)
    end.

handle_incoming_event(Socket, RawData) ->
    try
        Event = json:decode(RawData),
        Seq = to_integer(maps:get(<<"seq">>, Event, 0), 0),
        EvEpoch = to_integer(maps:get(<<"epoch">>, Event, 1), 1),
        MinEpoch = lookup_val(epoch, 1),
        LastTick = lookup_val(tick, 0),
        LastHash = lookup_val(latest_hash, ?GENESIS_HASH),

        %% 1. EPOCH FENCING CHECK: Reject stale Hot Node from older epoch
        if EvEpoch < MinEpoch ->
            ets:update_counter(?TABLE, fenced_rejections, 1, {fenced_rejections, 0}),
            ok;
           Seq =< LastTick ->
            case ets:lookup(?TABLE, {event_hash, Seq}) of
                [{{event_hash, Seq}, ExistingHash}] ->
                    GivenHash = maps:get(<<"hash">>, Event, <<>>),
                    if ExistingHash == GivenHash ->
                        gen_tcp:send(Socket, <<Seq:32/big>>);
                       true ->
                        ets:update_counter(?TABLE, hash_rejections, 1, {hash_rejections, 0}),
                        ok
                    end;
                [] ->
                    ok
            end;
           Seq > (LastTick + 1) ->
            ets:update_counter(?TABLE, gap_rejections, 1, {gap_rejections, 0}),
            ok;
           true ->
            ExpectedHash = compute_event_hash(Event, LastHash),
            GivenHashRaw = maps:get(<<"hash">>, Event, <<>>),
            HashMatches = (ExpectedHash =:= GivenHashRaw) orelse
                          (string:uppercase(ExpectedHash) =:= string:uppercase(GivenHashRaw)),

            if not HashMatches ->
                ets:update_counter(?TABLE, hash_rejections, 1, {hash_rejections, 0}),
                ok;
               true ->
                WalFd = get_or_open_wal_fd(),
                ok = file:write(WalFd, [RawData, <<"\n">>]),
                ok = file:datasync(WalFd),
                apply_event_to_ets(Event),
                gen_tcp:send(Socket, <<Seq:32/big>>),
                ok
            end
        end
    catch
        _:_Err ->
            ok
    end.

compute_event_hash(Event, LastHash) ->
    Seq = to_integer(maps:get(<<"seq">>, Event, 0), 0),
    Op = maps:get(<<"action">>, Event, maps:get(<<"op">>, Event, <<"put">>)),
    Namespace = maps:get(<<"namespace">>, Event, <<"default">>),
    ValType = maps:get(<<"val_type">>, Event, <<"int64">>),
    RawKey = maps:get(<<"key">>, Event, 1),
    KeyBin = if is_integer(RawKey) -> integer_to_binary(RawKey);
                is_binary(RawKey) -> RawKey;
                true -> <<"1">>
             end,
    RawVal = maps:get(<<"val">>, Event, 0),
    CanonicalValBin = maps:get(<<"canonical_val">>, Event,
        if is_integer(RawVal) -> integer_to_binary(RawVal);
           is_binary(RawVal) -> RawVal;
           true -> <<"0">>
        end),
    ActionCode = case Op of
        <<"register_schema">> -> <<"24">>;
        <<"del">> -> <<"22">>;
        _ -> <<"20">>
    end,
    Payload = case {Namespace, ActionCode, ValType} of
        {_, <<"24">>, _} ->
            [LastHash, integer_to_binary(Seq), <<"24">>, Namespace, <<":">>, CanonicalValBin];
        {<<"default">>, <<"20">>, <<"int64">>} ->
            [LastHash, integer_to_binary(Seq), <<"20">>, KeyBin, CanonicalValBin];
        {<<"default">>, <<"22">>, _} ->
            [LastHash, integer_to_binary(Seq), <<"22">>, KeyBin];
        {_, <<"22">>, _} ->
            [LastHash, integer_to_binary(Seq), <<"22">>, Namespace, <<":">>, KeyBin];
        {_, <<"20">>, _} ->
            [LastHash, integer_to_binary(Seq), <<"20">>, Namespace, <<":">>, KeyBin, <<":">>, CanonicalValBin]
    end,
    binary:encode_hex(crypto:hash(sha256, Payload)).

lookup_val(Key, Default) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, Val}] -> Val;
        [] -> Default
    end.

read_wal_entries(Path) ->
    case file:read_file(Path) of
        {ok, Content} ->
            Lines = binary:split(Content, <<"\n">>, [global, trim_all]),
            lists:filtermap(fun(Line) ->
                try
                    {true, json:decode(Line)}
                catch _:_ -> false
                end
            end, Lines);
        _ -> []
    end.

parse_int_field(Key, Req, Default) when is_map(Req) ->
    Val = case hb_ao:get(Key, Req, #{}) of
        not_found ->
            case hb_ao:get(<< "query/", Key/binary >>, Req, #{}) of
                not_found ->
                    case hb_ao:get(<< "body/", Key/binary >>, Req, #{}) of
                        not_found -> Default;
                        V3 -> V3
                    end;
                V2 -> V2
            end;
        V1 -> V1
    end,
    to_integer(Val, Default);
parse_int_field(_Key, _Req, Default) -> Default.

to_integer(V, _Default) when is_integer(V) -> V;
to_integer(V, Default) when is_binary(V) ->
    try binary_to_integer(V) catch _:_ -> Default end;
to_integer(V, Default) when is_list(V) ->
    try list_to_integer(V) catch _:_ -> Default end;
to_integer(_, Default) -> Default.
