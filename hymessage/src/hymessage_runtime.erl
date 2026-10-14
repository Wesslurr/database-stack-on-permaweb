-module(hymessage_runtime).
-behaviour(gen_server).

-export([start_link/0, ensure_started/0, submit/2, submit/3, get_state/0, reset/0]).
-export([join/1, sync_events/1, replay/1]).
-export([db_put/3, db_get/1, db_del/2, db_snapshot/0, db_restore/0]).
-export([submit_from_req/3, get_state_from_req/3, reset_from_req/3, join_from_req/3, sync_from_req/3, replay_from_req/3]).
-export([put_from_req/3, get_from_req/3, del_from_req/3, snapshot_from_req/3, restore_from_req/3]).
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
    gen_server:call(?SERVER, {replay, Events}, 10000).

db_put(Key, Val, Token) when is_integer(Key), is_integer(Val) ->
    ensure_started(),
    gen_server:call(?SERVER, {db_put, Key, Val, Token}, 5000).

db_get(Key) when is_integer(Key) ->
    ensure_started(),
    gen_server:call(?SERVER, {db_get, Key}, 5000).

db_del(Key, Token) when is_integer(Key) ->
    ensure_started(),
    gen_server:call(?SERVER, {db_del, Key, Token}, 5000).

db_snapshot() ->
    ensure_started(),
    gen_server:call(?SERVER, db_snapshot, 5000).

db_restore() ->
    ensure_started(),
    gen_server:call(?SERVER, db_restore, 5000).

reset() ->
    ensure_started(),
    gen_server:call(?SERVER, reset, 5000).

%% Request Handlers

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
    Key = parse_int_field(<<"key">>, Req, 1),
    Val = parse_int_field(<<"val">>, Req, 0),
    Token = parse_bin_field(<<"token">>, Req, undefined),
    case db_put(Key, Val, Token) of
        {ok, Res} -> Res;
        {error, Reason} ->
            #{ <<"ok">> => false, <<"status">> => 403, <<"error">> => hb_util:bin(Reason) }
    end.

get_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Key = parse_int_field(<<"key">>, Req, 1),
    case db_get(Key) of
        {ok, Res} -> Res;
        {error, Reason} ->
            #{ <<"ok">> => false, <<"status">> => 404, <<"error">> => hb_util:bin(Reason) }
    end.

del_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Key = parse_int_field(<<"key">>, Req, 1),
    Token = parse_bin_field(<<"token">>, Req, undefined),
    case db_del(Key, Token) of
        {ok, Res} -> Res;
        {error, Reason} ->
            #{ <<"ok">> => false, <<"status">> => 403, <<"error">> => hb_util:bin(Reason) }
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
    Events = parse_events_field(Req),
    case replay(Events) of
        {ok, Res} -> Res;
        {error, Reason} ->
            #{ <<"ok">> => false, <<"status">> => 400, <<"error">> => hb_util:bin(Reason) }
    end.

reset_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    ok = reset(),
    #{
        <<"ok">> => true,
        <<"device">> => <<"hymessage@1.0">>,
        <<"message">> => <<"state reset successfully">>
    }.

parse_int_field(Key, Req, Default) when is_map(Req) ->
    Val = case hb_ao:get(Key, Req, #{}) of
        not_found ->
            case hb_ao:get(<< "query/", Key/binary >>, Req, #{}) of
                not_found ->
                    case hb_ao:get(<< "body/", Key/binary >>, Req, #{}) of
                        not_found ->
                            case hb_ao:get(<<"body">>, Req, #{}) of
                                Bin when is_binary(Bin), byte_size(Bin) > 0 ->
                                    try json:decode(Bin) of
                                        Decoded when is_map(Decoded) ->
                                            maps:get(Key, Decoded, Default);
                                        _ -> Default
                                    catch _:_ -> Default
                                    end;
                                _ -> Default
                            end;
                        V3 -> V3
                    end;
                V2 -> V2
            end;
        V1 -> V1
    end,
    to_integer(Val, Default);
parse_int_field(_Key, _Req, Default) ->
    Default.

parse_bin_field(Key, Req, Default) when is_map(Req) ->
    case hb_ao:get(Key, Req, #{}) of
        not_found ->
            case hb_ao:get(<< "query/", Key/binary >>, Req, #{}) of
                not_found ->
                    case hb_ao:get(<< "body/", Key/binary >>, Req, #{}) of
                        not_found -> Default;
                        V3 when is_binary(V3) -> V3;
                        _ -> Default
                    end;
                V2 when is_binary(V2) -> V2;
                _ -> Default
            end;
        V1 when is_binary(V1) -> V1;
        _ -> Default
    end;
parse_bin_field(_Key, _Req, Default) ->
    Default.

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

init([]) ->
    init_ets(),
    start_stream_hub(),
    NodeMsg = get_node_msg(),
    WasmBase = init_wasm(NodeMsg),
    {ok, #state{wasm_base = WasmBase, node_msg = NodeMsg}}.

init_ets() ->
    case ets:info(?TABLE) of
        undefined ->
            ets:new(?TABLE, [public, named_table, set]),
            ets:insert(?TABLE, {seq, 0}),
            ets:insert(?TABLE, {latest_hash, ?GENESIS_HASH}),
            ets:insert(?TABLE, {state_root, 0}),
            ets:insert(?TABLE, {history, []}),
            ets:insert(?TABLE, {last_su_ack, 0}),
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

init_wasm(NodeMsg) ->
    WasmIdFile = "/home/weslur/database/hydb/out/wasm_id.txt",
    case file:read_file(WasmIdFile) of
        {ok, RawId} ->
            WasmID = string:trim(RawId),
            WasmMsg = #{ <<"device">> => <<"wasm-64@1.0">>, <<"image">> => WasmID },
            case hb_ao:resolve(WasmMsg, <<"init">>, NodeMsg) of
                {ok, Base} -> Base;
                _ -> undefined
            end;
        _ -> undefined
    end.

start_stream_hub() ->
    case whereis(hymessage_stream_hub) of
        undefined ->
            Pid = spawn_link(fun() -> stream_hub_listen(8736) end),
            register(hymessage_stream_hub, Pid);
        _ -> ok
    end.

stream_hub_listen(Port) ->
    case gen_tcp:listen(Port, [binary, {packet, 4}, {active, true}, {reuseaddr, true}]) of
        {ok, ListenSock} ->
            stream_hub_accept(ListenSock);
        {error, _Reason} ->
            ok
    end.

stream_hub_accept(ListenSock) ->
    case gen_tcp:accept(ListenSock) of
        {ok, Socket} ->
            Worker = spawn(fun() -> stream_socket_worker(Socket) end),
            gen_tcp:controlling_process(Socket, Worker),
            ets:insert(hymessage_subscribers, {Socket, os:system_time(millisecond)}),
            stream_hub_accept(ListenSock);
        {error, closed} ->
            ok;
        {error, _} ->
            timer:sleep(50),
            stream_hub_accept(ListenSock)
    end.

stream_socket_worker(Socket) ->
    receive
        {tcp, Socket, <<AckSeq:32/big>>} ->
            ets:insert(?TABLE, {last_su_ack, AckSeq}),
            case whereis(?SERVER) of
                undefined -> ok;
                ServerPid -> ServerPid ! {su_ack, AckSeq}
            end,
            stream_socket_worker(Socket);
        {tcp, Socket, Bin} when is_binary(Bin) ->
            try json:decode(Bin) of
                #{<<"ack_seq">> := AckSeq} ->
                    ets:insert(?TABLE, {last_su_ack, to_integer(AckSeq, 0)}),
                    case whereis(?SERVER) of
                        undefined -> ok;
                        ServerPid -> ServerPid ! {su_ack, to_integer(AckSeq, 0)}
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

broadcast_and_await_su(Event, Seq) ->
    case ets:tab2list(hymessage_subscribers) of
        [] ->
            {false, 0};
        Subscribers ->
            Payload = try iolist_to_binary(json:encode(Event)) catch _:_ -> <<>> end,
            ValidSubscribers = lists:filter(fun({Sock, _}) ->
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
                Awaited = await_su_ack(Seq, 25),
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
                    true
            after 1 ->
                Now = os:perf_counter(millisecond),
                if (Now - T0) >= TimeoutMs -> false;
                   true -> await_su_ack_loop(Seq, T0, TimeoutMs)
                end
            end
    end.


handle_call({join, PlayerId}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    Token = binary:encode_hex(crypto:strong_rand_bytes(16)),
    ets:insert(?SESSIONS_TABLE, {Token, PlayerId, os:system_time(second)}),
    
    WasmRes = case WasmBase of
        undefined -> 1;
        _ ->
            Req = hb_maps:merge(WasmBase, #{
                <<"function">> => <<"compute">>,
                <<"parameters">> => [10, PlayerId]
            }, NodeMsg),
            case hb_ao:resolve(Req, <<"compute">>, NodeMsg) of
                {ok, R} ->
                    case hb_ao:get(<<"results/output">>, R, NodeMsg) of
                        [Count] -> Count;
                        _ -> 1
                    end;
                _ -> 1
            end
    end,
    
    Result = #{
        <<"ok">> => true,
        <<"device">> => <<"hymessage@1.0">>,
        <<"session_token">> => Token,
        <<"player_id">> => PlayerId,
        <<"player_count">> => WasmRes
    },
    {reply, {ok, Result}, State};

handle_call({db_put, Key, Val, Token}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    T0 = os:perf_counter(nanosecond),
    
    SessionCheck = case Token of
        undefined -> ok;
        _ ->
            case ets:lookup(?SESSIONS_TABLE, Token) of
                [{Token, _, _}] -> ok;
                [] -> invalid_token
            end
    end,

    case SessionCheck of
        invalid_token ->
            {reply, {error, invalid_session_token}, State};
        ok ->
            Seq = ets:update_counter(?TABLE, seq, 1),
            [{latest_hash, PrevHash}] = ets:lookup(?TABLE, latest_hash),
            
            % Hash Chain: SHA-256(PrevHash + Seq + Action(20) + Key + Val)
            Payload = iolist_to_binary([
                PrevHash,
                integer_to_binary(Seq),
                <<"20">>,
                integer_to_binary(Key),
                integer_to_binary(Val)
            ]),
            NewHash = binary:encode_hex(crypto:hash(sha256, Payload)),
            
            % Execute O(1) in WASM living database
            StateRoot = case WasmBase of
                undefined -> 0;
                _ ->
                    Req = hb_maps:merge(WasmBase, #{
                        <<"function">> => <<"db_put">>,
                        <<"parameters">> => [Key, Val, Seq]
                    }, NodeMsg),
                    case hb_ao:resolve(Req, <<"compute">>, NodeMsg) of
                        {ok, WasmRes} ->
                            case hb_ao:get(<<"results/output">>, WasmRes, NodeMsg) of
                                [Root] -> Root;
                                _ -> 0
                            end;
                        _ -> 0
                    end
            end,
            
            ets:insert(?TABLE, {latest_hash, NewHash}),
            ets:insert(?TABLE, {state_root, StateRoot}),
            
            [{history, Hist}] = ets:lookup(?TABLE, history),
            Event = #{
                <<"seq">> => Seq,
                <<"action">> => <<"put">>,
                <<"key">> => Key,
                <<"val">> => Val,
                <<"prev_hash">> => PrevHash,
                <<"hash">> => NewHash,
                <<"state_root">> => StateRoot
            },
            NewHist = [Event | lists:sublist(Hist, 199)],
            ets:insert(?TABLE, {history, NewHist}),
            
            % Semi-Synchronous Quorum: Broadcast to SU subscribers and await 1 ACK
            {Replicated, SuCount} = broadcast_and_await_su(Event, Seq),
            
            T1 = os:perf_counter(nanosecond),
            DurationMs = ((T1 - T0) / 1000.0) / 1000.0,
            
            Result = #{
                <<"ok">> => true,
                <<"device">> => <<"hymessage@1.0">>,
                <<"seq">> => Seq,
                <<"op">> => <<"put">>,
                <<"key">> => Key,
                <<"val">> => Val,
                <<"state_root">> => StateRoot,
                <<"hash">> => NewHash,
                <<"replicated">> => Replicated,
                <<"su_count">> => SuCount,
                <<"duration_ms">> => DurationMs
            },
            {reply, {ok, Result}, State}
    end;

handle_call({db_get, Key}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    T0 = os:perf_counter(nanosecond),
    Val = case WasmBase of
        undefined -> -1;
        _ ->
            Req = hb_maps:merge(WasmBase, #{
                <<"function">> => <<"db_get">>,
                <<"parameters">> => [Key]
            }, NodeMsg),
            case hb_ao:resolve(Req, <<"compute">>, NodeMsg) of
                {ok, WasmRes} ->
                    case hb_ao:get(<<"results/output">>, WasmRes, NodeMsg) of
                        [V] -> V;
                        _ -> -1
                    end;
                _ -> -1
            end
    end,
    T1 = os:perf_counter(nanosecond),
    DurationMs = ((T1 - T0) / 1000.0) / 1000.0,
    Result = #{
        <<"ok">> => true,
        <<"device">> => <<"hymessage@1.0">>,
        <<"key">> => Key,
        <<"val">> => Val,
        <<"found">> => (Val >= 0),
        <<"duration_ms">> => DurationMs
    },
    {reply, {ok, Result}, State};

handle_call({db_del, Key, _Token}, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    T0 = os:perf_counter(nanosecond),
    Seq = ets:update_counter(?TABLE, seq, 1),
    [{latest_hash, PrevHash}] = ets:lookup(?TABLE, latest_hash),
    
    Payload = iolist_to_binary([
        PrevHash,
        integer_to_binary(Seq),
        <<"22">>,
        integer_to_binary(Key)
    ]),
    NewHash = binary:encode_hex(crypto:hash(sha256, Payload)),
    
    StateRoot = case WasmBase of
        undefined -> 0;
        _ ->
            Req = hb_maps:merge(WasmBase, #{
                <<"function">> => <<"db_del">>,
                <<"parameters">> => [Key, Seq]
            }, NodeMsg),
            case hb_ao:resolve(Req, <<"compute">>, NodeMsg) of
                {ok, WasmRes} ->
                    case hb_ao:get(<<"results/output">>, WasmRes, NodeMsg) of
                        [Root] -> Root;
                        _ -> 0
                    end;
                _ -> 0
            end
    end,
    
    ets:insert(?TABLE, {latest_hash, NewHash}),
    ets:insert(?TABLE, {state_root, StateRoot}),
    
    T1 = os:perf_counter(nanosecond),
    DurationMs = ((T1 - T0) / 1000.0) / 1000.0,
    Result = #{
        <<"ok">> => true,
        <<"device">> => <<"hymessage@1.0">>,
        <<"seq">> => Seq,
        <<"op">> => <<"del">>,
        <<"key">> => Key,
        <<"state_root">> => StateRoot,
        <<"hash">> => NewHash,
        <<"duration_ms">> => DurationMs
    },
    {reply, {ok, Result}, State};

handle_call(db_snapshot, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    BytesWritten = case WasmBase of
        undefined -> 0;
        _ ->
            Req = hb_maps:merge(WasmBase, #{
                <<"function">> => <<"db_snapshot_export">>,
                <<"parameters">> => []
            }, NodeMsg),
            case hb_ao:resolve(Req, <<"compute">>, NodeMsg) of
                {ok, WasmRes} ->
                    case hb_ao:get(<<"results/output">>, WasmRes, NodeMsg) of
                        [Bytes] -> Bytes;
                        _ -> 0
                    end;
                _ -> 0
            end
    end,
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
    ImportStatus = case WasmBase of
        undefined -> -1;
        _ ->
            Req = hb_maps:merge(WasmBase, #{
                <<"function">> => <<"db_snapshot_import">>,
                <<"parameters">> => [Bytes]
            }, NodeMsg),
            case hb_ao:resolve(Req, <<"compute">>, NodeMsg) of
                {ok, WasmRes} ->
                    case hb_ao:get(<<"results/output">>, WasmRes, NodeMsg) of
                        [Status] -> Status;
                        _ -> -1
                    end;
                _ -> -1
            end
    end,
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
            
            WasmState = case WasmBase of
                undefined -> 0;
                _ ->
                    Req = hb_maps:merge(WasmBase, #{
                        <<"function">> => <<"compute">>,
                        <<"parameters">> => [Action, Value]
                    }, NodeMsg),
                    case hb_ao:resolve(Req, <<"compute">>, NodeMsg) of
                        {ok, WasmRes} ->
                            case hb_ao:get(<<"results/output">>, WasmRes, NodeMsg) of
                                [OutputVal] -> OutputVal;
                                OutputVal when is_integer(OutputVal) -> OutputVal;
                                _ -> 0
                            end;
                        _ -> 0
                    end
            end,
            
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
    [{seq, Seq}] = ets:lookup(?TABLE, seq),
    [{latest_hash, LatestHash}] = ets:lookup(?TABLE, latest_hash),
    [{history, Hist}] = ets:lookup(?TABLE, history),
    ActiveSessions = ets:info(?SESSIONS_TABLE, size),
    
    {RootHash, Count} = case WasmBase of
        undefined -> {0, 0};
        _ ->
            ReqR = hb_maps:merge(WasmBase, #{ <<"function">> => <<"db_root_hash">>, <<"parameters">> => [] }, NodeMsg),
            ReqC = hb_maps:merge(WasmBase, #{ <<"function">> => <<"db_count">>, <<"parameters">> => [] }, NodeMsg),
            R = case hb_ao:resolve(ReqR, <<"compute">>, NodeMsg) of
                {ok, ResR} -> case hb_ao:get(<<"results/output">>, ResR, NodeMsg) of [ValR] -> ValR; _ -> 0 end;
                _ -> 0
            end,
            C = case hb_ao:resolve(ReqC, <<"compute">>, NodeMsg) of
                {ok, ResC} -> case hb_ao:get(<<"results/output">>, ResC, NodeMsg) of [ValC] -> ValC; _ -> 0 end;
                _ -> 0
            end,
            {R, C}
    end,
    
    ActiveSubscribers = case ets:info(hymessage_subscribers, size) of
        undefined -> 0;
        Sz -> Sz
    end,
    LastSuAck = case ets:lookup(?TABLE, last_su_ack) of
        [{last_su_ack, AckS}] -> AckS;
        _ -> 0
    end,
    Result = #{
        <<"ok">> => true,
        <<"device">> => <<"hymessage@1.0">>,
        <<"seq">> => Seq,
        <<"latest_hash">> => LatestHash,
        <<"db_root_hash">> => RootHash,
        <<"db_records_count">> => Count,
        <<"history_length">> => length(Hist),
        <<"active_sessions">> => ActiveSessions,
        <<"active_su_subscribers">> => ActiveSubscribers,
        <<"last_su_ack">> => LastSuAck
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
        Op = maps:get(<<"action">>, Ev, maps:get(<<"op">>, Ev, <<"put">>)),
        Key = to_integer(maps:get(<<"key">>, Ev, 1), 1),
        Val = to_integer(maps:get(<<"val">>, Ev, 0), 0),
        PrevHash = case Acc of
            [] -> 
                case ets:lookup(?TABLE, latest_hash) of
                    [{latest_hash, H}] -> H;
                    _ -> ?GENESIS_HASH
                end;
            [LastEv | _] -> maps:get(<<"hash">>, LastEv)
        end,
        ActionCode = case Op of
            <<"put">> -> <<"20">>;
            <<"del">> -> <<"22">>;
            _ -> <<"20">>
        end,
        Payload = iolist_to_binary([
            PrevHash,
            integer_to_binary(Seq),
            ActionCode,
            integer_to_binary(Key),
            if Op == <<"del">> -> <<>>; true -> integer_to_binary(Val) end
        ]),
        NewHash = binary:encode_hex(crypto:hash(sha256, Payload)),
        StateRoot = case WasmBase of
            undefined -> 0;
            _ ->
                Func = case Op of <<"del">> -> <<"db_del">>; _ -> <<"db_put">> end,
                Params = case Op of <<"del">> -> [Key, Seq]; _ -> [Key, Val, Seq] end,
                Req = hb_maps:merge(WasmBase, #{
                    <<"function">> => Func,
                    <<"parameters">> => Params
                }, NodeMsg),
                case hb_ao:resolve(Req, <<"compute">>, NodeMsg) of
                    {ok, WasmRes} ->
                        case hb_ao:get(<<"results/output">>, WasmRes, NodeMsg) of
                            [Root] -> Root;
                            _ -> 0
                        end;
                    _ -> 0
                end
        end,
        ets:insert(?TABLE, {seq, Seq}),
        ets:insert(?TABLE, {latest_hash, NewHash}),
        ets:insert(?TABLE, {state_root, StateRoot}),
        NewEv = #{
            <<"seq">> => Seq,
            <<"action">> => Op,
            <<"key">> => Key,
            <<"val">> => Val,
            <<"prev_hash">> => PrevHash,
            <<"hash">> => NewHash,
            <<"state_root">> => StateRoot
        },
        [NewEv | Acc]
    end,
    AppliedReversed = lists:foldl(ApplyFun, [], SortedEvents),
    [{history, OldHist}] = ets:lookup(?TABLE, history),
    NewHistory = lists:sublist(AppliedReversed ++ OldHist, 100),
    ets:insert(?TABLE, {history, NewHistory}),
    [{seq, FinalSeq}] = ets:lookup(?TABLE, seq),
    [{state_root, FinalRoot}] = ets:lookup(?TABLE, state_root),
    [{latest_hash, FinalHash}] = ets:lookup(?TABLE, latest_hash),
    T1 = os:perf_counter(nanosecond),
    DurationMs = ((T1 - T0) / 1000.0) / 1000.0,
    Result = #{
        <<"ok">> => true,
        <<"device">> => <<"hymessage@1.0">>,
        <<"replayed_count">> => length(SortedEvents),
        <<"seq">> => FinalSeq,
        <<"state_root">> => FinalRoot,
        <<"latest_hash">> => FinalHash,
        <<"duration_ms">> => DurationMs
    },
    {reply, {ok, Result}, State};

handle_call(reset, _From, State = #state{wasm_base = WasmBase, node_msg = NodeMsg}) ->
    ets:insert(?TABLE, {seq, 0}),
    ets:insert(?TABLE, {latest_hash, ?GENESIS_HASH}),
    ets:insert(?TABLE, {state_root, 0}),
    ets:insert(?TABLE, {history, []}),
    ets:insert(?TABLE, {last_su_ack, 0}),
    ets:delete_all_objects(?SESSIONS_TABLE),
    ActiveBase = case WasmBase of
        undefined -> init_wasm(NodeMsg);
        _ -> WasmBase
    end,
    case ActiveBase of
        undefined -> ok;
        _ ->
            Req = hb_maps:merge(ActiveBase, #{ <<"function">> => <<"db_reset">>, <<"parameters">> => [] }, NodeMsg),
            hb_ao:resolve(Req, <<"compute">>, NodeMsg)
    end,
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
