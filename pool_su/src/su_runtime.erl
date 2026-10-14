-module(su_runtime).
-behaviour(gen_server).

-export([start_link/0, ensure_started/0, reset/0, is_connected/0, get_current_tick/0, get_state_root/0]).
-export([get_tick/0, get_state/0, get_key/1, get_wal/0, get_status/0]).
-export([get_tick_from_req/3, get_state_from_req/3, get_key_from_req/3, get_wal_from_req/3, get_status_from_req/3, reset_from_req/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TABLE, su_ram_store).
-define(SERVER, ?MODULE).
-define(WAL_PATH, "/home/weslur/database/pool_su/wal.log").
-define(HOT_NODE_PORT, 8736).

-record(state, {
    sub_pid,
    hot_host = "127.0.0.1",
    hot_port = ?HOT_NODE_PORT
}).

is_connected() ->
    ensure_started(),
    lookup_val(connected_to_hot, false).

get_current_tick() ->
    ensure_started(),
    lookup_val(tick, 0).

get_state_root() ->
    ensure_started(),
    lookup_val(state_root, 0).

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

init([]) ->
    init_ets(),
    SubPid = spawn_link(fun() -> subscriber_loop(?SERVER, "127.0.0.1", ?HOT_NODE_PORT) end),
    {ok, #state{sub_pid = SubPid}}.

init_ets() ->
    case ets:info(?TABLE) of
        undefined ->
            ets:new(?TABLE, [public, named_table, set]),
            ets:insert(?TABLE, {tick, 0}),
            ets:insert(?TABLE, {state_root, 0}),
            ets:insert(?TABLE, {latest_hash, <<"0000000000000000000000000000000000000000000000000000000000000000">>}),
            ets:insert(?TABLE, {total_replicated, 0}),
            ets:insert(?TABLE, {connected_to_hot, false}),
            ets:insert(?TABLE, {history, []});
        _ -> ok
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

get_status() ->
    ensure_started(),
    gen_server:call(?SERVER, get_status, 5000).

%% Request Handlers for HyperBEAM Device Routing

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

get_status_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    case get_status() of
        {ok, Res} -> Res;
        Res -> Res
    end.

reset_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    case reset() of
        {ok, Res} -> Res;
        Res -> Res
    end.

%% GenServer Callbacks

handle_call(get_tick, _From, State) ->
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    Hash = lookup_val(latest_hash, <<>>),
    KVCount = ets:foldl(fun({{kv, _}, _}, Acc) -> Acc + 1; (_, Acc) -> Acc end, 0, ?TABLE),
    Reply = #{
        <<"ok">> => true,
        <<"tick">> => Tick,
        <<"latest_tick">> => Tick,
        <<"state_root">> => Root,
        <<"latest_hash">> => Hash,
        <<"records_count">> => KVCount,
        <<"source">> => <<"RAM">>
    },
    {reply, Reply, State};

handle_call(get_state, _From, State) ->
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    AllKV = ets:foldl(fun({{kv, K}, V}, Acc) ->
                          Acc#{ integer_to_binary(K) => V };
                         (_, Acc) -> Acc
                      end, #{}, ?TABLE),
    Reply = #{
        <<"ok">> => true,
        <<"tick">> => Tick,
        <<"latest_tick">> => Tick,
        <<"state_root">> => Root,
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
                <<"source">> => <<"RAM">>
            },
            {reply, Reply, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call(get_wal, _From, State) ->
    Entries = read_wal_entries(?WAL_PATH),
    Reply = #{
        <<"ok">> => true,
        <<"wal_file">> => list_to_binary(?WAL_PATH),
        <<"count">> => length(Entries),
        <<"total_lines">> => length(Entries),
        <<"entries">> => Entries
    },
    {reply, Reply, State};

handle_call(get_status, _From, State) ->
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    Connected = lookup_val(connected_to_hot, false),
    Total = lookup_val(total_replicated, 0),
    Reply = #{
        <<"role">> => <<"SU-HyperBEAM-Node">>,
        <<"node_name">> => <<"hb_su">>,
        <<"connected_to_hot">> => Connected,
        <<"latest_tick_ram">> => Tick,
        <<"state_root_ram">> => Root,
        <<"total_replicated">> => Total,
        <<"wal_file">> => list_to_binary(?WAL_PATH)
    },
    {reply, Reply, State};

handle_call(reset, _From, State) ->
    ets:insert(?TABLE, {tick, 0}),
    ets:insert(?TABLE, {state_root, 0}),
    ets:insert(?TABLE, {latest_hash, <<"0000000000000000000000000000000000000000000000000000000000000000">>}),
    ets:insert(?TABLE, {total_replicated, 0}),
    ets:insert(?TABLE, {history, []}),
    ets:match_delete(?TABLE, {{kv, '_'}, '_'}),
    _ = file:delete(?WAL_PATH),
    {reply, #{ <<"ok">> => true, <<"reset">> => true }, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Background Subscriber Loop

subscriber_loop(Server, Host, Port) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, 4}, {active, true}, {reuseaddr, true}], 2000) of
        {ok, Socket} ->
            ets:insert(?TABLE, {connected_to_hot, true}),
            su_socket_loop(Socket, Server, Host, Port);
        {error, _Reason} ->
            ets:insert(?TABLE, {connected_to_hot, false}),
            timer:sleep(500),
            subscriber_loop(Server, Host, Port)
    end.

su_socket_loop(Socket, Server, Host, Port) ->
    receive
        {tcp, Socket, Data} ->
            handle_incoming_event(Socket, Data),
            su_socket_loop(Socket, Server, Host, Port);
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
            su_socket_loop(Socket, Server, Host, Port)
    end.

handle_incoming_event(Socket, RawData) ->
    try
        Event = json:decode(RawData),
        Seq = maps:get(<<"seq">>, Event, 0),
        
        %% 1. FAST ACK: send 4-byte sequence number back to Hot Node immediately (<0.05ms)
        gen_tcp:send(Socket, <<Seq:32/big>>),
        
        %% 2. UPDATE RAM DUAL-STORE
        ets:insert(?TABLE, {tick, Seq}),
        case maps:get(<<"state_root">>, Event, undefined) of
            undefined -> ok;
            Root -> ets:insert(?TABLE, {state_root, Root})
        end,
        case maps:get(<<"hash">>, Event, undefined) of
            undefined -> ok;
            Hash -> ets:insert(?TABLE, {latest_hash, Hash})
        end,
        ets:update_counter(?TABLE, total_replicated, 1, {total_replicated, 0}),
        
        %% If KV update
        case maps:get(<<"key">>, Event, undefined) of
            undefined -> ok;
            Key ->
                Val = maps:get(<<"val">>, Event, 0),
                ets:insert(?TABLE, {{kv, Key}, Val})
        end,
        
        %% 3. APPEND ON-DISK WAL
        file:write_file(?WAL_PATH, [RawData, <<"\n">>], [append]),
        ok
    catch
        _:_Err ->
            ok
    end.

%% Utilities

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
