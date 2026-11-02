#!/usr/bin/env escript
main([NodeStr]) ->
    Node = list_to_atom(NodeStr),
    net_kernel:start([inspector, shortnames]),
    {ok, CookieBin} = file:read_file("/home/weslur/.erlang.cookie"),
    Cookie = list_to_atom(string:trim(binary_to_list(CookieBin))),
    erlang:set_cookie(Node, Cookie),
    case rpc:call(Node, erlang, memory, []) of
        {badrpc, Reason} ->
            io:format("BadRPC to ~p: ~p~n", [Node, Reason]);
        Mem ->
            io:format("=== Memory breakdown for ~p ===~n", [Node]),
            lists:foreach(fun({K, V}) ->
                io:format("  ~-15s: ~8.2f MB~n", [K, V / (1024*1024)])
            end, Mem),
            Procs = rpc:call(Node, erlang, processes, []),
            ProcList = [
                case rpc:call(Node, erlang, process_info, [P, [memory, registered_name, current_function]]) of
                    Info when is_list(Info) ->
                        {proplists:get_value(memory, Info, 0), P,
                         proplists:get_value(registered_name, Info, undefined),
                         proplists:get_value(current_function, Info, undefined)};
                    _ -> {0, P, undefined, undefined}
                end || P <- Procs
            ],
            SortedProcs = lists:sublist(lists:reverse(lists:sort(ProcList)), 5),
            io:format("Top 5 Processes by memory:~n"),
            lists:foreach(fun({M, P, Reg, Func}) ->
                io:format("  PID ~p (~p) at ~p: ~.2f MB~n", [P, Reg, Func, M / (1024*1024)])
            end, SortedProcs),
            EtsTables = rpc:call(Node, ets, all, []),
            EtsList = [
                begin
                    Name = rpc:call(Node, ets, info, [T, name]),
                    Words = rpc:call(Node, ets, info, [T, memory]),
                    Size = rpc:call(Node, ets, info, [T, size]),
                    {Words * 8, T, Name, Size}
                end || T <- EtsTables
            ],
            SortedEts = lists:sublist(lists:reverse(lists:sort(EtsList)), 5),
            io:format("Top 5 ETS tables by memory:~n"),
            lists:foreach(fun({Bytes, T, Name, Size}) ->
                io:format("  Table ~p (~p) size ~p: ~.2f MB~n", [T, Name, Size, Bytes / (1024*1024)])
            end, SortedEts)
    end.
