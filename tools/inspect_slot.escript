#!/usr/bin/env escript
main(_) ->
    Node = 'hb-primary@DESKTOP-EDTU3OG',
    net_kernel:start([inspector, shortnames]),
    {ok, CookieBin} = file:read_file("/home/weslur/.erlang.cookie"),
    Cookie = list_to_atom(string:trim(binary_to_list(CookieBin))),
    erlang:set_cookie(Node, Cookie),
    EtsTables = rpc:call(Node, ets, all, []),
    Table = hd([T || T <- EtsTables, rpc:call(Node, ets, info, [T, name]) =:= hb_store_volatile, rpc:call(Node, ets, info, [T, size]) > 1000]),
    % Find all keys starting with ProcID or containing /10
    Matches = rpc:call(Node, ets, select, [Table, [{{'$1', '$2'}, [{is_binary, '$1'}], ['$$']}]]),
    SlotKeys = [K || [K, _V] <- Matches, binary:match(K, <<"/10">>) =/= nomatch],
    io:format("Found ~p keys matching '/10':~n", [length(SlotKeys)]),
    lists:foreach(fun(K) ->
        [{_, V}] = rpc:call(Node, ets, lookup, [Table, K]),
        io:format("  ~p -> ~p~n", [K, V])
    end, lists:sublist(SlotKeys, 10)).
