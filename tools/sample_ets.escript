#!/usr/bin/env escript
main([NodeStr]) ->
    Node = list_to_atom(NodeStr),
    net_kernel:start([inspector, shortnames]),
    {ok, CookieBin} = file:read_file("/home/weslur/.erlang.cookie"),
    Cookie = list_to_atom(string:trim(binary_to_list(CookieBin))),
    erlang:set_cookie(Node, Cookie),
    EtsTables = rpc:call(Node, ets, all, []),
    LargeT = hd(lists:reverse(lists:keysort(2, [
        {T, rpc:call(Node, ets, info, [T, size]), rpc:call(Node, ets, info, [T, name])}
        || T <- EtsTables, rpc:call(Node, ets, info, [T, name]) =:= hb_store_volatile
    ]))),
    {TId, Size, _} = LargeT,
    io:format("Inspecting table ~p with size ~p~n", [TId, Size]),
    First = rpc:call(Node, ets, first, [TId]),
    sample_keys(Node, TId, First, 20).

sample_keys(_Node, _TId, '$end_of_table', _Count) -> ok;
sample_keys(_Node, _TId, _Key, 0) -> ok;
sample_keys(Node, TId, Key, Count) ->
    Val = rpc:call(Node, ets, lookup, [TId, Key]),
    io:format("Key: ~100p~n  Val Type/Size: ~p~n", [Key, case Val of
        [{_, V}] when is_binary(V) -> {binary, byte_size(V)};
        [{_, V}] when is_map(V) -> {map, map_size(V)};
        [{_, V}] -> {other, V};
        Other -> Other
    end]),
    Next = rpc:call(Node, ets, next, [TId, Key]),
    sample_keys(Node, TId, Next, Count - 1).
