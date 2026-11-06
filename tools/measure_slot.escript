#!/usr/bin/env escript
%% Measure the sizes of the largest binaries held in a node's volatile store.
%% Usage: escript tools/measure_slot.escript hb-primary@HOST
main([NodeStr]) ->
    Node = list_to_atom(NodeStr),
    net_kernel:start([slot_inspector, shortnames]),
    {ok, CookieBin} = file:read_file("/home/weslur/.erlang.cookie"),
    Cookie = list_to_atom(string:trim(binary_to_list(CookieBin))),
    erlang:set_cookie(Node, Cookie),
    Tables = rpc:call(Node, ets, all, []),
    Rows = lists:append([
        case rpc:call(Node, ets, tab2list, [T]) of
            L when is_list(L) -> [{T, K, V} || {K, V} <- L];
            _ -> []
        end || T <- Tables
    ]),
    Sized = [
        {erlang:external_size(V), T, K}
        || {T, K, V} <- Rows,
           erlang:external_size(V) > 2000
    ],
    Sorted = lists:reverse(lists:sort(Sized)),
    io:format("Top 10 cached values by size (~p total):~n", [length(Sized)]),
    lists:foreach(
        fun({Size, T, K}) ->
            Name = rpc:call(Node, ets, info, [T, name]),
            io:format("  ~10w bytes  table=~p key=~P~n", [Size, Name, K, 8])
        end,
        lists:sublist(Sorted, 10)
    ).
