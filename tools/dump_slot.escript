#!/usr/bin/env escript
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
    Biggest = lists:last(lists:sort([{erlang:external_size(V), V} || {_T, _K, V} <- Rows])),
    {Size, V} = Biggest,
    io:format("Largest value: ~p bytes, type=~p~n", [Size, type(V)]),
    dump(V, 0).

dump(V, Depth) when is_map(V), Depth < 3 ->
    lists:foreach(
        fun({K, Val}) ->
            io:format("~s~p => ~p bytes (~p)~n",
                [lists:duplicate(Depth * 2, $\s), K,
                 erlang:external_size(Val), type(Val)]),
            case type(Val) of
                map -> dump(Val, Depth + 1);
                list -> dump(Val, Depth + 1);
                _ -> ok
            end
        end,
        lists:sort(maps:to_list(V))
    );
dump(V, Depth) when is_list(V), Depth < 3 ->
    io:format("~s(list of ~p)~n", [lists:duplicate(Depth * 2, $\s), length(V)]),
    case V of
        [H | _] when is_map(H); is_list(H) -> dump(H, Depth + 1);
        _ -> ok
    end;
dump(_, _) -> ok.

type(V) when is_map(V) -> map;
type(V) when is_list(V) -> list;
type(V) when is_binary(V) -> {binary, byte_size(V)};
type(V) -> V.
