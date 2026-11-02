#!/usr/bin/env escript
main(_) ->
    Node = 'hb-primary@DESKTOP-EDTU3OG',
    net_kernel:start([inspector, shortnames]),
    {ok, CookieBin} = file:read_file("/home/weslur/.erlang.cookie"),
    Cookie = list_to_atom(string:trim(binary_to_list(CookieBin))),
    erlang:set_cookie(Node, Cookie),
    EtsTables = rpc:call(Node, ets, all, []),
    Fun = fun() ->
        Tabs = ets:all(),
        BigT = hd([T || T <- Tabs, ets:info(T, name) =:= hb_store_volatile, ets:info(T, size) > 1000]),
        Keys = [K || {K, _} <- ets:tab2list(BigT)],
        {length(Keys),
         [K || K <- Keys, binary:match(K, <<"primary">>) =/= nomatch],
         [K || K <- Keys, byte_size(K) < 30]}
    end,
    {Total, ProcKeys, ShortKeys} = rpc:call(Node, erlang, apply, [Fun, []]),
    io:format("Total: ~p~nProcKeys (~p): ~p~nShortKeys (~p): ~p~n", [
        Total, length(ProcKeys), lists:sublist(ProcKeys, 10),
        length(ShortKeys), lists:sublist(ShortKeys, 10)
    ]).
