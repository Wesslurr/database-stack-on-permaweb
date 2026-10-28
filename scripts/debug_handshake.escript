#!/usr/bin/env escript
main(_) ->
    [ShortHost | _] = string:tokens(net_adm:localhost(), "."),
    ClientName = list_to_atom("dbg_" ++ integer_to_list(erlang:unique_integer([positive]))),
    net_kernel:start([ClientName, shortnames]),
    Cookie = 'ZYMGDKLVTLAIUSQDATKZ',
    HotNode = list_to_atom("hb_hot@" ++ ShortHost),
    erlang:set_cookie(HotNode, Cookie),
    
    SDNodes = [list_to_atom(N ++ "@" ++ ShortHost) || N <- ["hb_sd_1", "hb_sd_2", "hb_sd_3"]],
    lists:foreach(fun(N) ->
        erlang:set_cookie(N, Cookie),
        Res = rpc:call(N, sd_runtime, ensure_started, []),
        io:format("[~p] sd_runtime:ensure_started: ~p~n", [N, Res])
    end, SDNodes),
    
    timer:sleep(1000),
    io:format("Active Subscribers on Hot: ~p~n", [rpc:call(HotNode, ets, tab2list, [hymessage_subscribers])]),
    lists:foreach(fun(N) ->
        io:format("[~p] connected_to_hot: ~p~n", [N, rpc:call(N, ets, lookup, [sd_ram_store, connected_to_hot])])
    end, SDNodes).
