#!/usr/bin/env escript
main(_) ->
    Node = 'hb-primary@DESKTOP-EDTU3OG',
    net_kernel:start([inspector, shortnames]),
    {ok, CookieBin} = file:read_file("/home/weslur/.erlang.cookie"),
    Cookie = list_to_atom(string:trim(binary_to_list(CookieBin))),
    erlang:set_cookie(Node, Cookie),
    
    io:format("=== Measuring Internal Resolution inside hb-primary ===~n"),
    % Call resolve on primary process inside Erlang directly (bypassing Cowboy HTTP and TCP socket)
    Path = <<"/~node-process@1.0/primary/push/output~message@1.0/body">>,
    Req = #{
        <<"path">> => Path,
        <<"method">> => <<"POST">>,
        <<"headers">> => #{<<"content-type">> => <<"application/json">>},
        <<"body">> => <<"{\"action\":\"Write-Point\",\"id\":\"prof_internal\",\"x\":10.0,\"y\":20.0}">>
    },
    
    Timings = [
        begin
            T0 = erlang:monotonic_time(microsecond),
            Res = rpc:call(Node, hb_ao, resolve, [Req, #{}]),
            Duration = erlang:monotonic_time(microsecond) - T0,
            {Duration, case Res of {ok, _} -> ok; Other -> Other end}
        end || _ <- lists:seq(1, 5)
    ],
    lists:foreach(fun({D, Status}) ->
        io:format("  Internal resolve: ~.2f ms (status: ~p)~n", [D / 1000.0, Status])
    end, Timings).
