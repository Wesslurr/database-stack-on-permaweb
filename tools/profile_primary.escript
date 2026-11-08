#!/usr/bin/env escript
main(_) ->
    Node = 'hb-primary@DESKTOP-EDTU3OG',
    net_kernel:start([inspector_p, shortnames]),
    {ok, CookieBin} = file:read_file("/home/weslur/.erlang.cookie"),
    Cookie = list_to_atom(string:trim(binary_to_list(CookieBin))),
    erlang:set_cookie(Node, Cookie),
    
    io:format("=== Profiling inside hb-primary ===~n"),
    RemoteCode = "
        Opts = hb_opts:get(#{node => true}),
        NodeProcs = maps:get(<<\"node_processes\">>, Opts, #{}),
        PrimaryProc = maps:get(<<\"primary\">>, NodeProcs),
        
        Req = #{
            <<\"path\">> => <<\"push\">>,
            <<\"body\">> => #{
                <<\"action\">> => <<\"Write-Point\">>,
                <<\"id\">> => <<\"probe_primary\">>,
                <<\"x\">> => 10.0,
                <<\"y\">> => 20.0
            }
        },
        
        % 1. Measure dev_push:push
        T0 = erlang:monotonic_time(microsecond),
        PushRes = dev_push:push(PrimaryProc, Req, Opts),
        T_Push = erlang:monotonic_time(microsecond) - T0,
        
        % 2. Measure dev_scheduler:schedule
        T1 = erlang:monotonic_time(microsecond),
        {ok, Assignment} = dev_scheduler:schedule(PrimaryProc, Req, Opts),
        T_Sched = erlang:monotonic_time(microsecond) - T1,
        
        % 3. Measure compute (Lua + cache write)
        Slot = hb_ao:get(<<\"slot\">>, Assignment, Opts),
        T2 = erlang:monotonic_time(microsecond),
        ComputeRes = dev_process:compute(PrimaryProc, #{<<\"path\">> => <<\"compute\">>, <<\"slot\">> => Slot}, Opts),
        T_Compute = erlang:monotonic_time(microsecond) - T2,
        
        {T_Push, T_Sched, T_Compute}.
    ",
    case rpc:call(Node, erl_eval, exprs, [element(2, erl_parse:parse_exprs(element(2, erl_scan:string(RemoteCode)))), []]) of
        {value, {TPush, TSched, TCompute}, _} ->
            io:format("dev_push:push total              : ~.2f ms~n", [TPush / 1000.0]),
            io:format("  -> dev_scheduler:schedule alone : ~.2f ms~n", [TSched / 1000.0]),
            io:format("  -> dev_process:compute alone    : ~.2f ms~n", [TCompute / 1000.0]);
        Other ->
            io:format("Error: ~p~n", [Other])
    end.
