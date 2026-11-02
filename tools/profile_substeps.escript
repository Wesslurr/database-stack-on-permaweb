#!/usr/bin/env escript
main(_) ->
    Node = 'hb-sd-1@DESKTOP-EDTU3OG',
    net_kernel:start([inspector, shortnames]),
    {ok, CookieBin} = file:read_file("/home/weslur/.erlang.cookie"),
    Cookie = list_to_atom(string:trim(binary_to_list(CookieBin))),
    erlang:set_cookie(Node, Cookie),
    
    io:format("=== Profiling Sub-steps of dev_push on hb-sd-1 ===~n"),
    
    % Let's run a remote eval to measure each step of dev_push
    RemoteCode = "
        ProcID = hb_util:human_id(hb_message:id(dev_process:as(#{<<\"node-role\">> => <<\"sd1\">>}, #{}, hb_opts:get(#{node => true})))),
        Opts = hb_opts:get(#{node => true}),
        Base = #{<<\"device\">> => <<\"process@1.0\">>, <<\"node-role\">> => <<\"sd1\">>},
        Req = #{
            <<\"path\">> => <<\"push\">>,
            <<\"body\">> => #{
                <<\"action\">> => <<\"Replicate\">>,
                <<\"id\">> => <<\"probe_entity\">>,
                <<\"x\">> => 10.0,
                <<\"y\">> => 20.0,
                <<\"tick\">> => 8888,
                <<\"relayed_by_signer\">> => <<\"test\">>,
                <<\"assignment_id\">> => <<\"test\">>
            }
        },
        
        % 1. Measure scheduler
        T0 = erlang:monotonic_time(microsecond),
        {ok, Assignment} = dev_scheduler:schedule(Base, Req, Opts),
        T_Sched = erlang:monotonic_time(microsecond) - T0,
        
        % 2. Measure compute (Lua)
        Slot = hb_ao:get(<<\"slot\">>, Assignment, Opts),
        T1 = erlang:monotonic_time(microsecond),
        Res = hb_ao:resolve(Base, #{<<\"path\">> => <<\"compute\">>, <<\"slot\">> => Slot}, Opts),
        T_Compute = erlang:monotonic_time(microsecond) - T1,
        
        {T_Sched, T_Compute}.
    ",
    case rpc:call(Node, erl_eval, exprs, [element(2, erl_parse:parse_exprs(element(2, erl_scan:string(RemoteCode)))), []]) of
        {value, {TSched, TCompute}, _} ->
            io:format("Schedule (Assignment + RSA Sign) : ~.2f ms~n", [TSched / 1000.0]),
            io:format("Compute (Lua + Store Cache)      : ~.2f ms~n", [TCompute / 1000.0]);
        Other ->
            io:format("Error: ~p~n", [Other])
    end.
