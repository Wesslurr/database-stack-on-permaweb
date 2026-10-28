#!/usr/bin/env escript
main(_) ->
    code:add_pathz("/home/weslur/database/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib/sd_runtime-1.0.0/ebin"),
    _ = application:ensure_all_started(inets),
    _ = application:ensure_all_started(ssl),
    ProcId = <<"UTrQ7TZWze81YT5x_ePdOTMkhiuor-oZfvREHAph2sc">>,
    io:format("Testing fetch_hyperbeam_process_result for ~s...~n", [ProcId]),
    T0 = erlang:system_time(millisecond),
    Res = sd_ao_client:fetch_hyperbeam_process_result("http://127.0.0.1:8733", ProcId),
    T1 = erlang:system_time(millisecond),
    io:format("Result in ~p ms: ~p~n", [T1 - T0, Res]).
