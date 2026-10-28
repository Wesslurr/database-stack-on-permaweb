#!/usr/bin/env escript
%% Test device loading inside HyperBEAM

main(_) ->
    code:add_paths(filelib:wildcard("vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib/*/ebin")),
    code:add_paths(filelib:wildcard("vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib")),
    application:ensure_all_started(hb),
    Opts = hb_opts:defaults(),
    io:format("Default preloaded-store: ~p~n", [hb_opts:get(<<"preloaded-store">>, Opts)]),
    Devices = [
        <<"process@1.0">>,
        <<"scheduler@1.0">>,
        <<"wasm-64@1.0">>,
        <<"lua@5.3a">>,
        <<"stack@1.0">>
    ],
    lists:foreach(fun(Dev) ->
        case hb_device_load:reference(Dev, Opts) of
            {ok, Mod} -> io:format("✅ Device ~s resolved to module: ~p~n", [Dev, Mod]);
            Err -> io:format("❌ Device ~s resolution error: ~p~n", [Dev, Err])
        end
    end, Devices),
    halt(0).
