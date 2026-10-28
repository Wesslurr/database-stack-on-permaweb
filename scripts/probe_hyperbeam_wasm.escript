#!/usr/bin/env escript
%% Probe HyperBEAM runtime for native AO process and WASM execution

main(_) ->
    code:add_paths(filelib:wildcard("vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib/*/ebin")),
    code:add_paths(filelib:wildcard("vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib")),
    io:format("=== HyperBEAM Core Probe ===~n"),
    case code:ensure_loaded(hb) of
        {module, hb} -> io:format("✅ Module hb loaded~n");
        Err1 -> io:format("❌ Module hb load error: ~p~n", [Err1])
    end,
    case code:ensure_loaded(hb_beamr) of
        {module, hb_beamr} -> io:format("✅ Module hb_beamr loaded~n");
        Err2 -> io:format("❌ Module hb_beamr load error: ~p~n", [Err2])
    end,
    case code:ensure_loaded(dev_process) of
        {module, dev_process} -> io:format("✅ Module dev_process loaded~n");
        Err3 -> io:format("❌ Module dev_process load error: ~p~n", [Err3])
    end,
    case code:ensure_loaded(dev_scheduler) of
        {module, dev_scheduler} -> io:format("✅ Module dev_scheduler loaded~n");
        Err4 -> io:format("❌ Module dev_scheduler load error: ~p~n", [Err4])
    end,
    case code:ensure_loaded(dev_wasm) of
        {module, dev_wasm} -> io:format("✅ Module dev_wasm loaded~n");
        Err5 -> io:format("❌ Module dev_wasm load error: ~p~n", [Err5])
    end,
    case code:ensure_loaded(dev_lua) of
        {module, dev_lua} -> io:format("✅ Module dev_lua loaded~n");
        Err6 -> io:format("❌ Module dev_lua load error: ~p~n", [Err6])
    end,
    halt(0).
