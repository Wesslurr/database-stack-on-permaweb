#!/usr/bin/env escript
%% Print exports and imports of aos-2-pure.wasm

main(_) ->
    code:add_paths(filelib:wildcard("vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib/*/ebin")),
    code:add_paths(filelib:wildcard("vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib")),
    WasmFile = "vendor/HyperBEAM/test/aos-2-pure-xs.wasm",
    {ok, WasmBin} = file:read_file(WasmFile),
    {ok, Port, Imports, Exports} = hb_beamr:start(WasmBin, wasm),
    io:format("=== EXPORTS ===~n"),
    lists:foreach(fun(E) -> io:format("  Export: ~p~n", [E]) end, Exports),
    io:format("=== IMPORTS ===~n"),
    lists:foreach(fun(I) -> io:format("  Import: ~p~n", [I]) end, Imports),
    hb_beamr:stop(Port),
    halt(0).
