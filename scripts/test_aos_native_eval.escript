#!/usr/bin/env escript
%% Test native AOS process execution inside HyperBEAM

main(_) ->
    code:add_paths(filelib:wildcard("vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib/*/ebin")),
    code:add_paths(filelib:wildcard("vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib")),
    application:ensure_all_started(hb),
    Opts = #{
        <<"priv-wallet">> => hb:wallet(),
        <<"store">> => hb_test_utils:test_store()
    },
    io:format("=== Step 1: Loading WASM image into cache ===~n"),
    WasmFile = "vendor/HyperBEAM/test/aos-2-pure-xs.wasm",
    {ok, Bin} = file:read_file(WasmFile),
    Msg0 = #{ <<"body">> => Bin, <<"device">> => <<"wasm-64@1.0">> },
    {ok, ImageID} = hb_cache:write(Msg0, Opts),
    io:format("Image cached with ID: ~s~n", [ImageID]),

    io:format("=== Step 2: Creating AOS Process Definition Stack ===~n"),
    ProcMsg = hb_message:commit(#{
        <<"type">> => <<"Process">>,
        <<"image">> => ImageID,
        <<"scheduler">> => hb:address(),
        <<"authority">> => hb:address()
    }, Opts),
    ProcID = hb_message:id(ProcMsg, all),
    io:format("Process ID: ~s~n", [ProcID]),

    Base = #{
        <<"device">> => <<"stack@1.0">>,
        <<"device-stack">> => [
            <<"wasi@1.0">>,
            <<"json-iface@1.0">>,
            <<"wasm-64@1.0">>,
            <<"multipass@1.0">>
        ],
        <<"input-prefix">> => <<"process">>,
        <<"output-prefix">> => <<"wasm">>,
        <<"passes">> => 2,
        <<"stack-keys">> => [<<"init">>, <<"compute">>],
        <<"process">> => ProcMsg
    },

    io:format("=== Step 3: Initializing Process Stack ===~n"),
    {ok, InitializedState} = hb_ao:resolve(Base, <<"init">>, Opts),
    io:format("Process stack initialized successfully.~n"),

    io:format("=== Step 4: Evaluating Lua code 'return 40+2' ===~n"),
    EvalReq1 = hb_message:commit(#{
        <<"path">> => <<"compute">>,
        <<"body">> => hb_message:commit(#{
            <<"action">> => <<"Eval">>,
            <<"data">> => <<"return 40+2">>,
            <<"target">> => ProcID
        }, Opts),
        <<"block-height">> => 1
    }, Opts),

    {ok, Res1} = hb_ao:resolve(InitializedState, EvalReq1, Opts),
    Data1 = hb_ao:get(<<"results/data">>, Res1, #{}),
    io:format("Eval result: ~p~n", [Data1]),
    case Data1 of
        <<"42">> -> io:format("✅ PASSED: return 40+2 produced 42!~n");
        _ -> io:format("❌ UNEXPECTED: ~p~n", [Data1])
    end,
    halt(0).
