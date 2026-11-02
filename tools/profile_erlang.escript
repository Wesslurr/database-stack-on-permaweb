#!/usr/bin/env escript
main(_) ->
    Node = 'hb-sd-1@DESKTOP-EDTU3OG',
    net_kernel:start([inspector, shortnames]),
    {ok, CookieBin} = file:read_file("/home/weslur/.erlang.cookie"),
    Cookie = list_to_atom(string:trim(binary_to_list(CookieBin))),
    erlang:set_cookie(Node, Cookie),
    
    io:format("=== Profiling inside hb-sd-1 ===~n"),
    
    % 1. Measure RSA signature time on node wallet
    Wallet = rpc:call(Node, hb_opts, get, [priv_wallet]),
    case Wallet of
        undefined -> io:format("Wallet not found~n");
        _ ->
            DataToSign = <<"test_data_to_sign_for_profiling_latency_1234567890">>,
            SignTimes = [
                begin
                    T0 = erlang:monotonic_time(microsecond),
                    _Sig = rpc:call(Node, ar_wallet, sign, [Wallet, DataToSign]),
                    erlang:monotonic_time(microsecond) - T0
                end || _ <- lists:seq(1, 10)
            ],
            io:format("RSA Sign times (us): ~p -> avg: ~.2f ms~n",
                SignTimes, (lists:sum(SignTimes) / length(SignTimes)) / 1000.0)
    end,
    
    % 2. Measure LMDB write time
    Opts = rpc:call(Node, hb_opts, get, []),
    Store = maps:get(<<"store">>, Opts, []),
    io:format("Store config: ~p~n", [Store]),
    LmdbStore = hd(Store),
    WriteTimes = [
        begin
            Key = <<"prof_key_", (integer_to_binary(I))/binary>>,
            Val = <<"prof_val_", (integer_to_binary(I))/binary>>,
            T0 = erlang:monotonic_time(microsecond),
            ok = rpc:call(Node, hb_store, write, [LmdbStore, #{Key => Val}, Opts]),
            erlang:monotonic_time(microsecond) - T0
        end || I <- lists:seq(1, 10)
    ],
    io:format("LMDB Write times (us): ~p -> avg: ~.2f ms~n",
        WriteTimes, (lists:sum(WriteTimes) / length(WriteTimes)) / 1000.0),
        
    % 3. Measure Lua compute execution time
    ProcDef = maps:get(<<"sd1">>, maps:get(<<"node_processes">>, Opts, #{}), #{}),
    io:format("ProcDef keys: ~p~n", [maps:keys(ProcDef)]).
