#!/usr/bin/env escript
%%! -pa vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib/hb-0.0.1/ebin vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib/b64veryfast-0.1.0/ebin vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib/jsone-1.8.0/ebin

main(_) ->
  Files = [
    {"secrets/wallet_genesis.json", "Genesis"},
    {"secrets/wallet.json", "Hot"},
    {"secrets/wallet_su.json", "SD-1"},
    {"secrets/wallet_sd_2.json", "SD-2"},
    {"secrets/wallet_sd_3.json", "SD-3"}
  ],
  lists:foreach(fun({F, Name}) ->
    {ok, Bin} = file:read_file(F),
    W = ar_wallet:from_json(Bin),
    Addr = hb_util:human_id(ar_wallet:to_address(W)),
    io:format("~s (~s) -> ~s~n", [Name, F, Addr])
  end, Files).
