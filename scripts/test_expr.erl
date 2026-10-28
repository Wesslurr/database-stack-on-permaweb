begin
  ServerRef = <<"XpOEPX668w7VNicVDHK81unzeeH8HcD83fKZRZKx9pk">>,
  NodeMsg = cowboy:get_env(ServerRef, node_msg, no_node_msg),
  WasmPath = "/home/weslur/database/vendor/HyperBEAM/test/aos-2-pure-xs.wasm",
  {ok, Bin} = file:read_file(WasmPath),
  Msg0 = #{ <<"body">> => Bin, <<"device">> => <<"wasm-64@1.0">> },
  {ok, ImageID} = hb_cache:write(Msg0, NodeMsg),
  io:format("WASM written to RocksDB: ~s~n", [ImageID]),
  ImageID
end.
