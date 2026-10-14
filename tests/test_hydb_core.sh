#!/usr/bin/env bash
set -euo pipefail
source /home/weslur/database/scripts/env.sh

"$HB_REL/bin/hb" eval '
  {ok, WasmIDRaw} = file:read_file("/home/weslur/database/hydb/out/wasm_id.txt"),
  WasmID = string:trim(WasmIDRaw),
  Wallet = hb:wallet(),
  ServerID = hb_util:human_id(ar_wallet:to_address(Wallet)),
  NodeMsg = cowboy:get_env(ServerID, node_msg),
  WasmMsg = #{ <<"device">> => <<"wasm-64@1.0">>, <<"image">> => WasmID },
  {ok, Base} = hb_ao:resolve(WasmMsg, <<"init">>, NodeMsg),

  % 1. Put Key 101 => Val 555
  Req1 = hb_maps:merge(Base, #{ <<"function">> => <<"db_put">>, <<"parameters">> => [101, 555, 1] }, NodeMsg),
  {ok, Res1} = hb_ao:resolve(Req1, <<"compute">>, NodeMsg),
  [Root1] = hb_ao:get(<<"results/output">>, Res1, NodeMsg),

  % 2. Put Key 102 => Val 888
  Req2 = hb_maps:merge(Res1, #{ <<"function">> => <<"db_put">>, <<"parameters">> => [102, 888, 2] }, NodeMsg),
  {ok, Res2} = hb_ao:resolve(Req2, <<"compute">>, NodeMsg),
  [Root2] = hb_ao:get(<<"results/output">>, Res2, NodeMsg),

  % 3. Get Key 101
  Req3 = hb_maps:merge(Res2, #{ <<"function">> => <<"db_get">>, <<"parameters">> => [101] }, NodeMsg),
  {ok, Res3} = hb_ao:resolve(Req3, <<"compute">>, NodeMsg),
  [Val101] = hb_ao:get(<<"results/output">>, Res3, NodeMsg),

  % 4. Snapshot Export
  Req4 = hb_maps:merge(Res3, #{ <<"function">> => <<"db_snapshot_export">>, <<"parameters">> => [] }, NodeMsg),
  {ok, Res4} = hb_ao:resolve(Req4, <<"compute">>, NodeMsg),
  [BytesExported] = hb_ao:get(<<"results/output">>, Res4, NodeMsg),

  % 5. Reset DB
  Req5 = hb_maps:merge(Res4, #{ <<"function">> => <<"db_reset">>, <<"parameters">> => [] }, NodeMsg),
  {ok, Res5} = hb_ao:resolve(Req5, <<"compute">>, NodeMsg),

  % 6. Snapshot Import and verify root hash restored!
  Req6 = hb_maps:merge(Res5, #{ <<"function">> => <<"db_snapshot_import">>, <<"parameters">> => [BytesExported] }, NodeMsg),
  {ok, Res6} = hb_ao:resolve(Req6, <<"compute">>, NodeMsg),
  [ImportStatus] = hb_ao:get(<<"results/output">>, Res6, NodeMsg),

  % 7. Verify Root after restore
  Req7 = hb_maps:merge(Res6, #{ <<"function">> => <<"db_root_hash">>, <<"parameters">> => [] }, NodeMsg),
  {ok, Res7} = hb_ao:resolve(Req7, <<"compute">>, NodeMsg),
  [RootRestored] = hb_ao:get(<<"results/output">>, Res7, NodeMsg),

  {hydb_test_results, [
    {root_step_1, Root1},
    {root_step_2, Root2},
    {val_key_101, Val101},
    {bytes_exported, BytesExported},
    {import_status, ImportStatus},
    {root_restored, RootRestored},
    {roots_match, Root2 =:= RootRestored}
  ]}.
'
