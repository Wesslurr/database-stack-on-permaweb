#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../scripts/env.sh"

DIR="$SCRIPT_DIR"
cd "$DIR"

echo "=== 1. Building HyDB (Rust -> wasm32-unknown-unknown) ==="
cargo build --target wasm32-unknown-unknown --release

mkdir -p "$DIR/out"
WASM_BIN="$DIR/target/wasm32-unknown-unknown/release/hydb.wasm"
cp "$WASM_BIN" "$DIR/out/hydb.wasm"

echo "HyDB WASM binary size: $(wc -c < "$DIR/out/hydb.wasm") bytes"

echo "=== 2. Writing HyDB WASM image to HyperBEAM cache store ==="
"$HB_REL/bin/hb" eval "
  File = \"$DIR/out/hydb.wasm\",
  {ok, Bin} = file:read_file(File),
  Msg0 = #{ <<\"body\">> => Bin },
  Wallet = hb:wallet(),
  ServerID = hb_util:human_id(ar_wallet:to_address(Wallet)),
  NodeMsg = cowboy:get_env(ServerID, node_msg),
  {ok, ID} = hb_cache:write(Msg0, NodeMsg),
  ok = file:write_file(\"$DIR/out/wasm_id.txt\", ID),
  {written_wasm_id, ID}.
"
WASM_ID=$(cat "$DIR/out/wasm_id.txt")
echo "WASM_ID=$WASM_ID"

echo "=== 3. Testing HyDB WASM directly in HyperBEAM node ==="
"$HB_REL/bin/hb" eval "
  WASM_ID = <<\"$WASM_ID\">>,
  Wallet = hb:wallet(),
  ServerID = hb_util:human_id(ar_wallet:to_address(Wallet)),
  NodeMsg = cowboy:get_env(ServerID, node_msg),
  WasmMsg = #{ <<\"device\">> => <<\"wasm-64@1.0\">>, <<\"image\">> => WASM_ID },
  {ok, Base} = hb_ao:resolve(WasmMsg, <<\"init\">>, NodeMsg),
  Req1 = hb_maps:merge(Base, #{ <<\"function\">> => <<\"db_put\">>, <<\"parameters\">> => [1, 100, 1] }, NodeMsg),
  {ok, Res1} = hb_ao:resolve(Req1, <<\"compute\">>, NodeMsg),
  [Root1] = hb_ao:get(<<\"results/output\">>, Res1, NodeMsg),
  Req2 = hb_maps:merge(Res1, #{ <<\"function\">> => <<\"db_get\">>, <<\"parameters\">> => [1] }, NodeMsg),
  {ok, Res2} = hb_ao:resolve(Req2, <<\"compute\">>, NodeMsg),
  [Val1] = hb_ao:get(<<\"results/output\">>, Res2, NodeMsg),
  {hydb_test_results, [{put_root, Root1}, {get_val, Val1}]}.
"
echo "=== HyDB build & test COMPLETE ==="
