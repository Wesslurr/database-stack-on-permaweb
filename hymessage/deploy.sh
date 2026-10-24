#!/usr/bin/env bash
# Deploy hymessage device + hywasm compute engine (Rust WASM)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../scripts/env.sh"

HMD="$DB_DIR/hymessage"
MOD="_hb_device_hymessage_1_0_h0"
OUT="$HMD/out"
ERL="${ERL:-$(command -v erl || echo "$HOME/erlang/27.0/bin/erl")}"
ERLC="${ERLC:-$(command -v erlc || echo "$HOME/erlang/27.0/bin/erlc")}"

echo '=== 1. Building HyDB (Rust -> WASM) ==='
cd "$DB_DIR/hydb"
bash build.sh

echo '=== 2. Compiling hydb_contract.erl & hymessage_runtime.erl ==='
mkdir -p "$HB_REL/lib/hymessage_runtime-1.0.0/ebin"
"$ERLC" -o "$HB_REL/lib/hymessage_runtime-1.0.0/ebin" "$HMD/src/hydb_contract.erl" "$HMD/src/hymessage_runtime.erl"
echo "hydb_contract.beam & hymessage_runtime.beam deployed to lib."

echo '=== 3. Building Gleam hymessage device ==='
cd "$HMD"
gleam build 2>&1 | tail -2

echo '=== 4. Renaming BEAM hymessage -> _hb_device_hymessage_1_0_h0 ==='
rm -rf "$OUT"
mkdir -p "$OUT/ebin"
SRC_BEAM="$HMD/build/dev/erlang/hymessage/ebin/hymessage.beam"
OUT_BEAM="$OUT/ebin/$MOD.beam"
SRC_BEAM="$SRC_BEAM" OUT_BEAM="$OUT_BEAM" "$ERL" -noshell -eval '
  Beam = os:getenv("SRC_BEAM"),
  Out = os:getenv("OUT_BEAM"),
  {ok, {hymessage, [{abstract_code, {raw_abstract_v1, Forms}}]}} = beam_lib:chunks(Beam, [abstract_code]),
  NewForms = [case F of {attribute, L, module, hymessage} -> {attribute, L, module, '\''_hb_device_hymessage_1_0_h0'\''}; _ -> F end || F <- Forms],
  {ok, '\''_hb_device_hymessage_1_0_h0'\'', OutBeam} = compile:forms(NewForms, []),
  ok = file:write_file(Out, OutBeam),
  {ok, {_, [{_, Exps}]}} = beam_lib:chunks(Out, [exports]),
  io:format("renamed ~p bytes exports=~p~n", [byte_size(OutBeam), Exps]),
  halt(0).
'

echo '=== 5. Packaging into Forge device.zip ==='
cd "$OUT"
ZIP_MOD="$MOD" "$ERL" -noshell -eval '
  Mod = os:getenv("ZIP_MOD"),
  ZipName = "ebin/" ++ Mod ++ ".beam",
  {ok, Beam} = file:read_file(ZipName),
  {ok, {_, Zip}} = zip:create("device.zip", [{ZipName, Beam}], [memory, {uncompress, all}]),
  ok = file:write_file("device.zip", Zip),
  {ok, List} = zip:list_dir("device.zip"),
  io:format("zip ok ~p bytes, entries=~p~n", [byte_size(Zip), List]),
  halt(0).
'
cd "$HMD"

echo '=== 6. Deploying gleam_stdlib ==='
STDLIB_EBIN=$(find build/dev/erlang/gleam_stdlib -type d -name ebin | head -1)
mkdir -p "$HB_REL/lib/gleam_stdlib-1.0.0"
rm -rf "$HB_REL/lib/gleam_stdlib-1.0.0/ebin"
cp -r "$STDLIB_EBIN" "$HB_REL/lib/gleam_stdlib-1.0.0/ebin"

echo '=== 7. Writing device archive to node cache ==='
if ! curl -sf --max-time 3 -o /dev/null http://127.0.0.1:8734/~meta@1.0/info; then
  echo 'node down — starting node'
  "$DB_DIR/scripts/start-node.sh"
fi

"$HB_REL/bin/hb" eval "
  Wallet = hb:wallet(),
  ServerID = hb_util:human_id(ar_wallet:to_address(Wallet)),
  NodeMsg = cowboy:get_env(ServerID, node_msg),
  {ok, Zip} = file:read_file(\"$OUT/device.zip\"),
  Msg = #{
    <<\"module-name\">> => <<\"$MOD\">>,
    <<\"body\">> => Zip,
    <<\"data-protocol\">> => <<\"ao\">>,
    <<\"variant\">> => <<\"ao.N.1\">>
  },
  {ok, ID} = hb_cache:write(Msg, NodeMsg),
  ok = file:write_file(\"$OUT/device_id.txt\", ID),
  {written, ID}.
" 2>&1 | tail -1
DEVICE_ID=$(cat "$OUT/device_id.txt")
if [ -z "$DEVICE_ID" ]; then echo 'device_id.txt empty'; exit 1; fi
echo "DEVICE_ID=$DEVICE_ID"

echo '=== 8. Updating config overlay & restarting node ==='
printf '{"trusted_devices": {"hymessage@1.0": "%s"}}\n' "$DEVICE_ID" > "$DB_DIR/config/hymessage.json"
cat "$DB_DIR/config/hymessage.json"
"$DB_DIR/scripts/stop-node.sh" || true
sleep 2
"$DB_DIR/scripts/start-node.sh"

echo '=== 9. Verification & Benchmarks ==='
# 9a: ping check
for i in 1 2 3 4 5; do
  RESP=$(curl -s --max-time 8 -H 'Accept: application/json' -w '\nHTTP_%{http_code} time=%{time_total}s' \
    'http://127.0.0.1:8734/~hymessage@1.0/ping' || true)
  echo "$RESP"
  if echo "$RESP" | grep -q 'pong'; then
    echo "Ping verified successfully."
    break
  fi
  sleep 2
done

# 9b: reset check
echo "--- Resetting state ---"
curl -s --max-time 8 -H 'Accept: application/json' 'http://127.0.0.1:8734/~hymessage@1.0/reset'
echo ""

# 9c: player join check (session token generation)
echo "--- Test Player Join (player_id=1) ---"
JOIN_RESP=$(curl -s --max-time 8 -H 'Accept: application/json' 'http://127.0.0.1:8734/~hymessage@1.0/join?player_id=1')
echo "$JOIN_RESP"
TOKEN=$(echo "$JOIN_RESP" | grep -o '"session_token":"[^"]*"' | cut -d: -f2 | tr -d '"')
echo "Obtained Session Token: $TOKEN"

# 9d: pipeline submit checks with session token
echo "--- Test Submit 1: action=1 (add), value=20 with token ---"
curl -s --max-time 8 -H 'Accept: application/json' -w '\nTime: %{time_total}s\n' \
  "http://127.0.0.1:8734/~hymessage@1.0/submit?action=1&value=20&token=$TOKEN"

echo "--- Test Submit 2: action=2 (multiply), value=3 (expect state=60) with token ---"
curl -s --max-time 8 -H 'Accept: application/json' -w '\nTime: %{time_total}s\n' \
  "http://127.0.0.1:8734/~hymessage@1.0/submit?action=2&value=3&token=$TOKEN"

echo "--- Query State ---"
curl -s --max-time 8 -H 'Accept: application/json' -w '\nTime: %{time_total}s\n' \
  'http://127.0.0.1:8734/~hymessage@1.0/state'

# 9e: HyDB Living Database Operations
echo "--- HyDB Put Key 201 => Val 999 ---"
curl -s --max-time 8 -H 'Accept: application/json' -w '\nTime: %{time_total}s\n' \
  'http://127.0.0.1:8734/~hymessage@1.0/put?key=201&val=999'

echo "--- HyDB Get Key 201 ---"
curl -s --max-time 8 -H 'Accept: application/json' -w '\nTime: %{time_total}s\n' \
  'http://127.0.0.1:8734/~hymessage@1.0/get?key=201'

echo "--- HyDB Snapshot Export ---"
curl -s --max-time 8 -H 'Accept: application/json' -w '\nTime: %{time_total}s\n' \
  'http://127.0.0.1:8734/~hymessage@1.0/snapshot'

echo "--- HyDB Reset & Snapshot Restore ---"
curl -s --max-time 8 -H 'Accept: application/json' 'http://127.0.0.1:8734/~hymessage@1.0/reset'
echo ""
curl -s --max-time 8 -H 'Accept: application/json' -w '\nTime: %{time_total}s\n' \
  'http://127.0.0.1:8734/~hymessage@1.0/restore'

echo "--- HyDB Verify Restored Key 201 ---"
curl -s --max-time 8 -H 'Accept: application/json' -w '\nTime: %{time_total}s\n' \
  'http://127.0.0.1:8734/~hymessage@1.0/get?key=201'

echo "--- Test Pool SU Sliding Window Sync (since seq=1) ---"
curl -s --max-time 8 -H 'Accept: application/json' -w '\nTime: %{time_total}s\n' \
  'http://127.0.0.1:8734/~hymessage@1.0/sync?since=1'

echo "=== DEPLOYMENT AND VERIFICATION COMPLETE ==="
