#!/usr/bin/env bash
# Deploy sd device (Shared Data Pool) on HyperBEAM
set -euo pipefail
source /home/weslur/database/scripts/env.sh
export PATH="/home/weslur/.local/bin:/home/weslur/erlang/27.0/bin:/home/weslur/.cargo/bin:$PATH"

DB_DIR="/home/weslur/database"
SDD="$DB_DIR/pool_sd"
MOD="_hb_device_sd_1_0_sd0"
OUT="$SDD/out"
ERL="/home/weslur/erlang/27.0/bin/erl"
ERLC="/home/weslur/erlang/27.0/bin/erlc"
HB_SU_REL="$DB_DIR/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb_su"

echo '=== 1. Compiling sd_runtime.erl ==='
mkdir -p "$HB_SU_REL/lib/sd_runtime-1.0.0/ebin"
"$ERLC" -o "$HB_SU_REL/lib/sd_runtime-1.0.0/ebin" "$SDD/src/sd_runtime.erl"
mkdir -p "$HB_REL/lib/sd_runtime-1.0.0/ebin"
cp "$HB_SU_REL/lib/sd_runtime-1.0.0/ebin/sd_runtime.beam" "$HB_REL/lib/sd_runtime-1.0.0/ebin/"
echo "sd_runtime.beam deployed to lib."

echo '=== 2. Building Gleam sd device ==='
cd "$SDD"
gleam build 2>&1 | tail -2

echo '=== 3. Renaming BEAM sd -> _hb_device_sd_1_0_sd0 ==='
rm -rf "$OUT"
mkdir -p "$OUT/ebin"
SRC_BEAM="$SDD/build/dev/erlang/sd/ebin/sd.beam"
OUT_BEAM="$OUT/ebin/$MOD.beam"
SRC_BEAM="$SRC_BEAM" OUT_BEAM="$OUT_BEAM" "$ERL" -noshell -eval '
  Beam = os:getenv("SRC_BEAM"),
  Out = os:getenv("OUT_BEAM"),
  {ok, {sd, [{abstract_code, {raw_abstract_v1, Forms}}]}} = beam_lib:chunks(Beam, [abstract_code]),
  NewForms = [case F of {attribute, L, module, sd} -> {attribute, L, module, '\''_hb_device_sd_1_0_sd0'\''}; _ -> F end || F <- Forms],
  {ok, '\''_hb_device_sd_1_0_sd0'\'', OutBeam} = compile:forms(NewForms, []),
  ok = file:write_file(Out, OutBeam),
  {ok, {_, [{_, Exps}]}} = beam_lib:chunks(Out, [exports]),
  io:format("renamed ~p bytes exports=~p~n", [byte_size(OutBeam), Exps]),
  halt(0).
'

echo '=== 4. Packaging into Forge device.zip ==='
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
cd "$SDD"

echo '=== 5. Deploying gleam_stdlib ==='
STDLIB_EBIN=$(find "$DB_DIR/hymessage/build/dev/erlang/gleam_stdlib" -type d -name ebin | head -1)
mkdir -p "$HB_SU_REL/lib/gleam_stdlib-1.0.0"
rm -rf "$HB_SU_REL/lib/gleam_stdlib-1.0.0/ebin"
cp -r "$STDLIB_EBIN" "$HB_SU_REL/lib/gleam_stdlib-1.0.0/ebin"

echo '=== 6. Writing device archive to SD node cache ==='
if ! curl -sf --max-time 3 -o /dev/null http://127.0.0.1:8735/~meta@1.0/info; then
  echo 'SD node down — starting SD node'
  "$DB_DIR/scripts/start-su-node.sh"
fi

export HB_ERL_SNAME="hb_su"
"$HB_SU_REL/bin/hb" eval "
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

echo '=== 7. Updating config overlay & restarting SD node ==='
printf '{"trusted_devices": {"sd@1.0": "%s", "scheduler@1.0": "%s"}}\n' "$DEVICE_ID" "$DEVICE_ID" > "$DB_DIR/config/sd.json"
cat "$DB_DIR/config/sd.json"
"$DB_DIR/scripts/stop-su-node.sh" || true
sleep 2
"$DB_DIR/scripts/start-su-node.sh"

echo '=== 8. Verification ==='
for i in 1 2 3 4 5; do
  RESP=$(curl -s --max-time 8 -H 'Accept: application/json' -w '\nHTTP_%{http_code} time=%{time_total}s' \
    'http://127.0.0.1:8735/~sd@1.0/ping' || true)
  echo "$RESP"
  if echo "$RESP" | grep -q 'pong'; then
    echo "SD Ping verified successfully."
    break
  fi
  sleep 2
done

echo "--- Querying SD Status ---"
curl -s --max-time 8 -H 'Accept: application/json' 'http://127.0.0.1:8735/~sd@1.0/status'
echo ""

echo "=== SD DEVICE DEPLOYMENT AND VERIFICATION COMPLETE ==="
