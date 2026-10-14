#!/usr/bin/env bash
# H-0: build device Gleam hymessage -> rename beam -> archive Forge -> trusted_devices -> ping
set -euo pipefail
source /home/weslur/database/scripts/env.sh
export PATH="/home/weslur/.local/bin:/home/weslur/erlang/27.0/bin:$PATH"

HMD="$DB_DIR/hymessage"
MOD="_hb_device_hymessage_1_0_h0"
OUT="$HMD/out"
ERL="/home/weslur/erlang/27.0/bin/erl"

echo '=== 1. gleam build ==='
cd "$HMD"
gleam build 2>&1 | tail -2

echo '=== 2. rename beam hymessage -> _hb_device_hymessage_1_0_h0 ==='
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
ls -la "$OUT/ebin"

echo '=== 3. zip archive (Forge format) ==='
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

echo '=== 4. deploy gleam_stdlib ke release lib (ERL_LIBS) ==='
STDLIB_EBIN=$(find build/dev/erlang/gleam_stdlib -type d -name ebin | head -1)
echo "stdlib: $STDLIB_EBIN ($(ls "$STDLIB_EBIN" | wc -l) beams)"
mkdir -p "$HB_REL/lib/gleam_stdlib-1.0.0"
rm -rf "$HB_REL/lib/gleam_stdlib-1.0.0/ebin"
cp -r "$STDLIB_EBIN" "$HB_REL/lib/gleam_stdlib-1.0.0/ebin"

echo '=== 5. tulis archive ke store node HIDUP (opts asli -> rocksdb) ==='
if ! curl -sf --max-time 3 -o /dev/null http://127.0.0.1:8734/~meta@1.0/info; then
  echo 'node mati — start dulu'
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
if [ -z "$DEVICE_ID" ]; then echo 'device_id.txt kosong'; exit 1; fi
echo "DEVICE_ID=$DEVICE_ID"

echo '=== 6. config overlay + restart node ==='
printf '{"trusted_devices": {"hymessage@1.0": "%s"}}\n' "$DEVICE_ID" > "$DB_DIR/config/hymessage.json"
cat "$DB_DIR/config/hymessage.json"
"$DB_DIR/scripts/stop-node.sh" || true
sleep 2
"$DB_DIR/scripts/start-node.sh"

echo '=== 7. TES PING ==='
for i in 1 2 3 4 5; do
  RESP=$(curl -s --max-time 8 -H 'Accept: application/json' -w '\nHTTP_%{http_code} time=%{time_total}s' \
    'http://127.0.0.1:8734/~hymessage@1.0/ping' || true)
  echo "$RESP"
  echo "$RESP" | grep -q 'pong' && { echo 'H-0 LULUS'; exit 0; }
  sleep 2
done
echo 'H-0 GAGAL — log node:'
tail -n 20 "$HB_REL/log/erlang.log.1" 2>/dev/null || true
exit 1
