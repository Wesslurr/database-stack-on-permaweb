#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

HMD="$DB_DIR/hymessage"
MOD="_hb_device_hymessage_1_0_h0"
OUT="$HMD/out"
ERL="${ERL:-$(command -v erl || echo "$HOME/erlang/27.0/bin/erl")}"
ERLC="${ERLC:-$(command -v erlc || echo "$HOME/erlang/27.0/bin/erlc")}"

echo '=== Compiling hydb_contract & hymessage_runtime ==='
mkdir -p "$HB_REL/lib/hymessage_runtime-1.0.0/ebin"
"$ERLC" -I "$HMD/src" -o "$HB_REL/lib/hymessage_runtime-1.0.0/ebin" "$HMD/src/hydb_contract.erl" "$HMD/src/hymessage_runtime.erl"
cp -f "$HB_REL/lib/hymessage_runtime-1.0.0/ebin/"* "$HB_REL/lib/hb-0.0.1/ebin/"

echo '=== Building Gleam hymessage ==='
cd "$HMD"
gleam build

echo '=== Renaming BEAM ==='
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
  halt(0).
'

echo '=== Packaging device.zip ==='
cd "$OUT"
ZIP_MOD="$MOD" "$ERL" -noshell -eval '
  Mod = os:getenv("ZIP_MOD"),
  ZipName = "ebin/" ++ Mod ++ ".beam",
  {ok, Beam} = file:read_file(ZipName),
  {ok, {_, Zip}} = zip:create("device.zip", [{ZipName, Beam}], [memory, {uncompress, all}]),
  ok = file:write_file("device.zip", Zip),
  halt(0).
'

echo '=== Writing device archive to node cache ==='
export HB_ERL_SNAME="hb_hot"
export HB_KEY="$DB_DIR/secrets/wallet.json"
export RUNNER_LOG_DIR="$DB_DIR/data/node/logs"
export VMARGS_PATH="$DB_DIR/config/vm_node.args"
"$HB_REL/bin/hb" eval "
  [ServerID | _] = maps:keys(ranch:info()),
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
echo "DEVICE_ID=$DEVICE_ID"
printf '{"trusted_devices": {"hymessage@1.0": "%s"}}\n' "$DEVICE_ID" > "$DB_DIR/config/hymessage.json"
echo "Repackaged and registered hymessage device successfully."
