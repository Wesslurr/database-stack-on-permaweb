#!/usr/bin/env bash
set -euo pipefail
/home/weslur/database/scripts/hb-su-eval.sh '
SuBeam = "/home/weslur/database/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/lib/hb-0.0.1/ebin/su_runtime.beam",
{ok, SuBin} = file:read_file(SuBeam),
code:purge(su_runtime),
{module, su_runtime} = code:load_binary(su_runtime, SuBeam, SuBin),
su_runtime:ensure_started(),
HashedMod = '\''_hb_device_scheduler_1_0_6l7y4l4ufu72gqsd36slwdpsfyn2tjefii7cvy3zcaa47hihfi4a'\'',
Beam = "/home/weslur/database/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb_su/lib/hb-0.0.1/ebin/_hb_device_scheduler_1_0_6l7y4l4ufu72gqsd36slwdpsfyn2tjefii7cvy3zcaa47hihfi4a.beam",
{ok, Bin} = file:read_file(Beam),
code:purge(HashedMod),
{module, HashedMod} = code:load_binary(HashedMod, Beam, Bin),
code:purge(dev_scheduler),
code:load_binary(dev_scheduler, "/home/weslur/database/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb_su/lib/hb-0.0.1/ebin/dev_scheduler.beam", Bin),
io:format("reloaded ~p: ~p, su_runtime: ~p~n", [HashedMod, code:is_loaded(HashedMod), code:is_loaded(su_runtime)]).
'
