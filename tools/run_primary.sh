#!/usr/bin/env bash
export PATH="/home/weslur/.local/bin:/home/weslur/.cargo/bin:$PATH"
export ERL_FLAGS="+S 4:4 +A 4 +MBas aobf"
cd /home/weslur/database/HyperBEAM
HB_PORT="8734" HB_CONFIG="/home/weslur/database/engine/config/primary.json" rebar3 shell --sname hb-primary
