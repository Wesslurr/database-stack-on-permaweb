#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
if [ -x /usr/bin/erl ]; then
    _clean_path=""
    IFS=':' read -ra _path_parts <<< "$PATH"
    for _p in "${_path_parts[@]}"; do
        case "$_p" in
            "$HOME"/erlang/*/bin) ;;
            *) [ -n "$_p" ] && _clean_path="${_clean_path:+$_clean_path:}$_p" ;;
        esac
    done
    export PATH="$_clean_path"
    unset _clean_path _path_parts _p
fi
[ -d "$HOME/.local/bin" ] && export PATH="$HOME/.local/bin:$PATH"
[ -d "$HOME/.cargo/bin" ] && export PATH="$HOME/.cargo/bin:$PATH"
export CFLAGS="${CFLAGS:-} -Wno-error=incompatible-pointer-types"
export ERL_FLAGS="+S 4:4 +A 4 +MBas aobf"
cd "$PROJECT_ROOT/HyperBEAM"
HB_PORT="8734" HB_CONFIG="$PROJECT_ROOT/engine/config/primary.json" erl -sname hb-primary -config config/app.config -pa _build/default/lib/*/ebin -eval '{ok, _} = application:ensure_all_started(hb).'
