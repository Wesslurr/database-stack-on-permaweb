#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HB_SRC="$PROJECT_ROOT/HyperBEAM"
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

start_node() {
    local SNAME="$1"
    local PORT="$2"
    local CFG="$3"
    echo "Starting $SNAME on port $PORT..."
    tmux kill-session -t "$SNAME" 2>/dev/null || true
    sleep 1
    tmux new-session -d -s "$SNAME" "bash --noprofile --norc -c 'export PATH=\"$PATH\" ERL_FLAGS=\"+S 4:4 +A 4 +MBas aobf\" && cd \"$HB_SRC\" && HB_PRINT=\"error,http_error,cron_error,hook_error,warning,compute_short\" HB_PORT=\"$PORT\" HB_CONFIG=\"$CFG\" erl -sname $SNAME -config config/app.config -pa _build/default/lib/*/ebin -eval \"{ok, _} = application:ensure_all_started(hb).\"'"
}

start_node hb-genesis 8733 "$PROJECT_ROOT/engine/config/genesis.json"
sleep 2
start_node hb-primary 8734 "$PROJECT_ROOT/engine/config/primary.json"
sleep 2
start_node hb-sd-1 8735 "$PROJECT_ROOT/engine/config/sd1.json"
sleep 2
start_node hb-sd-2 8736 "$PROJECT_ROOT/engine/config/sd2.json"
sleep 2
echo "All nodes launched."
