#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
for _extra in "/usr/local/cargo/bin" "$HOME/.cargo/bin" "$HOME/.local/bin"; do
    case ":$PATH:" in
        *":$_extra:"*) ;;
        *) [ -d "$_extra" ] && export PATH="$_extra:$PATH" ;;
    esac
done

wait_for_apt() {
    while sudo fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 || sudo fuser /var/lib/apt/lists/lock >/dev/null 2>&1; do
        echo "  [Wait] Background apt/dpkg lock is active, waiting 2s..."
        sleep 2
    done
}

echo "=== [Codespace Setup] Installing Erlang/OTP 27, Rebar3, C/C++ Build Tools ==="
wait_for_apt
sudo apt-get update
wait_for_apt
sudo apt-get install -y \
    software-properties-common curl git tmux psmisc procps \
    build-essential cmake libtool autoconf automake pkg-config \
    libssl-dev zip unzip ca-certificates jq

# Add RabbitMQ modern Erlang PPA for Ubuntu 22.04 (provides Erlang 27)
wait_for_apt
sudo add-apt-repository -y ppa:rabbitmq/rabbitmq-erlang
wait_for_apt
sudo apt-get update
wait_for_apt
sudo apt-get install -y \
    erlang-base erlang-dev erlang-parsetools erlang-eunit erlang-asn1 \
    erlang-crypto erlang-public-key erlang-ssl erlang-syntax-tools \
    erlang-tools erlang-inets erlang-edoc erlang-xmerl erlang-os-mon \
    erlang-runtime-tools

# Install Rebar3
if ! command -v rebar3 &>/dev/null; then
    echo "=== [Codespace Setup] Installing Rebar3 ==="
    sudo curl -fsSL -o /usr/local/bin/rebar3 https://s3.amazonaws.com/rebar3/rebar3
    sudo chmod +x /usr/local/bin/rebar3
fi

# Ensure Rust wasm32 target is installed
if command -v rustup &>/dev/null; then
    echo "=== [Codespace Setup] Adding Rust wasm32-unknown-unknown target ==="
    rustup target add wasm32-unknown-unknown
fi

# Ensure Node.js 20+ and ArLocal npm fallback are installed (for environments where Docker Hub times out)
NODE_MAJOR=$(node -v 2>/dev/null | sed -E 's/^v([0-9]+).*/\1/' || echo "0")
if [ "$NODE_MAJOR" -lt 18 ] 2>/dev/null; then
    echo "=== [Codespace Setup] Installing Node.js 20 LTS ==="
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
    wait_for_apt
    sudo apt-get install -y nodejs
fi
if ! command -v arlocal &>/dev/null && [ ! -x "$PROJECT_ROOT/tools/.arlocal/node_modules/.bin/arlocal" ]; then
    echo "=== [Codespace Setup] Installing arlocal fallback ==="
    npm install --no-audit --no-fund --prefix "$PROJECT_ROOT/tools/.arlocal" arlocal || true
fi

# Ensure Docker daemon is responding before pulling ArLocal image
echo "=== [Codespace Setup] Checking Docker daemon ==="
DOCKER_OK=false
for _i in {1..30}; do
    if docker info >/dev/null 2>&1; then
        echo "  [OK] Docker daemon is ready."
        DOCKER_OK=true
        break
    fi
    sleep 1
done

# Pre-fetch ArLocal Docker image if Docker daemon is ready
if [ "$DOCKER_OK" = true ]; then
    if ! docker image inspect ao-localnet-arlocal:latest &>/dev/null; then
        echo "=== [Codespace Setup] Fetching official ArLocal Docker image ==="
        if timeout 25 docker pull textury/arlocal:latest; then
            docker tag textury/arlocal:latest ao-localnet-arlocal:latest
            echo "  [OK] ArLocal Docker image ready."
        else
            echo "  [WARN] Failed to pull textury/arlocal:latest (timeout or Docker Hub unavailable); Node.js arlocal fallback will be used."
        fi
    fi
else
    echo "  [WARN] Docker daemon not yet ready in postCreateCommand; cluster_up.sh will use Docker or Node.js arlocal on launch."
fi

# Initialize unmodified HyperBEAM git submodule
echo "=== [Codespace Setup] Initializing HyperBEAM submodule ==="
git submodule update --init --recursive

# Build Rust WASM process engines (primary_engine.wasm & sd_engine.wasm)
if [ -d "engine/processes/primary" ] && [ -d "engine/processes/shared_data" ]; then
    echo "=== [Codespace Setup] Compiling Rust WASM Engines (primary, shared_data) ==="
    (cd engine/processes/primary && cargo build --target wasm32-unknown-unknown --release)
    (cd engine/processes/shared_data && cargo build --target wasm32-unknown-unknown --release)
fi

# Precompile unmodified HyperBEAM & verify external HyperBEAM Forge devices
if [ -f "HyperBEAM/rebar.config" ]; then
    echo "=== [Codespace Setup] Compiling HyperBEAM & Packaging External Forge Devices ==="
    export CFLAGS="${CFLAGS:-} -Wno-error=incompatible-pointer-types"
    (
        cd HyperBEAM
        rebar3 compile
        for DEV_NAME in l1_checkpoint hs_wasm_output hs_push; do
            DEV_SRC="../engine/devices/${DEV_NAME}/src"
            DEV_OUT="../engine/devices/${DEV_NAME}/_build/device-packages"
            if [ -d "$DEV_SRC" ]; then
                rebar3 device package --device-src "$DEV_SRC" --output-dir "$DEV_OUT"
                rebar3 device verify --device-src "$DEV_SRC" --output-dir "$DEV_OUT"
            fi
        done
    )
fi

# Generate workspace-local node configs & populate engine/data/wasm_store
echo "=== [Codespace Setup] Building node configurations ==="
python3 tools/build_configs.py

echo "=== [Codespace Setup] Complete! Ready to launch with ./start.sh ==="
