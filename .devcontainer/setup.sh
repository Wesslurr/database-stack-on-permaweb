#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

wait_for_apt() {
    while sudo fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 || sudo fuser /var/lib/apt/lists/lock >/dev/null 2>&1; do
        echo "  [Wait] Background apt/dpkg lock is active, waiting 2s..."
        sleep 2
    done
}

echo "=== [Codespace Setup] Installing Erlang/OTP 26, Rebar3, C/C++ Build Tools ==="
wait_for_apt
sudo apt-get update
wait_for_apt
sudo apt-get install -y software-properties-common curl git tmux build-essential cmake libtool autoconf automake jq

# Add RabbitMQ modern Erlang PPA for Ubuntu 22.04 (provides Erlang 27)
wait_for_apt
sudo add-apt-repository -y ppa:rabbitmq/rabbitmq-erlang
wait_for_apt
sudo apt-get update
wait_for_apt
sudo apt-get install -y erlang-base erlang-dev erlang-parsetools erlang-eunit erlang-asn1 erlang-crypto erlang-public-key erlang-ssl erlang-syntax-tools erlang-tools erlang-inets erlang-edoc erlang-xmerl

# Install Rebar3
if ! command -v rebar3 &>/dev/null; then
    echo "=== [Codespace Setup] Installing Rebar3 ==="
    sudo curl -fsSL -o /usr/local/bin/rebar3 https://s3.amazonaws.com/rebar3/rebar3
    sudo chmod +x /usr/local/bin/rebar3
fi

# Ensure Docker daemon is responding before building image
echo "=== [Codespace Setup] Checking Docker daemon ==="
for i in {1..30}; do
    if docker info >/dev/null 2>&1; then
        echo "  [OK] Docker daemon is ready."
        break
    fi
    sleep 1
done

# Fetch official ArLocal Docker image if not present
if ! docker image inspect ao-localnet-arlocal:latest &>/dev/null; then
    echo "=== [Codespace Setup] Fetching official ArLocal Docker image ==="
    docker pull textury/arlocal:latest
    docker tag textury/arlocal:latest ao-localnet-arlocal:latest
    echo "  [OK] ArLocal Docker image ready."
fi

# Initialize git submodule
echo "=== [Codespace Setup] Initializing HyperBEAM submodule ==="
git submodule update --init --recursive || true

# Patch rebar.config for Erlang OTP 26/27 EEP-49 maybe expression support
if [ -f "HyperBEAM/rebar.config" ] && ! grep -q "feature, maybe_expr" "HyperBEAM/rebar.config" 2>/dev/null; then
    sed -i 's/{erl_opts, \[/{erl_opts, [\n    {feature, maybe_expr, enable},/' "HyperBEAM/rebar.config"
fi
if [ -f "HyperBEAM/rebar.config" ] && ! grep -q "add, cowlib" "HyperBEAM/rebar.config" 2>/dev/null; then
    sed -i 's/{override, gun, \[{deps, \[cowlib\]}\]}/{override, gun, [{deps, [cowlib]}]},\n    {add, cowlib, [{erl_opts, [{feature, maybe_expr, enable}]}]},\n    {add, gun, [{erl_opts, [{feature, maybe_expr, enable}]}]}/' "HyperBEAM/rebar.config"
fi

for F in HyperBEAM/_build/default/lib/cowlib/src/cow_capsule.erl HyperBEAM/_build/default/lib/gun/src/gun.erl; do
    if [ -f "$F" ] && ! grep -q "feature(maybe_expr" "$F"; then
        sed -i '1s/^/-feature(maybe_expr, enable).\n/' "$F"
    fi
done

# Build configs
echo "=== [Codespace Setup] Building node configurations ==="
python3 tools/build_configs.py || true

echo "=== [Codespace Setup] Complete! Ready to launch with ./start.sh ==="
