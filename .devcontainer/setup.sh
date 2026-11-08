#!/usr/bin/env bash
set -euo pipefail

echo "=== [Codespace Setup] Installing Erlang/OTP 26, Rebar3, C/C++ Build Tools ==="
sudo apt-get update
sudo apt-get install -y software-properties-common curl git tmux build-essential cmake libtool autoconf automake jq

# Add RabbitMQ modern Erlang 26 PPA for Ubuntu 22.04
sudo add-apt-repository -y ppa:rabbitmq/rabbitmq-erlang-26
sudo apt-get update
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

# Build ArLocal Docker image if not present
if ! docker image inspect ao-localnet-arlocal:latest &>/dev/null; then
    echo "=== [Codespace Setup] Building ArLocal Docker image ==="
    cat << 'EOF' | docker build -t ao-localnet-arlocal:latest -
FROM node:18-alpine
RUN npm install -g arlocal
EXPOSE 1984
ENTRYPOINT ["npx", "arlocal", "1984"]
EOF
    echo "  [OK] ArLocal Docker image built and tagged."
fi

# Initialize git submodule
echo "=== [Codespace Setup] Initializing HyperBEAM submodule ==="
git submodule update --init --recursive

# Pre-compile HyperBEAM
echo "=== [Codespace Setup] Pre-compiling HyperBEAM ==="
(cd HyperBEAM && rebar3 compile)

# Build configs
echo "=== [Codespace Setup] Building node configurations ==="
python3 tools/build_configs.py

echo "=== [Codespace Setup] Complete! Ready to launch with ./tools/cluster_up.sh ==="
