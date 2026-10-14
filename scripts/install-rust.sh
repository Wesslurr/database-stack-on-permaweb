#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
if command -v cargo >/dev/null 2>&1; then
  echo "[rust] already installed: $(cargo --version)"
  exit 0
fi
echo "[rust] installing rustup (user-level, minimal profile)..."
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
source "$HOME/.cargo/env"
cargo --version
