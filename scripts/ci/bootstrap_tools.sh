#!/usr/bin/env bash
# Install the baseline tools expected by Bitbucket CI steps.
#
# Bitbucket runs each step in a fresh container, so do not rely on a previous
# step having installed uv or the Databricks CLI.

set -euo pipefail

DATABRICKS_CLI_VERSION="${DATABRICKS_CLI_VERSION:-1.4.0}"

export UV_CACHE_DIR="${UV_CACHE_DIR:-.uv-cache}"
export UV_LINK_MODE="${UV_LINK_MODE:-copy}"
export PATH="$HOME/.local/bin:$PATH"

mkdir -p "$HOME/.local/bin" "$UV_CACHE_DIR"

install_os_packages() {
  local missing=()
  local cmd
  for cmd in curl git gzip jq tar; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      missing+=("$cmd")
    fi
  done

  if [[ "${#missing[@]}" -eq 0 ]]; then
    return 0
  fi

  if ! command -v apt-get >/dev/null 2>&1; then
    printf 'Missing required commands and apt-get is unavailable: %s\n' "${missing[*]}" >&2
    return 1
  fi

  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates curl git gzip jq tar
  rm -rf /var/lib/apt/lists/*
}

install_uv() {
  if command -v uv >/dev/null 2>&1; then
    return 0
  fi

  curl -LsSf https://astral.sh/uv/install.sh | sh
}

install_databricks_cli() {
  if command -v databricks >/dev/null 2>&1; then
    local current_version
    current_version="$(databricks version 2>/dev/null || true)"
    if [[ "$current_version" == *"v${DATABRICKS_CLI_VERSION}"* ]]; then
      return 0
    fi
    printf 'Installing Databricks CLI v%s; found: %s\n' \
      "$DATABRICKS_CLI_VERSION" "${current_version:-not installed}"
  fi

  local tmp
  tmp="$(mktemp -d)"
  curl -fsSL "https://github.com/databricks/cli/releases/download/v${DATABRICKS_CLI_VERSION}/databricks_cli_${DATABRICKS_CLI_VERSION}_linux_amd64.tar.gz" \
    | tar xz -C "$tmp"
  install -m 0755 "$tmp/databricks" "$HOME/.local/bin/databricks"
  rm -rf "$tmp"
}

install_os_packages
install_uv
install_databricks_cli

uv --version
databricks version
