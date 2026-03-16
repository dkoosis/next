#!/usr/bin/env bash
# Codex cached container refresh for Next
# Runs when a cached container is reused for a new task.
# Invoked as: bash /workspace/next/.codex/maintenance.sh
# Keep lightweight — setup.sh already installed tools.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

ARCH=$(uname -m)
case "$ARCH" in
  x86_64)  ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *) ARCH="amd64" ;;
esac
PREBUILT_DIR="$REPO_DIR/.bin/linux-$ARCH"
INSTALL_DIR="/usr/local/bin"

# shellcheck source=lib-doctor.sh
source "$(dirname "$0")/lib-doctor.sh"

echo "=== next maintenance ==="

# Refresh go modules
if ! download_go_modules "refreshed"; then
  fatal "go mod download failed"
fi

# Rebuild snipe index
if have snipe; then
  snipe index --embed-mode=off --enrich=false 2>/dev/null && echo "  snipe index rebuilt" || echo "  snipe index skipped"
fi

# Repair: restore missing prebuilt tools
restore_prebuilt_tools

# Go version compatibility
check_go_version

# Verify required tools (after repair) — uses REQUIRED_TOOLS from lib
for tool in "${REQUIRED_TOOLS[@]}"; do
  if ! have "$tool"; then
    printf "  MISSING  %s\n" "$tool"
    fatal "MISSING required tool: $tool"
  fi
done

# Write report + human-readable summary + exit code
doctor_exit "maintenance"
