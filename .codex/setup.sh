#!/usr/bin/env bash
# Codex cloud environment setup for Next
# Auto-discovered by Codex from .codex/setup.sh on first container creation.
# Cached ~12h; .codex/maintenance.sh refreshes cached containers.
# Exports don't persist into agent phase — use ~/.bashrc or install to PATH.
set -euo pipefail

# Derive repo root from this script's location (.codex/setup.sh → parent)
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ARCH=$(uname -m)
case "$ARCH" in
  x86_64)  ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *)
    echo "Unsupported architecture: $ARCH" >&2
    exit 1
    ;;
esac
PREBUILT_DIR="$REPO_DIR/.bin/linux-$ARCH"
INSTALL_DIR="/usr/local/bin"

# shellcheck source=lib-doctor.sh
source "$(dirname "$0")/lib-doctor.sh"

# --- Timing + verbose helpers ---
SETUP_START=$(date +%s)
STEP_START=$SETUP_START
CURRENT_STEP=""
step() {
  local now=$(date +%s)
  if [ -n "$CURRENT_STEP" ]; then
    echo "  done ($(( now - STEP_START ))s)"
  fi
  STEP_START=$now
  CURRENT_STEP="$1"
  echo ""
  echo "--- $1 ---"
}
finish() {
  local now=$(date +%s)
  if [ -n "$CURRENT_STEP" ]; then
    echo "  done ($(( now - STEP_START ))s)"
  fi
  echo ""
  echo "=== setup finished in $(( now - SETUP_START ))s ==="
}

echo "=== next sandbox setup ==="
echo "  arch: $ARCH"
echo "  repo: $REPO_DIR"
echo "  prebuilt dir: $PREBUILT_DIR"

# --- 1. System aliases ---
step "1. System aliases"
if have fdfind && ! have fd; then
  ln -sf "$(command -v fdfind)" "$INSTALL_DIR/fd"
  echo "  aliased fdfind -> fd"
else
  echo "  nothing to do"
fi

# --- 2. Prebuilt binaries ---
step "2. Prebuilt binaries"
# Only tools NOT in codex-universal image. jq, rg, fd come from apt.
# golangci-lint: image has v2.1.6, we override to v2.11.3.
# Zero go install calls — everything is cross-compiled and checked in.
if [ -d "$PREBUILT_DIR" ]; then
  echo "  copying from $PREBUILT_DIR ..."
  for tool in "$PREBUILT_DIR"/*; do
    [ -f "$tool" ] || continue
    toolname=$(basename "$tool")
    size=$(du -h "$tool" | cut -f1)
    cp "$tool" "$INSTALL_DIR/$toolname"
    chmod +x "$INSTALL_DIR/$toolname"
    echo "  installed $toolname ($size)"
  done
else
  echo "  WARNING: prebuilt dir not found"
fi

# --- 3. Node tools ---
step "3. Node tools (jscpd)"
if have npm; then
  echo "  npm install -g jscpd@4 ..."
  npm install -g jscpd@4 --silent 2>&1 | tail -3 && echo "  installed jscpd" || warn "optional tool jscpd failed to install"
else
  echo "  npm not found, skipping"
fi

# --- 4. Snipe index (conditional) ---
step "4. Snipe index"
if [ -f "$REPO_DIR/.snipe/index.db" ]; then
  echo "  index.db exists from git — skipping rebuild (maintenance.sh will refresh)"
elif have snipe; then
  cd "$REPO_DIR"
  echo "  no index.db found, building ..."
  snipe index --embed-mode=off --enrich=false 2>&1 | tail -5 && echo "  snipe index built" || echo "  snipe index skipped"
else
  echo "  snipe not found, skipping"
fi

# --- 5. Go module cache ---
step "5. Go module cache"
echo "  running go mod download ..."
if ! download_go_modules; then
  fatal "go mod download failed"
fi

# --- 5b. Warm test build cache ---
step "5b. Warm test build cache"
echo "  compiling test binaries (runs no tests) ..."
cd "$REPO_DIR"
if go test -run='^$' -count=1 ./... >/dev/null 2>&1; then
  echo "  test cache warm"
else
  warn "test cache warmup failed (non-fatal)"
fi

# --- 6. Diagnose environment ---
step "6. Environment check"

echo "  6a. Go version compatibility"
check_go_version
echo "  go: $ACTUAL_GO_VER (go.mod wants $(grep '^go ' "$REPO_DIR/go.mod" | awk '{print $2}'))"

echo "  6b. golangci-lint version check"
if have golangci-lint; then
  LINT_GO_VER=$(golangci_lint_go_version)
  echo "  golangci-lint built with go$LINT_GO_VER"
  if [ -n "$LINT_GO_VER" ] && [ -n "$ACTUAL_GO_VER" ] && [ "$LINT_GO_VER" != "$ACTUAL_GO_VER" ]; then
    LINT_NUM=$(version_to_int "$LINT_GO_VER")
    ACTUAL_NUM=$(version_to_int "$ACTUAL_GO_VER")
    if [ "$LINT_NUM" -lt "$ACTUAL_NUM" ]; then
      warn "golangci-lint built with go$LINT_GO_VER; sandbox has go$ACTUAL_GO_VER — rebuild prebuilt with newer Go"
    fi
  fi
fi

echo "  6c. Restore missing prebuilt tools"
restore_prebuilt_tools

echo "  6d. Required tools"
for tool in "${REQUIRED_TOOLS[@]}"; do
  if have "$tool"; then
    printf "  ok  %s\n" "$tool"
  else
    printf "  MISSING  %s\n" "$tool"
    fatal "MISSING required tool: $tool"
  fi
done

echo "  6e. Optional tools"
for tool in "${OPTIONAL_TOOLS[@]}"; do
  if have "$tool"; then
    printf "  ok  %s (optional)\n" "$tool"
  else
    printf "  skip  %s (optional)\n" "$tool"
    warn "optional tool $tool not available"
  fi
done

# --- 7. Done ---
finish
echo "SETUP_COMPLETE"  # canary: if this prints, setup.sh ran to completion
doctor_exit "setup"
