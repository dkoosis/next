#!/usr/bin/env bash
# Source this file to activate the Go development environment for Codex/Claude sandbox
# Usage: source .codex/activate.sh
# Generic — works for any Go project with Makefile or magefile.go
#
# NOTE: For Codex cloud, .codex/setup.sh runs automatically on container creation.
# This file is for local use and as a fallback when the agent sources it via AGENTS.md.

# Detect platform
_CODEX_OS=$(uname -s | tr '[:upper:]' '[:lower:]')
_CODEX_ARCH=$(uname -m)
case "$_CODEX_ARCH" in
  x86_64) _CODEX_ARCH="amd64" ;;
  aarch64|arm64) _CODEX_ARCH="arm64" ;;
esac
_CODEX_PLATFORM="${_CODEX_OS}-${_CODEX_ARCH}"
_CODEX_PROJECT=$(basename "$PWD")

export GOTOOLCHAIN=local
export GOPROXY="https://proxy.golang.org,direct"
export GOSUMDB="sum.golang.org"

# Repo-local caches
export GOCACHE="$PWD/.codex/cache/go-build"
export GOMODCACHE="$PWD/.codex/cache/mod"
export GOLANGCI_LINT_CACHE="$PWD/.codex/cache/golangci-lint"
mkdir -p "$GOCACHE" "$GOMODCACHE" "$GOLANGCI_LINT_CACHE" 2>/dev/null || true

# Performance
export GOMAXPROCS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
ulimit -n 4096 2>/dev/null || true

# Ubuntu fd-find installs as fdfind; ensure fd is available
if command -v fdfind >/dev/null 2>&1 && ! command -v fd >/dev/null 2>&1; then
  mkdir -p "$PWD/bin" 2>/dev/null || true
  ln -sf "$(command -v fdfind)" "$PWD/bin/fd" 2>/dev/null || true
fi

# PATH: repo bins first
export PATH="$PWD/bin:$PATH"

# Helper: available commands
codex-help() {
  echo "Build & QA:"
  if [ -f Makefile ]; then
    echo "  make              # Default (qa + install)"
    echo "  make fast         # Quick: vet + test"
    echo "  make qa           # Full: vet + lint + race tests + dupl + vuln"
    echo "  make doctor       # Validate toolchain"
    echo "  make changed      # Validate only modified packages"
  fi
  echo ""
  echo "Formatting:"
  echo "  gofumpt -w <file>    # Format Go file (strict)"
  echo "  goimports -w <file>  # Fix imports"
  echo ""
  echo "Code Navigation:"
  echo "  snipe def <symbol>   # Jump to definition"
  echo "  snipe callers <sym>  # Find callers"
  echo "  snipe search \"text\"  # Text search"
}

# Report tool status
_TOOLS_OK=0
_TOOLS_MISS=0
for _t in golangci-lint snipe jq go; do
  if command -v "$_t" >/dev/null 2>&1; then
    _TOOLS_OK=$((_TOOLS_OK + 1))
  else
    _TOOLS_MISS=$((_TOOLS_MISS + 1))
  fi
done

echo "${_CODEX_PROJECT} environment activated (${_CODEX_PLATFORM})"
echo "  Tools: ${_TOOLS_OK}/4 core (go, golangci-lint, snipe, jq)"
if [ "$_TOOLS_MISS" -gt 0 ]; then
  echo "  WARNING: ${_TOOLS_MISS} tool(s) missing — run 'make doctor' for details"
fi
echo "  Run 'codex-help' for available commands"

unset _CODEX_OS _CODEX_ARCH _CODEX_PLATFORM _TOOLS_OK _TOOLS_MISS _CODEX_PROJECT _t
