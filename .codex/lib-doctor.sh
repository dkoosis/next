#!/usr/bin/env bash
# Shared helpers for sandbox doctor scripts (setup.sh, maintenance.sh).
# Sourced, not executed directly.
# Requires: REPO_DIR, INSTALL_DIR set before sourcing.

REPORT_FILE="$REPO_DIR/.codex/setup-report.json"
FATALS=()
WARNINGS=()

# Canonical tool lists — single source of truth
REQUIRED_TOOLS=(go golangci-lint snipe jq rg fd)
OPTIONAL_TOOLS=(govulncheck jscpd gofumpt goimports bat)

have() {
  command -v "$1" >/dev/null 2>&1
}

warn() {
  WARNINGS+=("$1")
}

fatal() {
  FATALS+=("$1")
}

version_to_int() {
  local major minor
  major=$(echo "$1" | cut -d. -f1)
  minor=$(echo "$1" | cut -d. -f2)
  echo $((major * 1000 + minor))
}

download_go_modules() {
  local label="${1:-downloaded}"
  cd "$REPO_DIR"
  go mod download && echo "  go modules $label"
}

golangci_lint_go_version() {
  golangci-lint version 2>&1 | grep -oP 'go\K[0-9]+\.[0-9]+' | head -1 || true
}

# Check Go toolchain version against go.mod requirement.
# Sets ACTUAL_GO_VER as a side-effect (used by golangci-lint check).
check_go_version() {
  REPO_GO_VER=$(grep '^go ' "$REPO_DIR/go.mod" | awk '{print $2}')
  ACTUAL_GO_VER=$(go version 2>/dev/null | grep -oP 'go\K[0-9]+\.[0-9]+' | head -1 || true)
  if [ -n "$REPO_GO_VER" ] && [ -n "$ACTUAL_GO_VER" ]; then
    local repo_minor
    repo_minor=$(echo "$REPO_GO_VER" | cut -d. -f1-2)
    local repo_num actual_num
    repo_num=$(version_to_int "$repo_minor")
    actual_num=$(version_to_int "$ACTUAL_GO_VER")
    if [ "$actual_num" -lt "$repo_num" ]; then
      fatal "Go version mismatch: sandbox has go$ACTUAL_GO_VER but go.mod requires go$REPO_GO_VER"
    fi
  fi
}

# JSON report writer
write_json_report() {
  local phase="${1:-setup}"
  local status="healthy"
  if [ ${#FATALS[@]} -gt 0 ]; then
    status="broken"
  elif [ ${#WARNINGS[@]} -gt 0 ]; then
    status="degraded"
  fi

  # Build tools object
  local tools_json="{}"
  local tool_entries=""
  for tool in "${REQUIRED_TOOLS[@]}" "${OPTIONAL_TOOLS[@]}"; do
    if have "$tool"; then
      local ver
      ver=$(timeout 5 "$tool" --version 2>/dev/null | head -1 | grep -oP '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1 || true)
      [ -z "$ver" ] && ver="unknown"
      tool_entries+=$(jq -n \
        --arg name "$tool" --arg ver "$ver" \
        '{($name): {ok:true, version:$ver}}')
    else
      tool_entries+=$(jq -n \
        --arg name "$tool" \
        '{($name): {ok:false, version:""}}')
    fi
  done
  tools_json=$(echo "${tool_entries}" | jq -s 'add')

  jq -n \
    --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg status "$status" \
    --arg phase "$phase" \
    --argjson fatals "$(if [ ${#FATALS[@]} -gt 0 ]; then printf '%s\n' "${FATALS[@]}" | jq -R . | jq -s .; else echo '[]'; fi)" \
    --argjson warnings "$(if [ ${#WARNINGS[@]} -gt 0 ]; then printf '%s\n' "${WARNINGS[@]}" | jq -R . | jq -s .; else echo '[]'; fi)" \
    --argjson tools "$tools_json" \
    '{timestamp:$ts, status:$status, phase:$phase, fatals:$fatals, warnings:$warnings, tools:$tools}' \
    > "$REPORT_FILE"
}

# Human-readable summary and exit code
doctor_exit() {
  local phase="${1:-setup}"
  write_json_report "$phase"

  if [ ${#FATALS[@]} -gt 0 ]; then
    echo ""
    echo "=== BROKEN: ${#FATALS[@]} fatal issue(s) ==="
    for issue in "${FATALS[@]}"; do
      echo "  FATAL: $issue"
    done
    echo "  Report: $REPORT_FILE"
    exit 1
  elif [ ${#WARNINGS[@]} -gt 0 ]; then
    echo ""
    echo "=== DEGRADED: ${#WARNINGS[@]} warning(s) ==="
    echo "  Report: $REPORT_FILE"
  else
    echo ""
    echo "=== $phase complete (healthy) ==="
  fi
}
