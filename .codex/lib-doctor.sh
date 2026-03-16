#!/usr/bin/env bash
# Shared helpers for sandbox doctor scripts (setup.sh, maintenance.sh).
# Sourced, not executed directly.
# Requires: REPO_DIR, PREBUILT_DIR, INSTALL_DIR set before sourcing.

REPORT_FILE="$REPO_DIR/.codex/setup-report.json"
FATALS=()
WARNINGS=()
REPAIRED_ISSUES=()
REPAIRED_ACTIONS=()
REPAIRED_SUCCESS=()

# Canonical tool lists — single source of truth
REQUIRED_TOOLS=(go next golangci-lint snipe jq rg fd gofumpt goimports govulncheck bat)
OPTIONAL_TOOLS=(jscpd)

have() {
  command -v "$1" >/dev/null 2>&1
}

warn() {
  WARNINGS+=("$1")
}

fatal() {
  FATALS+=("$1")
}

repaired() {
  REPAIRED_ISSUES+=("$1")
  REPAIRED_ACTIONS+=("$2")
  REPAIRED_SUCCESS+=("$3")
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

restore_prebuilt_tools() {
  [ -d "$PREBUILT_DIR" ] || return 0
  for tool in "$PREBUILT_DIR"/*; do
    [ -f "$tool" ] || continue
    local toolname
    toolname=$(basename "$tool")
    if ! have "$toolname"; then
      cp "$tool" "$INSTALL_DIR/$toolname"
      chmod +x "$INSTALL_DIR/$toolname"
      # Validate the binary actually runs
      if "$INSTALL_DIR/$toolname" --version >/dev/null 2>&1 || "$INSTALL_DIR/$toolname" version >/dev/null 2>&1; then
        repaired "missing $toolname" "restored from prebuilt" "true"
        echo "  restored $toolname from prebuilts"
      else
        rm -f "$INSTALL_DIR/$toolname"
        repaired "missing $toolname" "prebuilt restore failed (bad binary)" "false"
        echo "  WARNING: $toolname prebuilt binary failed validation, removed"
      fi
    fi
  done
  return 0
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

# JSON report writer — uses jq to guarantee valid JSON regardless of
# special characters in diagnostic messages.
write_json_report() {
  local phase="${1:-setup}"
  local status="healthy"
  if [ ${#FATALS[@]} -gt 0 ]; then
    status="broken"
  elif [ ${#WARNINGS[@]} -gt 0 ] || [ ${#REPAIRED_ISSUES[@]} -gt 0 ]; then
    status="degraded"
  fi

  # Build repaired array as JSON
  local repaired_json="[]"
  if [ ${#REPAIRED_ISSUES[@]} -gt 0 ]; then
    repaired_json="["
    for i in "${!REPAIRED_ISSUES[@]}"; do
      [ "$i" -gt 0 ] && repaired_json+=","
      repaired_json+=$(jq -n \
        --arg issue "${REPAIRED_ISSUES[$i]}" \
        --arg action "${REPAIRED_ACTIONS[$i]}" \
        --argjson success "${REPAIRED_SUCCESS[$i]}" \
        '{issue:$issue, action:$action, success:$success}')
    done
    repaired_json+="]"
  fi

  # Build tools object — cross-reference REPAIRED_ISSUES to set repaired flag
  local tools_json="{}"
  local tool_entries=""
  for tool in "${REQUIRED_TOOLS[@]}" "${OPTIONAL_TOOLS[@]}"; do
    local was_repaired=false
    for ri in "${REPAIRED_ISSUES[@]}"; do
      if [[ "$ri" == *"$tool"* ]]; then
        was_repaired=true
        break
      fi
    done
    if have "$tool"; then
      local ver
      ver=$(timeout 5 "$tool" --version 2>/dev/null | head -1 | grep -oP '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1 || true)
      [ -z "$ver" ] && ver="unknown"
      tool_entries+=$(jq -n \
        --arg name "$tool" --arg ver "$ver" --argjson rep "$was_repaired" \
        '{($name): {ok:true, version:$ver, repaired:$rep}}')
    else
      tool_entries+=$(jq -n \
        --arg name "$tool" --argjson rep "$was_repaired" \
        '{($name): {ok:false, version:"", repaired:$rep}}')
    fi
  done
  # Merge tool entries
  tools_json=$(echo "${tool_entries}" | jq -s 'add')

  # Assemble final report
  jq -n \
    --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg status "$status" \
    --arg phase "$phase" \
    --argjson fatals "$(if [ ${#FATALS[@]} -gt 0 ]; then printf '%s\n' "${FATALS[@]}" | jq -R . | jq -s .; else echo '[]'; fi)" \
    --argjson warnings "$(if [ ${#WARNINGS[@]} -gt 0 ]; then printf '%s\n' "${WARNINGS[@]}" | jq -R . | jq -s .; else echo '[]'; fi)" \
    --argjson repaired "$repaired_json" \
    --argjson tools "$tools_json" \
    '{timestamp:$ts, status:$status, phase:$phase, fatals:$fatals, warnings:$warnings, repaired:$repaired, tools:$tools}' \
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
    if [ ${#REPAIRED_ISSUES[@]} -gt 0 ]; then
      echo "  (${#REPAIRED_ISSUES[@]} other issue(s) were auto-repaired)"
    fi
    echo "  Report: $REPORT_FILE"
    exit 1
  elif [ ${#WARNINGS[@]} -gt 0 ] || [ ${#REPAIRED_ISSUES[@]} -gt 0 ]; then
    echo ""
    echo "=== DEGRADED: ${#WARNINGS[@]} warning(s), ${#REPAIRED_ISSUES[@]} repaired ==="
    echo "  Report: $REPORT_FILE"
  else
    rm -f "$REPO_DIR/.codex/setup-issues.txt"
    echo ""
    echo "=== $phase complete (healthy) ==="
  fi
}
