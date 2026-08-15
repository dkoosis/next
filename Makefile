.PHONY: help check audit deploy build selfcheck test test-race lint vet qa fast changed doctor install clean all dupl vuln snipe-index cross amd64-sandbox arm64-sandbox

# Strict shell for recipes: fail on first error, undefined var, or pipe failure.
SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z0-9_-]+:.*?## / { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

check: vet lint test build selfcheck ## Full repo: vet + lint + test + build + conform
	@echo "=== check pass ==="

audit: check test-race ## Exhaustive: check + race + dupl + vuln
	@if command -v jscpd >/dev/null 2>&1; then jscpd .; else echo "skip: jscpd not installed"; fi
	@if command -v govulncheck >/dev/null 2>&1; then govulncheck ./...; else echo "skip: govulncheck not installed"; fi
	@echo "=== audit pass ==="

deploy: install ## Build and install the next binary
	@echo "=== deployed ==="

build: ## Compile everything
	go build ./...

# Dogfood the fleet gate (sd-th5.23): conform is pinned as a go.mod tool
# dependency (go.sum-verified); bumping the pin is a deliberate PR.
selfcheck: ## Run conform (fleet SDLC checker) against this repo
	go tool conform

# Everything: QA + install
all: qa install ## QA + install
	@echo "=== all pass ==="

# Full QA: vet + lint + race tests + dupl + vuln
qa: snipe-index ## Full QA: vet + lint + race tests + dupl + vuln
	go vet ./...
	golangci-lint run ./...
	go test -race -timeout=5m -count=1 -cover ./...
	@if command -v jscpd >/dev/null 2>&1; then jscpd .; else echo "skip: jscpd not installed"; fi
	@if command -v govulncheck >/dev/null 2>&1; then govulncheck ./...; else echo "skip: govulncheck not installed"; fi
	@echo "=== qa pass ==="

# Quick iteration: vet + test (no lint, no race)
fast: vet test ## Quick iteration: vet + test (no lint, no race)
	@echo "=== fast pass ==="

vet: ## go vet
	go vet ./...

lint: ## golangci-lint (version pinned in .sandbox/project.conf)
	golangci-lint run ./...

test: ## Unit tests with coverage
	go test -count=1 -cover ./...

test-race: ## Unit tests under the race detector
	go test -race -timeout=5m -count=1 -cover ./...

dupl: ## Duplicate-code scan (jscpd)
	jscpd .

vuln: ## Vulnerability scan (govulncheck)
	govulncheck ./...

# Validate only modified packages
changed: ## Validate only modified packages
	@PKGS=$$( { git diff --name-only HEAD -- '*.go'; git ls-files --others --exclude-standard -- '*.go'; } \
		| xargs dirname 2>/dev/null | sort -u | sed 's|^|./|' | grep -v '^\./$$'); \
	if [ -z "$$PKGS" ]; then \
		echo "no changed Go packages"; \
	else \
		echo "changed packages: $$PKGS"; \
		go vet $$PKGS && \
		golangci-lint run $$PKGS && \
		go test -count=1 -cover $$PKGS && \
		echo "=== changed pass ==="; \
	fi

install: ## go install the next binary
	go install .

clean: ## Remove built binary and runtime data
	rm -f next
	rm -rf .quality/

# ── Sandbox prebuilt versions ──
# golangci-lint version has ONE home: .sandbox/project.conf (conform lint-pin).
GOLANGCI_LINT_VER := $(shell awk -F= '/^GOLANGCI_LINT_VERSION=/{print $$2}' .sandbox/project.conf)
GOVULNCHECK_VER   ?= v1.1.4
GOFUMPT_VER       ?= v0.9.2
GOIMPORTS_VER     ?= v0.39.0
MAGE_VER          ?= v1.15.0
BAT_VER           ?= v0.25.0
SNIPE_SRC         ?= $(HOME)/Projects/snipe
GOMOD_VER         := $(shell awk '/^go /{print $$2}' go.mod)

# ── Sandbox prebuilt cross-compilation ──
# Mutually exclusive: building one arch deletes the other.
# Default (cross) builds amd64 — the Codex sandbox architecture.
cross: amd64-sandbox ## Cross-compile sandbox prebuilts (default: linux/amd64)

amd64-sandbox: ## Build linux/amd64 sandbox prebuilts
	@echo "=== sandbox: linux/amd64 ==="
	@rm -rf .bin/linux-arm64
	@$(MAKE) --no-print-directory _sandbox-build SANDBOX_ARCH=amd64

arm64-sandbox: ## Build linux/arm64 sandbox prebuilts
	@echo "=== sandbox: linux/arm64 ==="
	@rm -rf .bin/linux-amd64
	@$(MAKE) --no-print-directory _sandbox-build SANDBOX_ARCH=arm64

_sandbox-build: ## Internal: cross-compile toolchain for the Codex sandbox
	@# Pre-flight: local Go must be >= go.mod target
	@LOCAL_GO=$$(go version | sed 's/.*go\([0-9]*\.[0-9]*\).*/\1/'); \
	MOD_MIN=$$(echo $(GOMOD_VER) | cut -d. -f1)$$(printf '%03d' $$(echo $(GOMOD_VER) | cut -d. -f2)); \
	LOC_MIN=$$(echo $$LOCAL_GO | cut -d. -f1)$$(printf '%03d' $$(echo $$LOCAL_GO | cut -d. -f2)); \
	if [ "$$LOC_MIN" -lt "$$MOD_MIN" ]; then \
		echo "FATAL: local go$$LOCAL_GO < go.mod go$(GOMOD_VER) — prebuilts would cause sandbox lint failures"; \
		echo "  Install Go >= $(GOMOD_VER) before running make cross"; \
		exit 1; \
	fi; \
	echo "  local go$$LOCAL_GO >= go.mod go$(GOMOD_VER) — ok"
	@mkdir -p .bin/linux-$(SANDBOX_ARCH)
	$(eval XBIN := $(shell go env GOPATH)/bin/linux_$(SANDBOX_ARCH))
	@echo "-- next"
	@CGO_ENABLED=0 GOOS=linux GOARCH=$(SANDBOX_ARCH) go build -trimpath -ldflags='-s -w' -o .bin/linux-$(SANDBOX_ARCH)/next .
	@echo "-- golangci-lint $(GOLANGCI_LINT_VER)"
	@CGO_ENABLED=0 GOOS=linux GOARCH=$(SANDBOX_ARCH) go install -trimpath -ldflags='-s -w' github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VER)
	@cp $(XBIN)/golangci-lint .bin/linux-$(SANDBOX_ARCH)/
	@echo "-- govulncheck $(GOVULNCHECK_VER)"
	@CGO_ENABLED=0 GOOS=linux GOARCH=$(SANDBOX_ARCH) go install -trimpath -ldflags='-s -w' golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VER)
	@cp $(XBIN)/govulncheck .bin/linux-$(SANDBOX_ARCH)/
	@echo "-- gofumpt $(GOFUMPT_VER)"
	@CGO_ENABLED=0 GOOS=linux GOARCH=$(SANDBOX_ARCH) go install -trimpath -ldflags='-s -w' mvdan.cc/gofumpt@$(GOFUMPT_VER)
	@cp $(XBIN)/gofumpt .bin/linux-$(SANDBOX_ARCH)/
	@echo "-- goimports $(GOIMPORTS_VER)"
	@CGO_ENABLED=0 GOOS=linux GOARCH=$(SANDBOX_ARCH) go install -trimpath -ldflags='-s -w' golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VER)
	@cp $(XBIN)/goimports .bin/linux-$(SANDBOX_ARCH)/
	@echo "-- snipe"
	@if [ -d "$(SNIPE_SRC)" ]; then \
		echo "  (from $(SNIPE_SRC))"; \
		cd "$(SNIPE_SRC)" && CGO_ENABLED=0 GOOS=linux GOARCH=$(SANDBOX_ARCH) \
			go build -trimpath -ldflags='-s -w' -o "$(CURDIR)/.bin/linux-$(SANDBOX_ARCH)/snipe" .; \
	else \
		CGO_ENABLED=0 GOOS=linux GOARCH=$(SANDBOX_ARCH) go install -trimpath -ldflags='-s -w' github.com/dkoosis/snipe@latest && \
			cp $(XBIN)/snipe .bin/linux-$(SANDBOX_ARCH)/; \
	fi
	@echo "-- bat $(BAT_VER)"
	@if [ -f ".bin/linux-$(SANDBOX_ARCH)/bat" ]; then \
		echo "  (exists, skipping download)"; \
	else \
		case "$(SANDBOX_ARCH)" in \
			amd64) BAT_TRIPLE="x86_64-unknown-linux-musl" ;; \
			arm64) BAT_TRIPLE="aarch64-unknown-linux-gnu" ;; \
		esac; \
		TMP=$$(mktemp -d); \
		echo "  downloading bat-$(BAT_VER)-$$BAT_TRIPLE"; \
		curl -fsSL "https://github.com/sharkdp/bat/releases/download/$(BAT_VER)/bat-$(BAT_VER)-$$BAT_TRIPLE.tar.gz" \
			| tar xz -C "$$TMP" && \
		cp "$$TMP"/bat-*/bat .bin/linux-$(SANDBOX_ARCH)/bat && \
		rm -rf "$$TMP"; \
	fi
	@# UPX compress all ELF binaries (skip scripts)
	@if command -v upx >/dev/null 2>&1; then \
		echo "-- upx compressing"; \
		for f in .bin/linux-$(SANDBOX_ARCH)/*; do \
			[ -f "$$f" ] || continue; \
			case "$$f" in *.upx) continue ;; esac; \
			file "$$f" | grep -q ELF && { \
				BEFORE=$$(du -h "$$f" | cut -f1); \
				upx -q --best --no-backup "$$f" >/dev/null 2>&1 && \
				AFTER=$$(du -h "$$f" | cut -f1); \
				echo "  $$(basename $$f): $$BEFORE -> $$AFTER"; \
			} || true; \
		done; \
		rm -f .bin/linux-$(SANDBOX_ARCH)/*.upx; \
	else \
		echo "-- upx not found, skipping (brew install upx)"; \
	fi
	@echo "-- result:"
	@du -sh .bin/linux-$(SANDBOX_ARCH)/
	@du -h .bin/linux-$(SANDBOX_ARCH)/* | sort -rh

# Freshen snipe index if stale
snipe-index: ## Freshen snipe index if stale
	@if command -v snipe >/dev/null 2>&1; then \
		state=$$(snipe status 2>/dev/null | jq -r '.results[0].state // "unknown"'); \
		if [ "$$state" != "fresh" ]; then \
			echo "snipe index stale ($$state), rebuilding..."; \
			snipe index --embed-mode=off --enrich=false; \
		else \
			echo "snipe index fresh"; \
		fi; \
	fi

# Validate toolchain
doctor: ## Validate toolchain
	@echo "=== doctor ==="
	@MISSING=0; \
	for tool in go golangci-lint snipe jq; do \
		if command -v "$$tool" >/dev/null 2>&1; then \
			printf "  ok  %-20s %s\n" "$$tool" "$$(command -v $$tool)"; \
		else \
			printf "  MISSING  %s\n" "$$tool"; \
			MISSING=$$((MISSING + 1)); \
		fi; \
	done; \
	for tool in govulncheck jscpd; do \
		if command -v "$$tool" >/dev/null 2>&1; then \
			printf "  ok  %-20s %s (optional)\n" "$$tool" "$$(command -v $$tool)"; \
		else \
			printf "  skip  %-20s (optional)\n" "$$tool"; \
		fi; \
	done; \
	echo ""; \
	echo "  go: $$(go version 2>/dev/null | cut -d' ' -f3)"; \
	if [ "$$MISSING" -gt 0 ]; then \
		echo ""; \
		echo "$$MISSING required tool(s) missing"; \
		exit 1; \
	fi; \
	echo "=== doctor pass ==="
