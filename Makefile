.PHONY: test test-race lint vet qa fast changed doctor install clean all dupl vuln snipe-index

# Everything: QA + install
all: qa install
	@echo "=== all pass ==="

# Full QA: vet + lint + race tests + dupl + vuln
qa: snipe-index
	go vet ./...
	golangci-lint run ./...
	go test -race -timeout=5m -count=1 -cover ./...
	@if command -v jscpd >/dev/null 2>&1; then jscpd .; else echo "skip: jscpd not installed"; fi
	@if command -v govulncheck >/dev/null 2>&1; then govulncheck ./...; else echo "skip: govulncheck not installed"; fi
	@echo "=== qa pass ==="

# Quick iteration: vet + test (no lint, no race)
fast: vet test
	@echo "=== fast pass ==="

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test:
	go test -count=1 -cover ./...

test-race:
	go test -race -timeout=5m -count=1 -cover ./...

dupl:
	jscpd .

vuln:
	govulncheck ./...

# Validate only modified packages
changed:
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

install:
	go install .

clean:
	rm -f next
	rm -rf .quality/

# Freshen snipe index if stale
snipe-index:
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
doctor:
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
