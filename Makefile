.PHONY: build test install clean

build:
	go build -o next .

install: build
	cp next ~/bin/next

test:
	@mkdir -p .quality
	@sqlite3 .quality/ledger.db < .quality/schema.sql 2>/dev/null || true
	@echo "Testing enqueue..."
	@find . -name '*.go' | head -5 | ./next enqueue --treatment=test
	@echo "\nTesting status..."
	@./next status
	@echo "\nTesting claim..."
	@./next claim --treatment=test --n=2
	@echo "\nTesting done..."
	@./next done --path=./main.go --treatment=test --result=abc123

clean:
	rm -f next
	rm -rf .quality/
