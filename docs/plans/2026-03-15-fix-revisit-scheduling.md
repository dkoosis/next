# Fix Revisit Scheduling (Issue #8)

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `claim` return items whose `next_at` has elapsed, so the `--revisit` flag actually works.

**Architecture:** The `done --revisit` command correctly sets `next_at`, but `buildClaimQuery()` only selects `done_at IS NULL`. We add an OR clause to also select rows where `next_at <= now`. When reclaiming a revisit item, we clear `done_at` and `next_at` so it behaves like a fresh pending item.

**Tech Stack:** Go, SQLite

**Implementation Order:** This is plan 2 of 4. Implement after #10 (done silent failure). Note: Plan #9 (atomic claim) **subsumes this plan's query changes** — its rewritten `buildClaimQuery` already includes the revisit predicate. If you plan to implement #9, you can skip this plan entirely and go #10 → #12 → #9. If implementing this plan standalone, be aware of the race condition documented below.

**Known Limitation (race condition):** Between the SELECT in `buildClaimQuery` and the post-claim UPDATE that clears `done_at/next_at` (Step 5a), another worker can claim the same revisit item. This is inherent to the SELECT-then-UPDATE two-step approach and is fully resolved by #9 (atomic claim with `UPDATE...RETURNING`).

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `main.go:324-347` | `buildClaimQuery()` — add revisit predicate |
| Modify | `main.go:271-313` | `claimCmd()` — clear done_at/next_at on reclaimed revisit items |
| Modify | `main_test.go` | Add revisit integration test |

---

## Chunk 1: Fix claim query and add test

### Task 1: Write the failing test

**Files:**
- Modify: `main_test.go` (append after line 640)

- [ ] **Step 1: Write failing test for revisit claim**

Add this test at the end of `main_test.go`:

```go
func TestClaimCmd_ReturnsRevisitItems_When_NextAtElapsed(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	targetPath := filepath.Join(tmpDir, "revisit.txt")
	if err := os.WriteFile(targetPath, []byte("revisit"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	absTarget, _ := filepath.Abs(targetPath)
	ph := pathHash(absTarget)

	// Insert a done item with next_at in the past.
	// Use SQLite datetime format — doneCmd uses DATETIME('now', ?) which produces this format.
	pastTime := time.Now().Add(-1 * time.Hour).UTC().Format("2006-01-02 15:04:05")
	if _, err := db.Exec(`
		INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, absTarget, ph, "hash-revisit", "lint", pastTime, "old-result", pastTime); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint", "--n", "10"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() {
		claimCmd()
	})

	trimmed := strings.TrimSpace(output)
	if trimmed != absTarget {
		t.Fatalf("expected %s, got %q", absTarget, trimmed)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestClaimCmd_ReturnsRevisitItems -v`
Expected: FAIL — the item has `done_at` set so current query skips it.

### Task 2: Fix the claim query

**Files:**
- Modify: `main.go:328-330`

- [ ] **Step 3: Update the WHERE clause in `buildClaimQuery()`**

In `main.go`, replace the current predicate (lines 328-330):

```go
	query := `
		SELECT path, path_hash
		  FROM queue
		 WHERE treatment = ?
		   AND done_at IS NULL
		   AND path_hash > ?`
```

With:

```go
	query := `
		SELECT path, path_hash
		  FROM queue
		 WHERE treatment = ?
		   AND (done_at IS NULL OR (next_at IS NOT NULL AND next_at <= DATETIME('now')))
		   AND path_hash > ?`
```

- [ ] **Step 4: Run the new test to verify it passes**

Run: `go test -run TestClaimCmd_ReturnsRevisitItems -v`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `go test -v ./...`
Expected: All tests pass.

### Task 2b: Clear done_at/next_at on reclaimed revisit items

Without this, the same revisit item would be returned on every `claim` call.

**Files:**
- Modify: `main.go:271-313` (`claimCmd()`)

- [ ] **Step 5a: Add clearing UPDATE after scanning results in claimCmd**

In `claimCmd()`, after the `for rows.Next()` loop and `rows.Err()` check (after line 310), add:

```go
	// Clear done_at/next_at on reclaimed revisit items so they aren't
	// returned again on the next claim call.
	// NOTE: This is a best-effort step — there is a race window between
	// the SELECT above and this UPDATE where another worker could also
	// claim the same item. Plan #9 (atomic claim) eliminates this race.
	for _, r := range results {
		if _, err := db.ExecContext(context.Background(), `
			UPDATE queue SET done_at = NULL, next_at = NULL
			 WHERE path_hash = ? AND treatment = ?
			   AND next_at IS NOT NULL AND next_at <= DATETIME('now')
		`, r.PathHash, *treatment); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to clear revisit state for %s: %v\n", r.PathHash, err)
		}
	}
```

- [ ] **Step 5b: Write test that a claimed revisit item is NOT returned again**

Add to `main_test.go`:

```go
func TestClaimCmd_DoesNotReturnRevisitTwice_When_AlreadyClaimed(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	targetPath := filepath.Join(tmpDir, "once.txt")
	if err := os.WriteFile(targetPath, []byte("once"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	absTarget, _ := filepath.Abs(targetPath)
	ph := pathHash(absTarget)

	pastTime := time.Now().Add(-1 * time.Hour).UTC().Format("2006-01-02 15:04:05")
	if _, err := db.Exec(`
		INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, absTarget, ph, "hash-once", "lint", pastTime, "old", pastTime); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// First claim should return the item.
	oldArgs := os.Args
	os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint", "--n", "10"}
	defer func() { os.Args = oldArgs }()

	out1 := captureStdout(t, func() { claimCmd() })
	if strings.TrimSpace(out1) != absTarget {
		t.Fatalf("first claim: expected %s, got %q", absTarget, out1)
	}

	// Second claim should return nothing.
	out2 := captureStdout(t, func() { claimCmd() })
	if strings.TrimSpace(out2) != "" {
		t.Fatalf("second claim: expected empty, got %q", out2)
	}
}
```

- [ ] **Step 5c: Run all revisit tests**

Run: `go test -run "TestClaimCmd.*(Revisit|Once)" -v`
Expected: All PASS.

### Task 3: Add test that future revisits are NOT claimed

**Files:**
- Modify: `main_test.go` (append)

- [ ] **Step 6: Write test for future revisit NOT being claimed**

```go
func TestClaimCmd_SkipsRevisitItems_When_NextAtInFuture(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	targetPath := filepath.Join(tmpDir, "future.txt")
	if err := os.WriteFile(targetPath, []byte("future"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	absTarget, _ := filepath.Abs(targetPath)
	ph := pathHash(absTarget)

	// Insert a done item with next_at in the future.
	// Use SQLite datetime format to match what DATETIME('now', ?) produces.
	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	futureTime := time.Now().Add(24 * time.Hour).UTC().Format("2006-01-02 15:04:05")
	if _, err := db.Exec(`
		INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, absTarget, ph, "hash-future", "lint", now, "result", futureTime); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint", "--n", "10"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() {
		claimCmd()
	})

	if strings.TrimSpace(output) != "" {
		t.Fatalf("expected no output for future revisit, got %q", output)
	}
}
```

- [ ] **Step 7: Run both revisit tests**

Run: `go test -run TestClaimCmd.*Revisit -v`
Expected: Both PASS.

- [ ] **Step 8: Run lint**

Run: `golangci-lint run ./...`
Expected: Clean.

- [ ] **Step 9: Commit**

```bash
git add main.go main_test.go
git commit -m "fix: claim query includes due revisit items (#8)

buildClaimQuery() now selects rows where next_at <= now, so
done --revisit items actually resurface for processing."
```
