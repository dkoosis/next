# Fix done Silent Failure (Issue #10)

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `done` exit non-zero when the target row doesn't exist, instead of silently succeeding.

**Architecture:** `doneCmd()` calls `db.ExecContext()` but discards the result. We extract the core logic into a testable `markDone()` function that returns an error instead of calling `fatal()`. `doneCmd()` becomes a thin wrapper. This lets us write a proper red/green test.

**Tech Stack:** Go, SQLite

**Implementation Order:** This is plan 1 of 4 — implement first. No dependencies. Plans #8 and #9 both modify `doneCmd()`; extracting `markDone()` here gives them a clean function to target. Recommended sequence: **#10 → #12 → #9** (skipping #8, since #9 subsumes it), or **#10 → #8 → #12 → #9** if you want incremental revisit support before atomic claim.

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `main.go:367-410` | Extract `markDone()` from `doneCmd()`, add RowsAffected check |
| Modify | `main_test.go` | Add test for missing path (proper red/green cycle) |

---

## Chunk 1: Extract testable function and add RowsAffected check

### Task 1: Write the failing test

**Files:**
- Modify: `main_test.go` (append)

- [ ] **Step 1: Write failing test for done with nonexistent path**

```go
func TestMarkDone_ReturnsError_When_PathNotInQueue(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Don't enqueue anything — markDone should fail.
	ph := pathHash("/nonexistent/path")
	err = markDone(db, ph, "lint", "some-result", "")
	if err == nil {
		t.Fatal("expected error for missing queue entry, got nil")
	}
	if !strings.Contains(err.Error(), "no matching queue entry") {
		t.Fatalf("unexpected error: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestMarkDone_ReturnsError -v`
Expected: FAIL — `markDone` is undefined.

### Task 2: Extract markDone and add RowsAffected check

**Files:**
- Modify: `main.go:367-410`

- [ ] **Step 3: Extract markDone function and rewrite doneCmd**

Replace lines 391-410 of `main.go` (from `ctx := context.Background()` through the end of `doneCmd`) with:

```go
	if err := markDone(db, ph, *treatment, *result, *revisit); err != nil {
		fatal("%v", err)
	}
}

// markDone marks a queue entry as complete. Returns an error if no matching row exists.
func markDone(db *sql.DB, pathHash, treatment, result, revisit string) error {
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)

	var res sql.Result
	var err error
	if revisit == "" {
		res, err = db.ExecContext(ctx, `
			UPDATE queue
			   SET done_at = ?, result = ?, next_at = NULL
			 WHERE path_hash = ? AND treatment = ?
		`, now, result, pathHash, treatment)
	} else {
		res, err = db.ExecContext(ctx, `
			UPDATE queue
			   SET done_at = ?, result = ?, next_at = DATETIME('now', ?)
			 WHERE path_hash = ? AND treatment = ?
		`, now, result, revisit, pathHash, treatment)
	}
	if err != nil {
		return fmt.Errorf("update error: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("no matching queue entry for path_hash=%s treatment=%s", pathHash, treatment)
	}
	return nil
}
```

This replaces the original lines 391-410 (from `ctx := context.Background()` through the closing brace of `doneCmd()`).

- [ ] **Step 4: Run the failing test — it should now pass**

Run: `go test -run TestMarkDone_ReturnsError -v`
Expected: PASS

- [ ] **Step 5: Write test that markDone succeeds for valid path**

```go
func TestMarkDone_Succeeds_When_PathInQueue(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	targetPath := filepath.Join(tmpDir, "valid.txt")
	if err := os.WriteFile(targetPath, []byte("valid"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	absTarget, _ := filepath.Abs(targetPath)
	ph := pathHash(absTarget)

	if _, err := db.Exec(testInsertSQL, absTarget, ph, "hash-valid", "lint"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := markDone(db, ph, "lint", "result-123", ""); err != nil {
		t.Fatalf("markDone: %v", err)
	}

	// Verify done_at was set.
	var doneAt sql.NullString
	if err := db.QueryRow("SELECT done_at FROM queue WHERE path_hash = ?", ph).Scan(&doneAt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !doneAt.Valid {
		t.Fatal("done_at not set")
	}
}
```

- [ ] **Step 6: Run full test suite**

Run: `go test -v ./...`
Expected: All tests pass, including existing `TestDoneCmd_MarksEntryDone`.

- [ ] **Step 7: Run lint**

Run: `golangci-lint run ./...`
Expected: Clean.

- [ ] **Step 8: Commit**

```bash
git add main.go main_test.go
git commit -m "fix: done exits non-zero when path not in queue (#10)

Extract markDone() from doneCmd() for testability. Check RowsAffected
after UPDATE — return error if 0 rows matched."
```
