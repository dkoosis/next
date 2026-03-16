# Atomic Claim with Leasing (Issue #9)

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `claim` atomic so concurrent workers get disjoint items, preventing duplicate processing.

**Architecture:** Add `claimed_at` and `claimed_by` columns to the queue table. Replace the SELECT-only claim with an `UPDATE ... RETURNING` that atomically leases rows. Add a `--worker` flag to identify the claimer, and a `--lease` duration for automatic expiry of stale claims. SQLite's write lock guarantees that concurrent `UPDATE...RETURNING` statements are serialized — only one writer proceeds at a time, ensuring disjoint claim sets.

**Tech Stack:** Go, SQLite (RETURNING requires SQLite 3.35+, which ncruces/go-sqlite3 provides)

**Implementation Order:** This is plan 4 of 4 — implement last. Depends on #10 (done silent failure) for the `markDone()` extraction. **This plan subsumes #8 (revisit scheduling)** — the rewritten `buildClaimQuery` already includes the revisit predicate, so #8 can be skipped if #9 is planned. If #10 is implemented first, Chunk 4 of this plan should target `markDone()` instead of `doneCmd()` (noted inline). If #8 was already implemented, its `buildClaimQuery` changes are overwritten here — no conflict, just redundant prior work.

**Note on scope:** This plan changes `claim`'s definition of "eligible" to include revisit items. The `status` command still counts `done_at IS NULL` as pending. If you want `status` to reflect claimable-including-revisits, that's a separate follow-up — it doesn't affect correctness of claiming.

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `schema.sql` | Add `claimed_at`, `claimed_by` columns |
| Modify | `main.go:132-154` | `openDB()` — add ALTER TABLE migration for existing DBs |
| Modify | `main.go:271-347` | Rewrite `claimCmd()` and `buildClaimQuery()` to use atomic UPDATE...RETURNING |
| Modify | `main.go:367-410` | Update `doneCmd()` to clear claim columns |
| Modify | `main_test.go` | Add concurrency test, fix existing sharding test |

---

## Chunk 1: Schema migration

### Task 1: Add lease columns to schema

**Files:**
- Modify: `schema.sql`
- Modify: `main.go:149-154` (`openDB()` — add migration before `return db, nil`)

- [ ] **Step 1: Write test that new columns exist after openDB**

Add to `main_test.go`:

```go
func TestOpenDB_HasClaimColumns_When_SchemaApplied(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Verify claimed_at and claimed_by columns exist.
	row := db.QueryRowContext(context.Background(),
		"SELECT sql FROM sqlite_master WHERE type='table' AND name='queue'")
	var ddl string
	if err := row.Scan(&ddl); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !strings.Contains(ddl, "claimed_at") {
		t.Fatalf("schema missing claimed_at column: %s", ddl)
	}
	if !strings.Contains(ddl, "claimed_by") {
		t.Fatalf("schema missing claimed_by column: %s", ddl)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestOpenDB_HasClaimColumns -v`
Expected: FAIL — columns don't exist yet.

- [ ] **Step 3: Add columns to schema.sql**

Replace `schema.sql` contents:

```sql
CREATE TABLE IF NOT EXISTS queue (
  path          TEXT NOT NULL,
  path_hash     TEXT NOT NULL,
  content_hash  TEXT NOT NULL,
  treatment     TEXT NOT NULL,
  done_at       TEXT,
  result        TEXT,
  next_at       TEXT,
  claimed_at    TEXT,
  claimed_by    TEXT,
  PRIMARY KEY (path_hash, treatment)
);
CREATE INDEX IF NOT EXISTS idx_queue_treatment_done ON queue(treatment, done_at);
CREATE INDEX IF NOT EXISTS idx_queue_next_at ON queue(next_at);
CREATE INDEX IF NOT EXISTS idx_queue_claimed_at ON queue(claimed_at);
```

- [ ] **Step 4: Add ALTER TABLE migration for existing databases**

In `openDB()` in `main.go`, add the migration **before** the `return db, nil` on line 153 (between the schema exec error check and the return):

```go
	// Migrate: add claim columns if missing (idempotent).
	// ALTER TABLE ADD COLUMN is a no-op error if column already exists.
	_, _ = db.ExecContext(ctx, `ALTER TABLE queue ADD COLUMN claimed_at TEXT`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE queue ADD COLUMN claimed_by TEXT`)

	return db, nil
```

This replaces the existing bare `return db, nil` on line 153.

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -run TestOpenDB_HasClaimColumns -v`
Expected: PASS.

- [ ] **Step 6: Run full test suite**

Run: `go test -v ./...`
Expected: All pass (existing tests don't touch new columns).

- [ ] **Step 7: Commit**

```bash
git add schema.sql main.go main_test.go
git commit -m "schema: add claimed_at, claimed_by columns for atomic claim (#9)"
```

---

## Chunk 2: Atomic claim implementation

### Task 2: Rewrite claim to use UPDATE...RETURNING

**Files:**
- Modify: `main.go:271-347`

- [ ] **Step 8: Write failing test for atomic claim**

```go
func TestClaimCmd_SetsClaimedAt_When_ItemsClaimed(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	targetPath := filepath.Join(tmpDir, "item.txt")
	if err := os.WriteFile(targetPath, []byte("item"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := db.Exec(testInsertSQL,
		targetPath, pathHash(targetPath), "hash-item", "lint"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint",
		"--n", "1", "--worker", "test-worker"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() {
		claimCmd()
	})

	if strings.TrimSpace(output) != targetPath {
		t.Fatalf("expected %s, got %q", targetPath, output)
	}

	// Verify claimed_at and claimed_by are set.
	db2, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB verify: %v", err)
	}
	defer func() { _ = db2.Close() }()

	var claimedAt, claimedBy sql.NullString
	err = db2.QueryRowContext(context.Background(),
		"SELECT claimed_at, claimed_by FROM queue WHERE path = ?", targetPath).
		Scan(&claimedAt, &claimedBy)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !claimedAt.Valid {
		t.Fatal("claimed_at not set after claim")
	}
	if !claimedBy.Valid || claimedBy.String != "test-worker" {
		t.Fatalf("claimed_by = %v, want test-worker", claimedBy)
	}
}
```

- [ ] **Step 9: Run test to verify it fails**

Run: `go test -run TestClaimCmd_SetsClaimedAt -v`
Expected: FAIL — `--worker` flag doesn't exist, claim doesn't UPDATE.

- [ ] **Step 10: Add `--worker` and `--lease` flags to claimCmd with validation**

In `claimCmd()`, add after the existing flag definitions (after line 280):

```go
	worker := fs.String("worker", "", "worker identifier for lease tracking (e.g. 'builder-01')")
	lease := fs.String("lease", "5 minutes", "lease duration (SQLite modifier, e.g. '5 minutes')")
```

Add worker default and lease validation after `validateShardFlags` call (after line 283):

```go
	// Default --worker to hostname:pid for debuggability.
	if *worker == "" {
		host, _ := os.Hostname()
		*worker = fmt.Sprintf("%s:%d", host, os.Getpid())
	}

	// Validate --lease is a reasonable SQLite time modifier.
	// Must match pattern like "5 minutes", "1 hours", "7 days".
	if *lease != "" {
		matched := false
		for _, suffix := range []string{"seconds", "minutes", "hours", "days"} {
			// Check for "<digits> <unit>" pattern.
			prefix := strings.TrimSuffix(*lease, " "+suffix)
			if prefix != *lease {
				if _, err := strconv.Atoi(prefix); err == nil {
					matched = true
					break
				}
			}
		}
		if !matched {
			fatal("error: --lease must be like '5 minutes', '1 hours', '7 days'")
		}
	}
```

- [ ] **Step 11: Rewrite buildClaimQuery to return an UPDATE...RETURNING**

Replace `buildClaimQuery` entirely:

```go
func buildClaimQuery(treatment, cursor, worker, lease string, n, shard, totalShards int) (string, []any) {
	// Inner SELECT finds eligible rows.
	inner := `
		SELECT rowid FROM queue
		 WHERE treatment = ?
		   AND (done_at IS NULL OR (next_at IS NOT NULL AND next_at <= DATETIME('now')))
		   AND (claimed_at IS NULL OR claimed_at <= DATETIME('now', '-' || ?))
		   AND path_hash > ?`
	args := []any{treatment, lease, cursor}

	if shard >= 0 {
		shardStart, shardEnd := calculateShardRange(shard, totalShards)
		inner += `
		   AND path_hash >= ?
		   AND path_hash < ?`
		args = append(args, shardStart, shardEnd)
	}

	inner += `
		 ORDER BY path_hash
		 LIMIT ?`
	args = append(args, n)

	// Outer UPDATE atomically claims those rows.
	query := fmt.Sprintf(`
		UPDATE queue
		   SET claimed_at = DATETIME('now'), claimed_by = ?
		 WHERE rowid IN (%s)
		 RETURNING path, path_hash`, inner)
	args = append([]any{worker}, args...)

	return query, args
}
```

- [ ] **Step 12: Update claimCmd to pass new args to buildClaimQuery**

Replace the `buildClaimQuery` call and the subsequent query logic in `claimCmd()`:

```go
	query, args := buildClaimQuery(*treatment, *cursor, *worker, *lease, *n, *shard, *totalShards)

	rows, err := db.QueryContext(context.Background(), query, args...)
```

The rest of `claimCmd()` (scanning rows, writing results) stays the same.

- [ ] **Step 13: Run the new test**

Run: `go test -run TestClaimCmd_SetsClaimedAt -v`
Expected: PASS.

- [ ] **Step 14: Fix the existing sharding test**

The existing `TestClaimCmd_ReturnsShardedItems_When_ShardSpecified` (line 551) runs `claimCmd()` twice per shard — once to collect results, once to verify no overlap. After this change, the first pass atomically claims items (sets `claimed_at`), so the second pass returns nothing (items are now claimed and lease hasn't expired).

Fix: remove the second verification loop (lines 617-639) since the first loop already proves disjoint results by checking `allResults` for duplicates. Replace the test's second loop with this assertion after the first loop:

```go
	// The first pass already collected all results. Verify uniqueness
	// was enforced — if any file appeared in two shards, allResults
	// would have fewer entries than numFiles.
	if len(allResults) != numFiles {
		t.Fatalf("got %d unique results across all shards, want %d (some files appeared in multiple shards)", len(allResults), numFiles)
	}
```

Also note: the first loop calls `claimCmd()` sequentially per shard (not concurrently), so each shard's UPDATE completes before the next starts. Items claimed by shard 0 won't be returned to shard 1 because `claimed_at` is now set.

- [ ] **Step 15: Run full test suite**

Run: `go test -v ./...`
Expected: All pass.

- [ ] **Step 16: Commit**

```bash
git add main.go main_test.go
git commit -m "feat: atomic claim with UPDATE...RETURNING and lease tracking (#9)"
```

---

## Chunk 3: Disjointness tests

### Task 3: Verify sequential claims produce disjoint results

SQLite's write lock serializes `UPDATE` statements — concurrent workers block on the lock and execute one at a time. The test verifies the end result (disjoint sets) via sequential calls, since SQLite's locking model guarantees serialization. True concurrency testing would require separate OS processes (the actual deployment model), which is better suited for integration tests outside this unit test file.

**Files:**
- Modify: `main_test.go`

- [ ] **Step 17: Write disjointness test (sequential claims)**

Since SQLite serializes writes via its write lock, the important property to test is that a claimed item is not returned to a subsequent caller. This test proves the atomic UPDATE works correctly:

```go
func TestClaimCmd_ReturnsDisjointSets_When_CalledSequentially(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	// Insert 20 items.
	for i := range 20 {
		p := filepath.Join(tmpDir, fmt.Sprintf("conc-%03d.txt", i))
		if err := os.WriteFile(p, fmt.Appendf(nil, "c-%d", i), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := db.Exec(testInsertSQL, p, pathHash(p), fmt.Sprintf("ch-%d", i), "lint"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 4 sequential claims of 5 each — should get all 20 with no overlap.
	allPaths := make(map[string]int)
	for w := range 4 {
		oldArgs := os.Args
		os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint",
			"--n", "5", "--worker", fmt.Sprintf("worker-%d", w)}

		output := captureStdout(t, func() {
			claimCmd()
		})
		os.Args = oldArgs

		for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" {
				continue
			}
			if prev, exists := allPaths[trimmed]; exists {
				t.Errorf("path %s claimed by both worker %d and worker %d", trimmed, prev, w)
			}
			allPaths[trimmed] = w
		}
	}

	if len(allPaths) != 20 {
		t.Fatalf("expected 20 unique claims, got %d", len(allPaths))
	}
}
```

- [ ] **Step 18: Write test that claimed items are skipped**

```go
func TestClaimCmd_SkipsClaimedItems_When_LeaseActive(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	p := filepath.Join(tmpDir, "single.txt")
	if err := os.WriteFile(p, []byte("single"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := db.Exec(testInsertSQL, p, pathHash(p), "ch-single", "lint"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// First claim should return the item.
	oldArgs := os.Args
	os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint",
		"--n", "1", "--worker", "w1"}
	out1 := captureStdout(t, func() { claimCmd() })
	os.Args = oldArgs

	if strings.TrimSpace(out1) != p {
		t.Fatalf("first claim: expected %s, got %q", p, out1)
	}

	// Second claim should return nothing (item is claimed, lease active).
	os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint",
		"--n", "1", "--worker", "w2"}
	out2 := captureStdout(t, func() { claimCmd() })
	os.Args = oldArgs

	if strings.TrimSpace(out2) != "" {
		t.Fatalf("second claim: expected empty, got %q", out2)
	}
}
```

- [ ] **Step 19: Run all tests**

Run: `go test -v ./...`
Expected: All pass.

- [ ] **Step 20: Run lint**

Run: `golangci-lint run ./...`
Expected: Clean.

- [ ] **Step 21: Commit**

```bash
git add main_test.go
git commit -m "test: atomic claim disjointness and lease skip (#9)"
```

---

## Chunk 4: Update done to clear claim

### Task 4: Clear claim columns when marking done

**Files:**
- Modify: `main.go:367-410`

- [ ] **Step 22: Update doneCmd UPDATE statements to clear claimed_at/claimed_by**

In the `doneCmd()` function, update both UPDATE statements to also clear claim columns.

The `if *revisit == ""` branch becomes:

```go
		res, err = db.ExecContext(ctx, `
			UPDATE queue
			   SET done_at = ?, result = ?, next_at = NULL,
			       claimed_at = NULL, claimed_by = NULL
			 WHERE path_hash = ? AND treatment = ?
		`, now, *result, ph, *treatment)
```

The `else` branch becomes:

```go
		res, err = db.ExecContext(ctx, `
			UPDATE queue
			   SET done_at = ?, result = ?, next_at = DATETIME('now', ?),
			       claimed_at = NULL, claimed_by = NULL
			 WHERE path_hash = ? AND treatment = ?
		`, now, *result, *revisit, ph, *treatment)
```

**Note:** If issue #10 (done silent failure) is implemented first, these UPDATE statements will be inside `markDone()` instead of `doneCmd()`. Apply the same `claimed_at = NULL, claimed_by = NULL` additions to both branches of `markDone()`.

- [ ] **Step 23: Run full test suite**

Run: `go test -v ./...`
Expected: All pass.

- [ ] **Step 24: Commit**

```bash
git add main.go
git commit -m "fix: done clears claimed_at/claimed_by columns (#9)"
```
