# Enqueue Directory Support (Issue #12)

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow `next enqueue` to accept directory paths, computing a content hash from the directory's file listing.

**Architecture:** When `enqueueFromStdin` encounters a directory, compute a hash from the sorted list of filenames + their individual content hashes. This makes the directory hash change when any file is added, removed, or modified. No new flags needed — directory detection is implicit via `os.Stat`.

**Tech Stack:** Go, SHA256

**Implementation Order:** This is plan 3 of 4. No hard dependencies, but implement after #10 for a clean codebase. Can be done before or after #8/#9.

**Design Decision — Shallow Hashing:** `dirHash` hashes only the immediate regular files in a directory (via `os.ReadDir`). Subdirectories and symlinks are silently skipped. This is intentional for Go package-level granularity (packages are flat by convention). If recursive directory hashing is needed in the future, `dirHash` can be extended to walk subdirectories — but that's a separate design choice with performance implications.

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `main.go:111-126` | Add `dirHash()` function next to `fileHash()` |
| Modify | `main.go:212-259` | Update `enqueueFromStdin()` to handle directories |
| Modify | `main_test.go` | Add directory enqueue tests |

---

## Chunk 1: Directory hashing

### Task 1: Write dirHash function with tests

**Files:**
- Create: function `dirHash()` in `main.go` (after `fileHash`, ~line 127)
- Modify: `main_test.go`

- [ ] **Step 1: Write failing test for dirHash**

```go
func TestDirHash_ReturnsStableHash_When_DirectoryExists(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	// Create files in deterministic order.
	for _, name := range []string{"a.go", "b.go", "c.txt"} {
		if err := os.WriteFile(filepath.Join(tmpDir, name), []byte("content-"+name), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	h1, err := dirHash(tmpDir)
	if err != nil {
		t.Fatalf("dirHash: %v", err)
	}
	if len(h1) != 64 {
		t.Fatalf("hash length = %d, want 64", len(h1))
	}

	// Same directory should produce same hash.
	h2, err := dirHash(tmpDir)
	if err != nil {
		t.Fatalf("dirHash second call: %v", err)
	}
	if h1 != h2 {
		t.Fatalf("dirHash not stable: %s != %s", h1, h2)
	}
}

func TestDirHash_ChangesHash_When_FileAdded(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "a.go"), []byte("a"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	h1, err := dirHash(tmpDir)
	if err != nil {
		t.Fatalf("dirHash before: %v", err)
	}

	// Add a new file.
	if err := os.WriteFile(filepath.Join(tmpDir, "b.go"), []byte("b"), 0o600); err != nil {
		t.Fatalf("write b: %v", err)
	}

	h2, err := dirHash(tmpDir)
	if err != nil {
		t.Fatalf("dirHash after: %v", err)
	}

	if h1 == h2 {
		t.Fatal("dirHash should change when file added")
	}
}

func TestDirHash_ChangesHash_When_FileModified(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	fpath := filepath.Join(tmpDir, "a.go")
	if err := os.WriteFile(fpath, []byte("original"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	h1, err := dirHash(tmpDir)
	if err != nil {
		t.Fatalf("dirHash before: %v", err)
	}

	if err := os.WriteFile(fpath, []byte("modified"), 0o600); err != nil {
		t.Fatalf("write modified: %v", err)
	}

	h2, err := dirHash(tmpDir)
	if err != nil {
		t.Fatalf("dirHash after: %v", err)
	}

	if h1 == h2 {
		t.Fatal("dirHash should change when file modified")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestDirHash -v`
Expected: FAIL — `dirHash` undefined.

- [ ] **Step 3: Implement dirHash**

Add after `fileHash()` in `main.go`:

```go
// dirHash computes a content hash for a directory by hashing the sorted
// list of (filename, content-hash) pairs for immediate regular files only.
// Subdirectories and symlinks are skipped (see design note at top of plan).
// Note: all empty directories produce the same hash (SHA256 of empty input);
// this is acceptable because path_hash is the primary key discriminator.
func dirHash(path string) (string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		fh, err := fileHash(filepath.Join(path, entry.Name()))
		if err != nil {
			return "", fmt.Errorf("hash %s: %w", entry.Name(), err)
		}
		// Write "name\0hash\n" for each file — sorted by os.ReadDir guarantee.
		fmt.Fprintf(h, "%s\x00%s\n", entry.Name(), fh)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
```

Note: `os.ReadDir` returns entries sorted by name, so the hash is deterministic.

- [ ] **Step 3b: Write test for empty directory**

```go
func TestDirHash_ReturnsStableHash_When_DirectoryEmpty(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	h, err := dirHash(tmpDir)
	if err != nil {
		t.Fatalf("dirHash empty dir: %v", err)
	}
	if len(h) != 64 {
		t.Fatalf("hash length = %d, want 64", len(h))
	}
}
```

- [ ] **Step 4: Run dirHash tests**

Run: `go test -run TestDirHash -v`
Expected: All 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add main.go main_test.go
git commit -m "feat: add dirHash for directory content hashing (#12)"
```

---

## Chunk 2: Update enqueue to accept directories

### Task 2: Modify enqueueFromStdin

**Files:**
- Modify: `main.go:226-254` (`enqueueFromStdin`)
- Modify: `main_test.go`

- [ ] **Step 6: Write failing test for directory enqueue**

```go
func TestEnqueueCmd_InsertsRow_When_InputIsDirectory(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

	dbPath := filepath.Join(tmpDir, "ledger.db")

	// Create a directory with files to enqueue.
	pkgDir := filepath.Join(tmpDir, "mypkg")
	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pkgDir, "a.go"), []byte("package mypkg"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pkgDir, "b.go"), []byte("package mypkg\nfunc B(){}"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	absPkg, _ := filepath.Abs(pkgDir)

	// Pipe the directory path to enqueue via stdin.
	inputFile, err := os.CreateTemp(tmpDir, "input")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	defer func() { _ = inputFile.Close() }()
	fmt.Fprintln(inputFile, pkgDir)
	if _, err := inputFile.Seek(0, 0); err != nil {
		t.Fatalf("seek: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "enqueue", "--db", dbPath, "--treatment", "simplify"}
	defer func() { os.Args = oldArgs }()

	oldStdin := os.Stdin
	os.Stdin = inputFile
	defer func() { os.Stdin = oldStdin }()

	output := captureStdout(t, func() {
		enqueueCmd()
	})

	if !strings.Contains(output, "enqueued 1 paths") {
		t.Fatalf("unexpected output: %q", output)
	}

	// Verify the row in DB.
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	var storedPath, storedHash string
	err = db.QueryRowContext(context.Background(),
		"SELECT path, content_hash FROM queue WHERE treatment = ?", "simplify").
		Scan(&storedPath, &storedHash)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if storedPath != absPkg {
		t.Fatalf("stored path = %s, want %s", storedPath, absPkg)
	}

	// Verify the content hash matches dirHash.
	wantHash, err := dirHash(absPkg)
	if err != nil {
		t.Fatalf("dirHash: %v", err)
	}
	if storedHash != wantHash {
		t.Fatalf("stored hash = %s, want %s", storedHash, wantHash)
	}
}
```

- [ ] **Step 7: Run test to verify it fails**

Run: `go test -run TestEnqueueCmd_InsertsRow_When_InputIsDirectory -v`
Expected: FAIL — directory is skipped with "is a directory" warning.

- [ ] **Step 8: Update enqueueFromStdin to handle directories**

In `enqueueFromStdin()` (lines 212-259), replace the `fileHash` call and surrounding logic (around lines 234-243):

```go
		ph := pathHash(absPath)

		// Compute content hash — file or directory.
		var ch string
		info, err := os.Stat(absPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: skipping %q: %v\n", absPath, err)
			continue
		}
		if info.IsDir() {
			ch, err = dirHash(absPath)
		} else {
			ch, err = fileHash(absPath)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: skipping %q: %v\n", absPath, err)
			continue
		}
```

This replaces the original lines 238-243 (the `ph := pathHash(absPath)` through the `fileHash` error check).

- [ ] **Step 9: Run the directory enqueue test**

Run: `go test -run TestEnqueueCmd_InsertsRow_When_InputIsDirectory -v`
Expected: PASS.

- [ ] **Step 10: Run full test suite**

Run: `go test -v ./...`
Expected: All pass.

- [ ] **Step 11: Run lint**

Run: `golangci-lint run ./...`
Expected: Clean.

- [ ] **Step 12: Commit**

```bash
git add main.go main_test.go
git commit -m "feat: enqueue accepts directory paths with content hashing (#12)

When a path is a directory, compute content hash from sorted file
listing + individual file hashes. Enables package-level sweeps."
```

---

## Chunk 3: Re-enqueue detection for directories

### Task 3: Verify content-change re-enqueue works for directories

**Files:**
- Modify: `main_test.go`

- [ ] **Step 13: Write test that directory re-enqueue detects changes**

```go
func TestEnqueueCmd_ReactivatesEntry_When_DirectoryContentChanges(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

	dbPath := filepath.Join(tmpDir, "ledger.db")

	pkgDir := filepath.Join(tmpDir, "mypkg")
	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pkgDir, "a.go"), []byte("v1"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	absPkg, _ := filepath.Abs(pkgDir)
	ph := pathHash(absPkg)

	// First enqueue.
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	ch1, _ := dirHash(absPkg)
	if _, err := db.Exec(upsertSQL, absPkg, ph, ch1, "lint"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Mark as done.
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec("UPDATE queue SET done_at = ?, result = 'ok' WHERE path_hash = ?", now, ph); err != nil {
		t.Fatalf("mark done: %v", err)
	}

	// Verify it's done.
	var doneAt sql.NullString
	if err := db.QueryRow("SELECT done_at FROM queue WHERE path_hash = ?", ph).Scan(&doneAt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !doneAt.Valid {
		t.Fatal("expected done_at to be set")
	}

	// Modify directory contents.
	if err := os.WriteFile(filepath.Join(pkgDir, "b.go"), []byte("new file"), 0o600); err != nil {
		t.Fatalf("write new file: %v", err)
	}

	// Re-enqueue with new content hash.
	ch2, _ := dirHash(absPkg)
	if ch1 == ch2 {
		t.Fatal("dirHash should have changed")
	}
	if _, err := db.Exec(upsertSQL, absPkg, ph, ch2, "lint"); err != nil {
		t.Fatalf("re-insert: %v", err)
	}

	// Verify done_at was cleared (content changed → reactivated).
	if err := db.QueryRow("SELECT done_at FROM queue WHERE path_hash = ?", ph).Scan(&doneAt); err != nil {
		t.Fatalf("scan after re-enqueue: %v", err)
	}
	if doneAt.Valid {
		t.Fatal("expected done_at to be NULL after content-change re-enqueue")
	}
}
```

- [ ] **Step 14: Run the re-enqueue test**

Run: `go test -run TestEnqueueCmd_ReactivatesEntry -v`
Expected: PASS (the existing upsert SQL already handles this via the IIF content_hash comparison).

- [ ] **Step 15: Run full suite + lint**

Run: `go test -v ./... && golangci-lint run ./...`
Expected: All pass, clean lint.

- [ ] **Step 16: Commit**

```bash
git add main_test.go
git commit -m "test: directory re-enqueue detects content changes (#12)"
```
