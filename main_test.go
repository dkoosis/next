package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func setupWorkDir(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	t.Chdir(tmpDir)
	return tmpDir
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&buf, r)
		close(done)
	}()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	os.Stdout = oldStdout
	<-done
	_ = r.Close()

	return buf.String()
}

//nolint:dupword // SQL NULL repetition is intentional
const testInsertSQL = `
	INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
	VALUES (?, ?, ?, ?, NULL, NULL, NULL)
`

func TestFileHash_ReturnsDigest_When_FileReadable(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "sample.txt")
	content := []byte("hello world\n")
	if err := os.WriteFile(filePath, content, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := fileHash(filePath)
	if err != nil {
		t.Fatalf("fileHash error: %v", err)
	}

	// Known SHA-256 digest for "hello world\n".
	const want = "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447"
	if got != want {
		t.Fatalf("fileHash = %s, want %s", got, want)
	}
}

func TestFileHash_ReturnsError_When_FileMissing(t *testing.T) {
	t.Parallel()

	if _, err := fileHash("/non-existent-file" + time.Now().Format("150405")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestOpenDB_CreatesSchema_When_SchemaPresent(t *testing.T) {
	tmpDir := setupWorkDir(t)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	var journalMode string
	if err := db.QueryRowContext(context.Background(), "PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("journal mode = %s, want wal", journalMode)
	}

	if _, err := db.Exec(testInsertSQL,
		"/tmp/file", pathHash("/tmp/file"), "hash", "lint"); err != nil {
		t.Fatalf("insert queue: %v", err)
	}
}

func TestOpenDB_ReturnsDB_When_SchemaMissing(t *testing.T) {
	tmpDir := setupWorkDir(t)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestEnqueueCmd_InsertsRows_When_InputContainsValidPaths(t *testing.T) {
	tmpDir := setupWorkDir(t)

	dbPath := filepath.Join(tmpDir, "ledger.db")

	validFile := filepath.Join(tmpDir, "valid.txt")
	if err := os.WriteFile(validFile, []byte("data"), 0o600); err != nil {
		t.Fatalf("write valid file: %v", err)
	}
	validAbs, err := filepath.Abs(validFile)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}

	inputFile, err := os.CreateTemp(tmpDir, "input")
	if err != nil {
		t.Fatalf("create temp input: %v", err)
	}
	defer func() { _ = inputFile.Close() }()

	_, _ = fmt.Fprintln(inputFile, "")
	_, _ = fmt.Fprintln(inputFile, filepath.Join(tmpDir, "missing.txt"))
	_, _ = fmt.Fprintln(inputFile, validAbs)
	_, _ = fmt.Fprintln(inputFile, validAbs)
	if _, seekErr := inputFile.Seek(0, 0); seekErr != nil {
		t.Fatalf("seek: %v", seekErr)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "enqueue", "--db", dbPath, "--treatment", "lint"}
	defer func() { os.Args = oldArgs }()

	oldStdin := os.Stdin
	os.Stdin = inputFile
	defer func() { os.Stdin = oldStdin }()

	output := captureStdout(t, func() {
		enqueueCmd()
	})

	if !strings.Contains(output, "enqueued 2 paths for treatment=lint") {
		t.Fatalf("unexpected output: %q", output)
	}

	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB verify: %v", err)
	}
	defer func() { _ = db.Close() }()

	row := db.QueryRowContext(context.Background(), "SELECT path, content_hash FROM queue WHERE treatment=?", "lint")
	var storedPath, storedHash string
	if scanErr := row.Scan(&storedPath, &storedHash); scanErr != nil {
		t.Fatalf("scan: %v", scanErr)
	}
	if storedPath != validAbs {
		t.Fatalf("stored path = %s, want %s", storedPath, validAbs)
	}
	wantHash, err := fileHash(validAbs)
	if err != nil {
		t.Fatalf("fileHash: %v", err)
	}
	if storedHash != wantHash {
		t.Fatalf("stored hash = %s, want %s", storedHash, wantHash)
	}

	countRow := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM queue")
	var count int
	if err := countRow.Scan(&count); err != nil {
		t.Fatalf("count scan: %v", err)
	}
	if count != 1 {
		t.Fatalf("queue count = %d, want 1", count)
	}
}

func TestClaimCmd_PrintsPendingPaths_When_CursorSpecified(t *testing.T) {
	tmpDir := setupWorkDir(t)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	paths := []string{
		filepath.Join(tmpDir, "a.txt"),
		filepath.Join(tmpDir, "b.txt"),
		filepath.Join(tmpDir, "c.txt"),
	}
	type candidate struct {
		path string
		hash string
	}
	candidates := make([]candidate, 0, len(paths))
	for i, p := range paths {
		if err := os.WriteFile(p, fmt.Appendf(nil, "file-%d", i), 0o600); err != nil {
			t.Fatalf("write file %s: %v", p, err)
		}
		if _, err := db.Exec(testInsertSQL,
			p, pathHash(p), fmt.Sprintf("hash-%d", i), "lint"); err != nil {
			t.Fatalf("insert %s: %v", p, err)
		}
		candidates = append(candidates, candidate{path: p, hash: pathHash(p)})
	}

	donePath := filepath.Join(tmpDir, "done.txt")
	if err := os.WriteFile(donePath, []byte("done"), 0o600); err != nil {
		t.Fatalf("write done file: %v", err)
	}
	if _, err := db.Exec(`
        INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
    `, donePath, pathHash(donePath), "hash-done", "lint", time.Now().UTC().Format(time.RFC3339), "ok"); err != nil {
		t.Fatalf("insert done: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].hash < candidates[j].hash
	})
	if len(candidates) < 3 {
		t.Fatalf("need at least 3 candidates, got %d", len(candidates))
	}
	cursor := candidates[0].hash
	expected := []string{candidates[1].path, candidates[2].path}
	oldArgs := os.Args
	os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint", "--cursor", cursor, "--n", "2"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() {
		claimCmd()
	})

	lines := strings.Split(strings.TrimSpace(output), "\n")
	var results []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			results = append(results, trimmed)
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d (%q)", len(results), results)
	}
	// UPDATE...RETURNING does not guarantee order, so sort both slices.
	sort.Strings(results)
	sort.Strings(expected)
	if results[0] != expected[0] {
		t.Fatalf("first result = %s, want %s", results[0], expected[0])
	}
	if results[1] != expected[1] {
		t.Fatalf("second result = %s, want %s", results[1], expected[1])
	}
}

func TestStatusCmd_ShowsCounts_When_FilteredByTreatment(t *testing.T) {
	tmpDir := setupWorkDir(t)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	pendingPath := filepath.Join(tmpDir, "pending.txt")
	if err := os.WriteFile(pendingPath, []byte("pending"), 0o600); err != nil {
		t.Fatalf("write pending: %v", err)
	}
	if _, err := db.Exec(testInsertSQL,
		pendingPath, pathHash(pendingPath), "hash-pending", "lint"); err != nil {
		t.Fatalf("insert pending: %v", err)
	}

	donePath := filepath.Join(tmpDir, "done.txt")
	if err := os.WriteFile(donePath, []byte("done"), 0o600); err != nil {
		t.Fatalf("write done: %v", err)
	}
	if _, err := db.Exec(`
        INSERT INTO queue (path, path_hash, content_hash, treatment, done_at, result, next_at)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
    `, donePath, pathHash(donePath), "hash-done", "lint", time.Now().UTC().Format(time.RFC3339), "ok"); err != nil {
		t.Fatalf("insert done: %v", err)
	}

	otherPath := filepath.Join(tmpDir, "other.txt")
	if err := os.WriteFile(otherPath, []byte("other"), 0o600); err != nil {
		t.Fatalf("write other: %v", err)
	}
	if _, err := db.Exec(testInsertSQL,
		otherPath, pathHash(otherPath), "hash-other", "other"); err != nil {
		t.Fatalf("insert other: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "status", "--db", dbPath, "--treatment", "lint"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() {
		statusCmd()
	})

	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected header + data + total, got %d", len(lines))
	}

	fields := strings.Fields(lines[1])
	if len(fields) != 3 {
		t.Fatalf("unexpected fields: %v", fields)
	}
	if fields[0] != "lint" {
		t.Fatalf("treatment field = %s, want lint", fields[0])
	}
	if fields[1] != "1" {
		t.Fatalf("pending = %s, want 1", fields[1])
	}
	if fields[2] != "1" {
		t.Fatalf("done = %s, want 1", fields[2])
	}
}

func TestResetCmd_DeletesEntries_When_Confirmed(t *testing.T) {
	tmpDir := setupWorkDir(t)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	lintPath := filepath.Join(tmpDir, "lint.txt")
	if writeErr := os.WriteFile(lintPath, []byte("lint"), 0o600); writeErr != nil {
		t.Fatalf("write lint: %v", writeErr)
	}
	if _, execErr := db.Exec(testInsertSQL,
		lintPath, pathHash(lintPath), "hash-lint", "lint"); execErr != nil {
		t.Fatalf("insert lint: %v", execErr)
	}

	otherPath := filepath.Join(tmpDir, "other.txt")
	if writeErr2 := os.WriteFile(otherPath, []byte("other"), 0o600); writeErr2 != nil {
		t.Fatalf("write other: %v", writeErr2)
	}
	if _, execErr2 := db.Exec(testInsertSQL,
		otherPath, pathHash(otherPath), "hash-other", "other"); execErr2 != nil {
		t.Fatalf("insert other: %v", execErr2)
	}

	if closeErr := db.Close(); closeErr != nil {
		t.Fatalf("close db: %v", closeErr)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "reset", "--db", dbPath, "--treatment", "lint", "--yes"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() {
		resetCmd()
	})

	if !strings.Contains(output, "deleted 1 entries") {
		t.Fatalf("unexpected output: %q", output)
	}

	dbVerify, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = dbVerify.Close() }()

	var lintCount int
	if err := dbVerify.QueryRow("SELECT COUNT(*) FROM queue WHERE treatment=?", "lint").Scan(&lintCount); err != nil {
		t.Fatalf("lint count: %v", err)
	}
	if lintCount != 0 {
		t.Fatalf("lint count = %d, want 0", lintCount)
	}

	var otherCount int
	if err := dbVerify.QueryRow("SELECT COUNT(*) FROM queue WHERE treatment=?", "other").Scan(&otherCount); err != nil {
		t.Fatalf("other count: %v", err)
	}
	if otherCount != 1 {
		t.Fatalf("other count = %d, want 1", otherCount)
	}
}

func TestDoneCmd_MarksEntryDone_When_PathProvided(t *testing.T) {
	tmpDir := setupWorkDir(t)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	targetPath := filepath.Join(tmpDir, "target.txt")
	if writeErr := os.WriteFile(targetPath, []byte("target"), 0o600); writeErr != nil {
		t.Fatalf("write target: %v", writeErr)
	}
	absTarget, absErr := filepath.Abs(targetPath)
	if absErr != nil {
		t.Fatalf("abs target: %v", absErr)
	}

	if _, execErr := db.Exec(testInsertSQL,
		absTarget, pathHash(absTarget), "hash-target", "lint"); execErr != nil {
		t.Fatalf("insert target: %v", execErr)
	}

	if closeErr := db.Close(); closeErr != nil {
		t.Fatalf("close db: %v", closeErr)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "done", "--db", dbPath, "--path", absTarget, "--treatment", "lint", "--result", "abc123"}
	defer func() { os.Args = oldArgs }()

	doneCmd()

	verifyDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = verifyDB.Close() }()

	row := verifyDB.QueryRow("SELECT done_at, result, next_at FROM queue WHERE path=?", absTarget)
	var doneAt, result, nextAt sql.NullString
	if err := row.Scan(&doneAt, &result, &nextAt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !doneAt.Valid {
		t.Fatal("done_at not set")
	}
	if !result.Valid || result.String != "abc123" {
		t.Fatalf("result = %v, want abc123", result)
	}
	if nextAt.Valid {
		t.Fatalf("expected next_at NULL, got %v", nextAt)
	}
}

// ----------------------------------------
// Sharding tests
// ----------------------------------------

func TestMarkDone_ReturnsError_When_PathNotInQueue(t *testing.T) {
	tmpDir := setupWorkDir(t)

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

func TestMarkDone_Succeeds_When_PathInQueue(t *testing.T) {
	tmpDir := setupWorkDir(t)

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

func TestCalculateShardRange_ReturnsCorrectRanges_When_TwoShards(t *testing.T) {
	t.Parallel()

	start0, end0 := calculateShardRange(0, 2)
	start1, end1 := calculateShardRange(1, 2)

	// Verify first shard starts at 0.
	expectedStart0 := "0000000000000000000000000000000000000000000000000000000000000000"
	if start0 != expectedStart0 {
		t.Fatalf("shard 0 start = %s, want %s", start0, expectedStart0)
	}

	// Verify shards are contiguous.
	if end0 != start1 {
		t.Fatalf("shard 0 end (%s) != shard 1 start (%s)", end0, start1)
	}

	// Verify last shard ends at max.
	expectedEnd1 := "ffffffffffffffff000000000000000000000000000000000000000000000000"
	if end1 != expectedEnd1 {
		t.Fatalf("shard 1 end = %s, want %s", end1, expectedEnd1)
	}

	// Verify no overlap.
	if start0 >= end0 {
		t.Fatalf("shard 0 start >= end")
	}
	if start1 >= end1 {
		t.Fatalf("shard 1 start >= end")
	}
}

func TestCalculateShardRange_ReturnsCorrectRanges_When_FourShards(t *testing.T) {
	t.Parallel()

	ranges := make([][2]string, 4)
	for i := range 4 {
		start, end := calculateShardRange(i, 4)
		ranges[i] = [2]string{start, end}
	}

	// Verify first shard starts at 0.
	if ranges[0][0] != "0000000000000000000000000000000000000000000000000000000000000000" {
		t.Fatalf("first shard doesn't start at 0: %s", ranges[0][0])
	}

	// Verify all shards are contiguous.
	for i := range 3 {
		if ranges[i][1] != ranges[i+1][0] {
			t.Fatalf("shard %d end (%s) != shard %d start (%s)", i, ranges[i][1], i+1, ranges[i+1][0])
		}
	}

	// Verify last shard ends at max.
	if ranges[3][1] != "ffffffffffffffff000000000000000000000000000000000000000000000000" {
		t.Fatalf("last shard doesn't end at max: %s", ranges[3][1])
	}
}

func TestEnqueueCmd_InsertsRow_When_InputIsDirectory(t *testing.T) {
	tmpDir := setupWorkDir(t)

	dbPath := filepath.Join(tmpDir, "ledger.db")

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

	wantHash, err := dirHash(absPkg)
	if err != nil {
		t.Fatalf("dirHash: %v", err)
	}
	if storedHash != wantHash {
		t.Fatalf("stored hash = %s, want %s", storedHash, wantHash)
	}
}

func TestEnqueueCmd_ReactivatesEntry_When_DirectoryContentChanges(t *testing.T) {
	tmpDir := setupWorkDir(t)

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

	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	ch1, _ := dirHash(absPkg)
	if _, err := db.Exec(upsertSQL, absPkg, ph, ch1, "", "lint"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec("UPDATE queue SET done_at = ?, result = 'ok' WHERE path_hash = ?", now, ph); err != nil {
		t.Fatalf("mark done: %v", err)
	}

	var doneAt sql.NullString
	if err := db.QueryRow("SELECT done_at FROM queue WHERE path_hash = ?", ph).Scan(&doneAt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !doneAt.Valid {
		t.Fatal("expected done_at to be set")
	}

	if err := os.WriteFile(filepath.Join(pkgDir, "b.go"), []byte("new file"), 0o600); err != nil {
		t.Fatalf("write new file: %v", err)
	}

	ch2, _ := dirHash(absPkg)
	if ch1 == ch2 {
		t.Fatal("dirHash should have changed")
	}
	if _, err := db.Exec(upsertSQL, absPkg, ph, ch2, "", "lint"); err != nil {
		t.Fatalf("re-insert: %v", err)
	}

	if err := db.QueryRow("SELECT done_at FROM queue WHERE path_hash = ?", ph).Scan(&doneAt); err != nil {
		t.Fatalf("scan after re-enqueue: %v", err)
	}
	if doneAt.Valid {
		t.Fatal("expected done_at to be NULL after content-change re-enqueue")
	}
}

func TestDirHash_ReturnsStableHash_When_DirectoryExists(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
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

func TestClaimCmd_SetsClaimedAt_When_ItemsClaimed(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	targetPath := filepath.Join(tmpDir, "item.txt")
	if err := os.WriteFile(targetPath, []byte("item"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := db.Exec(testInsertSQL, targetPath, pathHash(targetPath), "hash-item", "lint"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint",
		"--n", "1", "--worker", "test-worker"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() { claimCmd() })

	if strings.TrimSpace(output) != targetPath {
		t.Fatalf("expected %s, got %q", targetPath, output)
	}

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

func TestClaimCmd_ReturnsDisjointSets_When_CalledSequentially(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

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

	allPaths := make(map[string]int)
	for w := range 4 {
		oldArgs := os.Args
		os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint",
			"--n", "5", "--worker", fmt.Sprintf("worker-%d", w)}
		output := captureStdout(t, func() { claimCmd() })
		os.Args = oldArgs

		for line := range strings.SplitSeq(strings.TrimSpace(output), "\n") {
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

func TestClaimCmd_SkipsClaimedItems_When_LeaseActive(t *testing.T) {
	tmpDir := setupWorkDir(t)
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

	oldArgs := os.Args
	os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint",
		"--n", "1", "--worker", "w1"}
	out1 := captureStdout(t, func() { claimCmd() })
	os.Args = oldArgs

	if strings.TrimSpace(out1) != p {
		t.Fatalf("first claim: expected %s, got %q", p, out1)
	}

	os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint",
		"--n", "1", "--worker", "w2"}
	out2 := captureStdout(t, func() { claimCmd() })
	os.Args = oldArgs

	if strings.TrimSpace(out2) != "" {
		t.Fatalf("second claim: expected empty, got %q", out2)
	}
}

func TestClaimCmd_ReturnsRevisitItems_When_NextAtElapsed(t *testing.T) {
	tmpDir := setupWorkDir(t)
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

	// Insert a done item with next_at in the past (SQLite datetime format).
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

	output := captureStdout(t, func() { claimCmd() })
	if strings.TrimSpace(output) != absTarget {
		t.Fatalf("expected %s, got %q", absTarget, output)
	}
}

func TestClaimCmd_SkipsRevisitItems_When_NextAtInFuture(t *testing.T) {
	tmpDir := setupWorkDir(t)
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

	output := captureStdout(t, func() { claimCmd() })
	if strings.TrimSpace(output) != "" {
		t.Fatalf("expected no output for future revisit, got %q", output)
	}
}

func TestOpenDB_HasClaimColumns_When_SchemaApplied(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

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

func TestOpenDB_MigratesOldSchema_When_ClaimColumnsMissing(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")

	// Create a DB with the old schema (no claimed_at/claimed_by columns).
	oldDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open old db: %v", err)
	}
	_, err = oldDB.Exec(`CREATE TABLE queue (
		path          TEXT NOT NULL,
		path_hash     TEXT NOT NULL,
		content_hash  TEXT NOT NULL,
		treatment     TEXT NOT NULL,
		done_at       TEXT,
		result        TEXT,
		next_at       TEXT,
		PRIMARY KEY (path_hash, treatment)
	);
	CREATE INDEX IF NOT EXISTS idx_queue_treatment_done ON queue(treatment, done_at);
	CREATE INDEX IF NOT EXISTS idx_queue_next_at ON queue(next_at);`)
	if err != nil {
		t.Fatalf("create old schema: %v", err)
	}
	if err := oldDB.Close(); err != nil {
		t.Fatalf("close old db: %v", err)
	}

	// openDB should migrate the old schema without error.
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB on old schema failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Verify the columns were added.
	row := db.QueryRowContext(context.Background(),
		"SELECT sql FROM sqlite_master WHERE type='table' AND name='queue'")
	var ddl string
	if err := row.Scan(&ddl); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !strings.Contains(ddl, "claimed_at") {
		t.Fatalf("migration did not add claimed_at: %s", ddl)
	}
	if !strings.Contains(ddl, "claimed_by") {
		t.Fatalf("migration did not add claimed_by: %s", ddl)
	}

	// Verify the index was created.
	var indexCount int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_queue_claimed_at'").Scan(&indexCount)
	if err != nil {
		t.Fatalf("check index: %v", err)
	}
	if indexCount != 1 {
		t.Fatalf("expected idx_queue_claimed_at index, found %d", indexCount)
	}
}

func TestClaimCmd_ReturnsShardedItems_When_ShardSpecified(t *testing.T) {
	tmpDir := setupWorkDir(t)

	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	// Create many files to ensure hash distribution across shards.
	numFiles := 100

	for i := range numFiles {
		path := filepath.Join(tmpDir, fmt.Sprintf("file-%03d.txt", i))
		if err := os.WriteFile(path, fmt.Appendf(nil, "content-%d", i), 0o600); err != nil {
			t.Fatalf("write file %d: %v", i, err)
		}
		hash := pathHash(path)

		if _, err := db.Exec(testInsertSQL,
			path, hash, fmt.Sprintf("content-hash-%d", i), "lint"); err != nil {
			t.Fatalf("insert file %d: %v", i, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Test with 4 shards.
	const totalShards = 4
	allResults := make(map[string]bool)
	var shardCounts [totalShards]int

	for shard := range totalShards {
		oldArgs := os.Args
		os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint",
			"--shard", strconv.Itoa(shard), "--total-shards", strconv.Itoa(totalShards),
			"--n", "1000"} // Request all items.

		output := captureStdout(t, func() {
			claimCmd()
		})

		for line := range strings.SplitSeq(strings.TrimSpace(output), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" {
				allResults[trimmed] = true
				shardCounts[shard]++
			}
		}
		os.Args = oldArgs
	}

	// Verify all files were returned across shards.
	if len(allResults) != numFiles {
		t.Fatalf("got %d unique results across all shards, want %d", len(allResults), numFiles)
	}

	// Verify each shard got some items (probabilistic, but should be true for 100 files).
	for i := range totalShards {
		if shardCounts[i] == 0 {
			t.Fatalf("shard %d returned 0 items", i)
		}
	}

}

func TestValidTimeModifier_AcceptsValidModifiers(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"5 minutes", "+14 days", "1 hours", "30 seconds", "-7 days"} {
		if !validTimeModifier(s) {
			t.Errorf("validTimeModifier(%q) = false, want true", s)
		}
	}
}

func TestValidTimeModifier_RejectsInvalidModifiers(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"", "never", "5", "minutes", "0 days", "-0 hours", "foo bar"} {
		if validTimeModifier(s) {
			t.Errorf("validTimeModifier(%q) = true, want false", s)
		}
	}
}

func TestValidLease_RejectsNonPositive(t *testing.T) {
	t.Parallel()
	if validLease("0 minutes") {
		t.Error("validLease accepted 0 minutes")
	}
	if validLease("-1 minutes") {
		t.Error("validLease accepted -1 minutes")
	}
}

func TestMarkDone_SetsNextAt_When_RevisitValid(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	ph := pathHash("/test/revisit")
	if _, err := db.Exec(testInsertSQL, "/test/revisit", ph, "hash1", "lint"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := markDone(db, ph, "lint", "result", "+14 days"); err != nil {
		t.Fatalf("markDone: %v", err)
	}

	var nextAt sql.NullString
	if err := db.QueryRow("SELECT next_at FROM queue WHERE path_hash = ?", ph).Scan(&nextAt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !nextAt.Valid {
		t.Fatal("next_at should be set for valid revisit modifier")
	}
}

// ----------------------------------------
// content-hash + op-spec-hash done-state
// ----------------------------------------

func TestEnqueueCmd_ReactivatesEntry_When_SpecHashChanges(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")

	validFile := filepath.Join(tmpDir, "unit.go")
	if err := os.WriteFile(validFile, []byte("package unit"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	absPath, err := filepath.Abs(validFile)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	ph := pathHash(absPath)

	runEnqueue := func(specHash string) {
		inputFile, cerr := os.CreateTemp(tmpDir, "input")
		if cerr != nil {
			t.Fatalf("create temp input: %v", cerr)
		}
		defer func() { _ = inputFile.Close() }()
		_, _ = fmt.Fprintln(inputFile, absPath)
		if _, serr := inputFile.Seek(0, 0); serr != nil {
			t.Fatalf("seek: %v", serr)
		}

		oldArgs := os.Args
		args := []string{"next", "enqueue", "--db", dbPath, "--treatment", "lint"}
		if specHash != "" {
			args = append(args, "--spec-hash", specHash)
		}
		os.Args = args
		defer func() { os.Args = oldArgs }()

		oldStdin := os.Stdin
		os.Stdin = inputFile
		defer func() { os.Stdin = oldStdin }()

		captureStdout(t, func() { enqueueCmd() })
	}

	// First enqueue under spec "v1", mark done.
	runEnqueue("v1")

	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB verify: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := markDone(db, ph, "lint", "result", ""); err != nil {
		t.Fatalf("markDone: %v", err)
	}

	var doneAt sql.NullString
	if err := db.QueryRow("SELECT done_at FROM queue WHERE path_hash = ?", ph).Scan(&doneAt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !doneAt.Valid {
		t.Fatal("expected done_at set after markDone")
	}

	// Content is unchanged, but the op spec changed ("v1" -> "v2") — the
	// unit must reopen even though its name and bytes are identical.
	runEnqueue("v2")

	if err := db.QueryRow("SELECT done_at FROM queue WHERE path_hash = ?", ph).Scan(&doneAt); err != nil {
		t.Fatalf("scan after spec-hash change: %v", err)
	}
	if doneAt.Valid {
		t.Fatal("expected done_at to be NULL after spec-hash change even though content is unchanged")
	}

	// A reopened unit must also shed any active lease — otherwise a claimed
	// row stays locked until lease expiry even though it needs a re-run.
	if _, err := db.Exec("UPDATE queue SET claimed_at = DATETIME('now'), claimed_by = 'w1' WHERE path_hash = ?", ph); err != nil {
		t.Fatalf("set claim: %v", err)
	}
	runEnqueue("v3")

	var claimedAt, claimedBy sql.NullString
	if err := db.QueryRow("SELECT claimed_at, claimed_by FROM queue WHERE path_hash = ?", ph).Scan(&claimedAt, &claimedBy); err != nil {
		t.Fatalf("scan claim after spec-hash change: %v", err)
	}
	if claimedAt.Valid || claimedBy.Valid {
		t.Fatal("expected claimed_at/claimed_by to be NULL after spec-hash change reopened the unit")
	}
}

func TestEnqueueCmd_StaysDone_When_ContentAndSpecHashUnchanged(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")

	validFile := filepath.Join(tmpDir, "unit.go")
	if err := os.WriteFile(validFile, []byte("package unit"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	absPath, err := filepath.Abs(validFile)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	ph := pathHash(absPath)

	runEnqueue := func() {
		inputFile, cerr := os.CreateTemp(tmpDir, "input")
		if cerr != nil {
			t.Fatalf("create temp input: %v", cerr)
		}
		defer func() { _ = inputFile.Close() }()
		_, _ = fmt.Fprintln(inputFile, absPath)
		if _, serr := inputFile.Seek(0, 0); serr != nil {
			t.Fatalf("seek: %v", serr)
		}

		oldArgs := os.Args
		os.Args = []string{"next", "enqueue", "--db", dbPath, "--treatment", "lint", "--spec-hash", "same"}
		defer func() { os.Args = oldArgs }()

		oldStdin := os.Stdin
		os.Stdin = inputFile
		defer func() { os.Stdin = oldStdin }()

		captureStdout(t, func() { enqueueCmd() })
	}

	runEnqueue()

	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB verify: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := markDone(db, ph, "lint", "result", ""); err != nil {
		t.Fatalf("markDone: %v", err)
	}

	// Re-enqueue with identical content and identical spec-hash.
	runEnqueue()

	var doneAt sql.NullString
	if err := db.QueryRow("SELECT done_at FROM queue WHERE path_hash = ?", ph).Scan(&doneAt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !doneAt.Valid {
		t.Fatal("expected done_at to remain set when content and spec-hash are both unchanged")
	}
}

// ----------------------------------------
// reconcile
// ----------------------------------------

func TestReconcileFromStdin_PrunesVanishedUnits_When_AbsentFromGroundTruth(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	survivorPath := filepath.Join(tmpDir, "survivor.go")
	vanishedPath := filepath.Join(tmpDir, "vanished.go")
	survivorHash := pathHash(survivorPath)
	vanishedHash := pathHash(vanishedPath)

	if _, err := db.Exec(testInsertSQL, survivorPath, survivorHash, "ch1", "lint"); err != nil {
		t.Fatalf("insert survivor: %v", err)
	}
	if _, err := db.Exec(testInsertSQL, vanishedPath, vanishedHash, "ch2", "lint"); err != nil {
		t.Fatalf("insert vanished: %v", err)
	}

	pruned, kept, err := reconcileFromStdin(db, strings.NewReader(survivorPath+"\n"), "lint")
	if err != nil {
		t.Fatalf("reconcileFromStdin: %v", err)
	}
	if kept != 1 {
		t.Fatalf("kept = %d, want 1", kept)
	}
	if len(pruned) != 1 || pruned[0].Path != vanishedPath {
		t.Fatalf("pruned = %+v, want [%s]", pruned, vanishedPath)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM queue WHERE treatment='lint'").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("remaining rows = %d, want 1", count)
	}

	var remainingPath string
	if err := db.QueryRow("SELECT path FROM queue WHERE treatment='lint'").Scan(&remainingPath); err != nil {
		t.Fatalf("scan remaining: %v", err)
	}
	if remainingPath != survivorPath {
		t.Fatalf("remaining path = %s, want %s", remainingPath, survivorPath)
	}
}

func TestReconcileCmd_ReportsPrunedAndKept_When_RunViaCmd(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	survivorPath := filepath.Join(tmpDir, "keep.go")
	vanishedPath := filepath.Join(tmpDir, "gone.go")
	if _, err := db.Exec(testInsertSQL, survivorPath, pathHash(survivorPath), "ch1", "sweep"); err != nil {
		t.Fatalf("insert survivor: %v", err)
	}
	if _, err := db.Exec(testInsertSQL, vanishedPath, pathHash(vanishedPath), "ch2", "sweep"); err != nil {
		t.Fatalf("insert vanished: %v", err)
	}
	_ = db.Close()

	inputFile, err := os.CreateTemp(tmpDir, "units")
	if err != nil {
		t.Fatalf("create temp input: %v", err)
	}
	defer func() { _ = inputFile.Close() }()
	_, _ = fmt.Fprintln(inputFile, survivorPath)
	if _, err := inputFile.Seek(0, 0); err != nil {
		t.Fatalf("seek: %v", err)
	}

	oldArgs := os.Args
	os.Args = []string{"next", "reconcile", "--db", dbPath, "--treatment", "sweep", "--units", "-"}
	defer func() { os.Args = oldArgs }()

	oldStdin := os.Stdin
	os.Stdin = inputFile
	defer func() { os.Stdin = oldStdin }()

	output := captureStdout(t, func() { reconcileCmd() })

	if !strings.Contains(output, "reconciled: 1 pruned, 1 kept for treatment=sweep") {
		t.Fatalf("unexpected output: %q", output)
	}
}

// ----------------------------------------
// gc
// ----------------------------------------

func TestStaleTreatments_ReturnsOnlyStale_When_MixedActivity(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	freshPath := filepath.Join(tmpDir, "fresh.go")
	stalePath := filepath.Join(tmpDir, "stale.go")

	if _, err := db.Exec(
		"INSERT INTO queue (path, path_hash, content_hash, treatment, updated_at) VALUES (?, ?, ?, ?, DATETIME('now'))",
		freshPath, pathHash(freshPath), "ch1", "fresh-treatment",
	); err != nil {
		t.Fatalf("insert fresh: %v", err)
	}
	if _, err := db.Exec(
		"INSERT INTO queue (path, path_hash, content_hash, treatment, updated_at) VALUES (?, ?, ?, ?, DATETIME('now', '-60 days'))",
		stalePath, pathHash(stalePath), "ch2", "stale-treatment",
	); err != nil {
		t.Fatalf("insert stale: %v", err)
	}

	stale, err := staleTreatments(db, "30 days")
	if err != nil {
		t.Fatalf("staleTreatments: %v", err)
	}
	if len(stale) != 1 || stale[0] != "stale-treatment" {
		t.Fatalf("stale = %v, want [stale-treatment]", stale)
	}
}

func TestGCCmd_DropsStaleTreatment_When_ConfirmedViaYesFlag(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	stalePath := filepath.Join(tmpDir, "stale.go")
	freshPath := filepath.Join(tmpDir, "fresh.go")
	if _, err := db.Exec(
		"INSERT INTO queue (path, path_hash, content_hash, treatment, updated_at) VALUES (?, ?, ?, ?, DATETIME('now', '-60 days'))",
		stalePath, pathHash(stalePath), "ch1", "old",
	); err != nil {
		t.Fatalf("insert stale: %v", err)
	}
	if _, err := db.Exec(
		"INSERT INTO queue (path, path_hash, content_hash, treatment, updated_at) VALUES (?, ?, ?, ?, DATETIME('now'))",
		freshPath, pathHash(freshPath), "ch2", "current",
	); err != nil {
		t.Fatalf("insert fresh: %v", err)
	}
	_ = db.Close()

	oldArgs := os.Args
	os.Args = []string{"next", "gc", "--db", dbPath, "--stale", "30 days", "--yes"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() { gcCmd() })
	if !strings.Contains(output, "gc: dropped treatment=old (1 entries)") {
		t.Fatalf("unexpected output: %q", output)
	}

	db2, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db2.Close() }()

	var oldCount, currentCount int
	if err := db2.QueryRow("SELECT COUNT(*) FROM queue WHERE treatment='old'").Scan(&oldCount); err != nil {
		t.Fatalf("count old: %v", err)
	}
	if err := db2.QueryRow("SELECT COUNT(*) FROM queue WHERE treatment='current'").Scan(&currentCount); err != nil {
		t.Fatalf("count current: %v", err)
	}
	if oldCount != 0 {
		t.Fatalf("old treatment count = %d, want 0", oldCount)
	}
	if currentCount != 1 {
		t.Fatalf("current treatment count = %d, want 1", currentCount)
	}
}

func TestGCCmd_ReportsNoStale_When_AllTreatmentsFresh(t *testing.T) {
	tmpDir := setupWorkDir(t)
	dbPath := filepath.Join(tmpDir, "ledger.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	freshPath := filepath.Join(tmpDir, "fresh.go")
	if _, err := db.Exec(
		"INSERT INTO queue (path, path_hash, content_hash, treatment, updated_at) VALUES (?, ?, ?, ?, DATETIME('now'))",
		freshPath, pathHash(freshPath), "ch1", "current",
	); err != nil {
		t.Fatalf("insert fresh: %v", err)
	}
	_ = db.Close()

	oldArgs := os.Args
	os.Args = []string{"next", "gc", "--db", dbPath, "--stale", "30 days", "--yes"}
	defer func() { os.Args = oldArgs }()

	output := captureStdout(t, func() { gcCmd() })
	if !strings.Contains(output, "gc: no stale treatments") {
		t.Fatalf("unexpected output: %q", output)
	}
}
