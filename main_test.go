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

func setupWorkDir(t *testing.T, withSchema bool) string {
	t.Helper()

	tmpDir := t.TempDir()
	if withSchema {
		qualityDir := filepath.Join(tmpDir, ".quality")
		if err := os.MkdirAll(qualityDir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}

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
	if err := r.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	<-done

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
	tmpDir := setupWorkDir(t, true)

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
	tmpDir := setupWorkDir(t, false)

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
	tmpDir := setupWorkDir(t, true)

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
	tmpDir := setupWorkDir(t, true)

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
	if results[0] != expected[0] {
		t.Fatalf("first result = %s, want %s", results[0], expected[0])
	}
	if results[1] != expected[1] {
		t.Fatalf("second result = %s, want %s", results[1], expected[1])
	}
}

func TestStatusCmd_ShowsCounts_When_FilteredByTreatment(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

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
	tmpDir := setupWorkDir(t, true)

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
	tmpDir := setupWorkDir(t, true)

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

func TestClaimCmd_ReturnsShardedItems_When_ShardSpecified(t *testing.T) {
	tmpDir := setupWorkDir(t, true)

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

	// Verify no overlap between shards by checking each file appears in exactly one shard.
	fileToShard := make(map[string]int)
	for shard := range totalShards {
		oldArgs := os.Args
		os.Args = []string{"next", "claim", "--db", dbPath, "--treatment", "lint",
			"--shard", strconv.Itoa(shard), "--total-shards", strconv.Itoa(totalShards),
			"--n", "1000"}

		output := captureStdout(t, func() {
			claimCmd()
		})

		for line := range strings.SplitSeq(strings.TrimSpace(output), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" {
				if prevShard, exists := fileToShard[trimmed]; exists {
					t.Fatalf("file %s appeared in both shard %d and shard %d", trimmed, prevShard, shard)
				}
				fileToShard[trimmed] = shard
			}
		}
		os.Args = oldArgs
	}
}
